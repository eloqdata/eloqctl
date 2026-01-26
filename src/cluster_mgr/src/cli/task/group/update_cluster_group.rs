use crate::cli::task::cassandra_ctl_task::CassandraCtlTask;
use crate::cli::task::download_task::DownloadTask;
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{Config, TaskGroup, UpdateClusterTaskGroup};
use crate::cli::task::monograph_log_ctl_task::MonographLogCtlTask;
use crate::cli::task::monograph_log_probe_task::MonographLogProbeTask;
use crate::cli::task::monograph_tx_ctl_task::{MonographTxCtlTask, ServerType};
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::task::task_utils::stop_with_hot_standby;
use crate::cli::task::unpack_file_task::UnpackFileTask;
use crate::cli::task::upload::upload_task_builder::{upload_tasks, UploadTaskBuilderType};
use crate::cli::SubCommand;
use indexmap::IndexMap;
use tracing::info;

#[async_trait::async_trait]
impl TaskGroup for UpdateClusterTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => {
                return Err(anyhow::anyhow!(
                    "Expected ClusterConfig for UpdateClusterTaskGroup"
                ))
            }
        };

        let (update_eloq, update_cass, password, force) = match &cmd_arg {
            SubCommand::Update {
                version,
                cassandra,
                password,
                force,
                ..
            } => (version.is_some(), cassandra.is_some(), password, force),
            _ => unreachable!(),
        };
        if !update_eloq && !update_cass {
            return Ok(TaskExecutionContext::dummy());
        }

        let deployment = &cluster_config.deployment;
        let cluster = deployment.cluster_name.clone();

        let mut downloads = vec![];
        let mut upload_img = IndexMap::new();
        let mut unpack_tasks = IndexMap::new();
        if update_eloq {
            downloads.push(cluster_config.deployment.tx_image().to_owned());
            if let Some(img) = cluster_config.deployment.log_image() {
                downloads.push(img.to_owned());
            }
            upload_img.extend(upload_tasks(UploadTaskBuilderType::EloqImage, config));
            unpack_tasks.extend(UnpackFileTask::unpack_eloqservers(cluster_config));
        }
        if update_cass {
            if let Some(storage_service) = &deployment.storage_service {
                let inner_cass = storage_service.inner_cass();
                if let Some(inner_cass) = inner_cass {
                    downloads.push(inner_cass.image_url());
                    upload_img.extend(upload_tasks(UploadTaskBuilderType::CassImage, config));
                    unpack_tasks.extend(UnpackFileTask::unpack_cassandra(cluster_config, true));
                }
            }
        }
        let download_task = DownloadTask::instances(DownloadTask::from_urls(downloads));
        let mut barrier = vec![download_task.len(), upload_img.len()];
        let mut executable = IndexMap::new();
        executable.extend(download_task);
        executable.extend(upload_img);

        // stop order: (standby-server -> voter-server ->) tx-server -> log-server -> kv-store
        let stop_cmd = SubCommand::Stop {
            cluster: cluster.clone(),
            tx: Some(true),
            log: true,
            store: update_cass,
            monitor: false,
            force: *force,
            all: false,
            password: password.clone(),
            nodes: Vec::new(),
        };

        if cluster_config
            .deployment
            .tx_service
            .standby_host_ports
            .is_some()
        {
            // this will succeed only if the tx-server is running properly. we can not trigger this action if the tx-server is down.
            stop_with_hot_standby(
                stop_cmd.clone(),
                cluster_config,
                &mut barrier,
                &mut executable,
            )
            .await;
        } else {
            // this means it will succeed even if the tx-server is not running. this is idempotent in that the result is the same whether the tx-server is running or not.
            let stop_tx =
                MonographTxCtlTask::from_config(stop_cmd.clone(), cluster_config, ServerType::Tx);
            barrier.push(stop_tx.len());
            executable.extend(stop_tx);
        }

        if deployment.log_service.is_some() {
            let stop_log = MonographLogCtlTask::from_config(stop_cmd.clone(), cluster_config);
            barrier.push(stop_log.len());
            executable.extend(stop_log);
        }
        if update_cass {
            let tasks = CassandraCtlTask::from_config(stop_cmd, cluster_config);
            barrier.push(tasks.len());
            executable.extend(tasks);
        }

        barrier.push(unpack_tasks.len());
        executable.extend(unpack_tasks);

        // start order: cassandra -> log-server -> tx-server
        let start_cmd = SubCommand::Start {
            cluster,
            nodes: Vec::new(),
        };
        if let Some(storage_service) = &deployment.storage_service {
            let inner_cass = storage_service.inner_cass();
            if inner_cass.is_some() {
                let tasks = CassandraCtlTask::from_config(start_cmd.clone(), cluster_config);
                let ba = CassandraCtlTask::start_barrier(tasks.len());
                barrier.extend(ba);
                executable.extend(tasks);
            }
        }
        if deployment.log_service.is_some() {
            let start_log = MonographLogCtlTask::from_config(start_cmd.clone(), cluster_config);
            barrier.push(start_log.len());
            executable.extend(start_log);
            let probe = MonographLogProbeTask::from_config(cluster_config);
            barrier.push(probe.len());
            executable.extend(probe);
        }

        // Add EloqStore Cloud mode data cleaning logic before starting txservice
        if let Some(storage_service) = &deployment.storage_service {
            if let Some(dss) = &storage_service.eloqdss {
                use crate::cli::task::eloq_store_data_clean_task::EloqStoreDataCleanTask;
                use crate::config::storage_service_config::DataStoreServiceBackend;
                match dss.backend_config() {
                    DataStoreServiceBackend::EloqStore(eloq_store_config) => {
                        if eloq_store_config.is_cloud_mode() {
                            // Check if we should skip cleanup based on eloq_store_reuse_local_files
                            // If eloq_store_reuse_local_files is true, skip cleanup to reuse local files
                            let should_skip_cleanup = eloq_store_config
                                .get_cloud_config()
                                .and_then(|cloud_config| cloud_config.eloq_store_reuse_local_files)
                                .unwrap_or(false);

                            if !should_skip_cleanup {
                                // Clean EloqStore data directories before starting txservice
                                // This ensures clean state after binary update
                                let config_for_clean = Config::Cluster(cluster_config.clone());
                                let clean_data_tasks = EloqStoreDataCleanTask::build_tasks(
                                    start_cmd.clone(),
                                    &config_for_clean,
                                );
                                if !clean_data_tasks.is_empty() {
                                    barrier.push(clean_data_tasks.len());
                                    executable.extend(clean_data_tasks);
                                }
                            } else {
                                info!("Skipping EloqStore data cleanup because eloq_store_reuse_local_files is enabled");
                            }
                        }
                    } // Future backends can be handled here
                }
            }
        }

        let start_tx =
            MonographTxCtlTask::from_config(start_cmd.clone(), cluster_config, ServerType::Tx);
        barrier.push(start_tx.len());
        executable.extend(start_tx);

        if cluster_config
            .deployment
            .tx_service
            .standby_host_ports
            .is_some()
        {
            let start_standby = MonographTxCtlTask::from_config(
                start_cmd.clone(),
                cluster_config,
                ServerType::Standby,
            );
            barrier.push(start_standby.len());
            executable.extend(start_standby);
        }

        if cluster_config
            .deployment
            .tx_service
            .voter_host_ports
            .is_some()
        {
            let start_voter = MonographTxCtlTask::from_config(
                start_cmd.clone(),
                cluster_config,
                ServerType::Voter,
            );
            barrier.push(start_voter.len());
            executable.extend(start_voter);
        }

        let check_version_tasks = ExecCustomCommand::build_task_by_host(
            format!(
                "{}/EloqKV/bin/eloqkv --version",
                cluster_config.install_dir()
            ),
            config,
            cluster_config.deployment.tx_service.merge_hosts(),
            Some("check_eloqkv_version".to_string()),
        );
        barrier.push(check_version_tasks.len());
        executable.extend(check_version_tasks);

        Ok(TaskExecutionContext {
            task_group: cmd_arg.as_ref().to_string(),
            barrier: Some(barrier),
            executable,
        })
    }
}
