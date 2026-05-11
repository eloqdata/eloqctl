use crate::cli::task::download_task::DownloadTask;
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::failover_op_task::FailoverOpTask;
use crate::cli::task::group::{Config, TaskGroup, UpdateClusterTaskGroup};
use crate::cli::task::monograph_log_ctl_task::MonographLogCtlTask;
use crate::cli::task::monograph_log_probe_task::MonographLogProbeTask;
use crate::cli::task::monograph_tx_ctl_task::{MonographTxCtlTask, ServerType};
use crate::cli::task::redis_op_task::{ClusterNodes, RedisOpTask};
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::task::unpack_file_task::UnpackFileTask;
use crate::cli::task::upload::upload_task_builder::{upload_tasks, UploadTaskBuilderType};
use crate::cli::SubCommand;
use crate::config::DeploymentPackage;
use indexmap::IndexMap;
use std::collections::HashMap;
use tokio::sync::watch;
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

        let (update_eloq, password, force) = match &cmd_arg {
            SubCommand::Update {
                version,
                password,
                force,
                ..
            } => (version.is_some(), password, force),
            _ => unreachable!(),
        };
        let redis_password = cluster_config.redis_password(password.clone());
        if !update_eloq {
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
            store: false,
            monitor: false,
            force: *force,
            all: false,
            password: redis_password.clone(),
            nodes: Vec::new(),
        };

        let has_standby = cluster_config
            .deployment
            .tx_service
            .standby_host_ports
            .is_some();

        if has_standby {
            // --- Round 1: failover masters, stop them ---
            let tx_host_ports = cluster_config.get_host_port_list(DeploymentPackage::MonographTx);
            let topo_task_id_1 = TaskId {
                cmd: "topology".to_string(),
                task: "check-topology-round1".to_string(),
                host: "_local".to_string(),
            };
            let (topo_tx_1, failover_rx_1) = watch::channel::<ClusterNodes>(ClusterNodes {
                masters: Vec::new(),
                replicas: Vec::new(),
            });
            let stop_nodes_rx_1 = failover_rx_1.clone();
            executable.insert(
                topo_task_id_1.clone(),
                TaskInstance {
                    task_input: HashMap::default(),
                    task: Box::new(RedisOpTask::new(
                        topo_task_id_1,
                        tx_host_ports.clone(),
                        "cluster topology".to_string(),
                        topo_tx_1,
                        redis_password.clone(),
                        true,
                    )),
                    task_host: TaskHost::Local,
                },
            );
            barrier.push(1);

            let mut failover_ids_1 = Vec::new();
            for node_addr in &tx_host_ports {
                if let Some((host, port_str)) = node_addr.split_once(':') {
                    if let Ok(port) = port_str.parse::<u16>() {
                        let fid = TaskId {
                            cmd: "failover".to_string(),
                            task: format!("failover-check-round1-{}", port_str),
                            host: host.to_string(),
                        };
                        executable.insert(
                            fid.clone(),
                            TaskInstance {
                                task_input: HashMap::default(),
                                task: Box::new(FailoverOpTask::new(
                                    fid.clone(),
                                    host.to_string(),
                                    port,
                                    String::new(),
                                    0u16,
                                    failover_rx_1.clone(),
                                    redis_password.clone(),
                                )),
                                task_host: TaskHost::Local,
                            },
                        );
                        failover_ids_1.push(fid);
                    }
                }
            }
            barrier.push(failover_ids_1.len());

            let stop_nodes_1 = MonographTxCtlTask::from_config_with_channel(
                SubCommand::Stop {
                    cluster: cluster.clone(),
                    tx: Some(true),
                    log: true,
                    store: false,
                    monitor: false,
                    force: *force,
                    all: false,
                    password: redis_password.clone(),
                    nodes: tx_host_ports,
                },
                cluster_config,
                ServerType::Node,
                Some(stop_nodes_rx_1),
            )
            .expect("stop nodes round1 error");
            barrier.push(stop_nodes_1.len());
            executable.extend(stop_nodes_1);
        } else {
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

        barrier.push(unpack_tasks.len());
        executable.extend(unpack_tasks);

        // start order: log-server -> tx-server
        let start_cmd = SubCommand::Start {
            cluster: cluster.clone(),
            nodes: Vec::new(),
        };
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
                                    None, // No filter, clean all nodes
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

        if has_standby {
            // --- Round 2: failover back, restart old standbys ---
            let standby_host_ports =
                cluster_config.get_host_port_list(DeploymentPackage::MonographStandby);
            let topo_task_id_2 = TaskId {
                cmd: "topology".to_string(),
                task: "check-topology-round2".to_string(),
                host: "_local".to_string(),
            };
            let (topo_tx_2, failover_rx_2) = watch::channel::<ClusterNodes>(ClusterNodes {
                masters: Vec::new(),
                replicas: Vec::new(),
            });
            let stop_nodes_rx_2 = failover_rx_2.clone();
            executable.insert(
                topo_task_id_2.clone(),
                TaskInstance {
                    task_input: HashMap::default(),
                    task: Box::new(RedisOpTask::new(
                        topo_task_id_2,
                        standby_host_ports.clone(),
                        "cluster topology".to_string(),
                        topo_tx_2,
                        redis_password.clone(),
                        true,
                    )),
                    task_host: TaskHost::Local,
                },
            );
            barrier.push(1);

            let mut failover_ids_2 = Vec::new();
            for node_addr in &standby_host_ports {
                if let Some((host, port_str)) = node_addr.split_once(':') {
                    if let Ok(port) = port_str.parse::<u16>() {
                        let fid = TaskId {
                            cmd: "failover".to_string(),
                            task: format!("failover-check-round2-{}", port_str),
                            host: host.to_string(),
                        };
                        executable.insert(
                            fid.clone(),
                            TaskInstance {
                                task_input: HashMap::default(),
                                task: Box::new(FailoverOpTask::new(
                                    fid.clone(),
                                    host.to_string(),
                                    port,
                                    String::new(),
                                    0u16,
                                    failover_rx_2.clone(),
                                    redis_password.clone(),
                                )),
                                task_host: TaskHost::Local,
                            },
                        );
                        failover_ids_2.push(fid);
                    }
                }
            }
            barrier.push(failover_ids_2.len());

            let stop_nodes_2 = MonographTxCtlTask::from_config_with_channel(
                SubCommand::Stop {
                    cluster: cluster.clone(),
                    tx: Some(true),
                    log: true,
                    store: false,
                    monitor: false,
                    force: *force,
                    all: false,
                    password: redis_password.clone(),
                    nodes: standby_host_ports,
                },
                cluster_config,
                ServerType::Node,
                Some(stop_nodes_rx_2),
            )
            .expect("stop nodes round2 error");
            barrier.push(stop_nodes_2.len());
            executable.extend(stop_nodes_2);

            // start_tx is idempotent (nodes already running from round 1)
            let start_tx_r2 =
                MonographTxCtlTask::from_config(start_cmd.clone(), cluster_config, ServerType::Tx);
            barrier.push(start_tx_r2.len());
            executable.extend(start_tx_r2);

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
