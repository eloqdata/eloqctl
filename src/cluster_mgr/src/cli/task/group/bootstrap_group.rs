use crate::cli::task::cassandra_ctl_task::CassandraCtlTask;
use crate::cli::task::copy_task::CopyTask;
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{Config, InstallDBTaskGroup, TaskGroup};
use crate::cli::task::monograph_bootstrap_task::MonographInstall;
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{upload_tasks, UploadTaskBuilderType};
use crate::cli::SubCommand;
use crate::config::deployment::Product;
use crate::config::DeploymentPackage;
use indexmap::IndexMap;
use tracing::info;

#[async_trait::async_trait]
impl TaskGroup for InstallDBTaskGroup {
    async fn tasks(
        &self,
        cmd_args: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => {
                return Err(anyhow::anyhow!(
                    "Expected ClusterConfig for InstallDBTaskGroup"
                ))
            }
        };

        let mut barrier = vec![];
        let mut executable = IndexMap::new();

        let install_cmd = SubCommand::Install {
            cluster: cluster_config.clone().deployment.cluster_name,
        };
        if let Some(storage_service) = &cluster_config.deployment.storage_service {
            if let Some(cass) = &storage_service.cassandra {
                if cass.internal().is_some() {
                    let upload_cass_config_task =
                        upload_tasks(UploadTaskBuilderType::CassConf, config);
                    barrier.push(upload_cass_config_task.len());
                    executable.extend(upload_cass_config_task);
                    if let Some(monitor) = cluster_config.deployment.monitor.as_ref() {
                        if let Some(mcac_collector) = &monitor.cassandra_collector {
                            let install_dir = cluster_config.install_dir();
                            let update_http_port_cmd =
                                mcac_collector.update_http_port_cmd(install_dir);
                            let cassandra_hosts =
                                cluster_config.get_host_list(DeploymentPackage::Storage);
                            let update_http_port_task = ExecCustomCommand::build_task_by_host(
                                update_http_port_cmd,
                                config,
                                cassandra_hosts,
                                None,
                            );
                            barrier.push(update_http_port_task.len());
                            executable.extend(update_http_port_task);
                        }
                    }
                    let cassandra_start =
                        CassandraCtlTask::from_config(install_cmd, cluster_config);
                    barrier.extend(CassandraCtlTask::start_barrier(cassandra_start.len()));
                    executable.extend(cassandra_start);
                }
            } else if let Some(dss) = &storage_service.eloqdss {
                // Handle DataStoreService: check backend type and handle accordingly
                use crate::config::storage_service_config::DataStoreServiceBackend;
                match dss.backend_config() {
                    DataStoreServiceBackend::EloqStore(eloq_store_config) => {
                        // EloqStore backend: handle rclone tasks if cloud mode is enabled
                        if eloq_store_config.is_cloud_mode() {
                            use crate::cli::task::rclone_config_task::RcloneConfigTask;
                            use crate::cli::task::rclone_ctl_task::RcloneCtlTask;
                            use crate::cli::task::rclone_dir_task::RcloneDirTask;

                            // 1. Create rclone config
                            let rclone_config_tasks =
                                RcloneConfigTask::build_tasks(install_cmd.clone(), config);
                            if !rclone_config_tasks.is_empty() {
                                barrier.push(rclone_config_tasks.len());
                                executable.extend(rclone_config_tasks);
                            }

                            // 2. Create rclone directory
                            let rclone_dir_tasks =
                                RcloneDirTask::build_tasks(install_cmd.clone(), config);
                            if !rclone_dir_tasks.is_empty() {
                                barrier.push(rclone_dir_tasks.len());
                                executable.extend(rclone_dir_tasks);
                            }

                            // 3. Start rclone service (must be before DSS for Remote Internal mode,
                            // or before bootstrap for Local mode)
                            let rclone_start_tasks =
                                RcloneCtlTask::from_config(install_cmd.clone(), config);
                            if !rclone_start_tasks.is_empty() {
                                barrier.push(rclone_start_tasks.len());
                                executable.extend(rclone_start_tasks);
                            }
                        }

                        // Start DSS server if using managed remote mode DataStoreService (i.e., not external)
                        // This must be after rclone is started for Remote Internal mode
                        // Note: Data directory cleanup for EloqStore Cloud mode is handled in Start flow,
                        // not in bootstrap/install flow. See CtrlDBTaskGroup::start_tasks for details.
                        if dss.is_remote_mode() && !dss.is_external() {
                            use crate::cli::task::monograph_dss_ctl_task::MonographDssCtlTask;
                            let start_dss =
                                MonographDssCtlTask::from_config(install_cmd, cluster_config);
                            if !start_dss.is_empty() {
                                barrier.push(start_dss.len());
                                executable.extend(start_dss);
                            }
                        }
                    } // Future backends can be handled here, e.g.:
                      // DataStoreServiceBackend::BigTable(_) => { ... }
                }
            }
        }

        // Bootstrap on all leader nodes to make cluster information durable.
        let conn_user = &cluster_config.connection.username;
        let ssh_port = cluster_config.connection.ssh_port();

        let host_ports = cluster_config
            .deployment
            .get_host_port_list(DeploymentPackage::MonographTx);
        info!(
            "InstallDBTaskGroup The bootstrap target is ={:?}",
            host_ports
        );

        let process_only_first_host_port =
            if let Some(storage_service) = &cluster_config.deployment.storage_service {
                storage_service.cassandra.is_some() || storage_service.dynamodb.is_some()
            } else {
                false
            };

        let num_hosts_to_process = if process_only_first_host_port {
            1
        } else {
            host_ports.len()
        };

        let bootstrap_tasks: IndexMap<TaskId, TaskInstance> = host_ports
            .iter()
            .take(num_hosts_to_process)
            .map(|host_port| {
                let mut parts = host_port.split(':');
                let bootstrap_host = parts.next().unwrap().to_string();
                let bootstrap_port = parts.next().unwrap().to_string();

                let install_db_host = TaskHost::Remote {
                    user: conn_user.clone(),
                    port: ssh_port as usize,
                    host: bootstrap_host,
                };
                MonographInstall::from_config(cluster_config, install_db_host, bootstrap_port)
            })
            .flat_map(|map| map.into_iter())
            .collect();

        barrier.push(bootstrap_tasks.len());
        executable.extend(bootstrap_tasks);

        if cluster_config.product() == Product::EloqSQL {
            if cluster_config.deployment.tx_service.tx_host_ports.len() > 1 {
                // download data generated by bootstrap
                let (fetch_id, fetch_task) = CopyTask::fetch_datafarm(cluster_config);
                executable.insert(fetch_id, fetch_task);
                barrier.push(1);
                // dispatch data generated by bootstrap
                let upload_data_dir_task = upload_tasks(UploadTaskBuilderType::DataDir, config);
                barrier.push(upload_data_dir_task.len());
                executable.extend(upload_data_dir_task);
            }
            // rm -rf cc_ng/ tx_log/
            let txsrv_home = cluster_config.deployment.tx_srv_home();
            let rm_log_data_cmd =
                format!("rm -rf {txsrv_home}/datafarm/cc_ng {txsrv_home}/datafarm/tx_log",);
            let rm_log_data_task_instance =
                ExecCustomCommand::from_config(&cmd_args, "rm_log", rm_log_data_cmd, config);
            barrier.push(rm_log_data_task_instance.len());
            executable.extend(rm_log_data_task_instance);
        }

        if cluster_config.deployment.codis.is_some() {
            let upload_codis_task = upload_tasks(UploadTaskBuilderType::Codis, config);
            barrier.push(upload_codis_task.len());
            executable.extend(upload_codis_task);
        }

        Ok(TaskExecutionContext {
            task_group: cmd_args.as_ref().to_string(),
            barrier: Some(barrier),
            executable,
        })
    }
}
