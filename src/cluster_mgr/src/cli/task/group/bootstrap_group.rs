use crate::cli::task::eloq_bootstrap_task::EloqInstall;
use crate::cli::task::group::{Config, InstallDBTaskGroup, TaskGroup};
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::SubCommand;
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
        let Config::Cluster(cluster_config) = config;

        let mut barrier = vec![];
        let mut executable = IndexMap::new();

        let install_cmd = SubCommand::Install {
            cluster: cluster_config.clone().deployment.cluster_name,
        };
        if let Some(storage_service) = &cluster_config.deployment.storage_service {
            if let Some(dss) = &storage_service.eloqdss {
                // Handle DataStoreService: check backend type and handle accordingly
                use crate::config::storage_service_config::DataStoreServiceBackend;
                match dss.backend_config() {
                    DataStoreServiceBackend::EloqStore(_eloq_store_config) => {
                        // EloqStore backend: Start DSS server if using managed remote mode DataStoreService (i.e., not external)
                        // Note: Data directory cleanup for EloqStore Cloud mode is handled in Start flow,
                        // not in bootstrap/install flow. See CtrlDBTaskGroup::start_tasks for details.
                        if dss.is_remote_mode() && !dss.is_external() {
                            use crate::cli::task::eloq_dss_ctl_task::EloqDssCtlTask;
                            let start_dss =
                                EloqDssCtlTask::from_config(install_cmd, cluster_config);
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
        let host_ports = cluster_config
            .deployment
            .get_host_port_list(DeploymentPackage::EloqTx);
        info!(
            "InstallDBTaskGroup The bootstrap target is ={:?}",
            host_ports
        );

        let process_only_first_host_port =
            if let Some(storage_service) = &cluster_config.deployment.storage_service {
                use crate::config::storage_service_config::DataStoreServiceBackend;
                storage_service.dynamodb.is_some()
                    || (storage_service.eloqdss.is_some()
                        && matches!(
                            storage_service.eloqdss.as_ref().unwrap().backend_config(),
                            DataStoreServiceBackend::EloqStore(_)
                        ))
            } else {
                false
            };

        // Check if we should skip bootstrap for eloqdss single node
        let mut skip_bootstrap = false;
        if let Some(storage_service) = &cluster_config.deployment.storage_service {
            // Skip bootstrap only if:
            // 1. Using eloqdss storage service
            // 2. Single node (host_ports.len() == 1)
            // 3. NOT in standby mode (standby_host_ports is None or empty)
            let has_standby = cluster_config
                .deployment
                .tx_service
                .standby_host_ports
                .as_ref()
                .map(|ports| !ports.is_empty())
                .unwrap_or(false);

            skip_bootstrap =
                storage_service.eloqdss.is_some() && host_ports.len() == 1 && !has_standby;
        }

        if !skip_bootstrap {
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

                    let install_db_host =
                        TaskHost::remote(&cluster_config.connection, bootstrap_host);
                    EloqInstall::from_config(cluster_config, install_db_host, bootstrap_port)
                })
                .flat_map(|map| map.into_iter())
                .collect();

            barrier.push(bootstrap_tasks.len());
            executable.extend(bootstrap_tasks);

            // Clean up local data directory on bootstrap nodes after bootstrap completes
            // Only for EloqStore Cloud mode
            if let Some(storage_service) = &cluster_config.deployment.storage_service {
                if let Some(dss) = storage_service.eloqdss.as_ref() {
                    use crate::config::storage_service_config::DataStoreServiceBackend;
                    if matches!(dss.backend_config(), DataStoreServiceBackend::EloqStore(_)) {
                        use crate::cli::task::eloq_store_data_clean_task::EloqStoreDataCleanTask;

                        // Get bootstrap hosts
                        let bootstrap_hosts: Vec<String> = host_ports
                            .iter()
                            .take(num_hosts_to_process)
                            .filter_map(|hp| hp.split(':').next().map(|h| h.to_string()))
                            .collect();

                        // Build cleanup tasks only for bootstrap nodes
                        let clean_tasks = EloqStoreDataCleanTask::build_tasks(
                            cmd_args.clone(),
                            config,
                            Some(&bootstrap_hosts), // Only clean bootstrap nodes
                        );

                        if !clean_tasks.is_empty() {
                            barrier.push(clean_tasks.len());
                            executable.extend(clean_tasks);
                        }
                    }
                }
            }
        } else {
            info!("InstallDBTaskGroup: Skipping bootstrap for eloqdss single node deployment");
        }

        Ok(TaskExecutionContext {
            task_group: cmd_args.as_ref().to_string(),
            barrier: Some(barrier),
            executable,
        })
    }
}
