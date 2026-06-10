use crate::cli::task::eloq_dss_ctl_task::EloqDssCtlTask;
use crate::cli::task::eloq_log_ctl_task::EloqLogCtlTask;
use crate::cli::task::eloq_log_probe_task::EloqLogProbeTask;
use crate::cli::task::eloq_store_data_clean_task::EloqStoreDataCleanTask;
use crate::cli::task::eloq_tx_ctl_task::{EloqTxCtlTask, ServerType};
use crate::cli::task::group::{Config, CtrlDBTaskGroup, MonitorCtlTaskGroup, TaskGroup};
use crate::cli::task::monitor_ctl_task::MonitorCtlTask;
use crate::cli::task::redis_op_task::{ClusterNodes, RedisOpTask};
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::task::task_utils::stop_with_hot_standby;
use crate::cli::task::topology_display_task::TopologyDisplayTask;
use crate::cli::task::topology_update_task::TopologyUpdateTask;
use crate::cli::{MonitorCommand, SubCommand};
use crate::config::config_base::DeployConfig;
use crate::config::deployment::Product;
use crate::config::DeploymentPackage;
use anyhow::Result;
use indexmap::IndexMap;
use std::collections::HashMap;
use tokio::sync::watch;
use tracing::{info, warn};

#[async_trait::async_trait]
impl TaskGroup for CtrlDBTaskGroup {
    async fn tasks(&self, cmd: SubCommand, config: &Config) -> Result<TaskExecutionContext> {
        let Config::Cluster(cluster_config) = config;

        let cmd_str = cmd.as_ref().to_owned();
        let (barrier, executable) = match cmd.clone() {
            SubCommand::Remove { cluster, force: _ } => {
                let (cluster, tx, log, store, monitor) = (cluster, true, true, true, true);
                let (mut barrier, mut tasks) = self
                    .stop_tasks(tx, log, store, cmd, cluster_config, true)
                    .await;
                if monitor && cluster_config.deployment.monitor.is_some() {
                    let stop_moni = SubCommand::Monitor {
                        cluster: Some(cluster.clone()),
                        command: MonitorCommand::Stop {
                            cluster: None,
                            components: vec![],
                        },
                    };
                    let TaskExecutionContext {
                        task_group: _,
                        barrier: ba,
                        executable,
                    } = MonitorCtlTaskGroup.tasks(stop_moni, config).await?;
                    if let Some(ba) = ba {
                        barrier.extend(ba);
                    } else {
                        barrier.push(executable.len());
                    }
                    tasks.extend(executable);
                }
                (barrier, tasks)
            }
            SubCommand::Restart { cluster } => {
                // Determine if we should stop DSS based on DataStoreService internal mode
                let mut should_stop_store = false;
                if let Some(storage) = &cluster_config.deployment.storage_service {
                    if let Some(dss) = &storage.eloqdss {
                        if dss.is_remote_mode() && !dss.is_external() {
                            should_stop_store = true;
                        } else {
                            // Also set should_stop_store to true if EloqStore Cloud mode is enabled
                            use crate::config::storage_service_config::DataStoreServiceBackend;
                            match dss.backend_config() {
                                DataStoreServiceBackend::EloqStore(eloq_store_config) => {
                                    if eloq_store_config.is_cloud_mode() && !dss.is_external() {
                                        // For EloqStore Cloud mode, stop DSS service
                                        should_stop_store = true;
                                    }
                                } // Future backends can be handled here.
                            }
                        }
                    }
                }

                let stop_cmd = SubCommand::Stop {
                    cluster: cluster.clone(),
                    tx: Some(true),
                    log: true,
                    store: should_stop_store,
                    monitor: false,
                    force: false,
                    all: false,
                    password: None,
                    nodes: Vec::new(),
                };
                let (mut barrier, mut executable) = self
                    .stop_tasks(
                        true,
                        true,
                        should_stop_store,
                        stop_cmd,
                        cluster_config,
                        false,
                    )
                    .await;
                let start_cmd = SubCommand::Start {
                    cluster,
                    nodes: Vec::new(),
                };
                let (b, exe) = self.start_tasks(start_cmd, cluster_config, false);
                barrier.extend(b);
                executable.extend(exe);
                (barrier, executable)
            }
            SubCommand::Start { .. } => self.start_tasks(cmd, cluster_config, false),
            SubCommand::Stop {
                cluster,
                tx,
                log,
                store,
                monitor,
                force: _,
                all,
                ..
            } => {
                let (cluster, tx, log, store, monitor) = if all {
                    (cluster, true, true, true, true)
                } else {
                    let mut final_store = store;
                    // If DataStoreService is in internal remote mode (managed by eloqctl),
                    // automatically set store to true to stop DSS
                    if !final_store {
                        if let Some(storage) = &cluster_config.deployment.storage_service {
                            if let Some(dss) = &storage.eloqdss {
                                if dss.is_remote_mode() && !dss.is_external() {
                                    final_store = true;
                                } else {
                                    // Also set store to true if EloqStore Cloud mode is enabled
                                    use crate::config::storage_service_config::DataStoreServiceBackend;
                                    match dss.backend_config() {
                                        DataStoreServiceBackend::EloqStore(eloq_store_config) => {
                                            if eloq_store_config.is_cloud_mode()
                                                && !dss.is_external()
                                            {
                                                // For EloqStore Cloud mode, stop DSS service
                                                final_store = true;
                                            }
                                        } // Future backends can be handled here.
                                    }
                                }
                            }
                        }
                    }
                    (cluster, tx.unwrap_or(true), log, final_store, monitor)
                };
                let (mut barrier, mut tasks) = self
                    .stop_tasks(tx, log, store, cmd, cluster_config, false)
                    .await;
                if monitor && cluster_config.deployment.monitor.is_some() {
                    let stop_moni = SubCommand::Monitor {
                        cluster: Some(cluster.clone()),
                        command: MonitorCommand::Stop {
                            cluster: None,
                            components: vec![],
                        },
                    };
                    let TaskExecutionContext {
                        task_group: _,
                        barrier: ba,
                        executable,
                    } = MonitorCtlTaskGroup.tasks(stop_moni, config).await?;
                    if let Some(ba) = ba {
                        barrier.extend(ba);
                    } else {
                        barrier.push(executable.len());
                    }
                    tasks.extend(executable);
                }
                (barrier, tasks)
            }
            SubCommand::Status { detail, .. } => {
                if detail {
                    let mut barrier = Vec::new();
                    let mut executable = IndexMap::new();

                    // Barrier group 1: fetch TX topology via Redis and update state
                    let (redis_tx, redis_rx) = watch::channel(ClusterNodes {
                        masters: Vec::new(),
                        replicas: Vec::new(),
                    });
                    let redis_task_id = TaskId {
                        cmd: "topology".to_string(),
                        task: "redis-topology".to_string(),
                        host: "local".to_string(),
                    };
                    let redis_task = RedisOpTask::new(
                        redis_task_id.clone(),
                        cluster_config.deployment.tx_service.tx_host_ports.clone(),
                        "cluster topology".to_string(),
                        redis_tx.clone(),
                        cluster_config.redis_password(None),
                        true,
                    )
                    .with_service_endpoints(cluster_config.connection.service_endpoints.clone());
                    barrier.push(1);
                    executable.insert(
                        redis_task_id.clone(),
                        TaskInstance {
                            task_input: HashMap::default(),
                            task: Box::new(redis_task),
                            task_host: TaskHost::Local,
                        },
                    );

                    // Barrier group 2: update topology from Redis result
                    let update_map =
                        TopologyUpdateTask::from_redis(cluster_config, redis_rx.clone(), None);
                    if !update_map.is_empty() {
                        barrier.push(update_map.len());
                        executable.extend(update_map);
                    }

                    // Barrier group 3: topology display
                    // Prepare display tasks for later
                    let display_map = TopologyDisplayTask::from_command(cmd.clone());
                    barrier.push(display_map.len());
                    executable.extend(display_map);

                    (barrier, executable)
                } else {
                    let tasks = self.status_tasks(cmd, cluster_config);
                    (vec![tasks.len()], tasks)
                }
            }
            _ => unreachable!(),
        };

        Ok(TaskExecutionContext {
            task_group: format!("cluster-control-{cmd_str}"),
            barrier: Some(barrier),
            executable,
        })
    }
}

impl CtrlDBTaskGroup {
    async fn stop_tasks(
        &self,
        tx: bool,
        log: bool,
        store: bool,
        cmd: SubCommand,
        config: &DeployConfig,
        is_from_remove: bool,
    ) -> (Vec<usize>, IndexMap<TaskId, TaskInstance>) {
        let deployment = &config.deployment;
        let mut barrier = vec![];
        let mut executable = IndexMap::new();

        // stop order: (standby-server -> voter-server ->) tx-server -> log-server -> kv-store
        if tx {
            let mut is_force_stop = false;
            let mut has_nodes = false;

            if let SubCommand::Stop {
                force,
                nodes: node_list,
                ..
            } = cmd.clone()
            {
                is_force_stop = force;
                if !node_list.is_empty() {
                    has_nodes = true;
                }
            };

            if has_nodes {
                let stop_nodes = EloqTxCtlTask::from_config_with_channel(
                    cmd.clone(),
                    config,
                    ServerType::Node,
                    None,
                )
                .expect("stop nodes error");
                barrier.push(stop_nodes.len());
                executable.extend(stop_nodes);
            } else if is_from_remove || is_force_stop {
                // Enter this branch when:
                // - The user explicitly requests to remove or force-stop the cluster.
                // - The cluster is already in a stopped state.
                // - The majority of nodes are unresponsive, making cluster information unavailable.
                if config.deployment.tx_service.standby_host_ports.is_some() {
                    let stop_standby =
                        EloqTxCtlTask::from_config(cmd.clone(), config, ServerType::Standby);
                    barrier.push(stop_standby.len());
                    executable.extend(stop_standby);
                }
                if config.deployment.tx_service.voter_host_ports.is_some() {
                    let stop_voter =
                        EloqTxCtlTask::from_config(cmd.clone(), config, ServerType::Voter);
                    barrier.push(stop_voter.len());
                    executable.extend(stop_voter);
                }
                let stop_tx = EloqTxCtlTask::from_config(cmd.clone(), config, ServerType::Tx);
                barrier.push(stop_tx.len());
                executable.extend(stop_tx);
            } else if config.deployment.tx_service.standby_host_ports.is_some() {
                stop_with_hot_standby(cmd.clone(), config, &mut barrier, &mut executable).await;
            } else {
                let stop_tx = EloqTxCtlTask::from_config(cmd.clone(), config, ServerType::Tx);
                barrier.push(stop_tx.len());
                executable.extend(stop_tx);
            }
        }

        if log && deployment.log_service.is_some() {
            let stop_log = EloqLogCtlTask::from_config(cmd.clone(), config);
            barrier.push(stop_log.len());
            executable.extend(stop_log);
        }
        if store {
            if let Some(storage) = &deployment.storage_service {
                if let Some(dss) = &storage.eloqdss {
                    // EloqDSS storage provider
                    // Stop DSS if in Remote Internal mode (not external)
                    if dss.is_remote_mode() && !dss.is_external() {
                        let stop_dss = EloqDssCtlTask::from_config(cmd.clone(), config);
                        if !stop_dss.is_empty() {
                            barrier.push(stop_dss.len());
                            executable.extend(stop_dss);
                        }
                    }

                    use crate::config::storage_service_config::DataStoreServiceBackend;
                    match dss.backend_config() {
                        DataStoreServiceBackend::EloqStore(_eloq_store_config) => {
                            // EloqStore backend: no rclone tasks needed
                        } // Future backends can be handled here, e.g.:
                          // DataStoreServiceBackend::BigTable(_) => { ... }
                    }
                } else if matches!(
                    storage.rocksdb,
                    Some(crate::config::storage_service_config::RocksDB::EloqDssRocksdb(_))
                ) {
                    // RocksDB storage provider (EloqDssRocksdb)
                    let stop_dss = EloqDssCtlTask::from_config(cmd.clone(), config);
                    if !stop_dss.is_empty() {
                        barrier.push(stop_dss.len());
                        executable.extend(stop_dss);
                    }
                }
            }
        }
        (barrier, executable)
    }

    pub fn start_tasks(
        &self,
        start_cmd: SubCommand,
        config: &DeployConfig,
        skip_log_service: bool,
    ) -> (Vec<usize>, IndexMap<TaskId, TaskInstance>) {
        let deployment = &config.deployment;
        let mut barrier = vec![];
        let mut executable = IndexMap::new();

        if let SubCommand::Start { nodes, cluster, .. } = &start_cmd {
            if !nodes.is_empty() {
                // Generate node-start tasks once for the provided node list to avoid
                // duplicate TaskIds and mismatched barrier sizes
                let start_nodes =
                    EloqTxCtlTask::from_config(start_cmd.clone(), config, ServerType::Node);
                barrier.push(start_nodes.len());
                executable.extend(start_nodes);
            } else {
                if let Some(storage) = &deployment.storage_service {
                    if let Some(dss) = &storage.eloqdss {
                        use crate::config::storage_service_config::DataStoreServiceBackend;
                        match dss.backend_config() {
                            DataStoreServiceBackend::EloqStore(eloq_store_config) => {
                                if eloq_store_config.is_cloud_mode() {
                                    // Check if we should skip cleanup based on eloq_store_reuse_local_files
                                    // If eloq_store_reuse_local_files is true, skip cleanup to reuse local files
                                    let should_skip_cleanup = eloq_store_config
                                        .get_cloud_config()
                                        .and_then(|cloud_config| {
                                            cloud_config.eloq_store_reuse_local_files
                                        })
                                        .unwrap_or(false);

                                    if !should_skip_cleanup {
                                        // Clean EloqStore data directories
                                        // This is called from both Start flow and Launch flow (Launch calls Start flow).
                                        // Local mode: clean on EloqKV nodes before starting EloqKV
                                        // Remote Internal mode: clean on DSS nodes before starting DSS
                                        //   - In Start/Restart flow: DSS is not running, so cleanup will execute
                                        //   - In Launch flow: DSS is already running (started in bootstrap),
                                        //     so cleanup will be skipped after checking process status
                                        let config_for_clean = Config::Cluster(config.clone());
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
                            } // Future backends can be handled here, e.g.:
                              // DataStoreServiceBackend::BigTable(_) => { ... }
                        }
                    }
                }

                // Start DSS only when rocksdb is ELOQDSS_ROCKSDB or DataStoreService Remote mode (only if not external)
                if let Some(storage) = &deployment.storage_service {
                    if matches!(
                        storage.rocksdb,
                        Some(crate::config::storage_service_config::RocksDB::EloqDssRocksdb(_))
                    ) || storage
                        .eloqdss
                        .as_ref()
                        .is_some_and(|ds| ds.is_remote_mode() && !ds.is_external())
                    {
                        use crate::cli::task::eloq_dss_ctl_task::EloqDssCtlTask;
                        let start_dss = EloqDssCtlTask::from_config(start_cmd.clone(), config);
                        barrier.push(start_dss.len());
                        executable.extend(start_dss);
                    }
                }

                // Check log service configuration and log decision
                // Skip if log service was already started earlier (e.g., during launch before bootstrap)
                if !skip_log_service {
                    if let Some(log_svc) = &deployment.log_service {
                        let log_nodes_count = log_svc.nodes.len();
                        let log_hosts: Vec<String> = log_svc
                            .nodes
                            .iter()
                            .map(|n| format!("{}:{}", n.host, n.port))
                            .collect();
                        info!(
                            "Log service is configured with {} node(s): {:?}. Starting log service...",
                            log_nodes_count, log_hosts
                        );
                        let start_log = EloqLogCtlTask::from_config(start_cmd.clone(), config);
                        if start_log.is_empty() {
                            warn!(
                                "Log service is configured but no start tasks were generated. \
                                 This may indicate a configuration error."
                            );
                        } else {
                            let start_log_len = start_log.len();
                            barrier.push(start_log_len);
                            executable.extend(start_log);
                            info!("Added log service start task(s) to execution plan");
                        }

                        let probe = EloqLogProbeTask::from_config(config);
                        if !probe.is_empty() {
                            let probe_len = probe.len();
                            barrier.push(probe_len);
                            executable.extend(probe);
                            info!("Added log service probe task(s) to execution plan");
                        }
                    } else {
                        warn!(
                            "Log service is not configured in deployment. \
                             Log service will not be started. \
                             To start log service, add 'log_service' configuration to your deployment YAML."
                        );
                    }
                } else {
                    info!(
                        "Skipping log service startup (already started earlier in launch sequence)"
                    );
                }

                let mut start_tx =
                    EloqTxCtlTask::from_config(start_cmd.clone(), config, ServerType::Tx);

                if config.deployment.tx_service.standby_host_ports.is_some() {
                    let start_standby =
                        EloqTxCtlTask::from_config(start_cmd.clone(), config, ServerType::Standby);
                    start_tx.extend(start_standby);
                }

                if config.deployment.tx_service.voter_host_ports.is_some() {
                    let start_voter =
                        EloqTxCtlTask::from_config(start_cmd.clone(), config, ServerType::Voter);
                    start_tx.extend(start_voter);
                }

                barrier.push(start_tx.len());
                executable.extend(start_tx);
            }

            // Wait until service processes are up after start. Redis cluster readiness is
            // checked separately through topology so we do not pin readiness to one tx port.
            let status_cmd = SubCommand::Status {
                cluster: cluster.clone(),
                user: None,
                password: None,
                wait: None,
                detail: false,
            };
            let status_tasks = self.status_tasks(status_cmd, config);
            barrier.push(status_tasks.len());
            executable.extend(status_tasks);

            if config.deployment.product == Product::EloqKV
                && config.deployment.cluster_mode.unwrap_or(false)
            {
                let topology_task_id = TaskId {
                    cmd: "topology".to_string(),
                    task: "wait-current-master".to_string(),
                    host: "_local".to_string(),
                };
                let (topology_tx, _) = watch::channel(ClusterNodes {
                    masters: Vec::new(),
                    replicas: Vec::new(),
                });
                let mut topology_nodes = config.get_host_port_list(DeploymentPackage::EloqTx);
                topology_nodes.extend(config.get_host_port_list(DeploymentPackage::EloqStandby));
                executable.insert(
                    topology_task_id.clone(),
                    TaskInstance {
                        task_input: HashMap::default(),
                        task: Box::new(
                            RedisOpTask::new(
                                topology_task_id,
                                topology_nodes,
                                "cluster topology".to_string(),
                                topology_tx,
                                config.redis_password(None),
                                true,
                            )
                            .with_service_endpoints(config.connection.service_endpoints.clone()),
                        ),
                        task_host: TaskHost::Local,
                    },
                );
                barrier.push(1);
            }
        }

        (barrier, executable)
    }

    fn status_tasks(
        &self,
        cmd: SubCommand,
        config: &DeployConfig,
    ) -> IndexMap<TaskId, TaskInstance> {
        let deployment = &config.deployment;
        let mut executable = IndexMap::new();
        let tx_status_cmd = match &cmd {
            SubCommand::Status {
                cluster,
                user,
                password,
                wait,
                detail,
            } => SubCommand::Status {
                cluster: cluster.clone(),
                user: user.clone(),
                password: password.clone(),
                wait: Some(wait.unwrap_or(0)),
                detail: *detail,
            },
            _ => cmd.clone(),
        };

        // DSS status (when rocksdb is EloqDssRocksdb or DataStoreService Remote mode, only if not external)
        if let Some(storage) = &deployment.storage_service {
            if matches!(
                storage.rocksdb,
                Some(crate::config::storage_service_config::RocksDB::EloqDssRocksdb(_))
            ) || storage
                .eloqdss
                .as_ref()
                .is_some_and(|ds| ds.is_remote_mode() && !ds.is_external())
            {
                let dss_tasks = EloqDssCtlTask::from_config(cmd.clone(), config);
                if !dss_tasks.is_empty() {
                    executable.extend(dss_tasks);
                }
            }
        }

        if deployment.log_service.is_some() {
            let tasks = EloqLogCtlTask::from_config(cmd.clone(), config);
            executable.extend(tasks);
        }
        if config.deployment.tx_service.standby_host_ports.is_some() {
            let status_standby =
                EloqTxCtlTask::from_config(tx_status_cmd.clone(), config, ServerType::Standby);
            executable.extend(status_standby);
        }
        if config.deployment.tx_service.voter_host_ports.is_some() {
            let status_voter = EloqTxCtlTask::from_config(cmd.clone(), config, ServerType::Voter);
            executable.extend(status_voter);
        }
        let status_tx = EloqTxCtlTask::from_config(tx_status_cmd, config, ServerType::Tx);
        executable.extend(status_tx);

        if deployment.monitor.is_some() {
            let monitor_status_cmd = SubCommand::Monitor {
                cluster: Some(deployment.cluster_name.clone()),
                command: MonitorCommand::Status {
                    cluster: None,
                    components: vec![],
                },
            };
            executable.extend(MonitorCtlTask::exporter_ctl_task(
                monitor_status_cmd.clone(),
                config,
            ));
            executable.extend(MonitorCtlTask::prometheus_ctl_task(
                monitor_status_cmd.clone(),
                config,
            ));
            executable.extend(MonitorCtlTask::alertmanager_ctl_task(
                monitor_status_cmd.clone(),
                config,
            ));
            executable.extend(MonitorCtlTask::grafana_ctl_task(
                monitor_status_cmd.clone(),
                config,
            ));
            executable.extend(MonitorCtlTask::prometheusalert_ctl_task(
                monitor_status_cmd,
                config,
            ));
        }

        executable
    }
}
