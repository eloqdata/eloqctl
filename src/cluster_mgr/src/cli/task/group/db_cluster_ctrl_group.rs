use super::MonitorCtlTaskGroup;
use crate::cli::task::cassandra_ctl_task::CassandraCtlTask;
use crate::cli::task::codis_task::{self, CodisTask};
use crate::cli::task::group::{Config, CtrlDBTaskGroup, TaskGroup};
use crate::cli::task::monograph_log_ctl_task::MonographLogCtlTask;
use crate::cli::task::monograph_log_probe_task::MonographLogProbeTask;
use crate::cli::task::monograph_tx_ctl_task::{MonographTxCtlTask, ServerType};
use crate::cli::task::task_base::{TaskExecutionContext, TaskId, TaskInstance};
use crate::cli::task::task_utils::stop_with_hot_standby;
use crate::cli::SubCommand;
use crate::config::config_base::DeployConfig;
use anyhow::Result;
use indexmap::IndexMap;

#[async_trait::async_trait]
impl TaskGroup for CtrlDBTaskGroup {
    async fn tasks(&self, cmd: SubCommand, config: &Config) -> Result<TaskExecutionContext> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => {
                return Err(anyhow::anyhow!(
                    "Expected ClusterConfig for CtrlDBTaskGroup"
                ))
            }
        };

        let cmd_str = cmd.as_ref().to_owned();
        let (barrier, executable) = match cmd.clone() {
            SubCommand::Remove { cluster } => {
                let (cluster, tx, log, store, monitor) = (cluster, true, true, true, true);
                let (mut barrier, mut tasks) =
                    self.stop_tasks(tx, log, store, cmd, &cluster_config, true);
                if monitor && cluster_config.deployment.monitor.is_some() {
                    let stop_moni = SubCommand::Monitor {
                        cluster: cluster.clone(),
                        command: "stop".to_string(),
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
                let stop_cmd = SubCommand::Stop {
                    cluster: cluster.clone(),
                    tx: Some(true),
                    log: true,
                    store: false,
                    monitor: false,
                    force: false,
                    all: false,
                    password: None,
                };
                let (mut barrier, mut executable) =
                    self.stop_tasks(true, true, false, stop_cmd, &cluster_config, false);
                let start_cmd = SubCommand::Start {
                    cluster,
                    nodes: Vec::new(),
                };
                let (b, exe) = self.start_tasks(start_cmd, &cluster_config);
                barrier.extend(b);
                executable.extend(exe);
                (barrier, executable)
            }
            SubCommand::Start { .. } => self.start_tasks(cmd, &cluster_config),
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
                    (cluster, tx.unwrap_or(true), log, store, monitor)
                };
                let (mut barrier, mut tasks) =
                    self.stop_tasks(tx, log, store, cmd, &cluster_config, false);
                if monitor && cluster_config.deployment.monitor.is_some() {
                    let stop_moni = SubCommand::Monitor {
                        cluster: cluster.clone(),
                        command: "stop".to_string(),
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
            SubCommand::Status { .. } => {
                let tasks = self.status_tasks(cmd, &cluster_config);
                (vec![tasks.len()], tasks)
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
    fn stop_tasks(
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

        if deployment.codis.is_some() {
            let codis_tasks = CodisTask::from_config(config, codis_task::Operation::Stop);
            if !codis_tasks.is_empty() {
                barrier.push(codis_tasks.len());
                executable.extend(codis_tasks);
            }
        }

        // stop order: (standby-server -> voter-server ->) tx-server -> log-server -> kv-store
        if tx {
            let mut is_force_stop = false;
            if let SubCommand::Stop { force, .. } = cmd.clone() {
                is_force_stop = force
            };

            if is_from_remove || is_force_stop {
                // Enter this branch when:
                // - The user explicitly requests to remove or force-stop the cluster.
                // - The cluster is already in a stopped state.
                // - The majority of nodes are unresponsive, making cluster information unavailable.
                if config.deployment.tx_service.standby_host_ports.is_some() {
                    let stop_standby =
                        MonographTxCtlTask::from_config(cmd.clone(), config, ServerType::Standby);
                    barrier.push(stop_standby.len());
                    executable.extend(stop_standby);
                }
                if config.deployment.tx_service.voter_host_ports.is_some() {
                    let stop_voter =
                        MonographTxCtlTask::from_config(cmd.clone(), config, ServerType::Voter);
                    barrier.push(stop_voter.len());
                    executable.extend(stop_voter);
                }
                let stop_tx = MonographTxCtlTask::from_config(cmd.clone(), config, ServerType::Tx);
                barrier.push(stop_tx.len());
                executable.extend(stop_tx);
            } else if config.deployment.tx_service.standby_host_ports.is_some() {
                stop_with_hot_standby(cmd.clone(), config, &mut barrier, &mut executable);
            } else {
                let stop_tx = MonographTxCtlTask::from_config(cmd.clone(), config, ServerType::Tx);
                barrier.push(stop_tx.len());
                executable.extend(stop_tx);
            }
        }

        if log && deployment.log_service.is_some() {
            let stop_log = MonographLogCtlTask::from_config(cmd.clone(), config);
            barrier.push(stop_log.len());
            executable.extend(stop_log);
        }
        if store && deployment.storage_service.inner_cass().is_some() {
            let tasks = CassandraCtlTask::from_config(cmd, config);
            barrier.push(tasks.len());
            executable.extend(tasks);
        }
        (barrier, executable)
    }

    fn start_tasks(
        &self,
        start_cmd: SubCommand,
        config: &DeployConfig,
    ) -> (Vec<usize>, IndexMap<TaskId, TaskInstance>) {
        let deployment = &config.deployment;
        let mut barrier = vec![];
        let mut executable = IndexMap::new();

        if let SubCommand::Start { nodes, cluster, .. } = &start_cmd {
            if !nodes.is_empty() {
                for _ in nodes {
                    let start_nodes = MonographTxCtlTask::from_config(
                        start_cmd.clone(),
                        config,
                        ServerType::Node,
                    );
                    barrier.push(start_nodes.len());
                    executable.extend(start_nodes);
                }
            } else {
                // Start order: cassandra -> log-server -> tx-server
                if deployment.storage_service.inner_cass().is_some() {
                    let tasks = CassandraCtlTask::from_config(start_cmd.clone(), config);
                    barrier.extend(CassandraCtlTask::start_barrier(tasks.len()));
                    executable.extend(tasks);
                }

                if deployment.log_service.is_some() {
                    let start_log = MonographLogCtlTask::from_config(start_cmd.clone(), config);
                    barrier.push(start_log.len());
                    executable.extend(start_log);

                    let probe = MonographLogProbeTask::from_config(config);
                    barrier.push(probe.len());
                    executable.extend(probe);
                }

                let start_tx =
                    MonographTxCtlTask::from_config(start_cmd.clone(), config, ServerType::Tx);
                barrier.push(start_tx.len());
                executable.extend(start_tx);

                if config.deployment.tx_service.standby_host_ports.is_some() {
                    let start_standby = MonographTxCtlTask::from_config(
                        start_cmd.clone(),
                        config,
                        ServerType::Standby,
                    );
                    barrier.push(start_standby.len());
                    executable.extend(start_standby);
                }

                if config.deployment.tx_service.voter_host_ports.is_some() {
                    let start_voter = MonographTxCtlTask::from_config(
                        start_cmd.clone(),
                        config,
                        ServerType::Voter,
                    );
                    barrier.push(start_voter.len());
                    executable.extend(start_voter);
                }

                if deployment.codis.is_some() {
                    let codis_tasks = CodisTask::from_config(config, codis_task::Operation::Start);
                    if !codis_tasks.is_empty() {
                        // Start dashboard first, then start all proxy servers
                        barrier.push(1);
                        barrier.push(codis_tasks.len() - 1);
                        executable.extend(codis_tasks);
                    }
                }
            }

            // Wait until cluster is ready for connection after start
            let status_cmd = SubCommand::Status {
                cluster: cluster.clone(),
                user: None,
                password: None,
                wait: Some(30),
            };
            let status_tasks = self.status_tasks(status_cmd, config);
            barrier.push(status_tasks.len());
            executable.extend(status_tasks);
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
        if deployment.log_service.is_some() {
            let tasks = MonographLogCtlTask::from_config(cmd.clone(), config);
            executable.extend(tasks);
        }
        if config.deployment.tx_service.standby_host_ports.is_some() {
            let status_standby =
                MonographTxCtlTask::from_config(cmd.clone(), config, ServerType::Standby);
            executable.extend(status_standby);
        }
        if config.deployment.tx_service.voter_host_ports.is_some() {
            let status_voter =
                MonographTxCtlTask::from_config(cmd.clone(), config, ServerType::Voter);
            executable.extend(status_voter);
        }
        let status_tx = MonographTxCtlTask::from_config(cmd, config, ServerType::Tx);
        executable.extend(status_tx);
        if deployment.codis.is_some() {
            //TODO
        }
        executable
    }
}
