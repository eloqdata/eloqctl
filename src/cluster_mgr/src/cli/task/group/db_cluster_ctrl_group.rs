use crate::cli::task::cassandra_ctl_task::CassandraCtlTask;
use crate::cli::task::codis_task::{self, CodisTask};
use crate::cli::task::group::{CtrlDBTaskGroup, TaskGroup};
use crate::cli::task::monograph_log_ctl_task::MonographLogCtlTask;
use crate::cli::task::monograph_log_probe_task::MonographLogProbeTask;
use crate::cli::task::monograph_tx_ctl_task::MonographTxCtlTask;
use crate::cli::task::task_base::{TaskExecutionContext, TaskId, TaskInstance};
use crate::cli::SubCommand;
use crate::config::config_base::DeployConfig;
use anyhow::Result;
use indexmap::IndexMap;

use super::MonitorCtlTaskGroup;

#[async_trait::async_trait]
impl TaskGroup for CtrlDBTaskGroup {
    async fn tasks(&self, cmd: SubCommand, config: DeployConfig) -> Result<TaskExecutionContext> {
        let cmd_str = cmd.as_ref().to_owned();
        let (barrier, executable) = match cmd.clone() {
            SubCommand::Restart { cluster } => {
                let stop_cmd = SubCommand::Stop {
                    cluster: cluster.clone(),
                    tx: Some(true),
                    log: true,
                    store: false,
                    monitor: false,
                    force: false,
                    all: false,
                };
                let (mut barrier, mut executable) =
                    self.stop_tasks(true, true, false, stop_cmd, &config);
                let start_cmd = SubCommand::Start { cluster };
                let (b, exe) = self.start_tasks(start_cmd, &config);
                barrier.extend(b);
                executable.extend(exe);
                (barrier, executable)
            }
            SubCommand::Start { .. } => self.start_tasks(cmd, &config),
            SubCommand::Stop {
                cluster,
                tx,
                log,
                store,
                monitor,
                force: _,
                all,
            } => {
                let (cluster, tx, log, store, monitor) = if all {
                    (cluster, true, true, true, true)
                } else {
                    (cluster, tx.unwrap_or(true), log, store, monitor)
                };
                let (mut barrier, mut tasks) = self.stop_tasks(tx, log, store, cmd, &config);
                if monitor && config.deployment.monitor.is_some() {
                    let stop_moni = SubCommand::Monitor {
                        cluster: cluster.clone(),
                        command: "stop".to_string(),
                    };
                    let TaskExecutionContext {
                        task_group: _,
                        barrier: ba,
                        executable,
                    } = MonitorCtlTaskGroup.tasks(stop_moni, config.clone()).await?;
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
                let tasks = self.status_tasks(cmd, &config);
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
    ) -> (Vec<usize>, IndexMap<TaskId, TaskInstance>) {
        let deployment = &config.deployment;
        let mut barrier = vec![];
        let mut executable = IndexMap::new();

        if deployment.codis.is_some() {
            let codis_tasks = CodisTask::from_config(&config, codis_task::Operation::Stop);
            if !codis_tasks.is_empty() {
                barrier.push(codis_tasks.len());
                executable.extend(codis_tasks);
            }
        }

        // stop order: tx-server -> log-server -> cassandra
        if tx {
            let stop_tx = MonographTxCtlTask::from_config(cmd.clone(), &config);
            barrier.push(stop_tx.len());
            executable.extend(stop_tx);
        }
        if log && deployment.log_service.is_some() {
            let stop_log = MonographLogCtlTask::from_config(cmd.clone(), &config);
            barrier.push(stop_log.len());
            executable.extend(stop_log);
        }
        if store && deployment.storage_service.inner_cass().is_some() {
            let tasks = CassandraCtlTask::from_config(cmd, &config);
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
        // start order: cassandra -> log-server -> tx-server
        if deployment.storage_service.inner_cass().is_some() {
            let tasks = CassandraCtlTask::from_config(start_cmd.clone(), &config);
            let ba = CassandraCtlTask::start_barrier(tasks.len());
            barrier.extend(ba);
            executable.extend(tasks);
        }
        if deployment.log_service.is_some() {
            let start_log = MonographLogCtlTask::from_config(start_cmd.clone(), &config);
            barrier.push(start_log.len());
            executable.extend(start_log);
            let probe = MonographLogProbeTask::from_config(&config);
            barrier.push(probe.len());
            executable.extend(probe);
        }
        let start_tx = MonographTxCtlTask::from_config(start_cmd.clone(), &config);
        barrier.push(start_tx.len());
        executable.extend(start_tx);

        if deployment.codis.is_some() {
            let codis_tasks = CodisTask::from_config(&config, codis_task::Operation::Start);
            if !codis_tasks.is_empty() {
                // start dashboard firstly, and then start all proxy servers
                barrier.push(1);
                barrier.push(codis_tasks.len() - 1);
                executable.extend(codis_tasks);
            }
        }

        let cluster = match start_cmd {
            SubCommand::Start { cluster } => cluster,
            _ => unreachable!(),
        };
        // wait until cluster is ready for connection after start
        let status_cmd = SubCommand::Status {
            cluster,
            user: None,
            password: None,
            wait: Some(30),
        };
        let status_tasks = self.status_tasks(status_cmd, &config);
        barrier.push(status_tasks.len());
        executable.extend(status_tasks);

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
            let tasks = MonographLogCtlTask::from_config(cmd.clone(), &config);
            executable.extend(tasks);
        }
        let start_tx = MonographTxCtlTask::from_config(cmd, &config);
        executable.extend(start_tx);
        if deployment.codis.is_some() {
            //TODO
        }
        executable
    }
}
