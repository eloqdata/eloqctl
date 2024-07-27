use crate::cli::task::cassandra_ctl_task::CassandraCtlTask;
use crate::cli::task::codis_task::{self, CodisTask};
use crate::cli::task::group::{CtrlDBTaskGroup, TaskGroup};
use crate::cli::task::monograph_log_ctl_task::MonographLogCtlTask;
use crate::cli::task::monograph_log_probe_task::MonographLogProbeTask;
use crate::cli::task::monograph_tx_ctl_task::MonographTxCtlTask;
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::SubCommand;
use crate::config::config_base::DeployConfig;
use crate::config::deployment;
use indexmap::IndexMap;

#[async_trait::async_trait]
impl TaskGroup for CtrlDBTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: DeployConfig,
    ) -> anyhow::Result<TaskExecutionContext> {
        let stop_all = match cmd_arg.clone() {
            SubCommand::Stop {
                cluster: _,
                force: _,
                all,
            } => all,
            _ => false,
        };
        let cmd_ref = cmd_arg.as_ref();
        let is_start_cmd = cmd_ref == "start" || cmd_ref == "restart";
        let has_cass = config.deployment.storage_service.inner_cass().is_some();
        // FIXME: stop order
        let (mut executable, mut barrier) = if has_cass && (is_start_cmd || stop_all) {
            let exe = CassandraCtlTask::from_config(cmd_arg.clone(), &config);
            let ba = CassandraCtlTask::start_barrier(exe.len());
            (exe, ba)
        } else {
            (IndexMap::new(), vec![])
        };

        let batch_cmd = match cmd_arg {
            SubCommand::Restart {
                cluster: ref cluster_name,
            } => {
                vec![
                    SubCommand::Stop {
                        cluster: cluster_name.clone(),
                        force: false,
                        all: false,
                    },
                    SubCommand::Start {
                        cluster: cluster_name.to_string(),
                    },
                ]
            }
            _ => {
                vec![cmd_arg.clone()]
            }
        };

        for cmd in batch_cmd {
            let curr_cmd_ref = cmd.as_ref();
            let log_srv_tasks = MonographLogCtlTask::from_config(cmd.clone(), &config);
            let log_probe_tasks = if curr_cmd_ref.eq("start") {
                MonographLogProbeTask::from_config(&config)
            } else {
                IndexMap::new()
            };
            let tx_srv_tasks = MonographTxCtlTask::from_config(cmd.clone(), &config);
            barrier.push(log_srv_tasks.len());
            if !log_probe_tasks.is_empty() {
                barrier.push(log_probe_tasks.len());
            }
            barrier.push(tx_srv_tasks.len());

            executable.extend(log_srv_tasks.into_iter());
            if !log_probe_tasks.is_empty() {
                executable.extend(log_probe_tasks.into_iter());
            }
            executable.extend(tx_srv_tasks.into_iter());
        }

        if config.deployment.product() == deployment::Product::EloqKV
            && config.deployment.codis.is_some()
        {
            let codis_tasks = if cmd_ref == "start" {
                CodisTask::from_config(&config, codis_task::Operation::Start)
            } else if cmd_ref == "stop" {
                CodisTask::from_config(&config, codis_task::Operation::Stop)
            } else {
                IndexMap::default()
            };
            if !codis_tasks.is_empty() {
                // start dashboard firstly, and then start all proxy servers
                barrier.push(1);
                barrier.push(codis_tasks.len() - 1);
                executable.extend(codis_tasks);
            }
        }

        let final_barrier = if is_start_cmd { Some(barrier) } else { None };
        Ok(TaskExecutionContext {
            task_group: format!("cluster-control-{cmd_ref}"),
            barrier: final_barrier,
            executable,
        })
    }
}
