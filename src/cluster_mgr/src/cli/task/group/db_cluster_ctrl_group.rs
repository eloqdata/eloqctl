use crate::cli::task::cassandra_ctl_task::CassandraCtlTask;
use crate::cli::task::group::{CtrlDBTaskGroup, TaskGroup};
use crate::cli::task::monograph_log_ctl_task::MonographLogCtlTask;
use crate::cli::task::monograph_log_probe_task::MonographLogProbeTask;
use crate::cli::task::monograph_tx_ctl_task::MonographTxCtlTask;
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::CommandArgs;
use crate::config::config_base::DeploymentConfig;
use crate::config::StorageProvider;
use indexmap::IndexMap;

#[async_trait::async_trait]
impl TaskGroup for CtrlDBTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: CommandArgs,
        config: DeploymentConfig,
    ) -> anyhow::Result<TaskExecutionContext> {
        let stop_all = match cmd_arg.clone() {
            CommandArgs::Stop {
                cluster: _,
                force: _,
                all: Some(stop_all),
            } => stop_all.to_lowercase().eq("true"),
            _ => false,
        };

        let cmd_ref = cmd_arg.as_ref();
        let storage_provider = config.get_monograph_storage()?;
        let is_start_cmd = (cmd_ref == "start" || cmd_ref == "restart")
            && storage_provider == StorageProvider::Cassandra;

        let (mut executable, mut barrier) = if is_start_cmd || stop_all {
            let exe = CassandraCtlTask::from_config(cmd_arg.clone(), &config);
            let ba = CassandraCtlTask::barrier(exe.len());
            (exe, ba)
        } else {
            (IndexMap::new(), vec![])
        };

        let batch_cmd = match cmd_arg {
            CommandArgs::Restart {
                cluster: ref cluster_name,
            } => {
                vec![
                    CommandArgs::Stop {
                        cluster: cluster_name.clone(),
                        force: Some("false".to_string()),
                        all: None,
                    },
                    CommandArgs::Start {
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

        let final_barrier = if is_start_cmd { Some(barrier) } else { None };
        Ok(TaskExecutionContext {
            task_group: format!("cluster-control-{cmd_ref}"),
            barrier: final_barrier,
            executable,
        })
    }
}
