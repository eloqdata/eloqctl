use crate::cli::task::group::{TaskGroup, UpdateConfigTaskGroup};
use crate::cli::task::monograph_tx_ctl_task::MonographTxCtlTask;
use crate::cli::task::monograph_tx_ctl_task::ServerType;
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::task::upload::upload_task_builder::{upload_tasks, UploadTaskBuilderType};
use crate::cli::SubCommand;
use crate::config::config_base::DeployConfig;
use indexmap::IndexMap;

#[async_trait::async_trait]
impl TaskGroup for UpdateConfigTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: DeployConfig,
    ) -> anyhow::Result<TaskExecutionContext> {
        let cluster_name = &config.deployment.cluster_name;
        let need_restart = match cmd_arg {
            SubCommand::UpdateConf {
                cluster: _,
                restart,
            } => restart,
            _ => unreachable!(),
        };
        let mut executable = IndexMap::new();
        let mut barrier = vec![];
        executable.extend(upload_tasks(UploadTaskBuilderType::TxConf, &config));

        // TODO(ZX) also restart standby and voter
        if need_restart {
            barrier.push(executable.len());
            let stop_tx_task = MonographTxCtlTask::from_config(
                SubCommand::Stop {
                    cluster: cluster_name.clone(),
                    tx: Some(true),
                    log: true,
                    store: false,
                    monitor: false,
                    force: false,
                    all: false,
                },
                &config,
                ServerType::Tx,
            );

            let start_tx_task = MonographTxCtlTask::from_config(
                SubCommand::Start {
                    cluster: cluster_name.to_string(),
                },
                &config,
                ServerType::Tx,
            );
            barrier.push(stop_tx_task.len());
            barrier.push(start_tx_task.len());
            executable.extend(stop_tx_task);
            executable.extend(start_tx_task);
        }

        Ok(TaskExecutionContext {
            task_group: "update-tx-conf".to_string(),
            barrier: if barrier.is_empty() {
                None
            } else {
                Some(barrier)
            },
            executable,
        })
    }
}
