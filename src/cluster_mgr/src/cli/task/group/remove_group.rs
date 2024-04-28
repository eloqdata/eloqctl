use std::collections::HashMap;

use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{CtrlDBTaskGroup, MonitorCtlTaskGroup, RemoveTaskGroup, TaskGroup};
use crate::cli::task::task_base::{
    merge_execution, TaskExecutionContext, TaskHost, TaskId, TaskInstance,
};
use crate::cli::CommandArgs;
use crate::config::config_base::DeploymentConfig;
use indexmap::IndexMap;
use itertools::Itertools;

#[async_trait::async_trait]
impl TaskGroup for RemoveTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: CommandArgs,
        config: DeploymentConfig,
    ) -> anyhow::Result<TaskExecutionContext> {
        let cluster = match cmd_arg.clone() {
            CommandArgs::Remove { cluster } => cluster,
            _ => {
                unreachable!()
            }
        };
        // terminate all process
        let (mut barrier, mut executable) = merge_execution(vec![
            MonitorCtlTaskGroup
                .tasks(
                    CommandArgs::Monitor {
                        cluster: cluster.clone(),
                        command: "stop".to_string(),
                    },
                    config.clone(),
                )
                .await?,
            CtrlDBTaskGroup
                .tasks(
                    CommandArgs::Stop {
                        cluster: cluster.clone(),
                        force: true,
                        all: true,
                    },
                    config.clone(),
                )
                .await?,
        ]);

        if let Some(logsv) = &config.deployment.log_service {
            // clean log service data
            let conn_user = &config.connection.username;
            let ssh_port = config.connection.ssh_port();
            let clean_tasks = logsv
                .log_directories()
                .into_iter()
                .map(|(host, dirs)| {
                    let content = dirs
                        .into_iter()
                        .map(|d| format!("rm -r {}", d))
                        .join(" && ");

                    let task_host = TaskHost::Remote {
                        user: conn_user.clone(),
                        port: ssh_port as usize,
                        hosts: host.clone(),
                    };
                    let task_id = TaskId {
                        cmd: cmd_arg.as_ref().to_string(),
                        task: format!("clean_log@{host}"),
                        host: host.clone(),
                    };
                    (
                        task_id.clone(),
                        TaskInstance {
                            task_input: HashMap::default(),
                            task: Box::new(ExecCustomCommand::new(
                                content,
                                task_id,
                                config.clone(),
                            )),
                            task_host,
                        },
                    )
                })
                .collect::<IndexMap<TaskId, TaskInstance>>();
            barrier.push(clean_tasks.len());
            executable.extend(clean_tasks);
        }
        // remove cluster directory
        let clean_tasks = ExecCustomCommand::from_config(
            &cmd_arg,
            "clean",
            format!("rm -r {}", config.install_dir()),
            &config,
        );
        barrier.push(clean_tasks.len());
        executable.extend(clean_tasks);

        Ok(TaskExecutionContext {
            task_group: cmd_arg.as_ref().to_string(),
            barrier: Some(barrier),
            executable,
        })
    }
}
