use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{Config, CustomCmdTaskGroup, TaskGroup};
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::SubCommand;

#[async_trait::async_trait]
impl TaskGroup for CustomCmdTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        match config {
            Config::Cluster(..) => {}
            _ => {
                return Err(anyhow::anyhow!(
                    "Expected ClusterConfig for CustomCmdTaskGroup"
                ))
            }
        };

        let cmd_ref = cmd_arg.as_ref().to_string();
        let user_command = match cmd_arg.clone() {
            SubCommand::Exec {
                command,
                topology_file: _,
            } => command,
            _ => {
                unreachable!()
            }
        };
        let exec_cmd_task_execution =
            ExecCustomCommand::from_config(&cmd_arg, "exec", user_command, &config);

        Ok(TaskExecutionContext {
            task_group: cmd_ref,
            barrier: None,
            executable: exec_cmd_task_execution,
        })
    }
}
