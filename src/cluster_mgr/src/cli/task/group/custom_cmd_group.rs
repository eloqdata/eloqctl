use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{CustomCmdTaskGroup, TaskGroup};
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::CommandArgs;
use crate::config::config_base::DeploymentConfig;

#[async_trait::async_trait]
impl TaskGroup for CustomCmdTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: CommandArgs,
        config: DeploymentConfig,
    ) -> anyhow::Result<TaskExecutionContext> {
        let cmd_ref = cmd_arg.as_ref().to_string();
        let user_command = match cmd_arg {
            CommandArgs::Exec {
                command,
                topology_file: _,
            } => command,
            _ => {
                unreachable!()
            }
        };
        let exec_cmd_task_execution = ExecCustomCommand::from_config(user_command, &config);

        Ok(TaskExecutionContext {
            task_group: cmd_ref,
            barrier: None,
            executable: exec_cmd_task_execution,
        })
    }
}
