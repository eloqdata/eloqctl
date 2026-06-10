use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{Config, CustomCmdTaskGroup, TaskGroup};
use crate::cli::task::task_base::{is_verbose_task_output, TaskExecutionContext};
use crate::cli::SubCommand;

#[async_trait::async_trait]
impl TaskGroup for CustomCmdTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        let Config::Cluster(_cluster_config) = config;

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
        if is_verbose_task_output() {
            println!("user_command: {user_command}");
        }
        let exec_cmd_task_execution =
            ExecCustomCommand::from_config(&cmd_arg, "exec", user_command, config);

        Ok(TaskExecutionContext {
            task_group: cmd_ref,
            barrier: None,
            executable: exec_cmd_task_execution,
        })
    }
}
