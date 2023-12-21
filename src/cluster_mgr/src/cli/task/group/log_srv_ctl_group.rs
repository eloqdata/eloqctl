use crate::cli::task::group::{LogServiceCtlTaskGroup, TaskGroup};
use crate::cli::task::monograph_log_ctl_task::MonographLogCtlTask;
use crate::cli::task::monograph_log_probe_task::MonographLogProbeTask;
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::CommandArgs;
use crate::config::config_base::DeploymentConfig;
use indexmap::IndexMap;

#[async_trait::async_trait]
impl TaskGroup for LogServiceCtlTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: CommandArgs,
        config: DeploymentConfig,
    ) -> anyhow::Result<TaskExecutionContext> {
        let log_ctl_cmd_name = match cmd_arg.clone() {
            CommandArgs::LogService {
                cluster: _,
                command: log_ctl_cmd,
            } => log_ctl_cmd,
            _ => unreachable!(),
        };
        let is_start_cmd = log_ctl_cmd_name.to_lowercase().eq("start")
            || log_ctl_cmd_name.to_lowercase().eq("status");
        let mut log_ctl_executable = IndexMap::new();
        let mut barrier = vec![];
        log_ctl_executable.extend(MonographLogCtlTask::from_config(cmd_arg, &config));
        barrier.push(log_ctl_executable.len());
        if is_start_cmd {
            let probe_task = MonographLogProbeTask::from_config(&config);
            barrier.push(probe_task.len());
            log_ctl_executable.extend(probe_task);
        }
        let barrier = if is_start_cmd { Some(barrier) } else { None };
        Ok(TaskExecutionContext {
            task_group: format!("log-srv-{log_ctl_cmd_name}"),
            barrier,
            executable: log_ctl_executable,
        })
    }
}
