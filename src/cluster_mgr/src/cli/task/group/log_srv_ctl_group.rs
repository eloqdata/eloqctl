use crate::cli::task::group::{Config, LogServiceCtlTaskGroup, TaskGroup};
use crate::cli::task::monograph_log_ctl_task::MonographLogCtlTask;
use crate::cli::task::monograph_log_probe_task::MonographLogProbeTask;
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::SubCommand;
use indexmap::IndexMap;

#[async_trait::async_trait]
impl TaskGroup for LogServiceCtlTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => {
                return Err(anyhow::anyhow!(
                    "Expected ClusterConfig for LogServiceCtlTaskGroup"
                ))
            }
        };

        let log_ctl_cmd_name = match cmd_arg.clone() {
            SubCommand::LogService {
                cluster: _,
                command: log_ctl_cmd,
            } => log_ctl_cmd,
            _ => unreachable!(),
        };
        let is_start_cmd = log_ctl_cmd_name.to_lowercase().eq("start")
            || log_ctl_cmd_name.to_lowercase().eq("status");
        let mut log_ctl_executable = IndexMap::new();
        let mut barrier = vec![];
        log_ctl_executable.extend(MonographLogCtlTask::from_config(cmd_arg, &cluster_config));
        barrier.push(log_ctl_executable.len());
        if is_start_cmd {
            let probe_task = MonographLogProbeTask::from_config(&cluster_config);
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
