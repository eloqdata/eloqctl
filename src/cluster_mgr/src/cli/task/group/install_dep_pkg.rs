use crate::cli::task::group::{Config, InstallDepPkgTaskGroup, TaskGroup};
use crate::cli::task::install_dep_pkg::DepPkgTask;
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::SubCommand;

#[async_trait::async_trait]
impl TaskGroup for InstallDepPkgTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => {
                return Err(anyhow::anyhow!(
                    "Expected ClusterConfig for InstallDepPkgTaskGroup"
                ))
            }
        };

        let install_runtime_deps = DepPkgTask::from_config(&cluster_config)?;
        Ok(TaskExecutionContext {
            task_group: cmd_arg.as_ref().to_string(),
            barrier: None,
            executable: install_runtime_deps,
        })
    }
}
