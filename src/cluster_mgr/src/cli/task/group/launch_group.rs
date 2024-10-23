use crate::cli::task::group::{
    CheckTaskGroup, CtrlDBTaskGroup, DeploymentTaskGroup, InstallDBTaskGroup,
    InstallDepPkgTaskGroup, LaunchTaskGroup, MonitorCtlTaskGroup, TaskGroup,
};
use crate::cli::task::task_base::{merge_execution, TaskExecutionContext};
use crate::cli::SubCommand;
use crate::config::config_base::DeployConfig;
use crate::config::CONFIG_PATH_DIR;
use std::env;

#[async_trait::async_trait]
impl TaskGroup for LaunchTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: DeployConfig,
    ) -> anyhow::Result<TaskExecutionContext> {
        let (skip_deps, topo_file) = match cmd_arg.clone() {
            SubCommand::Launch {
                topology_file,
                skip_deps,
            } => (skip_deps, topology_file),
            SubCommand::Demo {
                product, skip_deps, ..
            } => {
                let topo = format!("{}/demo-{product}.yaml", env::var(CONFIG_PATH_DIR)?);
                (skip_deps, topo)
            }
            _ => {
                unreachable!()
            }
        };
        let dep_tasks = if skip_deps {
            TaskExecutionContext::dummy()
        } else {
            let cmd = SubCommand::RunDeps {
                topology_file: topo_file.clone(),
            };
            InstallDepPkgTaskGroup.tasks(cmd, config.clone()).await?
        };

        let exe_ctx = vec![
            dep_tasks,
            CheckTaskGroup
                .tasks(
                    SubCommand::Check {
                        topology_file: topo_file.clone(),
                    },
                    config.clone(),
                )
                .await?,
            DeploymentTaskGroup
                .tasks(
                    SubCommand::Deploy {
                        topology_file: topo_file.clone(),
                    },
                    config.clone(),
                )
                .await?,
            InstallDBTaskGroup
                .tasks(
                    SubCommand::Install {
                        cluster: config.deployment.cluster_name.clone(),
                    },
                    config.clone(),
                )
                .await?,
            CtrlDBTaskGroup
                .tasks(
                    SubCommand::Start {
                        cluster: config.deployment.cluster_name.clone(),
                        nodes: Vec::new(),
                    },
                    config.clone(),
                )
                .await?,
            MonitorCtlTaskGroup
                .tasks(
                    SubCommand::Monitor {
                        cluster: config.deployment.cluster_name.clone(),
                        command: "start".to_string(),
                    },
                    config.clone(),
                )
                .await?,
        ];
        let (barrier, executable) = merge_execution(exe_ctx);

        Ok(TaskExecutionContext {
            task_group: cmd_arg.as_ref().to_string(),
            barrier: Some(barrier),
            executable,
        })
    }
}
