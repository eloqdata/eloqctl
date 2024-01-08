use crate::cli::task::group::{
    CtrlDBTaskGroup, DeploymentTaskGroup, InstallDBTaskGroup, InstallRuntimeDepsTaskGroup,
    MonitorCtlTaskGroup, PlayTaskGroup, TaskGroup,
};
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::CommandArgs;
use crate::config::config_base::DeploymentConfig;
use indexmap::IndexMap;
use tracing::info;

#[async_trait::async_trait]
impl TaskGroup for PlayTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: CommandArgs,
        config: DeploymentConfig,
    ) -> anyhow::Result<TaskExecutionContext> {
        let cmd_ref = cmd_arg.as_ref().to_string();
        let topo_file = match cmd_arg {
            CommandArgs::Play { topology_file } => topology_file,
            _ => {
                unreachable!()
            }
        };

        // let topo_file = match cmd_arg {
        //     CommandArgs::Play { topology_file } => match topology_file {
        //         Some(fp) => fp,
        //         None => "".to_string(),
        //     },
        //     _ => {
        //         unreachable!()
        //     }
        // };

        let groups = vec![
            InstallRuntimeDepsTaskGroup
                .tasks(
                    CommandArgs::RunDeps {
                        topology_file: topo_file.clone(),
                    },
                    config.clone(),
                )
                .await?,
            DeploymentTaskGroup
                .tasks(
                    CommandArgs::Deploy {
                        topology_file: topo_file.clone(),
                    },
                    config.clone(),
                )
                .await?,
            InstallDBTaskGroup
                .tasks(
                    CommandArgs::Install {
                        cluster: config.deployment.cluster_name.clone(),
                    },
                    config.clone(),
                )
                .await?,
            CtrlDBTaskGroup
                .tasks(
                    CommandArgs::Start {
                        cluster: config.deployment.cluster_name.clone(),
                    },
                    config.clone(),
                )
                .await?,
            MonitorCtlTaskGroup
                .tasks(
                    CommandArgs::Monitor {
                        cluster: config.deployment.cluster_name.clone(),
                        command: "start".to_string(),
                    },
                    config.clone(),
                )
                .await?,
        ];

        let mut barrier = vec![];
        let mut executable = IndexMap::new();
        for tasks in groups {
            info!(
                "Play step {} has barrier {:?} and tasks {}",
                tasks.task_group,
                tasks.barrier,
                tasks.executable.len()
            );
            if let Some(b) = tasks.barrier {
                barrier.extend(b);
            } else {
                barrier.push(tasks.executable.len());
            }
            executable.extend(tasks.executable);
        }
        Ok(TaskExecutionContext {
            task_group: cmd_ref,
            barrier: Some(barrier),
            executable,
        })
    }
}
