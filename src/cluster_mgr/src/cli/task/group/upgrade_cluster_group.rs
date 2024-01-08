use crate::cli::task::group::{TaskGroup, UpgradeClusterTaskGroup};
use crate::cli::task::monograph_log_ctl_task::MonographLogCtlTask;
use crate::cli::task::monograph_log_probe_task::MonographLogProbeTask;
use crate::cli::task::monograph_tx_ctl_task::MonographTxCtlTask;
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::task::unpack_file_task::UnpackFileTask;
use crate::cli::task::upload::upload_task_builder::{upload_tasks, UploadTaskBuilderType};
use crate::cli::CommandArgs;
use crate::config::config_base::DeploymentConfig;
use indexmap::IndexMap;

#[async_trait::async_trait]
impl TaskGroup for UpgradeClusterTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: CommandArgs,
        config: DeploymentConfig,
    ) -> anyhow::Result<TaskExecutionContext> {
        let deployment_ref = &config.deployment;
        let cluster = deployment_ref.clone().cluster_name;
        let stop_cmd = CommandArgs::Stop {
            cluster: cluster.clone(),
            force: None,
            all: None,
        };
        let mut stop_monograph = MonographTxCtlTask::from_config(stop_cmd.clone(), &config);
        let has_log_srv = deployment_ref.log_service.is_some();
        if has_log_srv {
            stop_monograph.extend(MonographLogCtlTask::from_config(stop_cmd, &config));
        }
        let mut upload_monograph_tasks = IndexMap::new();
        upload_monograph_tasks.extend(upload_tasks(UploadTaskBuilderType::MonographAll, &config));
        upload_monograph_tasks.extend(upload_tasks(UploadTaskBuilderType::MonitorConf, &config));

        let unpack_tasks = UnpackFileTask::from_config(&config)?;

        let start_cmd = CommandArgs::Start { cluster };
        let mut start_all_tasks = IndexMap::new();
        if has_log_srv {
            start_all_tasks.extend(MonographLogCtlTask::from_config(start_cmd.clone(), &config));
            start_all_tasks.extend(MonographLogProbeTask::from_config(&config));
        }
        start_all_tasks.extend(MonographTxCtlTask::from_config(start_cmd, &config));

        let barrier = vec![
            stop_monograph.len(),
            upload_monograph_tasks.len(),
            unpack_tasks.len(),
            start_all_tasks.len(),
        ];
        let mut executable = IndexMap::new();

        executable.extend(stop_monograph);
        executable.extend(upload_monograph_tasks);
        executable.extend(unpack_tasks);
        executable.extend(start_all_tasks);

        let cmd_ref = cmd_arg.as_ref();
        Ok(TaskExecutionContext {
            task_group: cmd_ref.to_string(),
            barrier: Some(barrier),
            executable,
        })
    }
}
