use crate::cli::task::download_task::DownloadTask;
use crate::cli::task::group::{TaskGroup, UpdateClusterTaskGroup};
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
impl TaskGroup for UpdateClusterTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: CommandArgs,
        config: DeploymentConfig,
    ) -> anyhow::Result<TaskExecutionContext> {
        let (update_eloq, update_cass) = match &cmd_arg {
            CommandArgs::Update {
                version, cassandra, ..
            } => (version.is_some(), cassandra.is_some()),
            _ => unreachable!(),
        };
        let deployment_ref = &config.deployment;
        let cluster = deployment_ref.cluster_name.clone();

        let download_task = DownloadTask::from_config(&config)?;
        let mut upload_img = IndexMap::new();
        let mut unpack_tasks = IndexMap::new();
        let mut upload_cnf = IndexMap::new();
        if update_eloq {
            upload_img.extend(upload_tasks(UploadTaskBuilderType::EloqImage, &config));
            unpack_tasks.extend(UnpackFileTask::unpack_eloq_servers_image(&config));
            upload_cnf.extend(upload_tasks(UploadTaskBuilderType::TxConf, &config));
        }
        if update_cass {
            upload_img.extend(upload_tasks(UploadTaskBuilderType::CassImage, &config));
            unpack_tasks.extend(UnpackFileTask::unpack_cassandra_image(&config));
            upload_cnf.extend(upload_tasks(UploadTaskBuilderType::CassConf, &config));
        }

        // stop tx-service and log-service
        let stop_cmd = CommandArgs::Stop {
            cluster: cluster.clone(),
            force: false,
            all: update_cass,
        };
        let mut stop_tasks = MonographTxCtlTask::from_config(stop_cmd.clone(), &config);
        if deployment_ref.log_service.is_some() {
            stop_tasks.extend(MonographLogCtlTask::from_config(stop_cmd, &config));
        }

        // start log-service and tx-service
        let start_cmd = CommandArgs::Start { cluster };
        let mut start_tasks = IndexMap::new();
        if deployment_ref.log_service.is_some() {
            start_tasks.extend(MonographLogCtlTask::from_config(start_cmd.clone(), &config));
            start_tasks.extend(MonographLogProbeTask::from_config(&config));
        }
        start_tasks.extend(MonographTxCtlTask::from_config(start_cmd, &config));

        let barrier = vec![
            download_task.len(),
            upload_img.len(),
            stop_tasks.len(),
            unpack_tasks.len(),
            upload_cnf.len(),
            start_tasks.len(),
        ];
        let mut executable = IndexMap::new();
        executable.extend(download_task);
        executable.extend(upload_img);
        executable.extend(stop_tasks);
        executable.extend(unpack_tasks);
        executable.extend(upload_cnf);
        executable.extend(start_tasks);
        Ok(TaskExecutionContext {
            task_group: cmd_arg.as_ref().to_string(),
            barrier: Some(barrier),
            executable,
        })
    }
}
