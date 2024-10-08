use crate::cli::task::cassandra_ctl_task::CassandraCtlTask;
use crate::cli::task::download_task::DownloadTask;
use crate::cli::task::group::{TaskGroup, UpdateClusterTaskGroup};
use crate::cli::task::monograph_log_ctl_task::MonographLogCtlTask;
use crate::cli::task::monograph_log_probe_task::MonographLogProbeTask;
use crate::cli::task::monograph_tx_ctl_task::{MonographTxCtlTask, ServerType};
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::task::task_utils::stop_with_hot_standby;
use crate::cli::task::unpack_file_task::UnpackFileTask;
use crate::cli::task::upload::upload_task_builder::{upload_tasks, UploadTaskBuilderType};
use crate::cli::SubCommand;
use crate::config::config_base::DeployConfig;
use indexmap::IndexMap;

#[async_trait::async_trait]
impl TaskGroup for UpdateClusterTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: DeployConfig,
    ) -> anyhow::Result<TaskExecutionContext> {
        let (update_eloq, update_cass) = match &cmd_arg {
            SubCommand::Update {
                version, cassandra, ..
            } => (version.is_some(), cassandra.is_some()),
            _ => unreachable!(),
        };
        if !update_eloq && !update_cass {
            return Ok(TaskExecutionContext::dummy());
        }

        let deployment = &config.deployment;
        let cluster = deployment.cluster_name.clone();

        let mut downloads = vec![];
        let mut upload_img = IndexMap::new();
        let mut unpack_tasks = IndexMap::new();
        if update_eloq {
            downloads.push(config.deployment.tx_image().to_owned());
            if let Some(img) = config.deployment.log_image() {
                downloads.push(img.to_owned());
            }
            upload_img.extend(upload_tasks(UploadTaskBuilderType::EloqImage, &config));
            unpack_tasks.extend(UnpackFileTask::unpack_eloqservers(&config));
        }
        if update_cass {
            downloads.push(deployment.storage_service.inner_cass().unwrap().image_url());
            upload_img.extend(upload_tasks(UploadTaskBuilderType::CassImage, &config));
            unpack_tasks.extend(UnpackFileTask::unpack_cassandra(&config, true));
        }
        let download_task = DownloadTask::instances(DownloadTask::from_urls(downloads));
        let mut barrier = vec![download_task.len(), upload_img.len()];
        let mut executable = IndexMap::new();
        executable.extend(download_task);
        executable.extend(upload_img);

        // stop order: (standby-server -> voter-server ->) tx-server -> log-server -> kv-store
        let stop_cmd = SubCommand::Stop {
            cluster: cluster.clone(),
            tx: Some(true),
            log: true,
            store: update_cass,
            monitor: false,
            force: false,
            all: false,
        };

        if config.deployment.tx_service.standby_host_ports.is_some() {
            stop_with_hot_standby(stop_cmd.clone(), &config, &mut barrier, &mut executable);
        } else {
            let stop_tx =
                MonographTxCtlTask::from_config(stop_cmd.clone(), &config, ServerType::Tx);
            barrier.push(stop_tx.len());
            executable.extend(stop_tx);
        }

        if deployment.log_service.is_some() {
            let stop_log = MonographLogCtlTask::from_config(stop_cmd.clone(), &config);
            barrier.push(stop_log.len());
            executable.extend(stop_log);
        }
        if update_cass {
            let tasks = CassandraCtlTask::from_config(stop_cmd, &config);
            barrier.push(tasks.len());
            executable.extend(tasks);
        }

        barrier.push(unpack_tasks.len());
        executable.extend(unpack_tasks);

        // start order: cassandra -> log-server -> tx-server
        let start_cmd = SubCommand::Start { cluster };
        if deployment.storage_service.inner_cass().is_some() {
            let tasks = CassandraCtlTask::from_config(start_cmd.clone(), &config);
            let ba = CassandraCtlTask::start_barrier(tasks.len());
            barrier.extend(ba);
            executable.extend(tasks);
        }
        if deployment.log_service.is_some() {
            let start_log = MonographLogCtlTask::from_config(start_cmd.clone(), &config);
            barrier.push(start_log.len());
            executable.extend(start_log);
            let probe = MonographLogProbeTask::from_config(&config);
            barrier.push(probe.len());
            executable.extend(probe);
        }
        let start_tx = MonographTxCtlTask::from_config(start_cmd, &config, ServerType::Tx);
        barrier.push(start_tx.len());
        executable.extend(start_tx);
        Ok(TaskExecutionContext {
            task_group: cmd_arg.as_ref().to_string(),
            barrier: Some(barrier),
            executable,
        })
    }
}
