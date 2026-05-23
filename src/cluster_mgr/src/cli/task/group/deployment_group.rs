use crate::cli::task::download_task::DownloadTask;
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{Config, DeploymentTaskGroup, TaskGroup};
use crate::cli::task::local_copy_task::LocalCopyTask;
use crate::cli::task::local_extract_task::LocalExtractTask;
use crate::cli::task::task_base::{TaskExecutionContext, TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{upload_tasks, UploadTaskBuilderType};
use crate::cli::SubCommand;
use crate::config::config_base::DEPLOYMENT_CHECK_SUCCESS_TASK;
use crate::state::state_mgr::STATE_MGR;
use indexmap::IndexMap;
use itertools::Itertools;

impl DeploymentTaskGroup {
    fn skip_success_task_execution(
        task_instances: &IndexMap<TaskId, TaskInstance>,
        success_task_ids: &[TaskId],
    ) -> IndexMap<TaskId, TaskInstance> {
        if success_task_ids.is_empty() {
            task_instances.clone()
        } else {
            task_instances
                .iter()
                .filter(|(task_id, _)| !success_task_ids.contains(task_id))
                .map(|(task_id, task_instance)| (task_id.clone(), task_instance.clone()))
                .collect::<IndexMap<TaskId, TaskInstance>>()
        }
    }
}

#[async_trait::async_trait]
impl TaskGroup for DeploymentTaskGroup {
    async fn tasks(
        &self,
        cmd_args: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => {
                return Err(anyhow::anyhow!(
                    "Expected ClusterConfig for DeploymentTaskGroup"
                ))
            }
        };

        let cmd_ref = cmd_args.as_ref().to_string();
        let cluster = &cluster_config.deployment.cluster_name;
        let success_task_entity = STATE_MGR
            .load_task_status_from_state(cluster.to_string(), Some(0), Some(vec![cmd_ref.clone()]))
            .await?;

        let success_task_vec = success_task_entity
            .iter()
            .map(|task_status_entity| {
                let task_id_string = &task_status_entity.task;
                TaskId::from_json_string(task_id_string.clone())
            })
            .collect_vec();

        let download_task = DownloadTask::from_config(cluster_config)?;
        let mut copy_or_download_task_instances = LocalCopyTask::form_config(cluster_config)?;
        copy_or_download_task_instances.extend(download_task);

        let need_skip_success_task = if let Some(ref opts) = cluster_config.conf_opts {
            if let Some(check) = opts.get(DEPLOYMENT_CHECK_SUCCESS_TASK) {
                *check
            } else {
                true
            }
        } else {
            true
        };
        let local_extract_task = LocalExtractTask::from_config(cluster_config)?;
        let db_upload_task = if need_skip_success_task {
            DeploymentTaskGroup::skip_success_task_execution(
                &upload_tasks(UploadTaskBuilderType::EloqAll, config),
                &success_task_vec,
            )
        } else {
            upload_tasks(UploadTaskBuilderType::EloqAll, config)
        };

        let upload_tx_conf = upload_tasks(UploadTaskBuilderType::TxConf, config);

        let mut mkdir_targets = vec![cluster_config.install_dir()];
        if cluster_config.deployment.tls_enabled() {
            let tls_dir = cluster_config.deployment.tls_cert_install_dir();
            if !mkdir_targets.contains(&tls_dir) {
                mkdir_targets.push(tls_dir);
            }
        }
        let mkdir_remote_dir = ExecCustomCommand::from_config(
            &cmd_args,
            "mkdir",
            format!("mkdir -p {}", mkdir_targets.join(" ")),
            config,
        );
        let upload_monitor_conf_tasks = upload_tasks(UploadTaskBuilderType::MonitorConf, config);

        let barrier = Some(vec![
            mkdir_remote_dir.len(),
            copy_or_download_task_instances.len(),
            local_extract_task.len(),
            db_upload_task.len(),
            upload_tx_conf.len(),
            upload_monitor_conf_tasks.len(),
        ]);
        let mut executable = IndexMap::new();
        executable.extend(mkdir_remote_dir);
        executable.extend(copy_or_download_task_instances);
        executable.extend(local_extract_task);
        executable.extend(db_upload_task);
        executable.extend(upload_tx_conf);
        executable.extend(upload_monitor_conf_tasks);
        Ok(TaskExecutionContext {
            task_group: cmd_ref,
            barrier,
            executable,
        })
    }
}
