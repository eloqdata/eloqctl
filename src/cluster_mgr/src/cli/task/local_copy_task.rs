use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::{download_dir, CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeploymentConfig;
use anyhow::anyhow;
use async_trait::async_trait;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use tracing::error;

const SOURCE_DIR: &str = "_source_dir";
const DEST_DIR: &str = "_dest_dir";

#[derive(Clone, Debug)]
pub struct LocalCopyTask {
    task_id: TaskId,
}

macro_rules! build_copy_task_instances {
    ($task_instance:expr,  $source_url:expr, $dest_url:expr, $task_name:expr) => {
        let task_id = TaskId {
            cmd: "deploy".to_string(),
            task: $task_name,
            host: "_local".to_string(),
        };

        $task_instance.insert(
            task_id.clone(),
            TaskInstance {
                task_input: HashMap::from([
                    (SOURCE_DIR.to_string(), TaskArgValue::Str($source_url)),
                    (DEST_DIR.to_string(), TaskArgValue::Str($dest_url)),
                ]),
                task: Box::new(LocalCopyTask::new(task_id)),
                task_host: TaskHost::Local,
            },
        )
    };
}

impl LocalCopyTask {
    pub fn form_config(
        config: &DeploymentConfig,
    ) -> anyhow::Result<IndexMap<TaskId, TaskInstance>> {
        let mut local_copy_task_instance = IndexMap::new();
        let download_links = config.deployment.all_download_links()?;
        download_links
            .iter()
            .filter(|(_, download_url)| download_url.is_local())
            .for_each(|(key, download_url)| {
                let copy_task_id = format!("copy_{key}");
                build_copy_task_instances!(
                    local_copy_task_instance,
                    download_url.get_url(),
                    download_url.file_name(),
                    copy_task_id
                );
            });
        Ok(local_copy_task_instance)
    }

    pub fn new(task_id: TaskId) -> Self {
        Self { task_id }
    }
}

#[async_trait]
impl TaskExecutor for LocalCopyTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        _task_host: TaskHost,
        task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        let source_dir_op = task_arg.get(SOURCE_DIR);
        assert!(source_dir_op.is_some());
        println!("{} execute.\n", self.task_id.pretty_string());
        let source_dir_string =
            TaskArgValue::into_inner_value::<String>(source_dir_op.unwrap().clone());
        let source_path = PathBuf::from(source_dir_string.as_str());
        if !source_path.exists() {
            error!("Source file path does not exist. {}", source_dir_string);
            Err(anyhow!(CmdErr::CopyTaskErr(source_dir_string)))
        } else {
            let download_dir = download_dir();

            let dest_file =
                TaskArgValue::into_inner_value::<String>(task_arg.get(DEST_DIR).unwrap().clone());
            let dest_file_path = download_dir.join(dest_file).to_str().unwrap().to_string();
            let mut copy_cmd = Command::new("cp");
            let copy_cmd_args = if source_path.is_dir() {
                vec!["-r".to_string(), source_dir_string, dest_file_path]
            } else {
                vec![source_dir_string, dest_file_path]
            };
            copy_cmd.args(copy_cmd_args);

            let cmd_str = copy_cmd.get_program().to_str().unwrap().to_string();
            let mut args_str = String::new();
            for arg_val in copy_cmd.get_args() {
                if let Some(arg) = arg_val.to_str() {
                    args_str.push_str(arg)
                }
            }

            let copy_cmd_str = format!("{cmd_str} {args_str}");
            let status = copy_cmd.status()?;

            let mut copy_task_rs = HashMap::from([
                (CMD.to_string(), TaskArgValue::Str(copy_cmd_str)),
                (CMD_OUTPUT.to_string(), TaskArgValue::Str("".to_string())),
            ]);
            if status.success() {
                copy_task_rs.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
            } else {
                error!("LocalCopyTask failed. command status code={:?}", status);
                if let Some(code) = status.code() {
                    copy_task_rs
                        .insert(CMD_STATUS.to_string(), TaskArgValue::Number(code as usize));
                } else {
                    copy_task_rs.insert(CMD_STATUS.to_string(), TaskArgValue::Number(usize::MAX));
                }
            }
            Ok(Some(copy_task_rs))
        }
    }
}
