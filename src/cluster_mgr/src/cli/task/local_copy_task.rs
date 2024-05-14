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
use tracing::{debug, error};

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
        debug!("execute {}", self.task_id.pretty_string());
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
            let to = download_dir.join(dest_file.clone());
            let mut copy_task_rs = HashMap::from([
                (
                    CMD.to_string(),
                    TaskArgValue::Str(format!("copy {source_dir_string} downloads/{dest_file}")),
                ),
                (CMD_OUTPUT.to_string(), TaskArgValue::Str("".to_string())),
                (CMD_STATUS.to_string(), TaskArgValue::Number(0)),
            ]);

            if to.exists() {
                println!("Success: The target file already exists {to:?}");
                return Ok(Some(copy_task_rs.clone()));
            }
            let from = PathBuf::from(source_dir_string);
            let copy_rs = std::fs::copy(from, to);

            if let Err(copy_err) = copy_rs {
                copy_task_rs.insert(
                    CMD_OUTPUT.to_string(),
                    TaskArgValue::Str(copy_err.to_string()),
                );
                copy_task_rs.insert(CMD_STATUS.to_string(), TaskArgValue::Number(100));
            }
            Ok(Some(copy_task_rs))
        }
    }
}
