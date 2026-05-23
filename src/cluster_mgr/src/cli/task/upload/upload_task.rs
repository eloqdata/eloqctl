use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
};
use crate::cli::task::upload::upload_task_builder::{TRANSFER_ARGS, TRANSFER_COMMAND};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::task_return_value;
use anyhow::anyhow;
use std::collections::HashMap;
use tokio::process::Command;
use tokio::time::{timeout, Duration};
use tracing::info;

#[derive(Debug, Clone)]
pub struct UploadTask {
    task_id: TaskId,
}

impl UploadTask {
    pub fn new(task_id: TaskId) -> Self {
        Self { task_id }
    }
}

#[async_trait::async_trait]
impl TaskExecutor for UploadTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        _task_host: TaskHost,
        task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        info!("execute {}", self.task_id.format_string());
        let sync = task_arg
            .get(TRANSFER_COMMAND)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "rsync".to_string());
        let sync_args = task_arg
            .get(TRANSFER_ARGS)
            .cloned()
            .map(TaskArgValue::into_inner_value::<Vec<String>>)
            .ok_or_else(|| {
                anyhow!(CmdErr::UploadErr(
                    sync.clone(),
                    "missing sync args".to_string()
                ))
            })?;

        info!("Running local sync: {}", sync);
        let output = timeout(
            Duration::from_secs(120),
            Command::new(&sync).args(&sync_args).output(),
        )
        .await
        .map_err(|_| anyhow!(CmdErr::UploadErr(sync.clone(), "timed out".to_string())))?
        .map_err(|e| anyhow!(CmdErr::UploadErr(sync.clone(), e.to_string())))?;
        let code = output.status.code().unwrap_or(-1);
        let command_output = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let error_detail = if command_output.trim().is_empty() {
            code.to_string()
        } else {
            format!("{}: {}", code, command_output.trim())
        };
        let mut result = std::collections::HashMap::new();
        result.insert(CMD.to_string(), TaskArgValue::Str(sync.clone()));
        result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(code));
        result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(command_output));
        task_return_value!(
            result,
            |status_code: i32| -> CmdErr {
                let detail = if error_detail.starts_with(&status_code.to_string()) {
                    error_detail.clone()
                } else {
                    status_code.to_string()
                };
                CmdErr::UploadErr(sync.clone(), detail)
            },
            "UploadTask"
        )
    }
}
