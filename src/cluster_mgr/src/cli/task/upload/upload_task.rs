use crate::cli::task::group::Config;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
};
use crate::cli::task::upload::upload_task_builder::SCP_COMMAND;
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::task_return_value;
use anyhow::anyhow;
use std::collections::HashMap;
use std::process::Command;
use tracing::info;

#[derive(Debug, Clone)]
pub struct UploadTask {
    config: Config,
    task_id: TaskId,
}

impl UploadTask {
    pub fn new(config: &Config, task_id: TaskId) -> Self {
        Self {
            config: config.clone(),
            task_id,
        }
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
        let user = &self.config.conn_user();
        let port = self.config.ssh_port() as usize;
        let auth_key = &self.config.conn_ssh_auth_key();

        info!("execute {}", self.task_id.format_string());
        let scp_command_opt = task_arg.get(SCP_COMMAND);
        assert!(scp_command_opt.is_some());
        let scp_command_value = scp_command_opt.unwrap();

        let scp = scp_command_value.clone().into_inner_value::<String>();

        info!("Running local scp: {}", scp);
        let status = Command::new("sh")
            .arg("-c")
            .arg(&scp)
            .status()
            .map_err(|e| anyhow!(CmdErr::UploadErr(scp.clone(), e.to_string())))?;
        let code = status.code().unwrap_or(-1);
        let mut result = std::collections::HashMap::new();
        result.insert(CMD.to_string(), TaskArgValue::Str(scp.clone()));
        result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(code));
        result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(String::new()));
        task_return_value!(
            result,
            |status_code: i32| -> CmdErr {
                CmdErr::UploadErr(scp.clone(), status_code.to_string())
            },
            "UploadTask"
        )
    }
}
