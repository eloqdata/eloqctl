use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::group::Config;
use crate::cli::task::task_base::CmdErr;
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId};
use crate::cli::task::upload::upload_task_builder::{SCP_COMMAND, SOURCE_IP};
use crate::task_return_value;
use std::collections::HashMap;
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

        let source_ip_opt = task_arg.get(SOURCE_IP).unwrap();
        let source_host = source_ip_opt.clone().into_inner_value::<String>();

        let source_task_host = TaskHost::Remote {
            user: user.to_string(),
            port,
            host: source_host,
        };
        let ssh_session =
            SSHSession::from_task_host(source_task_host, auth_key.to_string()).await?;
        let upload_task_result = ssh_session.command(scp.as_str(), CollectOutput).await?;
        ssh_session.close().await?;
        task_return_value!(
            upload_task_result,
            |status_code: i32| -> CmdErr { CmdErr::UploadErr(scp, status_code.to_string()) },
            "UploadTask"
        )
    }
}
