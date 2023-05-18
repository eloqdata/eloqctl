use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::task_base::CmdErr;
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId};
use crate::config::config_base::DeploymentConfig;
use crate::task_return_value;
use std::collections::HashMap;
use crate::cli::task::upload::upload_task_builder::SCP_COMMAND;

#[derive(Debug, Clone)]
pub struct UploadTask {
    config: DeploymentConfig,
    task_id: TaskId,
}

impl UploadTask {
    pub fn new(config: DeploymentConfig, task_id: TaskId) -> Self {
        Self { config, task_id }
    }
}

#[async_trait::async_trait]
impl TaskExecutor for UploadTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        println!("{} execute.\n", self.task_id.pretty_string());
        let scp_command_opt = task_arg.get(SCP_COMMAND);
        assert!(scp_command_opt.is_some());
        let scp_command_value = scp_command_opt.unwrap();

        let scp = scp_command_value.clone().into_inner_value::<String>();
        let remote_install_dir = self.config.install_dir();
        let mkdir = format!("mkdir -p {remote_install_dir}");

        let remote_cmd = format!("{mkdir};{scp}");
        println!("UploadTask remote command {remote_cmd}\n");
        let ssh_session =
            SSHSession::from_task_host(task_host, self.config.connection.ssh_auth_key().unwrap())
                .await?;
        let upload_task_result = ssh_session
            .command(remote_cmd.as_str(), CollectOutput)
            .await?;
        ssh_session.close().await?;
        task_return_value!(
            upload_task_result,
            |status_code: usize| -> CmdErr { CmdErr::UploadErr(scp, status_code.to_string()) },
            "UploadTask"
        )
    }
}
