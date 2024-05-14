use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::{ssh, CommandArgs, CMD_OUTPUT};
use crate::config::config_base::DeploymentConfig;
use crate::task_return_value;
use async_trait::async_trait;
use indexmap::IndexMap;
use std::collections::HashMap;
use tracing::{debug, info};

#[derive(Clone, Debug)]
pub struct ExecCustomCommand {
    cmd: String,
    task_id: TaskId,
    config: DeploymentConfig,
}

impl ExecCustomCommand {
    pub fn build_task_by_host(
        cmd_string: String,
        config: &DeploymentConfig,
        hosts: Vec<String>,
        task_name: Option<String>,
    ) -> IndexMap<TaskId, TaskInstance> {
        let conn_user = &config.connection.username;
        let ssh_port = config.connection.ssh_port();
        hosts
            .iter()
            .map(|host| {
                let task_host = TaskHost::Remote {
                    user: conn_user.clone(),
                    port: ssh_port as usize,
                    hosts: host.clone(),
                };
                let task = if let Some(input_task_name) = &task_name {
                    input_task_name.to_string()
                } else {
                    format!("exec_cmd_in_{host}")
                };
                let task_id = TaskId {
                    cmd: "exec_cmd_by_hosts".to_string(),
                    task,
                    host: host.to_string(),
                };

                (
                    task_id.clone(),
                    TaskInstance {
                        task_input: HashMap::default(),
                        task: Box::new(ExecCustomCommand::new(
                            cmd_string.clone(),
                            task_id,
                            config.clone(),
                        )),
                        task_host,
                    },
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>()
    }

    pub fn from_config(
        cmd: &CommandArgs,
        task: &str,
        content: String,
        config: &DeploymentConfig,
    ) -> IndexMap<TaskId, TaskInstance> {
        let all_hosts = config.get_unique_host_list();
        let conn_user = &config.connection.username;
        let ssh_port = config.connection.ssh_port();
        all_hosts
            .iter()
            .map(|host_val| {
                let task_host = TaskHost::Remote {
                    user: conn_user.clone(),
                    port: ssh_port as usize,
                    hosts: host_val.clone(),
                };
                let task_id = TaskId {
                    cmd: cmd.as_ref().to_string(),
                    task: format!("{task}@{host_val}"),
                    host: host_val.clone(),
                };
                (
                    task_id.clone(),
                    TaskInstance {
                        task_input: HashMap::default(),
                        task: Box::new(ExecCustomCommand::new(
                            content.clone(),
                            task_id,
                            config.clone(),
                        )),
                        task_host,
                    },
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>()
    }

    pub fn new(cmd: String, task_id: TaskId, config: DeploymentConfig) -> Self {
        Self {
            cmd,
            task_id,
            config,
        }
    }
}

#[async_trait]
impl TaskExecutor for ExecCustomCommand {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        debug!("execute {}", self.task_id.pretty_string());
        let ssh_session = ssh::SSHSession::from_task_host(
            task_host,
            self.config.connection.ssh_auth_key().unwrap(),
        )
        .await?;
        let (conn_host, _) = ssh_session.ssh_conn_info();
        let exec_cmd_rs = ssh_session
            .command(self.cmd.clone().as_str(), CollectOutput)
            .await?;

        if let Some(output) = exec_cmd_rs.get(CMD_OUTPUT) {
            println!(
                r#"Host {} Cmd {} output
              {}"#,
                conn_host,
                self.cmd,
                TaskArgValue::into_inner_value::<String>(output.clone())
            );
        }
        ssh_session.close().await?;
        task_return_value!(
            exec_cmd_rs,
            |status_code: i32| -> CmdErr {
                CmdErr::ExecUserCmdErr(self.cmd.clone(), status_code.to_string())
            },
            "ExecCustomCommand"
        )
    }
}
