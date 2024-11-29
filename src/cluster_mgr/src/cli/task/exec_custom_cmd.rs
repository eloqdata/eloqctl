use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::task::group::Config;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::{ssh, SubCommand, CMD_OUTPUT};
use crate::task_return_value;
use async_trait::async_trait;
use indexmap::IndexMap;
use std::collections::HashMap;
use tracing::{debug, info};

#[derive(Clone, Debug)]
pub struct ExecCustomCommand {
    cmd: String,
    task_id: TaskId,
    config: Config,
}

impl ExecCustomCommand {
    pub fn build_task_by_host(
        cmd_string: String,
        config: &Config,
        hosts: Vec<String>,
        task_name: Option<String>,
    ) -> IndexMap<TaskId, TaskInstance> {
        let conn_user = config.conn_user();
        let ssh_port = config.ssh_port();

        hosts
            .iter()
            .map(|host| {
                let task_host = TaskHost::Remote {
                    user: conn_user.to_string(),
                    port: ssh_port as usize,
                    host: host.clone(),
                };
                let task = task_name
                    .clone()
                    .unwrap_or_else(|| format!("exec_cmd_in_{host}"));
                let task_id = TaskId {
                    cmd: "exec_cmd_by_hosts".to_string(),
                    task,
                    host: host.clone(),
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
        cmd: &SubCommand,
        task: &str,
        content: String,
        config: &Config,
    ) -> IndexMap<TaskId, TaskInstance> {
        let conn_user = config.conn_user();
        let ssh_port = config.ssh_port();
        let all_hosts = config.get_unique_host_list();

        all_hosts
            .iter()
            .map(|host_val| {
                let task_host = TaskHost::Remote {
                    user: conn_user.to_string(),
                    port: ssh_port as usize,
                    host: host_val.clone(),
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

    pub fn from_path(
        cmd: &SubCommand,
        task: String,
        content: String,
        config: &Config,
        dest_host: &Option<String>,
        dest_user: &Option<String>,
    ) -> (TaskId, TaskInstance) {
        let conn_user = &config.conn_user();
        let (user, host) =
            if let (Some(dest_user), Some(dest_host)) = (dest_user.clone(), dest_host.clone()) {
                (dest_user, dest_host)
            } else {
                (conn_user.to_string(), "localhost".to_string())
            };

        let ssh_port = config.ssh_port() as usize;
        let task_host = TaskHost::Remote {
            user: user,
            port: ssh_port,
            host: host,
        };

        let task_id = TaskId {
            cmd: cmd.as_ref().to_string(),
            task: task,
            host: "_local".to_string(),
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
    }

    pub fn new(cmd: String, task_id: TaskId, config: Config) -> Self {
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
        info!("execute {}", self.task_id.format_string());

        let auth_key = self.config.conn_ssh_auth_key();
        let ssh_session = ssh::SSHSession::from_task_host(task_host, auth_key).await?;
        let (host, _) = ssh_session.ssh_conn_info();
        let exec_cmd_rs = ssh_session
            .command(self.cmd.clone().as_str(), CollectOutput)
            .await?;

        if let Some(output) = exec_cmd_rs.get(CMD_OUTPUT) {
            debug!(
                "Host {host} Cmd {} output {}",
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
