use crate::cli::config::DeploymentConfig;
use crate::cli::task::ssh_conn::SSH_EXEC_CMD_OUTPUT;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::{ssh_conn_info, task_return_value};
use async_trait::async_trait;
use itertools::Itertools;
use std::collections::HashMap;
use tracing::info;

#[derive(Clone, Debug)]
pub struct ExecCustomCommand {
    cmd: String,
    task_id: TaskId,
    config: DeploymentConfig,
}

impl ExecCustomCommand {
    pub fn from_config(cmd_string: String, config: &DeploymentConfig) -> Vec<TaskInstance> {
        let all_hosts = config.get_host_as_map();
        let conn_user = &config.connection.username;
        let ssh_port = config.connection.ssh_port();
        all_hosts
            .values()
            .flat_map(|hosts| {
                hosts
                    .iter()
                    .map(|host_val| {
                        let task_host = TaskHost::Remote {
                            user: conn_user.clone(),
                            port: ssh_port as usize,
                            hosts: host_val.clone(),
                        };
                        TaskInstance {
                            task_input: HashMap::default(),
                            task: Box::new(ExecCustomCommand::new(
                                cmd_string.clone(),
                                TaskId {
                                    cmd: "exec_cmd".to_string(),
                                    task: "".to_string(),
                                },
                                config.clone(),
                            )),
                            task_host,
                        }
                    })
                    .collect_vec()
            })
            .collect_vec()
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
        ssh_conn_info! {
            self.config.connection.clone(),
            task_host,
            ssh_conn_rs,
            _conn_user,
            conn_host
        }

        let ssh_conn = ssh_conn_rs?;
        let exec_cmd_rs = ssh_conn.run_cmd_sync_output(self.cmd.clone())?;

        if let Some(output) = exec_cmd_rs.get(SSH_EXEC_CMD_OUTPUT) {
            println!(
                r#"Host {} Cmd {} output
              {}"#,
                conn_host,
                self.cmd,
                TaskArgValue::into_inner_value::<String>(output.clone())
            );
        }
        task_return_value!(
            exec_cmd_rs,
            |status_code: usize| -> CmdErr {
                CmdErr::ExecUserCmdErr(self.cmd.clone(), status_code.to_string())
            },
            "UserCustomCommand"
        )
    }
}
