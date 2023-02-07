use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::config::config_base::DeploymentConfig;
use crate::task_return_value;
use async_trait::async_trait;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct RuntimeDepsInstallation {
    install_dep_cmd: String,
    task_id: TaskId,
    config: DeploymentConfig,
}

impl RuntimeDepsInstallation {
    pub fn from_config(
        config: &DeploymentConfig,
    ) -> anyhow::Result<IndexMap<TaskId, TaskInstance>> {
        let all_host_map = config.get_host_as_map();

        let os_and_deps_pair = DeploymentConfig::load_runtime_deps_by_os(None)?;
        let os_name = os_and_deps_pair.0;
        let dep_cmd_partial = match os_name.as_str() {
            "ubuntu" => {
               "sudo apt-get update && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends"
            }
            "centos" => {
                "sudo yum install -y epel-release && sudo yum update -y && sudo yum install -y"
            }
            _=> {
                panic!("For now MonographDB only run on Ubuntu or Centos");
            }
        };
        let dep_pkg = os_and_deps_pair.1;
        let install_dep_cmd = format!("{dep_cmd_partial} {dep_pkg}");

        let conn_user = config.connection.clone().username;
        let ssh_port = config.connection.ssh_port();
        let host_values = all_host_map
            .values()
            .into_iter()
            .flat_map(|entry| entry.iter().cloned().collect_vec())
            .collect_vec();

        let install_dep_task = host_values
            .iter()
            .map(|host_name| {
                let task_id = TaskId {
                    cmd: "run_deps".to_string(),
                    task: format!("{os_name}_install_deps"),
                    host: host_name.clone(),
                };
                (
                    task_id.clone(),
                    TaskInstance {
                        task_input: HashMap::new(),
                        task: Box::new(RuntimeDepsInstallation::new(
                            install_dep_cmd.clone(),
                            task_id,
                            config.clone(),
                        )),
                        task_host: TaskHost::Remote {
                            user: conn_user.clone(),
                            port: ssh_port as usize,
                            hosts: host_name.to_string(),
                        },
                    },
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>();

        Ok(install_dep_task)
    }

    pub fn new(install_dep_cmd: String, task_id: TaskId, config: DeploymentConfig) -> Self {
        Self {
            install_dep_cmd,
            task_id,
            config,
        }
    }
}

#[async_trait]
impl TaskExecutor for RuntimeDepsInstallation {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        println!("{} execute.\n", self.task_id.pretty_string());
        let ssh_session =
            SSHSession::from_task_host(task_host, self.config.connection.ssh_auth_key().unwrap())
                .await?;
        let install_dep_cmd_rs = ssh_session
            .command(self.install_dep_cmd.clone().as_str(), CollectOutput)
            .await?;

        ssh_session.close().await?;
        task_return_value!(
            install_dep_cmd_rs,
            |status_code: usize| -> CmdErr {
                CmdErr::ExecUserCmdErr(self.install_dep_cmd.clone(), status_code.to_string())
            },
            "RuntimeDepsInstallation"
        )
    }
}
