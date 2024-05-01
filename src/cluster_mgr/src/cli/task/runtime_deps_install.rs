use crate::cli::ssh::SSHSession;
use crate::cli::task::task_base::{
    ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::config::config_base::DeploymentConfig;
use async_trait::async_trait;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::vec;
use users::get_current_uid;

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
        let os_and_deps_pair = DeploymentConfig::load_runtime_deps_by_os(None, None)?;
        let os_name = os_and_deps_pair.0;
        let os_version = os_and_deps_pair.1;
        println!("RuntimeDep from_config = {os_name}");
        let  dep_cmd_partial = match os_name.as_str() {
            "ubuntu" => {
               vec![
                "apt-get update", 
                "DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends"]
            }
            "centos" => {
                match os_version.as_str() {
                    "8" => 
                    vec![
                        "dnf install https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm -y", 
                        "/usr/bin/crb enable", 
                        "yum install -y epel-release", 
                        "yum update -y", 
                        "yum install -y"],
                    "7"=>  vec![
                        "yum install -y epel-release", 
                        "yum update -y", 
                        "yum install -y"],
                    _ => unreachable!()
                }
            }
            _=> {
                panic!("For now MonographDB only run on Ubuntu or Centos7/Centos8");
            }
        };
        let dep_cmd_partial = if get_current_uid() == 0 {
            dep_cmd_partial.join(" && ")
        } else {
            dep_cmd_partial
                .iter()
                .map(|e| format!("sudo {}", e))
                .collect::<Vec<String>>()
                .join(" && ")
        };
        let dep_pkg = os_and_deps_pair.2;
        let install_dep_cmd = format!("{dep_cmd_partial} {dep_pkg}");

        let conn_user = config.connection.clone().username;
        let ssh_port = config.connection.ssh_port();
        let host_values = config.get_unique_host_list();
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
        let ssh_session = SSHSession::from_task_host(
            task_host.clone(),
            self.config.connection.ssh_auth_key().unwrap(),
        )
        .await?;
        let (code, out) = ssh_session.execute(&self.install_dep_cmd).await?;
        ssh_session.close().await?;
        if code != 0 {
            let host = match task_host {
                TaskHost::Local => "127.0.0.1".to_owned(),
                TaskHost::Remote {
                    user: _,
                    port: _,
                    hosts,
                } => hosts,
            };
            anyhow::bail!(
                "install dependency failed on {host}, code={code}:\n{}\n{out}",
                self.install_dep_cmd
            )
        }
        Ok(None)
    }
}
