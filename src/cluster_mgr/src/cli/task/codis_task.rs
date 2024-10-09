use std::collections::HashMap;

use super::task_base::{
    ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::{
    cli::ssh::{SSHCommandOption, SSHSession},
    config::{config_base::DeployConfig, deployment::Codis, CODIS_DASHBOARD_CNF, CODIS_PROXY_CNF},
};
use indexmap::IndexMap;

pub enum Operation {
    Start,
    Stop,
}

#[derive(Debug, Clone)]
pub struct CodisTask {
    ssh_key: String,
    command: String,
    host: String,
}

impl CodisTask {
    pub fn from_config(config: &DeployConfig, op: Operation) -> IndexMap<TaskId, TaskInstance> {
        let codis_conf = config.deployment.codis.as_ref().unwrap();
        let mut all_tasks = IndexMap::default();
        match op {
            Operation::Start => {
                Self::start_dashboard(config).task_instance(config, &mut all_tasks);
                codis_conf.proxy.iter().for_each(|ip| {
                    Self::start_proxy(config, ip.to_owned()).task_instance(config, &mut all_tasks);
                });
            }
            Operation::Stop => {
                Self::stop_server(config, codis_conf.dashboard.clone(), "dashboard")
                    .task_instance(config, &mut all_tasks);
                codis_conf.proxy.iter().for_each(|ip| {
                    Self::stop_server(config, ip.to_owned(), "proxy")
                        .task_instance(config, &mut all_tasks);
                });
            }
        }
        all_tasks
    }

    fn start_dashboard(config: &DeployConfig) -> Self {
        let dir = Codis::dir(&config.install_dir());
        let binary = format!("{dir}/codis-dashboard");
        let conf_path = format!("{dir}/{CODIS_DASHBOARD_CNF}");
        let log_path = format!("{dir}/dashboard.log");
        let pid_path = format!("{dir}/dashboard.pid");
        let damon_path = format!("{dir}/dashboard.out");
        let cmd = format!("nohup {binary} --config={conf_path} --log={log_path} --log-level=INFO --pidfile={pid_path} > {damon_path} 2>&1 < /dev/null &");
        CodisTask {
            ssh_key: config.connection.ssh_auth_key().unwrap(),
            command: cmd,
            host: config.deployment.codis.as_ref().unwrap().dashboard.clone(),
        }
    }

    fn stop_server(config: &DeployConfig, host: String, name: &str) -> Self {
        let dir = Codis::dir(&config.install_dir());
        let pid_path = format!("{dir}/{name}.pid");
        let command = format!("kill -2 $(cat {pid_path})");
        CodisTask {
            ssh_key: config.connection.ssh_auth_key().unwrap(),
            command,
            host,
        }
    }

    fn start_proxy(config: &DeployConfig, host: String) -> Self {
        let codis_conf = config.deployment.codis.as_ref().unwrap();
        let dir = Codis::dir(&config.install_dir());
        let binary = format!("{dir}/codis-proxy");
        let conf_path = format!("{dir}/{CODIS_PROXY_CNF}");
        let log_path = format!("{dir}/proxy.log");
        let pid_path = format!("{dir}/proxy.pid");
        let damon_path = format!("{dir}/proxy.out");
        let dashboard = format!("{}:18080", codis_conf.dashboard);
        let cmd = format!("nohup {binary} --config={conf_path} --dashboard={dashboard} --log={log_path} --log-level=INFO --pidfile={pid_path} > {damon_path} 2>&1 < /dev/null &");
        CodisTask {
            ssh_key: config.connection.ssh_auth_key().unwrap(),
            command: cmd,
            host,
        }
    }

    fn task_instance(self, config: &DeployConfig, tasks: &mut IndexMap<TaskId, TaskInstance>) {
        let host = TaskHost::Remote {
            user: config.connection.username.clone(),
            port: config.connection.ssh_port() as usize,
            host: self.host.clone(),
        };
        let inst = TaskInstance {
            task_input: HashMap::default(),
            task: Box::new(self),
            task_host: host,
        };
        tasks.insert(inst.task.identifier(), inst);
    }
}

#[async_trait::async_trait]
impl TaskExecutor for CodisTask {
    fn identifier(&self) -> TaskId {
        TaskId {
            cmd: "codis".to_owned(),
            task: self.command.clone(),
            host: self.host.clone(),
        }
    }

    async fn execute(
        &self,
        host: TaskHost,
        _input: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        let sess = SSHSession::from_task_host(host, self.ssh_key.clone()).await?;
        let output = sess
            .command(&self.command, SSHCommandOption::CollectOutput)
            .await?;
        Ok(Some(output))
    }
}
