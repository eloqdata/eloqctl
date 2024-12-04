use serde::Serialize;
use std::collections::HashMap;

use super::task_base::{
    ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::ssh::{SSHCommandOption, SSHSession};
use crate::cli::ProxyCommand;
use crate::config::proxy_config_base::ProxyConfig;
use indexmap::IndexMap;
use reqwest;

#[derive(Debug, Clone)]
pub struct ProxyCtlTask {
    ssh_key: String,
    user: String,
    port: usize,
    host: String,
    task_type: ProxyTaskType,
}

#[derive(Debug, Clone)]
pub enum ProxyTaskType {
    StartProxy {
        command: String,
    },
    StopProxy {
        command: String,
    },
    AddCluster {
        cluster_name: String,
        request_body: String,
    },
    RemoveCluster {
        cluster_name: String,
        token: String,
    },
}

impl ProxyCtlTask {
    pub fn from_config(
        command: ProxyCommand,
        ssh_key: String,
        user: String,
        port: usize,
        proxy_hosts: Vec<String>,
        args: &HashMap<String, String>,
        proxy_config: &ProxyConfig,
    ) -> IndexMap<TaskId, TaskInstance> {
        let mut all_tasks = IndexMap::default();

        match command {
            ProxyCommand::Start {
                config: config_path,
            } => {
                for host in &proxy_hosts {
                    let task = Self::start_proxy(
                        ssh_key.clone(),
                        user.clone(),
                        port,
                        host.clone(),
                        args,
                        proxy_config,
                    );

                    task.task_instance(&mut all_tasks);
                }
            }
            ProxyCommand::Stop { .. } => {
                for host in &proxy_hosts {
                    let task = Self::stop_proxy(
                        ssh_key.clone(),
                        user.clone(),
                        port,
                        host.clone(),
                        proxy_config,
                    );
                    task.task_instance(&mut all_tasks);
                }
            }
            ProxyCommand::List { proxy_name } => {
                todo!()
            }
            ProxyCommand::Add { cluster_name, .. } => {
                todo!()
                // let task =
                //     Self::add_cluster(ssh_key.clone(), proxy_hosts.clone(), &cluster_name, args);
                // task.task_instance(&mut all_tasks);
            }
            ProxyCommand::Remove { cluster_name, .. } => {
                todo!()
                // let task =
                //     Self::remove_cluster(ssh_key.clone(), proxy_hosts.clone(), &cluster_name, args);
                // task.task_instance(&mut all_tasks);
            }
        }
        all_tasks
    }

    fn start_proxy(
        ssh_key: String,
        user: String,
        port: usize,
        host: String,
        args: &HashMap<String, String>,
        proxy_config: &ProxyConfig,
    ) -> Self {
        let bin_path = args.get("proxy_bin").expect("proxy_bin is required");

        let config_file = proxy_config.proxy_service.proxy_conf_path();
        let log_path = format!("{}/proxy.log", proxy_config.proxy_service.install_dir());
        let pid_path = format!("{}/proxy.pid", proxy_config.proxy_service.install_dir());

        let command = format!(
            "chmod +x {}; nohup {} --config={} > {} 2>&1 & echo $! > {}",
            bin_path, bin_path, config_file, log_path, pid_path
        );

        Self {
            ssh_key,
            user,
            port,
            host,
            task_type: ProxyTaskType::StartProxy { command },
        }
    }

    fn stop_proxy(
        ssh_key: String,
        user: String,
        port: usize,
        host: String,
        proxy_config: &ProxyConfig,
    ) -> Self {
        let pid_path = format!("{}/proxy.pid", proxy_config.proxy_service.install_dir());

        let command = format!("kill $(cat {})", pid_path);

        Self {
            ssh_key,
            user,
            port,
            host,
            task_type: ProxyTaskType::StopProxy { command },
        }
    }

    fn add_cluster(
        ssh_key: String,
        host: String,
        cluster_name: &str,
        args: &HashMap<String, String>,
    ) -> Self {
        let web_service_addr = args
            .get("web_service_addr")
            .expect("web_service_addr is required");

        // Get cluster details from args
        let cluster_id = args
            .get("cluster_id")
            .expect("cluster_id is required")
            .to_string();
        let addrs = args
            .get("cluster_addrs")
            .expect("cluster_addrs is required");
        let token = args
            .get("cluster_token")
            .expect("cluster_token is required")
            .to_string();
        let password = args
            .get("cluster_password")
            .expect("cluster_password is required")
            .to_string();

        let addrs: Vec<String> = addrs.split(',').map(|s| s.trim().to_string()).collect();

        #[derive(Serialize)]
        struct AddClusterRequest {
            cluster_id: String,
            addrs: Vec<String>,
            token: String,
            password: String,
        }

        let request_body = AddClusterRequest {
            cluster_id,
            addrs,
            token,
            password,
        };

        Self {
            ssh_key,
            user: String::new(), // Not needed for local tasks
            port: 0,             // Not needed for local tasks
            host: web_service_addr.to_string(),
            task_type: ProxyTaskType::AddCluster {
                cluster_name: cluster_name.to_string(),
                request_body: serde_json::to_string(&request_body).unwrap(),
            },
        }
    }

    fn remove_cluster(
        ssh_key: String,
        host: String,
        cluster_name: &str,
        args: &HashMap<String, String>,
    ) -> Self {
        let web_service_addr = args
            .get("web_service_addr")
            .expect("web_service_addr is required");
        let token = args
            .get("cluster_token")
            .expect("cluster_token is required")
            .to_string();

        Self {
            ssh_key,
            user: String::new(), // Not needed for local tasks
            port: 0,             // Not needed for local tasks
            host: web_service_addr.to_string(),
            task_type: ProxyTaskType::RemoveCluster {
                cluster_name: cluster_name.to_string(),
                token,
            },
        }
    }

    fn task_instance(self, tasks: &mut IndexMap<TaskId, TaskInstance>) {
        let host = match &self.task_type {
            ProxyTaskType::AddCluster { .. } | ProxyTaskType::RemoveCluster { .. } => {
                TaskHost::Local
            }
            _ => TaskHost::Remote {
                user: self.user.clone(),
                port: self.port,
                host: self.host.clone(),
            },
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
impl TaskExecutor for ProxyCtlTask {
    fn identifier(&self) -> TaskId {
        let cmd = match &self.task_type {
            ProxyTaskType::StartProxy { .. } => "proxy_start",
            ProxyTaskType::StopProxy { .. } => "proxy_stop",
            ProxyTaskType::AddCluster { .. } => "proxy_add_cluster",
            ProxyTaskType::RemoveCluster { .. } => "proxy_remove_cluster",
        }
        .to_owned();

        TaskId {
            cmd,
            task: format!("{:?}", self.task_type),
            host: self.host.clone(),
        }
    }

    async fn execute(
        &self,
        host: TaskHost,
        _input: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        match &self.task_type {
            ProxyTaskType::StartProxy { command } => {
                let sess = SSHSession::from_task_host(host.clone(), self.ssh_key.clone()).await?;

                let output = sess
                    .command(&command, SSHCommandOption::CollectOutput)
                    .await?;
                Ok(Some(output))
            }
            ProxyTaskType::StopProxy { command } => {
                let sess = SSHSession::from_task_host(host.clone(), self.ssh_key.clone()).await?;
                let output = sess
                    .command(&command, SSHCommandOption::CollectOutput)
                    .await?;
                Ok(Some(output))
            }
            ProxyTaskType::AddCluster {
                cluster_name,
                request_body,
            } => {
                let url = format!("http://{}/cluster", self.host);
                let client = reqwest::Client::new();
                let res = client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .body(request_body.clone())
                    .send()
                    .await?;
                let status = res.status();
                let text = res.text().await?;
                if status.is_success() {
                    panic!("")
                } else {
                    Err(anyhow::anyhow!(
                        "Failed to add cluster {}: {}",
                        cluster_name,
                        text
                    ))
                }
            }
            ProxyTaskType::RemoveCluster {
                cluster_name,
                token,
            } => {
                let url = format!("http://{}/cluster/{}", self.host, token);
                let client = reqwest::Client::new();
                let res = client.delete(&url).send().await?;
                let status = res.status();
                let text = res.text().await?;
                if status.is_success() {
                    panic!("")
                } else {
                    Err(anyhow::anyhow!(
                        "Failed to remove cluster {}: {}",
                        cluster_name,
                        text
                    ))
                }
            }
        }
    }
}
