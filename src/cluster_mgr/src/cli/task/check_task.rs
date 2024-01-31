use crate::{
    cli::{
        ssh,
        task::task_base::{ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId},
    },
    config::{config_base::DeploymentConfig, DeploymentPackage},
};
use anyhow::Ok;
use std::collections::HashMap;
use tracing::error;

#[derive(Debug, Clone)]
pub struct CheckTask {
    kind: DeploymentPackage,
    host: String,
    config: DeploymentConfig,
}

impl CheckTask {
    pub fn new(kind: DeploymentPackage, host: String, config: DeploymentConfig) -> Self {
        CheckTask { kind, host, config }
    }

    async fn check_tx_sv(&self, host: TaskHost) -> anyhow::Result<Option<ExecutionValue>> {
        let ssh_k = self.config.connection.ssh_auth_key().unwrap();
        let sess = ssh::SSHSession::from_task_host(host, ssh_k).await?;
        for p in sess.used_tcp_ports().await? {
            if self.config.deployment.port.contains(p) {
                error!("tx-service socket {}:{p} is already used", self.host);
            }
            if let Some(moni) = &self.config.deployment.monitor {
                if moni.node_exporter_port == p {
                    error!("node exporter socket {}:{p} is already used", self.host);
                }
                if moni.mysql_exporter_port == p {
                    error!("mysql exporter socket {}:{p} is already used", self.host);
                }
            }
        }
        Ok(None)
    }

    async fn check_log_sv(&self, host: TaskHost) -> anyhow::Result<Option<ExecutionValue>> {
        let needed = self
            .config
            .deployment
            .log_service
            .as_ref()
            .unwrap()
            .host_used_ports(&self.host);
        let ssh_k = self.config.connection.ssh_auth_key().unwrap();
        let sess = ssh::SSHSession::from_task_host(host, ssh_k).await?;
        for p in sess.used_tcp_ports().await? {
            if needed.contains(&p) {
                error!("log-service socket {}:{p} is already used", self.host);
            }
            if let Some(moni) = &self.config.deployment.monitor {
                if moni.node_exporter_port == p {
                    error!("node exporter socket {}:{p} is already used", self.host);
                }
            }
        }
        Ok(None)
    }

    async fn check_kv_store(
        &self,
        host: TaskHost,
        input: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        assert!(self.config.deployment.storage_service.cassandra.is_some());
        let ssh_k = self.config.connection.ssh_auth_key().unwrap();
        let sess = ssh::SSHSession::from_task_host(host, ssh_k).await?;
        for p in sess.used_tcp_ports().await? {
            if let Some(v) = input.get(&p.to_string()) {
                let name = v.clone().into_inner_value::<String>();
                error!("cassandra {name} socket {}:{p} is already used", self.host);
            }
            if let Some(moni) = &self.config.deployment.monitor {
                if moni.node_exporter_port == p {
                    error!("node exporter socket {}:{p} is already used", self.host);
                }
                if let Some(cc) = &moni.cassandra_collector {
                    if cc.mcac_port == p {
                        error!("cassandra mcac socket {}:{p} is already used", self.host);
                    }
                }
            }
        }
        Ok(None)
    }

    async fn check_prometheus(&self, host: TaskHost) -> anyhow::Result<Option<ExecutionValue>> {
        let ssh_k = self.config.connection.ssh_auth_key().unwrap();
        let sess = ssh::SSHSession::from_task_host(host, ssh_k).await?;
        let port = self
            .config
            .deployment
            .monitor
            .as_ref()
            .unwrap()
            .prometheus
            .port;
        for p in sess.used_tcp_ports().await? {
            if port == p {
                error!("prometheus socket {}:{p} is already used", self.host);
            }
        }
        Ok(None)
    }

    async fn check_grafana(&self, host: TaskHost) -> anyhow::Result<Option<ExecutionValue>> {
        let ssh_k = self.config.connection.ssh_auth_key().unwrap();
        let sess = ssh::SSHSession::from_task_host(host, ssh_k).await?;
        let port = self
            .config
            .deployment
            .monitor
            .as_ref()
            .unwrap()
            .prometheus
            .port;
        for p in sess.used_tcp_ports().await? {
            if port == p {
                error!("grafana socket {}:{p} is already used", self.host);
            }
        }
        Ok(None)
    }
}

#[async_trait::async_trait]
impl TaskExecutor for CheckTask {
    fn identifier(&self) -> TaskId {
        TaskId {
            cmd: "check".to_owned(),
            task: self.kind.as_ref().to_owned(),
            host: self.host.clone(),
        }
    }

    async fn execute(
        &self,
        host: TaskHost,
        input: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        match self.kind {
            DeploymentPackage::MonographTx => self.check_tx_sv(host).await,
            DeploymentPackage::Storage => self.check_kv_store(host, input).await,
            DeploymentPackage::Prometheus => self.check_prometheus(host).await,
            DeploymentPackage::Grafana => self.check_grafana(host).await,
            DeploymentPackage::MonographLog => self.check_log_sv(host).await,
        }
    }
}
