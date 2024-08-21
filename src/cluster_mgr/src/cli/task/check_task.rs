use crate::{
    cli::{
        ssh,
        task::task_base::{ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId},
    },
    config::{config_base::DeployConfig, DeploymentPackage},
};
use anyhow::{bail, Ok, Result};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CheckTask {
    kind: DeploymentPackage,
    host: String,
    config: DeployConfig,
}

impl CheckTask {
    pub fn new(kind: DeploymentPackage, host: String, config: DeployConfig) -> Self {
        CheckTask { kind, host, config }
    }

    async fn check_tx_sv(&self, host: TaskHost) -> Result<Option<ExecutionValue>> {
        let ssh_k = self.config.connection.ssh_auth_key().unwrap();
        let sess = ssh::SSHSession::from_task_host(host, ssh_k).await?;
        for p in sess.used_tcp_ports().await? {
            if let Some(txsrv) = &self.config.deployment.tx_service {
                if txsrv.client_port() == p {
                    bail!("tx-service client socket {}:{p} is already used", self.host);
                }
                if Some(p) == txsrv.port {
                    bail!("tx-service socket {}:{p} is already used", self.host);
                }
            }
            if let Some(moni) = &self.config.deployment.monitor {
                if moni.node_exporter.port == p {
                    bail!("node exporter socket {}:{p} is already used", self.host);
                }
                if let Some(myex) = &moni.mysql_exporter {
                    if myex.port == p {
                        bail!("mysql exporter socket {}:{p} is already used", self.host);
                    }
                }
            }
        }
        Ok(None)
    }

    async fn check_log_sv(&self, host: TaskHost) -> Result<Option<ExecutionValue>> {
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
                bail!("log-service socket {}:{p} is already used", self.host);
            }
            if let Some(moni) = &self.config.deployment.monitor {
                if moni.node_exporter.port == p {
                    bail!("node exporter socket {}:{p} is already used", self.host);
                }
            }
        }
        Ok(None)
    }

    async fn check_kv_store(
        &self,
        host: TaskHost,
        input: HashMap<String, TaskArgValue>,
    ) -> Result<Option<ExecutionValue>> {
        assert!(self.config.deployment.storage_service.cassandra.is_some());
        let ssh_k = self.config.connection.ssh_auth_key().unwrap();
        let sess = ssh::SSHSession::from_task_host(host, ssh_k).await?;
        for p in sess.used_tcp_ports().await? {
            if let Some(v) = input.get(&p.to_string()) {
                let name = v.clone().into_inner_value::<String>();
                bail!("cassandra {name} socket {}:{p} is already used", self.host);
            }
            if let Some(moni) = &self.config.deployment.monitor {
                if moni.node_exporter.port == p {
                    bail!("node exporter socket {}:{p} is already used", self.host);
                }
                if let Some(cc) = &moni.cassandra_collector {
                    if cc.mcac_port == p {
                        bail!("cassandra mcac socket {}:{p} is already used", self.host);
                    }
                }
            }
        }
        Ok(None)
    }

    async fn check_prometheus(&self, host: TaskHost) -> Result<Option<ExecutionValue>> {
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
                bail!("prometheus socket {}:{p} is already used", self.host);
            }
        }
        Ok(None)
    }

    async fn check_grafana(&self, host: TaskHost) -> Result<Option<ExecutionValue>> {
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
                bail!("grafana socket {}:{p} is already used", self.host);
            }
        }
        Ok(None)
    }

    async fn check_codis(&self, host: TaskHost) -> Result<Option<ExecutionValue>> {
        let codis = self.config.deployment.codis.as_ref().unwrap();
        let need = if self.host == codis.dashboard {
            vec![18080]
        } else if codis.proxy.contains(&self.host) {
            vec![11080, 19000]
        } else {
            unreachable!()
        };
        let ssh_k = self.config.connection.ssh_auth_key().unwrap();
        let sess = ssh::SSHSession::from_task_host(host, ssh_k).await?;
        for p in sess.used_tcp_ports().await? {
            if need.contains(&p) {
                bail!("codis socket {}:{p} is already used", self.host);
            }
            if let Some(moni) = &self.config.deployment.monitor {
                if moni.node_exporter.port == p {
                    bail!("node exporter socket {}:{p} is already used", self.host);
                }
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
    ) -> Result<Option<ExecutionValue>> {
        match self.kind {
            DeploymentPackage::MonographTx => self.check_tx_sv(host).await,
            DeploymentPackage::Storage => self.check_kv_store(host, input).await,
            DeploymentPackage::Prometheus => self.check_prometheus(host).await,
            DeploymentPackage::Grafana => self.check_grafana(host).await,
            DeploymentPackage::MonographLog => self.check_log_sv(host).await,
            DeploymentPackage::Codis => self.check_codis(host).await,
        }
    }
}
