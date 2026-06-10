use crate::cli::task::task_utils::configured_eloq_metrics_port;
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

    fn configured_ports_for_host(&self) -> Vec<u16> {
        self.config
            .deployment
            .get_host_port_list(self.kind.clone())
            .into_iter()
            .filter_map(|host_port| {
                let (host, port) = host_port.rsplit_once(':')?;
                if host == self.host {
                    port.parse::<u16>().ok()
                } else {
                    None
                }
            })
            .collect()
    }

    async fn check_tx_sv(&self, host: TaskHost) -> Result<Option<ExecutionValue>> {
        let ssh_k = self.config.connection.ssh_auth_key().unwrap();
        let sess = ssh::SSHSession::from_task_host(host, ssh_k).await?;
        let needed = self.configured_ports_for_host();
        let metrics_port = configured_eloq_metrics_port(&self.config);
        for p in sess.used_tcp_ports().await? {
            if needed.contains(&p) {
                bail!("tx-service socket {}:{p} is already used", self.host);
            }
            if metrics_port == Some(p) {
                bail!("eloq metrics socket {}:{p} is already used", self.host);
            }
            if let Some(moni) = &self.config.deployment.monitor {
                if let Some(noex) = &moni.node_exporter {
                    if noex.port == p {
                        bail!("node exporter socket {}:{p} is already used", self.host);
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
                if let Some(noex) = &moni.node_exporter {
                    if noex.port == p {
                        bail!("node exporter socket {}:{p} is already used", self.host);
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
            .as_ref()
            .unwrap()
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
            .grafana
            .as_ref()
            .unwrap()
            .port;
        for p in sess.used_tcp_ports().await? {
            if port == p {
                bail!("grafana socket {}:{p} is already used", self.host);
            }
        }
        Ok(None)
    }

    async fn check_alertmanager(&self, host: TaskHost) -> Result<Option<ExecutionValue>> {
        let ssh_k = self.config.connection.ssh_auth_key().unwrap();
        let sess = ssh::SSHSession::from_task_host(host, ssh_k).await?;
        let port = self
            .config
            .deployment
            .monitor
            .as_ref()
            .unwrap()
            .alertmanager
            .as_ref()
            .unwrap()
            .port;
        for p in sess.used_tcp_ports().await? {
            if port == p {
                bail!("alertmanager socket {}:{p} is already used", self.host);
            }
        }
        Ok(None)
    }

    async fn check_prometheusalert(&self, host: TaskHost) -> Result<Option<ExecutionValue>> {
        let ssh_k = self.config.connection.ssh_auth_key().unwrap();
        let sess = ssh::SSHSession::from_task_host(host, ssh_k).await?;
        let port = self
            .config
            .deployment
            .monitor
            .as_ref()
            .unwrap()
            .alertmanager
            .as_ref()
            .unwrap()
            .webhook_adapter_port;
        for p in sess.used_tcp_ports().await? {
            if port == p {
                bail!(
                    "alertmanager-webhook-adapter socket {}:{p} is already used",
                    self.host
                );
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
            DeploymentPackage::EloqTx => self.check_tx_sv(host).await,
            DeploymentPackage::EloqStandby => self.check_tx_sv(host).await,
            DeploymentPackage::EloqVoter => self.check_tx_sv(host).await,
            DeploymentPackage::Storage => {
                let _ = input;
                Ok(None)
            }
            DeploymentPackage::Prometheus => self.check_prometheus(host).await,
            DeploymentPackage::Alertmanager => self.check_alertmanager(host).await,
            DeploymentPackage::Grafana => self.check_grafana(host).await,
            DeploymentPackage::PrometheusAlert => self.check_prometheusalert(host).await,
            DeploymentPackage::EloqLog => self.check_log_sv(host).await,
        }
    }
}
