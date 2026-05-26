use crate::cli::task::redis_op_task::{ClusterNodes, RedisOpTask};
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::connection::ServiceEndpoint;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::sleep;

const REPLICA_READY_RETRIES: usize = 120;
const REPLICA_READY_RETRY_DELAY: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
pub struct WaitReplicaReadyTask {
    task_id: TaskId,
    startup_nodes: Vec<String>,
    source_host: String,
    source_port: u16,
    target_host: String,
    target_port: u16,
    password: Option<String>,
    service_endpoints: Option<HashMap<String, ServiceEndpoint>>,
}

impl WaitReplicaReadyTask {
    pub fn new(
        task_id: TaskId,
        startup_nodes: Vec<String>,
        source_host: String,
        source_port: u16,
        target_host: String,
        target_port: u16,
        password: Option<String>,
    ) -> Self {
        Self {
            task_id,
            startup_nodes,
            source_host,
            source_port,
            target_host,
            target_port,
            password,
            service_endpoints: None,
        }
    }

    pub fn with_service_endpoints(
        mut self,
        service_endpoints: Option<HashMap<String, ServiceEndpoint>>,
    ) -> Self {
        self.service_endpoints = service_endpoints;
        self
    }

    fn find_connected_master<'a>(
        &self,
        cluster_nodes: &'a ClusterNodes,
    ) -> Option<&'a crate::cli::task::redis_op_task::NodeInfo> {
        cluster_nodes.masters.iter().find(|node| {
            node.ip == self.source_host && node.port == self.source_port && node.connected
        })
    }

    fn find_connected_target_replica<'a>(
        &self,
        cluster_nodes: &'a ClusterNodes,
    ) -> Option<&'a crate::cli::task::redis_op_task::NodeInfo> {
        cluster_nodes.replicas.iter().find(|node| {
            node.ip == self.target_host && node.port == self.target_port && node.connected
        })
    }

    async fn fetch_cluster_nodes(&self) -> Result<ClusterNodes> {
        let task_id = TaskId {
            cmd: "topology".to_string(),
            task: format!("{}-topology", self.task_id.task),
            host: "_local".to_string(),
        };
        let (tx, _rx) = watch::channel(ClusterNodes {
            masters: Vec::new(),
            replicas: Vec::new(),
        });
        let result = RedisOpTask::new(
            task_id,
            self.startup_nodes.clone(),
            "cluster topology".to_string(),
            tx,
            self.password.clone(),
            true,
        )
        .with_service_endpoints(self.service_endpoints.clone())
        .execute(TaskHost::Local, HashMap::default())
        .await?;

        let values = result.ok_or_else(|| anyhow::anyhow!("missing topology task result"))?;
        let output = values
            .get(CMD_OUTPUT)
            .cloned()
            .unwrap_or_else(|| TaskArgValue::Str("missing cluster topology output".to_string()));
        let status = values
            .get(CMD_STATUS)
            .cloned()
            .unwrap_or(TaskArgValue::Number(1));

        match (status, output) {
            (TaskArgValue::Number(0), TaskArgValue::Str(json)) => {
                Ok(serde_json::from_str::<ClusterNodes>(&json)?)
            }
            (_, TaskArgValue::Str(err)) => Err(anyhow::anyhow!(err)),
            _ => Err(anyhow::anyhow!("unexpected topology task output")),
        }
    }
}

#[async_trait]
impl TaskExecutor for WaitReplicaReadyTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        _task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> Result<Option<ExecutionValue>> {
        let mut task_result = HashMap::from([(
            CMD.to_string(),
            TaskArgValue::Str("wait replica ready".to_string()),
        )]);

        let source = format!("{}:{}", self.source_host, self.source_port);
        let target = format!("{}:{}", self.target_host, self.target_port);
        let mut last_seen =
            String::from("required nodes not yet observed as connected in cluster topology");

        for _ in 0..REPLICA_READY_RETRIES {
            match self.fetch_cluster_nodes().await {
                Ok(cluster_nodes) => {
                    let masters = cluster_nodes
                        .masters
                        .iter()
                        .map(|node| {
                            format!(
                                "{}:{} ({})",
                                node.ip,
                                node.port,
                                if node.connected {
                                    "connected"
                                } else {
                                    "disconnected"
                                }
                            )
                        })
                        .collect::<Vec<_>>();
                    let replicas = cluster_nodes
                        .replicas
                        .iter()
                        .map(|node| {
                            format!(
                                "{}:{} ({})",
                                node.ip,
                                node.port,
                                if node.connected {
                                    "connected"
                                } else {
                                    "disconnected"
                                }
                            )
                        })
                        .collect::<Vec<_>>();
                    if self.find_connected_master(&cluster_nodes).is_some()
                        && self.find_connected_target_replica(&cluster_nodes).is_some()
                    {
                        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
                        task_result.insert(
                            CMD_OUTPUT.to_string(),
                            TaskArgValue::Str(format!(
                                "Master {source} and replica {target} are connected and ready for failover. Masters: {}. Replicas: {}",
                                masters.join(", "),
                                replicas.join(", ")
                            )),
                        );
                        return Ok(Some(task_result));
                    }
                    last_seen = if masters.is_empty() && replicas.is_empty() {
                        "cluster currently reports no masters or replicas".to_string()
                    } else {
                        format!(
                            "masters currently visible: {}; replicas currently visible: {}",
                            if masters.is_empty() {
                                "<none>".to_string()
                            } else {
                                masters.join(", ")
                            },
                            if replicas.is_empty() {
                                "<none>".to_string()
                            } else {
                                replicas.join(", ")
                            }
                        )
                    };
                }
                Err(err) => {
                    last_seen = err.to_string();
                }
            }
            sleep(REPLICA_READY_RETRY_DELAY).await;
        }

        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
        task_result.insert(
            CMD_OUTPUT.to_string(),
            TaskArgValue::Str(format!(
                "Master {source} and replica {target} did not both become connected in time: {last_seen}"
            )),
        );
        Ok(Some(task_result))
    }
}
