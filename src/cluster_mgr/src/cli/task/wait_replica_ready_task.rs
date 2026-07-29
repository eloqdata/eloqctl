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
use tracing::info;

const REPLICA_READY_RETRIES: usize = 120;
const REPLICA_READY_RETRY_DELAY: Duration = Duration::from_secs(2);
const WAIT_PROGRESS_LOG_INTERVAL: usize = 5;

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

#[derive(Clone, Debug)]
pub struct WaitNodeReadyTask {
    task_id: TaskId,
    startup_nodes: Vec<String>,
    target_host: String,
    target_port: u16,
    required_role: Option<&'static str>,
    password: Option<String>,
    service_endpoints: Option<HashMap<String, ServiceEndpoint>>,
}

fn describe_nodes(nodes: &[crate::cli::task::redis_op_task::NodeInfo]) -> String {
    let described = nodes
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

    if described.is_empty() {
        "<none>".to_string()
    } else {
        described.join(", ")
    }
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
            voters: Vec::new(),
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

impl WaitNodeReadyTask {
    pub fn new(
        task_id: TaskId,
        startup_nodes: Vec<String>,
        target_host: String,
        target_port: u16,
        password: Option<String>,
    ) -> Self {
        Self {
            task_id,
            startup_nodes,
            target_host,
            target_port,
            required_role: None,
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

    pub fn require_master(mut self) -> Self {
        self.required_role = Some("master");
        self
    }

    fn find_connected_target<'a>(
        &self,
        cluster_nodes: &'a ClusterNodes,
    ) -> Option<(&'static str, &'a crate::cli::task::redis_op_task::NodeInfo)> {
        cluster_nodes
            .masters
            .iter()
            .find(|node| {
                node.ip == self.target_host && node.port == self.target_port && node.connected
            })
            .map(|node| ("master", node))
            .or_else(|| {
                cluster_nodes
                    .replicas
                    .iter()
                    .find(|node| {
                        node.ip == self.target_host
                            && node.port == self.target_port
                            && node.connected
                    })
                    .map(|node| ("replica", node))
            })
            .or_else(|| {
                cluster_nodes
                    .voters
                    .iter()
                    .find(|node| {
                        node.ip == self.target_host
                            && node.port == self.target_port
                            && node.connected
                    })
                    .map(|node| ("voter", node))
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
            voters: Vec::new(),
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

        for attempt in 1..=REPLICA_READY_RETRIES {
            match self.fetch_cluster_nodes().await {
                Ok(cluster_nodes) => {
                    let masters = describe_nodes(&cluster_nodes.masters);
                    let replicas = describe_nodes(&cluster_nodes.replicas);
                    if self.find_connected_master(&cluster_nodes).is_some()
                        && self.find_connected_target_replica(&cluster_nodes).is_some()
                    {
                        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
                        task_result.insert(
                            CMD_OUTPUT.to_string(),
                            TaskArgValue::Str(format!(
                                "Master {source} and replica {target} are connected and ready for failover. Masters: {}. Replicas: {}",
                                masters,
                                replicas
                            )),
                        );
                        return Ok(Some(task_result));
                    }
                    last_seen = format!(
                        "masters currently visible: {masters}; replicas currently visible: {replicas}"
                    );
                }
                Err(err) => {
                    last_seen = err.to_string();
                }
            }
            if attempt == 1 || attempt % WAIT_PROGRESS_LOG_INTERVAL == 0 {
                info!(
                    "Waiting for master {source} and replica {target} to become connected ({attempt}/{REPLICA_READY_RETRIES}): {last_seen}"
                );
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

#[async_trait]
impl TaskExecutor for WaitNodeReadyTask {
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
            TaskArgValue::Str("wait node ready".to_string()),
        )]);

        let target = format!("{}:{}", self.target_host, self.target_port);
        let mut last_seen =
            String::from("target node not yet observed as connected in cluster topology");

        for attempt in 1..=REPLICA_READY_RETRIES {
            match self.fetch_cluster_nodes().await {
                Ok(cluster_nodes) => {
                    let has_connected_master =
                        cluster_nodes.masters.iter().any(|node| node.connected);
                    if has_connected_master {
                        if let Some((role, _node)) = self.find_connected_target(&cluster_nodes) {
                            if self.required_role.is_some_and(|required| required != role) {
                                last_seen = format!(
                                    "target node {target} is connected as {role}, waiting for {}",
                                    self.required_role.unwrap()
                                );
                                if attempt == 1 || attempt % WAIT_PROGRESS_LOG_INTERVAL == 0 {
                                    info!(
                                        "Waiting for node {target} to become connected as {} ({attempt}/{REPLICA_READY_RETRIES}): {last_seen}",
                                        self.required_role.unwrap()
                                    );
                                }
                                sleep(REPLICA_READY_RETRY_DELAY).await;
                                continue;
                            }
                            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
                            task_result.insert(
                                CMD_OUTPUT.to_string(),
                                TaskArgValue::Str(format!(
                                    "Node {target} is connected as {role}; cluster has a connected master"
                                )),
                            );
                            return Ok(Some(task_result));
                        }
                    }
                    last_seen = format!(
                        "masters: {}; replicas: {}; voters: {}",
                        describe_nodes(&cluster_nodes.masters),
                        describe_nodes(&cluster_nodes.replicas),
                        describe_nodes(&cluster_nodes.voters)
                    );
                }
                Err(err) => {
                    last_seen = err.to_string();
                }
            }
            if attempt == 1 || attempt % WAIT_PROGRESS_LOG_INTERVAL == 0 {
                let role = self
                    .required_role
                    .map(|role| format!(" as {role}"))
                    .unwrap_or_default();
                info!(
                    "Waiting for node {target} to become connected{role} ({attempt}/{REPLICA_READY_RETRIES}): {last_seen}"
                );
            }
            sleep(REPLICA_READY_RETRY_DELAY).await;
        }

        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
        task_result.insert(
            CMD_OUTPUT.to_string(),
            TaskArgValue::Str(format!(
                "Node {target} did not become connected in cluster topology in time: {last_seen}"
            )),
        );
        Ok(Some(task_result))
    }
}
