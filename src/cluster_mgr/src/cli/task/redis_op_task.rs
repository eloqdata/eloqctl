use crate::cli::task::grpc::cc_request::{
    CheckCkptStatusResponse, CkptStatus, NotifyShutdownCkptResponse,
};
use crate::cli::task::grpc::GrpcClient;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::task_return_value;
use anyhow::{anyhow, Error, Result};
use async_trait::async_trait;
use futures::future::join_all;
use redis::cluster::ClusterClient;
use redis::{ErrorKind, RedisError, RedisResult, Value};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::{error, info, warn};

// only used in stop cluster with standby
#[derive(Clone, Debug)]
pub struct RedisOpTask {
    task_id: TaskId,
    redis_host_ports: Vec<String>,
    redis_cmd: String,
    sender: watch::Sender<ClusterNodes>,
    password: Option<String>,
    skip_checkpoint: bool,
}

impl RedisOpTask {
    pub fn new(
        task_id: TaskId,
        redis_host_ports: Vec<String>,
        redis_cmd: String,
        sender: watch::Sender<ClusterNodes>,
        password: Option<String>,
    ) -> Self {
        Self {
            task_id,
            redis_host_ports,
            redis_cmd,
            sender,
            password,
            skip_checkpoint: false,
        }
    }

    pub fn new_and_skip_checkpoint(
        task_id: TaskId,
        redis_host_ports: Vec<String>,
        redis_cmd: String,
        sender: watch::Sender<ClusterNodes>,
        password: Option<String>,
        skip_checkpoint: bool,
    ) -> Self {
        Self {
            task_id,
            redis_host_ports,
            redis_cmd,
            sender,
            password,
            skip_checkpoint,
        }
    }

    // Helper method to check if the sender has any receivers
    fn has_receivers(&self) -> bool {
        self.sender.receiver_count() > 0
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct NodeInfo {
    pub ip: String,
    pub port: u16,
}

// Implement Eq and Hash for NodeInfo
impl PartialEq for NodeInfo {
    fn eq(&self, other: &Self) -> bool {
        self.ip == other.ip && self.port == other.port
    }
}

impl Eq for NodeInfo {}

impl Hash for NodeInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.ip.hash(state);
        self.port.hash(state);
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct ClusterNodes {
    pub masters: Vec<NodeInfo>,
    pub replicas: Vec<NodeInfo>,
}

const MAX_RETRIES: usize = 500;
const RETRY_DELAY: Duration = Duration::from_secs(2);

async fn query_ckpt_status_with_retry(
    master: NodeInfo,
    trigger_ckpt_ts: u64,
    max_retries: usize,
    retry_delay: Duration,
) -> Result<CheckCkptStatusResponse> {
    let ip = &master.ip;
    let port = master.port + 10000 + 1;
    let url = format!("http://{}:{}", ip, port);
    let mut retries = 0;

    loop {
        let mut grpc_client = GrpcClient::new(&url).await.map_err(|e| {
            error!("Failed to create GrpcClient for {}: {}", url, e);
            e
        })?;

        let response = grpc_client
            .query_ckpt_status(trigger_ckpt_ts)
            .await
            .map_err(|e| {
                error!("Failed to query ckpt for {}: {}", url, e);
                e
            })?;

        match CkptStatus::from_i32(response.status) {
            Some(CkptStatus::CkptFinished) => {
                info!("Checkpoint finished for {}: {:#?}", url, response);
                return Ok(response);
            }
            Some(CkptStatus::CkptRunning) => {
                if retries >= max_retries {
                    error!(
                        "Maximum retries reached for {}. Checkpoint is still running.",
                        url
                    );
                    return Err(anyhow!(
                        "Checkpoint is still running after max retries for {}",
                        url
                    ));
                } else {
                    retries += 1;
                    info!(
                        "Checkpoint is still running for {}. Retrying... (Attempt {}/{})",
                        url, retries, max_retries
                    );
                    sleep(retry_delay).await;
                }
            }
            Some(CkptStatus::CkptFailed) => {
                error!("Checkpoint failed for {}: {:#?}", url, response);
                return Err(anyhow!("Checkpoint failed for {}", url));
            }
            _ => {
                error!("Unexpected checkpoint status for {}: {:#?}", url, response);
                return Err(anyhow!("Unexpected checkpoint status for {}", url));
            }
        }
    }
}

pub fn parse_cluster_nodes(value: Value) -> RedisResult<Vec<ClusterNodes>> {
    // Extract the cluster nodes string from the value
    let nodes_str = match value {
        Value::Data(bytes) => {
            // Extract the actual string data (handle quoted strings)
            let raw_str = String::from_utf8_lossy(&bytes).to_string();
            if raw_str.starts_with('"') && raw_str.ends_with('"') {
                // Remove the surrounding quotes
                raw_str[1..raw_str.len() - 1].to_string()
            } else {
                raw_str
            }
        }
        _ => {
            return Err(RedisError::from((
                ErrorKind::TypeError,
                "Unexpected format for CLUSTER NODES",
                format!("{:?}", value),
            )))
        }
    };

    let mut cluster_nodes_list = Vec::new();
    let mut masters = Vec::new();
    let mut replicas = Vec::new();

    // Parse each line of the CLUSTER NODES output
    for line in nodes_str.lines() {
        if line.trim().is_empty() {
            continue; // Skip empty lines
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 8 {
            continue; // Skip malformed lines
        }

        // Parse IP and port from the ip:port@cport format
        let ip_port = parts[1];
        let ip_port_parts: Vec<&str> = if ip_port.contains('@') {
            ip_port.split([':', '@']).collect()
        } else {
            ip_port.split(':').collect()
        };

        if ip_port_parts.len() < 2 {
            continue; // Skip malformed IP:port
        }

        let ip = ip_port_parts[0].to_string();
        let port = match ip_port_parts[1].parse::<u16>() {
            Ok(p) => p,
            Err(_) => continue, // Skip if port is not a valid u16
        };

        // Check if node is master or replica
        let flags = parts[2];
        let is_master = !flags.contains("slave");

        // Add node to appropriate list
        let node_info = NodeInfo { ip, port };

        if is_master {
            masters.push(node_info);
        } else {
            replicas.push(node_info);
        }
    }

    // Group all masters and replicas into one ClusterNodes
    if !masters.is_empty() || !replicas.is_empty() {
        cluster_nodes_list.push(ClusterNodes { masters, replicas });
    }

    Ok(cluster_nodes_list)
}

#[async_trait]
impl TaskExecutor for RedisOpTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        _task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        let mut task_result =
            HashMap::from([(CMD.to_string(), TaskArgValue::Str(self.redis_cmd.clone()))]);

        let nodes: Vec<String> = self
            .redis_host_ports
            .iter()
            .map(|host_port| {
                if let Some(password) = &self.password {
                    format!("redis://:{}@{}", password, host_port)
                } else {
                    format!("redis://{}", host_port)
                }
            })
            .collect();

        let nodes_info = nodes.join(", ");
        let client = ClusterClient::new(nodes)?;
        // Use synchronous connection
        let mut con = match client.get_connection() {
            Ok(connection) => connection,
            Err(err) => {
                error!(
                    "Can not connect to the cluster. Attempted nodes: [{}]. Error: {}",
                    nodes_info, err
                );
                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(err.to_string()));
                task_return_value!(
                    task_result,
                    |status_code: i32| -> CmdErr {
                        CmdErr::RedisOpErr(
                            "Can not connect to the cluster".to_string(),
                            status_code.to_string(),
                        )
                    },
                    "RedisOpTask"
                )
            }
        };

        // Use the command provided in redis_cmd
        let cmd_lower = self.task_id.cmd.to_lowercase();
        let result = match cmd_lower.as_str() {
            "topology" => {
                let query_result = redis::cmd("CLUSTER").arg("NODES").query::<Value>(&mut con);

                // Closing connection explicitly if successful or failed
                drop(con); // Manually close connection
                query_result
            }
            _ => {
                error!("Unsupported command: {}", self.redis_cmd);
                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                task_result.insert(
                    CMD_OUTPUT.to_string(),
                    TaskArgValue::Str("Unsupported command".to_string()),
                );
                return Ok(Some(task_result));
            }
        };

        // Processing the result
        match result {
            Ok(value) => {
                let cluster_nodes = parse_cluster_nodes(value)?;
                let mut unique_masters = HashSet::new();
                let mut unique_replicas = HashSet::new();

                for slot in &cluster_nodes {
                    for master in &slot.masters {
                        unique_masters.insert(master.clone());
                    }
                    for replica in &slot.replicas {
                        unique_replicas.insert(replica.clone());
                    }
                }

                // Continue with checkpoint tasks only if skip_checkpoint is false
                if !self.skip_checkpoint {
                    let mut trigger_ckpt_tasks = Vec::new();
                    for master in &unique_masters {
                        let ip = &master.ip;
                        let port = master.port + 10000 + 1;

                        let task = async move {
                            let url = format!("http://{}:{}", ip, port);
                            let mut grpc_client = GrpcClient::new(&url).await.map_err(|e| {
                                error!("Failed to create GrpcClient for {}: {}", url, e);
                                e
                            })?;
                            let response = grpc_client.trigger_ckpt().await.map_err(|e| {
                                error!("Failed to trigger ckpt for {}: {}", url, e);
                                e
                            })?;
                            info!("Successfully trigger ckpt for {}: {:#?}", url, response);
                            Ok(response)
                        };
                        trigger_ckpt_tasks.push(task);
                    }
                    // Execute all tasks concurrently
                    let trigger_responses: Vec<
                        std::result::Result<NotifyShutdownCkptResponse, Error>,
                    > = join_all(trigger_ckpt_tasks).await;

                    let mut has_error = false;
                    let mut error_message = String::new();
                    let mut trigger_ckpt_ts_list = Vec::new();

                    for result in trigger_responses {
                        match result {
                            Ok(response) => {
                                let ts = response.trigger_ckpt_ts;
                                trigger_ckpt_ts_list.push(ts);
                            }
                            Err(e) => {
                                has_error = true;
                                error_message = e.to_string();
                                break;
                            }
                        }
                    }

                    if has_error {
                        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                        task_result
                            .insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(error_message));
                        return Ok(Some(task_result));
                    }

                    // Now, implement the retry logic for query_ckpt_status
                    let mut query_ckpt_tasks = Vec::new();
                    for (i, master) in unique_masters.iter().enumerate() {
                        let master = master.clone();
                        let trigger_ckpt_ts = trigger_ckpt_ts_list[i];

                        let task = query_ckpt_status_with_retry(
                            master,
                            trigger_ckpt_ts,
                            MAX_RETRIES,
                            RETRY_DELAY,
                        );
                        query_ckpt_tasks.push(task);
                    }

                    // Execute all tasks concurrently
                    let query_responses: Vec<Result<CheckCkptStatusResponse>> =
                        join_all(query_ckpt_tasks).await;

                    let mut has_error = false;
                    let mut error_message = String::new();

                    for result in query_responses {
                        match result {
                            Ok(_response) => {
                                // Checkpoint finished successfully
                            }
                            Err(e) => {
                                has_error = true;
                                error_message = e.to_string();
                                break;
                            }
                        }
                    }

                    if has_error {
                        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                        task_result
                            .insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(error_message));
                        return Ok(Some(task_result));
                    }
                } else {
                    info!("Skipping checkpoint tasks because enable_data_store is false in at least one node configuration");
                }

                // Convert HashSets to Vectors
                let unique_masters: Vec<NodeInfo> = unique_masters.into_iter().collect();
                let unique_replicas: Vec<NodeInfo> = unique_replicas.into_iter().collect();

                // For debugging: print the unique masters and replicas
                for master in &unique_masters {
                    info!("Masters:  {}:{}", master.ip, master.port);
                }
                for replica in &unique_replicas {
                    info!("Replicas:  {}:{}", replica.ip, replica.port);
                }

                let cluster_nodes = ClusterNodes {
                    masters: unique_masters,
                    replicas: unique_replicas,
                };

                let response_str = serde_json::to_string(&cluster_nodes)?;
                info!("Redis response: {}", response_str);
                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
                task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(response_str));

                // Send the cluster_nodes to the receiver only if there are active receivers
                if self.has_receivers() {
                    if let Err(err) = self.sender.send(cluster_nodes) {
                        error!("Failed to send cluster nodes result to channel: {}", err);
                        // Don't treat this as a task failure since we already have the data we need
                        warn!("Channel error, but continuing with task execution");
                    }
                } else {
                    info!("No active receivers for cluster node data, skipping channel send");
                }
            }
            Err(err) => {
                error!("Error executing Redis command: {}", err);
                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(err.to_string()));
            }
        }

        Ok(Some(task_result))
    }
}
