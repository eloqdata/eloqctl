use crate::cli::task::grpc::cc_request::{
    CheckCkptStatusResponse, CkptStatus, NotifyShutdownCkptResponse,
};
use crate::cli::task::grpc::GrpcClient;
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::connection::{resolve_service_endpoint, ServiceEndpoint};
use anyhow::{anyhow, Error, Result};
use async_trait::async_trait;
use futures::future::join_all;
use redis::{Client, ErrorKind, RedisError, RedisResult, Value};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
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
    service_endpoints: Option<HashMap<String, ServiceEndpoint>>,
    topology_retries: usize,
}

impl RedisOpTask {
    pub fn new(
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
            service_endpoints: None,
            topology_retries: TOPOLOGY_RETRIES,
        }
    }

    pub fn with_service_endpoints(
        mut self,
        service_endpoints: Option<HashMap<String, ServiceEndpoint>>,
    ) -> Self {
        self.service_endpoints = service_endpoints;
        self
    }

    pub fn with_topology_retries(mut self, topology_retries: usize) -> Self {
        self.topology_retries = topology_retries.max(1);
        self
    }

    // Helper method to check if the sender has any receivers
    fn has_receivers(&self) -> bool {
        self.sender.receiver_count() > 0
    }

    fn service_host_port(&self, host_port: &str) -> String {
        let Some((host, port)) = host_port.rsplit_once(':') else {
            return host_port.to_string();
        };
        let Ok(port) = port.parse::<u16>() else {
            return host_port.to_string();
        };
        let (endpoint_host, endpoint_port) =
            resolve_service_endpoint(self.service_endpoints.as_ref(), host, port);
        format!("{endpoint_host}:{endpoint_port}")
    }

    fn grpc_url(&self, host: &str, port: u16) -> String {
        let (endpoint_host, endpoint_port) =
            resolve_service_endpoint(self.service_endpoints.as_ref(), host, port);
        format!("http://{endpoint_host}:{endpoint_port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn spawn_fake_cluster_node(reply: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 1024];
            loop {
                let len = stream.read(&mut request).unwrap_or(0);
                if len == 0 {
                    break;
                }
                let command = String::from_utf8_lossy(&request[..len]).to_ascii_uppercase();
                if command.contains("CLIENT") {
                    let response_count = command.matches("SETINFO").count().max(1);
                    for _ in 0..response_count {
                        stream.write_all(b"+OK\r\n").unwrap();
                    }
                }

                if command.contains("CLUSTER") && command.contains("NODES") {
                    let response = format!("${}\r\n{}\r\n", reply.len(), reply);
                    stream.write_all(response.as_bytes()).unwrap();
                    break;
                } else if command.contains("HELLO") || !command.contains("CLIENT") {
                    stream.write_all(b"+OK\r\n").unwrap();
                } else {
                    stream.flush().unwrap();
                }
            }
        });
        addr.to_string()
    }

    #[tokio::test]
    async fn topology_task_uses_standby_startup_node_when_master_is_down() {
        let topology = "0000000000000000000000000000000000000001 127.0.0.1:6389@0 master - 0 0 0 connected 0-16383\n\
                        0000000000000000000000000000000000000000 127.0.0.1:6379@0 slave 0000000000000000000000000000000000000001 0 0 0 disconnected";
        let standby = spawn_fake_cluster_node(topology);
        let (tx, _rx) = watch::channel(ClusterNodes {
            masters: Vec::new(),
            replicas: Vec::new(),
        });
        let task = RedisOpTask::new(
            TaskId {
                cmd: "topology".to_string(),
                task: "wait-current-master".to_string(),
                host: "_local".to_string(),
            },
            vec!["127.0.0.1:1".to_string(), standby],
            "cluster topology".to_string(),
            tx,
            None,
            true,
        );

        let result = task
            .execute(TaskHost::Local, HashMap::default())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            result.get(CMD_STATUS),
            Some(&TaskArgValue::Number(0)),
            "topology readiness should pass when any startup node reports a master"
        );
        let output = result.get(CMD_OUTPUT).unwrap().to_string();
        assert!(
            output.contains("6389"),
            "expected current master in {output}"
        );
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeInfo {
    pub ip: String,
    pub port: u16,
    pub connected: bool,
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

// Implement Display for NodeInfo so we can easily convert it to string
impl fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.ip, self.port)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterNodes {
    pub masters: Vec<NodeInfo>,
    pub replicas: Vec<NodeInfo>,
}

const MAX_RETRIES: usize = 500;
const RETRY_DELAY: Duration = Duration::from_secs(2);
const TOPOLOGY_RETRIES: usize = 60;
const TOPOLOGY_RETRY_DELAY: Duration = Duration::from_secs(1);
const REDIS_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

async fn query_ckpt_status_with_retry(
    master: NodeInfo,
    trigger_ckpt_ts: u64,
    max_retries: usize,
    retry_delay: Duration,
    service_endpoints: Option<HashMap<String, ServiceEndpoint>>,
) -> Result<CheckCkptStatusResponse> {
    let ip = &master.ip;
    let port = master.port + 10000 + 1;
    let (endpoint_host, endpoint_port) =
        resolve_service_endpoint(service_endpoints.as_ref(), ip, port);
    let url = format!("http://{}:{}", endpoint_host, endpoint_port);
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
        Value::BulkString(bytes) => {
            // Extract the actual string data (handle quoted strings)
            let raw_str = String::from_utf8_lossy(&bytes).to_string();
            if raw_str.starts_with('"') && raw_str.ends_with('"') {
                // Remove the surrounding quotes
                raw_str[1..raw_str.len() - 1].to_string()
            } else {
                raw_str
            }
        }
        Value::SimpleString(s) => s,
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
        let connected = parts[7].eq_ignore_ascii_case("connected");
        let node_info = NodeInfo {
            ip,
            port,
            connected,
        };

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

/// Parse INFO SERVER output for single-node deployments.
/// Treats the node as master and extracts tcp_port; IP is derived from host:port string.
pub fn parse_cluster_nodes_single(value: Value, default_host: &str) -> RedisResult<ClusterNodes> {
    let info_str = match value {
        Value::BulkString(bytes) => String::from_utf8_lossy(&bytes).to_string(),
        Value::SimpleString(s) => s,
        _ => {
            return Err(RedisError::from((
                ErrorKind::TypeError,
                "Unexpected format for INFO SERVER",
                format!("{:?}", value),
            )))
        }
    };

    let mut host_parts = default_host.split(':');
    let ip = host_parts.next().unwrap_or(default_host).to_string();
    let fallback_port = host_parts.next().and_then(|p| p.parse::<u16>().ok());

    let mut port: Option<u16> = None;
    for line in info_str.lines() {
        if let Some(rest) = line.strip_prefix("tcp_port:") {
            port = rest.trim().parse::<u16>().ok();
            break;
        }
    }

    let port = match (port, fallback_port) {
        (Some(p), _) => p,
        (None, Some(p)) => p,
        (None, None) => {
            return Err(RedisError::from((
                ErrorKind::TypeError,
                "Could not determine tcp_port from INFO SERVER or host",
            )))
        }
    };

    Ok(ClusterNodes {
        masters: vec![NodeInfo {
            ip,
            port,
            connected: true,
        }],
        replicas: vec![],
    })
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

        // Fix: Split any comma-separated host:port entries
        let mut expanded_nodes = Vec::new();
        for host_port in &self.redis_host_ports {
            // Split by comma if present
            if host_port.contains(',') {
                let split_nodes: Vec<String> = host_port
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                expanded_nodes.extend(split_nodes);
            } else {
                expanded_nodes.push(host_port.clone());
            }
        }

        let mapped_nodes: Vec<String> = expanded_nodes
            .iter()
            .map(|host_port| self.service_host_port(host_port))
            .collect();

        let nodes: Vec<String> = mapped_nodes
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
        info!(
            "RedisOpTask: Attempting to connect to Redis cluster with nodes: [{}]",
            nodes_info
        );

        let is_single_node = expanded_nodes.len() == 1;

        // Use the command provided in redis_cmd
        let cmd_lower = self.task_id.cmd.to_lowercase();
        let mut last_error: Option<String> = None;
        let result = match (cmd_lower.as_str(), is_single_node) {
            ("topology", true) => {
                // Single-node: INFO SERVER via non-cluster client
                let single_url = if let Some(password) = &self.password {
                    format!("redis://:{}@{}", password, mapped_nodes[0])
                } else {
                    format!("redis://{}", mapped_nodes[0])
                };

                let single_client = match Client::open(single_url.clone()) {
                    Ok(c) => c,
                    Err(err) => {
                        error!(
                            "RedisOpTask: Failed to create single-node client for {}: {}",
                            single_url, err
                        );
                        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                        task_result
                            .insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(err.to_string()));
                        return Ok(Some(task_result));
                    }
                };

                let mut con = match single_client.get_connection_with_timeout(REDIS_CONNECT_TIMEOUT)
                {
                    Ok(c) => c,
                    Err(err) => {
                        error!(
                            "RedisOpTask: Cannot connect to single node {}: {}",
                            single_url, err
                        );
                        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                        task_result
                            .insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(err.to_string()));
                        return Ok(Some(task_result));
                    }
                };

                let mut query_result = redis::cmd("INFO").arg("SERVER").query::<Value>(&mut con);
                for attempt in 1..=TOPOLOGY_RETRIES {
                    if query_result.is_ok() {
                        break;
                    }
                    last_error = query_result.as_ref().err().map(ToString::to_string);
                    info!(
                        "RedisOpTask: topology query failed, retrying {attempt}/{TOPOLOGY_RETRIES}: {:?}",
                        last_error
                    );
                    sleep(TOPOLOGY_RETRY_DELAY).await;
                    query_result = redis::cmd("INFO").arg("SERVER").query::<Value>(&mut con);
                }
                drop(con);
                query_result
            }
            ("topology", false) => {
                // Multi-node readiness only needs one reachable seed that can report topology.
                // ClusterClient may fail during failover/restart while a direct CLUSTER NODES
                // query against the new master is already available.
                let mut query_result: RedisResult<Value> = Err(RedisError::from((
                    ErrorKind::IoError,
                    "cluster topology query did not run",
                )));
                for attempt in 1..=self.topology_retries {
                    for node_url in &nodes {
                        let client = match Client::open(node_url.clone()) {
                            Ok(client) => client,
                            Err(err) => {
                                last_error = Some(err.to_string());
                                continue;
                            }
                        };
                        let mut con =
                            match client.get_connection_with_timeout(REDIS_CONNECT_TIMEOUT) {
                                Ok(connection) => connection,
                                Err(err) => {
                                    last_error = Some(err.to_string());
                                    continue;
                                }
                            };
                        query_result = redis::cmd("CLUSTER").arg("NODES").query::<Value>(&mut con);
                        if query_result.is_ok() {
                            info!(
                                "RedisOpTask: Successfully queried Redis topology from {}",
                                node_url
                            );
                            break;
                        }
                        last_error = query_result.as_ref().err().map(ToString::to_string);
                    }
                    if query_result.is_ok() {
                        break;
                    }
                    info!(
                        "RedisOpTask: cluster topology query failed, retrying {attempt}/{}. Attempted nodes: [{}]. Last error: {:?}",
                        self.topology_retries,
                        nodes_info, last_error
                    );
                    if attempt < self.topology_retries {
                        sleep(TOPOLOGY_RETRY_DELAY).await;
                    }
                }
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
                let cluster_nodes = if is_single_node {
                    vec![parse_cluster_nodes_single(value, &expanded_nodes[0])?]
                } else {
                    parse_cluster_nodes(value)?
                };
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

                if unique_masters.is_empty() {
                    let msg = last_error.unwrap_or_else(|| {
                        "cluster topology query did not return any master".to_string()
                    });
                    task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                    task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(msg));
                    return Ok(Some(task_result));
                }

                // Continue with checkpoint tasks only if skip_checkpoint is false
                if !self.skip_checkpoint {
                    let mut trigger_ckpt_tasks = Vec::new();
                    for master in &unique_masters {
                        let ip = &master.ip;
                        let port = master.port + 10000 + 1;

                        let task = async move {
                            let url = self.grpc_url(ip, port);
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
                            self.service_endpoints.clone(),
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
