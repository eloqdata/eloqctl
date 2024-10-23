use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::task_return_value;
use async_trait::async_trait;
use redis::cluster::ClusterClient;
use redis::{ErrorKind, FromRedisValue, RedisError, RedisResult, Value};
use serde::Serialize;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use tokio::sync::watch;
use tracing::{error, info};

#[derive(Clone, Debug)]
pub struct RedisOpTask {
    task_id: TaskId,
    redis_host_ports: Vec<String>,
    redis_cmd: String,
    sender: watch::Sender<ClusterNodes>,
}

impl RedisOpTask {
    pub fn new(
        task_id: TaskId,
        redis_host_ports: Vec<String>,
        redis_cmd: String,
        sender: watch::Sender<ClusterNodes>,
    ) -> Self {
        Self {
            task_id,
            redis_host_ports,
            redis_cmd,
            sender,
        }
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

fn parse_cluster_slots(value: Value) -> RedisResult<Vec<ClusterNodes>> {
    // Ensure the top-level value is an array
    let slots_array = match value {
        Value::Bulk(slots) => slots,
        _ => {
            return Err(RedisError::from((
                ErrorKind::TypeError,
                "Expected bulk array for slots",
            )))
        }
    };

    let mut cluster_slots = Vec::new();

    for slot_value in slots_array {
        // Each slot is an array
        let slot_info = match slot_value {
            Value::Bulk(slot_info) => slot_info,
            _ => {
                return Err(RedisError::from((
                    ErrorKind::TypeError,
                    "Expected bulk array in slot info",
                )))
            }
        };

        // Extract start_slot, end_slot, master, replicas
        if slot_info.len() < 3 {
            return Err(RedisError::from((
                ErrorKind::TypeError,
                "Slot info array too short",
            )));
        }

        // Master node info
        let mut masters = Vec::new();
        let master_node_info = parse_node_info(&slot_info[2])?;
        masters.push(master_node_info);

        // Replicas node info
        let mut replicas = Vec::new();
        for replica_value in &slot_info[3..] {
            let replica_node_info = parse_node_info(replica_value)?;
            replicas.push(replica_node_info);
        }

        let cluster_slot = ClusterNodes { masters, replicas };

        cluster_slots.push(cluster_slot);
    }

    Ok(cluster_slots)
}

fn parse_node_info(value: &Value) -> RedisResult<NodeInfo> {
    // Node info is an array
    let node_info = match value {
        Value::Bulk(node_info) => node_info,
        _ => {
            return Err(RedisError::from((
                ErrorKind::TypeError,
                "Expected bulk array in node info",
            )))
        }
    };

    if node_info.len() < 2 {
        return Err(RedisError::from((
            ErrorKind::TypeError,
            "Node info array too short",
        )));
    }

    let ip = String::from_redis_value(&node_info[0])?;
    let port = u16::from_redis_value(&node_info[1])?;
    let node_info = NodeInfo { ip, port };

    Ok(node_info)
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
        let mut task_result = HashMap::new();
        task_result.insert(CMD.to_string(), TaskArgValue::Str(self.redis_cmd.clone()));

        // Use a vector of node addresses to create a ClusterClient, the ClusterClient will handle the case where if a connection to one server is failed, move to another one in the cluster.
        let nodes: Vec<String> = self
            .redis_host_ports
            .iter()
            .map(|host_port| format!("redis://{}", host_port))
            .collect();
        let nodes_info: String = nodes.clone().join(", ");

        // username and password version:
        // let nodes: Vec<String> = self
        //     .redis_host_ports
        //     .iter()
        //     .map(|host_port| format!("redis://{}:{}@{}", username, password, host_port))
        //     .collect();

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
                let query_result = redis::cmd("CLUSTER").arg("SLOTS").query::<Value>(&mut con);

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
                // Parse the cluster slots
                let cluster_nodes = parse_cluster_slots(value)?;

                // Use HashSet to collect unique masters and replicas
                use std::collections::HashSet;

                let mut masters = HashSet::new();
                let mut replicas = HashSet::new();

                for slot in &cluster_nodes {
                    for master in &slot.masters {
                        masters.insert(master.clone());
                    }
                    for replica in &slot.replicas {
                        replicas.insert(replica.clone());
                    }
                }

                // Convert HashSets to Vectors
                let unique_masters: Vec<NodeInfo> = masters.into_iter().collect();
                let unique_replicas: Vec<NodeInfo> = replicas.into_iter().collect();

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

                // Send the cluster_nodes to the receiver
                if let Err(err) = self.sender.send(cluster_nodes) {
                    error!("Failed to send cluster nodes: {}", err);
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
