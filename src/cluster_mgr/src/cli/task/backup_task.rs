use crate::cli::task::grpc::GrpcClient;
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::state::snapshot_info_operation::{SnapshotEntity, SnapshotOperation};
use crate::state::state_base::StateOperation;
use crate::state::state_mgr::{SNAPSHOT_STATUS_STATE, STATE_MGR};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::join_all;
use local_ip_address::local_ip;
use redis::cluster::ClusterClient;
use redis::{ErrorKind, FromRedisValue, RedisError, RedisResult, Value};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::time::sleep;
use tracing::error;
use tracing::info;

#[derive(Clone, Debug)]
pub struct BackupTask {
    task_id: TaskId,
    redis_host_ports: Vec<String>,
    cluster_name: String,
    path: String,
    snapshot_ts: DateTime<Utc>,
    password: Option<String>,
    dest_host: Option<String>,
    dest_user: Option<String>,
}

impl BackupTask {
    pub fn new(
        task_id: TaskId,
        redis_host_ports: Vec<String>,
        cluster_name: String,
        path: String,
        snapshot_ts: DateTime<Utc>,
        password: Option<String>,
        dest_host: Option<String>,
        dest_user: Option<String>,
    ) -> Self {
        Self {
            task_id,
            redis_host_ports,
            cluster_name,
            path,
            snapshot_ts,
            password,
            dest_host,
            dest_user,
        }
    }

    pub fn pretty_string(current_date_time: DateTime<Utc>) -> String {
        current_date_time.format("%Y-%m-%d-%H-%M-%S").to_string()
    }

    // save to sqlite inside eloqctl
    async fn save_snapshot_info(
        &self,
        current_date_time: DateTime<Utc>,
        status: i64,
        dest_host: String,
        dest_user: String,
    ) {
        // Get the snapshot operation for state management
        let snapshot_operation =
            STATE_MGR.get_state_operation::<SnapshotOperation>(SNAPSHOT_STATUS_STATE);

        let put_rs = snapshot_operation
            .put(SnapshotEntity {
                cluster_name: self.cluster_name.clone(),
                snapshot_ts: current_date_time.into(),
                snapshot_status: status,
                snapshot_path: format!(
                    "{}/{}/{}",
                    self.path.clone(),
                    self.cluster_name.clone(),
                    Self::pretty_string(current_date_time)
                ),
                dest_host: dest_host,
                dest_user: dest_user,
            })
            .await;

        // Handle potential error in saving snapshot info
        if let Err(put_err) = put_rs {
            panic!("Failed to write snapshot info to database: {:?}", put_err);
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

#[async_trait]
impl TaskExecutor for BackupTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        // it is a local task that trigger a rpc call
        match task_host {
            TaskHost::Local => {}
            _ => unreachable!(),
        }

        let mut task_result = HashMap::new();
        task_result.insert(
            CMD.to_string(),
            TaskArgValue::Str(self.task_id.format_string()),
        );

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
        let mut con = match client.get_connection() {
            Ok(connection) => connection,
            Err(err) => {
                println!(
                    "Cannot connect to the cluster. Attempted nodes: [{}]. Error: {}",
                    nodes_info, err
                );
                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(err.to_string()));
                return Ok(Some(task_result));
            }
        };

        let result = redis::cmd("CLUSTER").arg("SLOTS").query::<Value>(&mut con);
        drop(con); // Close connection

        match result {
            Ok(value) => {
                let cluster_nodes = parse_cluster_slots(value)?;
                let mut masters = HashSet::new();
                for slot in &cluster_nodes {
                    for master in &slot.masters {
                        masters.insert(master.clone());
                    }
                }

                // Collect tasks for concurrent execution
                let mut tasks = Vec::new();

                let (dest_host, dest_user) = if let (Some(dest_host), Some(dest_user)) =
                    (&self.dest_host, &self.dest_user)
                {
                    // Both are Some, use the provided values
                    (dest_host.clone(), dest_user.clone())
                } else {
                    println!("dest_host or dest_user is None, use default values");
                    (local_ip().unwrap().to_string(), whoami::username())
                };
                println!("dest_host:{dest_host},dest_user:{dest_user}");

                self.save_snapshot_info(
                    self.snapshot_ts.clone(),
                    2,
                    dest_host.clone(),
                    dest_user.clone(),
                )
                .await;

                for node in &masters {
                    // clone for async move
                    let node = node.clone();
                    let dest_host = dest_host.clone();
                    let dest_user = dest_user.clone();

                    let backup_name = format!(
                        "snapshot-{}-{}:{}-{}",
                        self.cluster_name.clone(),
                        node.ip,
                        node.port + 10000 + 1,
                        Self::pretty_string(self.snapshot_ts)
                    );
                    let task = async move {
                        let url = format!("http://{}:{}", node.ip, node.port + 10000 + 1);
                        let mut grpc_client = GrpcClient::new(&url).await.map_err(|e| {
                            error!("Failed to create GrpcClient for {}: {}", url, e);
                            anyhow::anyhow!(e)
                        })?;

                        let response = grpc_client
                            .trigger_snapshot(
                                backup_name.clone(),
                                dest_host.clone(),
                                dest_user.clone(),
                                format!(
                                    "{}/{}/{}",
                                    self.path.clone(),
                                    self.cluster_name.clone(),
                                    Self::pretty_string(self.snapshot_ts)
                                ),
                            )
                            .await
                            .map_err(|e| {
                                error!("Failed to trigger snapshot on {}: {}", url, e);
                                anyhow::anyhow!(e)
                            })?;

                        info!(
                            "Triggered snapshot on {}: backup_name={}, result={}",
                            url, backup_name, response.result
                        );
                        Ok::<(String, String, String), anyhow::Error>((
                            url,
                            backup_name,
                            response.result,
                        ))
                    };
                    tasks.push(task);
                }

                // Execute all tasks concurrently
                let results = join_all(tasks).await;

                let mut success = true;
                let mut task_ids = Vec::new();
                let mut errors = Vec::new();
                let mut all_responses_finished = true;

                for result in results {
                    match result {
                        Ok((url, backup_name, response_result)) => {
                            task_ids.push((url, backup_name));
                            if response_result.to_lowercase() != "finished" {
                                all_responses_finished = false;
                            }
                        }
                        Err(e) => {
                            success = false;
                            errors.push(e.to_string());
                        }
                    }
                }

                if success && all_responses_finished {
                    self.save_snapshot_info(self.snapshot_ts.clone(), 0, dest_host, dest_user)
                        .await;

                    task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
                    task_result.insert(
                        CMD_OUTPUT.to_string(),
                        TaskArgValue::Str("All snapshots completed successfully".to_string()),
                    );
                } else if !success {
                    self.save_snapshot_info(self.snapshot_ts.clone(), 1, dest_host, dest_user)
                        .await;

                    task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                    task_result.insert("errors".to_string(), TaskArgValue::Str(errors.join("; ")));
                    task_result.insert(
                        CMD_OUTPUT.to_string(),
                        TaskArgValue::Str("Failed to trigger snapshot on some masters".to_string()),
                    );
                } else {
                    let mut status_check_tasks = Vec::new();
                    for (url, backup_name) in &task_ids {
                        let url = url.clone();
                        let task = async move {
                            let mut grpc_client = match GrpcClient::new(&url).await {
                                Ok(client) => client,
                                Err(e) => {
                                    error!("Failed to create GrpcClient for {}: {}", url, e);
                                    return Err(anyhow::anyhow!(e));
                                }
                            };

                            loop {
                                match grpc_client
                                    .query_snapshot_status(backup_name.to_string())
                                    .await
                                {
                                    Ok(response) => {
                                        if response.result.to_lowercase() == "finished" {
                                            info!("Snapshot finished for {}: {:#?}", url, response);
                                            break Ok((url.clone(), true));
                                        } else {
                                            info!(
                                                "Snapshot in progress for {}: {:#?}",
                                                url, response
                                            );
                                            // Wait before checking again
                                            sleep(Duration::from_secs(2)).await;
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to query snapshot status for {}: {}",
                                            url, e
                                        );
                                        // You can decide to retry or fail immediately
                                        break Err(anyhow::anyhow!(e));
                                    }
                                }
                            }
                        };
                        status_check_tasks.push(task);
                    }
                    // Execute all status check tasks concurrently
                    let status_results = join_all(status_check_tasks).await;

                    let mut all_finished = true;
                    let mut status_errors = Vec::new();

                    for result in status_results {
                        match result {
                            Ok((_url, _finished)) => {
                                // Do nothing, already printed in the task
                            }
                            Err(e) => {
                                all_finished = false;
                                status_errors.push(e.to_string());
                            }
                        }
                    }

                    if all_finished {
                        self.save_snapshot_info(self.snapshot_ts.clone(), 0, dest_host, dest_user)
                            .await;

                        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
                        task_result.insert(
                            CMD_OUTPUT.to_string(),
                            TaskArgValue::Str("All snapshots completed successfully".to_string()),
                        );
                    } else {
                        self.save_snapshot_info(self.snapshot_ts.clone(), 1, dest_host, dest_user)
                            .await;

                        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                        task_result.insert(
                            "errors".to_string(),
                            TaskArgValue::Str(status_errors.join("; ")),
                        );
                        task_result.insert(
                            CMD_OUTPUT.to_string(),
                            TaskArgValue::Str(
                                "Some snapshots failed or did not complete successfully"
                                    .to_string(),
                            ),
                        );
                    }
                }

                Ok(Some(task_result))
            }
            Err(err) => {
                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(err.to_string()));
                Ok(Some(task_result))
            }
        }
    }
}
