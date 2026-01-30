use crate::cli::task::backup_utils::{extract_all_manifests, extract_backup_ts, join_manifests};
use crate::cli::task::grpc::cc_request::ClusterBackupResponse;
use crate::cli::task::grpc::GrpcClient;
use crate::cli::task::redis_op_task::parse_cluster_nodes;
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::storage_service_config::DataStoreServiceBackend;
use crate::state::snapshot_info_operation::{SnapshotEntity, SnapshotOperation};
use crate::state::state_base::StateOperation;
use crate::state::state_mgr::{SNAPSHOT_STATUS_STATE, STATE_MGR};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use local_ip_address::local_ip;
use redis::cluster::ClusterClient;
use redis::Value;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::time::sleep;
use tracing::error;
use tracing::info;

#[derive(Clone, Debug)]
pub struct BackupConfig {
    pub path: String,
    pub snapshot_ts: DateTime<Utc>,
    pub password: Option<String>,
    pub dest_host: Option<String>,
    pub dest_user: Option<String>,
}

#[derive(Clone, Debug)]
pub struct BackupTask {
    task_id: TaskId,
    redis_host_ports: Vec<String>,
    cluster_name: String,
    back_up_config: BackupConfig,
}

impl BackupTask {
    pub fn new(
        task_id: TaskId,
        redis_host_ports: Vec<String>,
        cluster_name: String,
        config: BackupConfig,
    ) -> Self {
        Self {
            task_id,
            redis_host_ports,
            cluster_name,
            back_up_config: config,
        }
    }

    pub fn format_string(current_date_time: DateTime<Utc>) -> String {
        current_date_time.format("%Y-%m-%d-%H-%M-%S").to_string()
    }

    /// Check if the cluster uses EloqStore cloud storage
    async fn is_eloqstore_cloud(&self) -> bool {
        match STATE_MGR
            .load_deployment_from_state(&self.cluster_name)
            .await
        {
            Ok(Some(config)) => config
                .deployment
                .storage_service
                .as_ref()
                .map(|s| {
                    s.eloqdss
                        .as_ref()
                        .map(|dss| {
                            matches!(
                                dss.backend_config(),
                                DataStoreServiceBackend::EloqStore(eloq_config)
                                    if eloq_config.is_cloud_mode()
                            )
                        })
                        .unwrap_or(false)
                })
                .unwrap_or(false),
            _ => false,
        }
    }

    // save to sqlite inside eloqctl
    async fn save_snapshot_info(
        &self,
        current_date_time: DateTime<Utc>,
        status: i64,
        dest_host: String,
        dest_user: String,
        snapshot_path: String,
    ) {
        // Get the snapshot operation for state management
        let snapshot_operation =
            STATE_MGR.get_state_operation::<SnapshotOperation>(SNAPSHOT_STATUS_STATE);

        let put_rs = snapshot_operation
            .put(SnapshotEntity {
                cluster_name: self.cluster_name.clone(),
                snapshot_ts: current_date_time,
                snapshot_status: status,
                snapshot_path,
                dest_host,
                dest_user,
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
        info!("execute {}", self.task_id.format_string());

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
                if let Some(password) = &self.back_up_config.password {
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

        let result = redis::cmd("CLUSTER").arg("NODES").query::<Value>(&mut con);
        drop(con); // Close connection

        match result {
            Ok(value) => {
                let cluster_nodes = parse_cluster_nodes(value)?;
                let mut masters = HashSet::new();
                for slot in &cluster_nodes {
                    for master in &slot.masters {
                        masters.insert(master.clone());
                    }
                }

                // Collect tasks for concurrent execution
                let mut tasks = Vec::new();

                let (dest_host, dest_user) = if self.back_up_config.path.is_empty() {
                    // Cloud storage - use empty strings
                    (String::new(), String::new())
                } else if let (Some(dest_host), Some(dest_user)) = (
                    &self.back_up_config.dest_host,
                    &self.back_up_config.dest_user,
                ) {
                    // Both are Some, use the provided values
                    (dest_host.clone(), dest_user.clone())
                } else {
                    println!("dest_host or dest_user is None, use default values");
                    (local_ip().unwrap().to_string(), whoami::username())
                };
                println!("dest_host:{dest_host},dest_user:{dest_user}");

                // Construct snapshot path based on storage type
                let initial_snapshot_path = if self.back_up_config.path.is_empty() {
                    // Cloud storage - will be updated with manifest filename later
                    String::new()
                } else {
                    // Local storage - construct path
                    format!(
                        "{}/{}/{}",
                        self.back_up_config.path.clone(),
                        self.cluster_name.clone(),
                        Self::format_string(self.back_up_config.snapshot_ts)
                    )
                };

                self.save_snapshot_info(
                    self.back_up_config.snapshot_ts,
                    2,
                    dest_host.clone(),
                    dest_user.clone(),
                    initial_snapshot_path,
                )
                .await;

                for node in &masters {
                    // Clone variables for the async block
                    let node_ip = node.ip.clone();
                    let node_port = node.port + 10000 + 1;
                    let dest_host = dest_host.clone();
                    let dest_user = dest_user.clone();

                    let backup_name = format!(
                        "snapshot-{}-{}:{}-{}",
                        self.cluster_name.clone(),
                        node.ip,
                        node.port,
                        Self::format_string(self.back_up_config.snapshot_ts)
                    );

                    // Create the async task
                    let task = async move {
                        let url = format!("http://{}:{}", node_ip, node_port);
                        let mut grpc_client = GrpcClient::new(&url).await.map_err(|e| {
                            error!("Failed to create GrpcClient for {}: {}", url, e);
                            anyhow::anyhow!(e)
                        })?;

                        let dest_path = if self.back_up_config.path.is_empty() {
                            "s3".to_string() // Pass "s3" as dest_path for cloud storage
                        } else {
                            format!(
                                "{}/{}/{}",
                                self.back_up_config.path.clone(),
                                self.cluster_name.clone(),
                                Self::format_string(self.back_up_config.snapshot_ts)
                            )
                        };

                        let response = grpc_client
                            .trigger_backup(
                                backup_name.clone(),
                                dest_host.clone(),
                                dest_user.clone(),
                                dest_path,
                            )
                            .await
                            .map_err(|e| {
                                error!("Failed to trigger backup on {}: {}", url, e);
                                anyhow::anyhow!(e)
                            })?;

                        info!(
                            "Triggered backup on {}: backup_name={}, result={}",
                            url, backup_name, response.result
                        );
                        Ok::<(String, String, ClusterBackupResponse), anyhow::Error>((
                            url,
                            backup_name,
                            response,
                        ))
                    };

                    // Await the task and handle the result
                    match task.await {
                        Ok(result) => {
                            tasks.push(result);
                            break; // Break the loop on the first successful response
                        }
                        Err(e) => {
                            error!("Error triggering backup on node {}: {}", node.ip.clone(), e);
                            continue; // Move to the next node on error
                        }
                    }
                }

                let mut trigger_backup_succeed = true;
                let mut backup_finished = true;
                let mut task_url = String::new();
                let mut task_backup_name = String::new();
                let mut trigger_response: Option<ClusterBackupResponse> = None;

                // Assert that tasks is empty if all nodes failed
                if tasks.is_empty() {
                    error!("All nodes failed to trigger backup.");
                    trigger_backup_succeed = false;
                } else {
                    assert!(tasks.len() == 1);
                    // Process the successful result(s)
                    for (url, backup_name, response) in tasks {
                        // Your processing logic here
                        // For example:
                        task_url = url;
                        task_backup_name = backup_name;
                        trigger_response = Some(response.clone());
                        if response.result.to_lowercase() != "finished" {
                            backup_finished = false;
                        }
                        // You can collect task_ids or check if all responses are finished
                    }
                }

                if trigger_backup_succeed && backup_finished {
                    // Print full response message content
                    if let Some(ref response) = trigger_response {
                        println!("Backup finished. Response details:");
                        println!("  backup_name: {}", response.backup_name);
                        println!("  result: {}", response.result);
                        println!("  backup_infos count: {}", response.backup_infos.len());
                        for (idx, info) in response.backup_infos.iter().enumerate() {
                            println!(
                                "  backup_info[{}]: ng_id={}, backup_files={:?}, backup_ts={}, status={:?}",
                                idx, info.ng_id, info.backup_files, info.backup_ts, info.status
                            );
                        }
                        info!("Backup finished. Full response: {:#?}", response);
                    }

                    // Extract snapshot_path based on storage type
                    let snapshot_path = if dest_host.is_empty() {
                        // Cloud storage - check storage type and extract accordingly
                        let is_eloqstore = self.is_eloqstore_cloud().await;
                        trigger_response
                            .as_ref()
                            .filter(|r| r.result.to_lowercase() == "finished")
                            .map(|r| {
                                if is_eloqstore {
                                    // EloqStore: extract backup_ts
                                    extract_backup_ts(r).unwrap_or_default()
                                } else {
                                    // RocksDB: extract manifest filenames
                                    let all_manifests = extract_all_manifests(r);
                                    join_manifests(&all_manifests)
                                }
                            })
                            .unwrap_or_default()
                    } else {
                        // Local storage - use existing path construction (single path)
                        if self.back_up_config.path.is_empty() {
                            String::new()
                        } else {
                            format!(
                                "{}/{}/{}",
                                self.back_up_config.path.clone(),
                                self.cluster_name.clone(),
                                Self::format_string(self.back_up_config.snapshot_ts)
                            )
                        }
                    };

                    self.save_snapshot_info(
                        self.back_up_config.snapshot_ts,
                        0,
                        dest_host,
                        dest_user,
                        snapshot_path,
                    )
                    .await;

                    task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
                    task_result.insert(
                        CMD_OUTPUT.to_string(),
                        TaskArgValue::Str("All snapshots completed successfully".to_string()),
                    );
                } else if !trigger_backup_succeed {
                    let snapshot_path = if self.back_up_config.path.is_empty() {
                        String::new()
                    } else {
                        format!(
                            "{}/{}/{}",
                            self.back_up_config.path.clone(),
                            self.cluster_name.clone(),
                            Self::format_string(self.back_up_config.snapshot_ts)
                        )
                    };

                    self.save_snapshot_info(
                        self.back_up_config.snapshot_ts,
                        1,
                        dest_host,
                        dest_user,
                        snapshot_path,
                    )
                    .await;

                    task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                    task_result.insert(
                        "errors".to_string(),
                        TaskArgValue::Str("trigger backup failed".to_string()),
                    );
                    task_result.insert(
                        CMD_OUTPUT.to_string(),
                        TaskArgValue::Str("Failed to trigger backup on some masters".to_string()),
                    );
                } else {
                    let task = async move {
                        let url = task_url.clone();
                        let backup_name = task_backup_name.clone();
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
                                        break Ok(Some(response));
                                    } else if response.result.to_lowercase() == "running" {
                                        info!("Snapshot in progress for {}: {:#?}", url, response);
                                        // Wait before checking again
                                        sleep(Duration::from_secs(2)).await;
                                    } else {
                                        unreachable!()
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to query snapshot status for {}: {}", url, e);
                                    // You can decide to retry or fail immediately
                                    break Err(anyhow::anyhow!(e));
                                }
                            }
                        }
                    };

                    // Await the task and handle the result
                    match task.await {
                        Ok(final_response) => {
                            // Print full response message content
                            if let Some(ref response) = final_response {
                                println!("Backup finished. Response details:");
                                println!("  backup_name: {}", response.backup_name);
                                println!("  result: {}", response.result);
                                println!("  backup_infos count: {}", response.backup_infos.len());
                                for (idx, info) in response.backup_infos.iter().enumerate() {
                                    println!(
                                        "  backup_info[{}]: ng_id={}, backup_files={:?}, backup_ts={}, status={:?}",
                                        idx, info.ng_id, info.backup_files, info.backup_ts, info.status
                                    );
                                }
                                info!("Backup finished. Full response: {:#?}", response);
                            }

                            // Extract manifest filename from response for cloud storage
                            // final_response is only Some when status is "finished" (from line 414-416)
                            let snapshot_path = if dest_host.is_empty() {
                                // Cloud storage - check storage type and extract accordingly
                                let is_eloqstore = self.is_eloqstore_cloud().await;
                                final_response
                                    .as_ref()
                                    .filter(|r| r.result.to_lowercase() == "finished")
                                    .map(|r| {
                                        if is_eloqstore {
                                            // EloqStore: extract backup_ts
                                            extract_backup_ts(r).unwrap_or_default()
                                        } else {
                                            // RocksDB: extract manifest filenames
                                            let all_manifests = extract_all_manifests(r);
                                            join_manifests(&all_manifests)
                                        }
                                    })
                                    .unwrap_or_default()
                            } else {
                                // Local storage - use existing path construction (single path)
                                format!(
                                    "{}/{}/{}",
                                    self.back_up_config.path.clone(),
                                    self.cluster_name.clone(),
                                    Self::format_string(self.back_up_config.snapshot_ts)
                                )
                            };

                            self.save_snapshot_info(
                                self.back_up_config.snapshot_ts,
                                0,
                                dest_host,
                                dest_user,
                                snapshot_path,
                            )
                            .await;

                            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
                            task_result.insert(
                                CMD_OUTPUT.to_string(),
                                TaskArgValue::Str(
                                    "All snapshots completed successfully".to_string(),
                                ),
                            );
                        }
                        Err(e) => {
                            error!("Error backup failed");
                            let snapshot_path = if self.back_up_config.path.is_empty() {
                                String::new()
                            } else {
                                format!(
                                    "{}/{}/{}",
                                    self.back_up_config.path.clone(),
                                    self.cluster_name.clone(),
                                    Self::format_string(self.back_up_config.snapshot_ts)
                                )
                            };

                            self.save_snapshot_info(
                                self.back_up_config.snapshot_ts,
                                1,
                                dest_host,
                                dest_user,
                                snapshot_path,
                            )
                            .await;

                            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                            task_result
                                .insert("errors".to_string(), TaskArgValue::Str(e.to_string()));
                            task_result.insert(
                                CMD_OUTPUT.to_string(),
                                TaskArgValue::Str("Backup failed on one or more nodes".to_string()),
                            );
                        }
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
