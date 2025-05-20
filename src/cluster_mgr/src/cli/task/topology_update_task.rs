use crate::cli::task::redis_op_task::ClusterNodes;
use crate::cli::task::task_base::TaskExecutor;
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskHost, TaskId, TaskInstance};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeployConfig;
use crate::state::state_base::StateOperation;
use crate::state::state_mgr::{STATE_MGR, TOPOLOGY_LOG_STATE, TOPOLOGY_TX_STATE};
use crate::state::topology_log_operation::{TopologyLogEntity, TopologyLogOperation};
use crate::state::topology_tx_operation::{TopologyTxEntity, TopologyTxOperation};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use std::collections::HashMap;
use tokio::sync::watch;
use tracing::{error, info};

// Update topology in t_topology_tx using live data from RedisOpTask
#[derive(Debug, Clone)]
pub struct TopologyUpdateFromRedisTask {
    task_id: TaskId,
    cluster_name: String,
    config: DeployConfig,
    receiver: watch::Receiver<ClusterNodes>,
}

impl TopologyUpdateFromRedisTask {
    /// Create tasks to update topology using data from a RedisOpTask channel
    pub fn from_redis(
        config: &DeployConfig,
        receiver: watch::Receiver<ClusterNodes>,
    ) -> IndexMap<TaskId, TaskInstance> {
        let mut map = IndexMap::new();
        let task_id = TaskId {
            cmd: "topology-update".to_string(),
            task: "redis".to_string(),
            host: "local".to_string(),
        };
        let task = Box::new(TopologyUpdateFromRedisTask {
            task_id: task_id.clone(),
            cluster_name: config.deployment.cluster_name.clone(),
            config: config.clone(),
            receiver,
        });
        map.insert(
            task_id.clone(),
            TaskInstance {
                task_input: HashMap::new(),
                task,
                task_host: TaskHost::Local,
            },
        );
        map
    }

    // Extract TX entries from DeployConfig
    fn extract_voter_topology(&self) -> Vec<TopologyTxEntity> {
        let mut tx_entities = Vec::new();
        let cluster_name = &self.config.deployment.cluster_name;
        let now = Utc::now();
        // Q? if voters are auto designated, how can we know this info? need info from tx_service?
        // Voters
        if let Some(voter_hosts) = &self.config.deployment.tx_service.voter_host_ports {
            for (i, host_port) in voter_hosts.iter().enumerate() {
                for entry in host_port.split(['|', ',']) {
                    if let Some((host, port_str)) = entry.split_once(':') {
                        if let Ok(port) = port_str.parse::<i32>() {
                            tx_entities.push(TopologyTxEntity {
                                cluster_name: cluster_name.clone(),
                                node_group_count: 0,
                                node_group_id: 0,
                                node_id: format!("voter-{}", i),
                                role: 2,
                                host: host.to_string(),
                                port,
                                create_timestamp: now,
                                update_timestamp: now,
                            });
                        }
                    }
                }
            }
        }
        tx_entities
    }

    // Extract log service entries from DeployConfig
    fn extract_log_topology(&self) -> Vec<TopologyLogEntity> {
        let mut log_entities = Vec::new();
        let cluster_name = &self.config.deployment.cluster_name;
        let now = Utc::now();
        if let Some(log_service) = &self.config.deployment.log_service {
            let log_nodes = &log_service.nodes;
            let log_count = log_nodes.len() as i32;
            for (i, node) in log_nodes.iter().enumerate() {
                log_entities.push(TopologyLogEntity {
                    cluster_name: cluster_name.clone(),
                    node_group_count: log_count,
                    node_group_id: i as i32,
                    node_id: format!("log-{}", i),
                    host: node.host.clone(),
                    port: node.port as i32,
                    data_dirs: Some(node.data_dir.join(",")),
                    create_timestamp: now,
                    update_timestamp: now,
                });
            }
        }
        log_entities
    }
}

#[async_trait]
impl TaskExecutor for TopologyUpdateFromRedisTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        _task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> Result<Option<ExecutionValue>> {
        let mut task_result = HashMap::new();
        task_result.insert(
            CMD.to_string(),
            TaskArgValue::Str("topology-update".to_string()),
        );

        info!(
            "Updating topology information for cluster: {}",
            self.config.deployment.cluster_name
        );

        let mut status_rx = self.receiver.clone();

        // Wait for RedisOpTask to send the updated cluster nodes
        if let Err(e) = status_rx.changed().await {
            let msg = format!("Failed to receive cluster nodes from channel: {}", e);
            error!("{}", msg);
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
            task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(msg));
            return Ok(Some(task_result));
        }
        let cluster_nodes = self.receiver.borrow().clone();
        let now = Utc::now();

        let tx_op = STATE_MGR.get_state_operation::<TopologyTxOperation>(TOPOLOGY_TX_STATE);
        let log_op = STATE_MGR.get_state_operation::<TopologyLogOperation>(TOPOLOGY_LOG_STATE);

        let mut success_count = 0;
        let mut failure_count = 0;

        // Update voter entries
        let voter_entries = self.extract_voter_topology();
        for tx_entity in voter_entries {
            match tx_op.put(tx_entity.clone()).await {
                Ok(_) => {
                    success_count += 1;
                    info!(
                        "Updated TX node: {}:{} in group {}",
                        tx_entity.host, tx_entity.port, tx_entity.node_group_id
                    );
                }
                Err(e) => {
                    failure_count += 1;
                    error!(
                        "Failed TX update for node {}:{} group {}: {}",
                        tx_entity.host, tx_entity.port, tx_entity.node_group_id, e
                    );
                }
            }
        }

        // Update log entries
        let log_entries = self.extract_log_topology();
        for log_entity in log_entries {
            match log_op.put(log_entity.clone()).await {
                Ok(_) => {
                    success_count += 1;
                    info!(
                        "Updated LOG node: {}:{} in group {}",
                        log_entity.host, log_entity.port, log_entity.node_group_id
                    );
                }
                Err(e) => {
                    failure_count += 1;
                    error!(
                        "Failed LOG update for node {}:{} group {}: {}",
                        log_entity.host, log_entity.port, log_entity.node_group_id, e
                    );
                }
            }
        }

        // Update master entries
        let master_count = cluster_nodes.masters.len() as i32;
        for master in cluster_nodes.masters.iter() {
            let entity = TopologyTxEntity {
                cluster_name: self.cluster_name.clone(),
                node_group_count: master_count,
                node_group_id: 0,
                node_id: master.to_string(),
                role: 0,
                host: master.ip.clone(),
                port: master.port as i32,
                create_timestamp: now,
                update_timestamp: now,
            };
            match tx_op.put(entity).await {
                Ok(_) => success_count += 1,
                Err(e) => {
                    failure_count += 1;
                    error!("Failed TX update for master {}: {}", master, e);
                }
            }
        }

        // Update replica entries
        let replica_count = cluster_nodes.replicas.len() as i32;
        for replica in cluster_nodes.replicas.iter() {
            let entity = TopologyTxEntity {
                cluster_name: self.cluster_name.clone(),
                node_group_count: replica_count,
                node_group_id: 1,
                node_id: replica.to_string(),
                role: 1,
                host: replica.ip.clone(),
                port: replica.port as i32,
                create_timestamp: now,
                update_timestamp: now,
            };
            match tx_op.put(entity).await {
                Ok(_) => success_count += 1,
                Err(e) => {
                    failure_count += 1;
                    error!("Failed TX update for replica {}: {}", replica, e);
                }
            }
        }

        let output = format!(
            "Topology update from Redis completed for cluster {}. Updated {} entries, {} failures.",
            self.cluster_name, success_count, failure_count
        );
        info!("{}", output);
        let status = if failure_count > 0 { 1 } else { 0 };
        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(status));
        task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(output));
        Ok(Some(task_result))
    }
}
