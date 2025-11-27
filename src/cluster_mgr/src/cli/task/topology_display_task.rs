use crate::cli::task::task_base::TaskExecutor;
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskHost, TaskId, TaskInstance};
use crate::cli::task::task_utils::NodeGroupId;
use crate::cli::SubCommand;
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::storage_service_config::RocksDB;
use crate::state::state_mgr::STATE_MGR;
use crate::state::topology_tx_operation::TopologyTxEntity;
use anyhow::Result;
use async_trait::async_trait;
use indexmap::IndexMap;
use prettytable::{format, row, Cell, Row, Table};
use std::collections::HashMap;
use tracing::{error, info};

#[derive(Debug, Clone)]
pub struct TopologyDisplayTask {
    task_id: TaskId,
    cluster_name: String,
}

impl TopologyDisplayTask {
    pub fn new(task_id: TaskId, cluster_name: String) -> Self {
        Self {
            task_id,
            cluster_name,
        }
    }

    pub fn from_command(command: SubCommand) -> IndexMap<TaskId, TaskInstance> {
        let mut executable = IndexMap::new();

        if let SubCommand::Status {
            cluster, detail, ..
        } = command
        {
            if detail {
                let task_id = TaskId {
                    cmd: "topology-display".to_string(),
                    task: "status".to_string(),
                    host: "local".to_string(),
                };
                let task = Box::new(TopologyDisplayTask::new(task_id.clone(), cluster));
                executable.insert(
                    task_id.clone(),
                    TaskInstance {
                        task_input: HashMap::default(),
                        task,
                        task_host: TaskHost::Local,
                    },
                );
            }
        }

        executable
    }
}

#[async_trait]
impl TaskExecutor for TopologyDisplayTask {
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
            TaskArgValue::Str("topology-display".to_string()),
        );

        info!(
            "Displaying topology information for cluster: {}",
            self.cluster_name
        );

        // Load TX and log topology from state
        let tx_entities_res = STATE_MGR
            .load_topology_tx_from_state(self.cluster_name.clone())
            .await;
        let log_entities_res = STATE_MGR
            .load_topology_log_from_state(self.cluster_name.clone())
            .await;

        // Handle TX load result
        let tx_entities = match tx_entities_res {
            Ok(entities) => {
                if entities.is_empty() {
                    let message = format!(
                        "No TX topology information found for cluster {}",
                        self.cluster_name
                    );
                    info!("{}", message);
                    task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
                    task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(message));
                    return Ok(Some(task_result));
                }
                entities
            }
            Err(e) => {
                let error_msg = format!("Failed to load TX topology information: {}", e);
                error!("{}", error_msg);
                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(error_msg));
                return Ok(Some(task_result));
            }
        };

        // Build TX table
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
        table.set_titles(row!["Node Group ID", "Node ID", "Host", "Port", "Role",]);

        let mut grouped_tx: HashMap<NodeGroupId, Vec<TopologyTxEntity>> = HashMap::new();
        for entity in tx_entities.clone() {
            grouped_tx
                .entry(entity.node_group_id)
                .or_default()
                .push(entity);
        }

        let mut group_ids: Vec<NodeGroupId> = grouped_tx.keys().cloned().collect();
        group_ids.sort();

        for group_id in group_ids {
            if let Some(nodes) = grouped_tx.get_mut(&group_id) {
                // Sort nodes within each group by role: Master(0) -> Replica(1) -> Voter(2)
                nodes.sort_by_key(|node| node.role);

                for node in nodes {
                    // Interpret role: 0=Master,1=Replica,2=Voter
                    let role = match node.role {
                        0 => "Master",
                        1 => "Replica",
                        2 => "Voter",
                        _ => "Unknown",
                    };

                    table.add_row(Row::new(vec![
                        Cell::new(&node.node_group_id.to_string()),
                        Cell::new(&node.node_id.to_string()),
                        Cell::new(&node.host),
                        Cell::new(&node.port.to_string()),
                        Cell::new(role),
                    ]));
                }
            }
        }

        let mut output = format!(
            "\n\nCluster TX Topology for {}:\n{}",
            self.cluster_name, table
        );

        // Append log service info if available
        if let Ok(log_entities) = log_entities_res {
            if !log_entities.is_empty() {
                let mut log_table = Table::new();
                log_table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
                log_table.set_titles(row![
                    "Log Group ID",
                    "Log Node ID",
                    "Host",
                    "Port",
                    "Role",
                    "Data Dirs"
                ]);

                // Sort by group id then node id
                let mut sorted_logs = log_entities;
                sorted_logs.sort_by(|a, b| {
                    a.node_group_id
                        .cmp(&b.node_group_id)
                        .then_with(|| a.node_id.cmp(&b.node_id))
                });

                let mut current_group: Option<u32> = None;
                for ent in sorted_logs {
                    let role = if current_group != Some(ent.node_group_id) {
                        current_group = Some(ent.node_group_id);
                        "Leader"
                    } else {
                        "Replica"
                    };

                    log_table.add_row(Row::new(vec![
                        Cell::new(&ent.node_group_id.to_string()),
                        Cell::new(&ent.node_id.to_string()),
                        Cell::new(&ent.host),
                        Cell::new(&ent.port.to_string()),
                        Cell::new(role),
                        Cell::new(ent.data_dirs.as_deref().unwrap_or("")),
                    ]));
                }
                output.push_str(&format!(
                    "\n\nLog Service Topology for {}:\n{}",
                    self.cluster_name, log_table
                ));
            }
        }

        // Append DSS service host/port if configured (RocksDBCloud or DataStoreService Remote mode)
        if let Ok(Some(deploy_cfg)) = STATE_MGR
            .load_deployment_from_state(self.cluster_name.as_str())
            .await
        {
            if let Some(storage) = &deploy_cfg.deployment.storage_service {
                if let Some(RocksDB::EloqDssRocksdb(dss_cfg)) = &storage.rocksdb {
                    if !dss_cfg.peer_host_ports.is_empty() {
                        let mut dss_table = Table::new();
                        dss_table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
                        dss_table.set_titles(row!["Host", "Port"]);

                        for hp in &dss_cfg.peer_host_ports {
                            if let Some((h, p)) = hp.split_once(':') {
                                dss_table.add_row(Row::new(vec![Cell::new(h), Cell::new(p)]));
                            }
                        }

                        output.push_str(&format!(
                            "\n\nDSS Service for {}:\n{}",
                            self.cluster_name, dss_table
                        ));
                    }
                }

                // Display DSS for DataStoreService Remote mode
                if let Some(ds_service) = &storage.eloqdss {
                    if ds_service.is_remote_mode() {
                        if let Some(peer_ports) = ds_service.peer_host_ports.as_ref() {
                            if !peer_ports.is_empty() {
                                let mut dss_table = Table::new();
                                dss_table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
                                dss_table.set_titles(row!["Host", "Port"]);

                                for hp in peer_ports {
                                    if let Some((h, p)) = hp.split_once(':') {
                                        dss_table
                                            .add_row(Row::new(vec![Cell::new(h), Cell::new(p)]));
                                    }
                                }

                                output.push_str(&format!(
                                    "\n\nDSS Service (DataStoreService) for {}:\n{}",
                                    self.cluster_name, dss_table
                                ));
                            }
                        }
                    }
                }
            }
        }

        info!("Successfully displayed topology information");
        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
        task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(output));

        Ok(Some(task_result))
    }
}
