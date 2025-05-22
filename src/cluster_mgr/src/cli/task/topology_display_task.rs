use crate::cli::task::task_base::TaskExecutor;
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskHost, TaskId, TaskInstance};
use crate::cli::SubCommand;
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::state::state_mgr::STATE_MGR;
use crate::state::topology_log_operation::TopologyLogEntity;
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

        let mut grouped_tx: HashMap<i32, Vec<TopologyTxEntity>> = HashMap::new();
        for entity in tx_entities.clone() {
            grouped_tx
                .entry(entity.node_group_id)
                .or_insert_with(Vec::new)
                .push(entity);
        }

        let mut group_ids: Vec<i32> = grouped_tx.keys().cloned().collect();
        group_ids.sort();

        // Store the last group ID before entering the loop
        let last_group_id = *group_ids.last().unwrap_or(&-1);

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

                // Add a blank row as separator between node groups (except after the last group)
                if group_id != last_group_id {
                    table.add_row(Row::new(vec![
                        Cell::new(""),
                        Cell::new(""),
                        Cell::new(""),
                        Cell::new(""),
                        Cell::new(""),
                    ]));
                }
            }
        }

        let mut output = format!(
            "\nCluster TX Topology for {}:\n{}",
            self.cluster_name,
            table.to_string()
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
                    "Data Dirs"
                ]);
                for ent in log_entities {
                    log_table.add_row(Row::new(vec![
                        Cell::new(&ent.node_group_id.to_string()),
                        Cell::new(&ent.node_id),
                        Cell::new(&ent.host),
                        Cell::new(&ent.port.to_string()),
                        Cell::new(ent.data_dirs.as_deref().unwrap_or("")),
                    ]));
                }
                output.push_str(&format!(
                    "\n\nLog Service Topology for {}:\n{}",
                    self.cluster_name,
                    log_table.to_string()
                ));
            }
        }

        info!("Successfully displayed topology information");
        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
        task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(output));

        Ok(Some(task_result))
    }
}
