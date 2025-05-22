use crate::cli::task::task_base::{
    ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::{upload_dir, CMD, CMD_OUTPUT, CMD_STATUS};
use crate::state::state_base::{QueryCondition, StateOperation};
use crate::state::state_mgr::{STATE_MGR, TOPOLOGY_TX_STATE};
use crate::state::topology_tx_operation::{ConfigJson, TopologyTxEntity, TopologyTxOperation};
use crate::StateValue;
use anyhow::{Context, Result};
use async_trait::async_trait;
use indexmap::IndexMap;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tracing::{error, info};

#[derive(Debug, Clone)]
pub struct IniConfigUpdateTask {
    task_id: TaskId,
    cluster_name: String,
    fields: Option<Vec<String>>,
    nodes: Option<Vec<String>>, // Optional list of nodes to update (format: "host:port")
}

impl IniConfigUpdateTask {
    /// Create tasks to update INI configuration fields
    pub fn new(
        cluster_name: &str,
        fields: Option<Vec<String>>,
        nodes: Option<Vec<String>>,
    ) -> IndexMap<TaskId, TaskInstance> {
        let mut map = IndexMap::new();
        let task_id = TaskId {
            cmd: "ini-config-update".to_string(),
            task: "update-fields".to_string(),
            host: "local".to_string(),
        };
        let task = Box::new(IniConfigUpdateTask {
            task_id: task_id.clone(),
            cluster_name: cluster_name.to_string(),
            fields,
            nodes,
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

    // Parse fields in format "field_1:value_1,field_2:value_2"
    fn parse_fields(&self) -> HashMap<String, String> {
        let mut result = HashMap::new();
        if let Some(fields) = &self.fields {
            for field_str in fields {
                if let Some((key, value)) = field_str.split_once(':') {
                    result.insert(key.trim().to_string(), value.trim().to_string());
                }
            }
        }
        result
    }

    // Parse INI content to ConfigJson
    fn parse_ini_to_config_json(&self, ini_content: &str) -> Result<ConfigJson> {
        let mut config = ConfigJson {
            eloq_data_path: String::new(),
            enable_data_store: false,
            enable_wal: false,
        };

        for line in ini_content.lines() {
            let line = line.trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }

            // Parse key-value pairs
            if let Some((key, value)) = line.split_once('=') {
                let key = key.trim();
                let value = value.trim();

                match key {
                    "eloq_data_path" => {
                        config.eloq_data_path = value.to_string();
                    }
                    "enable_data_store" => {
                        config.enable_data_store = value.to_lowercase() == "true"
                            || value == "1"
                            || value.to_lowercase() == "yes";
                    }
                    "enable_wal" => {
                        config.enable_wal = value.to_lowercase() == "true"
                            || value == "1"
                            || value.to_lowercase() == "yes";
                    }
                    // Add more fields as needed
                    _ => {}
                }
            }
        }

        Ok(config)
    }

    // Convert ConfigJson back to INI format
    fn config_json_to_ini(&self, config: &ConfigJson) -> String {
        let mut result = String::new();

        // Add all fields
        result.push_str(&format!("eloq_data_path = {}\n", config.eloq_data_path));
        result.push_str(&format!(
            "enable_data_store = {}\n",
            config.enable_data_store
        ));
        result.push_str(&format!("enable_wal = {}\n", config.enable_wal));

        result
    }

    // Update configuration for a specific entity with field updates
    async fn update_entity_config(
        &self,
        tx_op: &Arc<&TopologyTxOperation>,
        entity: &mut TopologyTxEntity,
        field_updates: &HashMap<String, String>,
    ) -> Result<bool> {
        let mut updated = false;

        // Apply field updates to ConfigJson structure
        for (field, value) in field_updates {
            match field.as_str() {
                "eloq_data_path" => {
                    entity.ini_config.eloq_data_path = value.clone();
                    updated = true;
                }
                "enable_data_store" => {
                    let bool_value = value.to_lowercase() == "true"
                        || value == "1"
                        || value.to_lowercase() == "yes";
                    entity.ini_config.enable_data_store = bool_value;
                    updated = true;
                }
                "enable_wal" => {
                    let bool_value = value.to_lowercase() == "true"
                        || value == "1"
                        || value.to_lowercase() == "yes";
                    entity.ini_config.enable_wal = bool_value;
                    updated = true;
                }
                // Add more fields as needed
                _ => {
                    info!("Ignoring unknown field: {}", field);
                }
            }
        }

        if updated {
            // Save the updated entity back to the database
            match tx_op.put(entity.clone()).await {
                Ok(_) => {
                    info!(
                        "Updated config for node {}:{} in group {}",
                        entity.host, entity.port, entity.node_group_id
                    );
                    Ok(true)
                }
                Err(e) => {
                    error!(
                        "Failed to update config for node {}:{} in group {}: {}",
                        entity.host, entity.port, entity.node_group_id, e
                    );
                    Err(anyhow::anyhow!(e))
                }
            }
        } else {
            // No changes were made
            info!(
                "No changes needed for node {}:{} in group {}",
                entity.host, entity.port, entity.node_group_id
            );
            Ok(false)
        }
    }

    // Set initial configuration from file
    async fn set_initial_config_from_file(
        &self,
        tx_op: &Arc<&TopologyTxOperation>,
        entities: &mut Vec<TopologyTxEntity>,
    ) -> Result<(usize, usize)> {
        let mut success_count = 0;
        let mut failure_count = 0;

        // Check for INI files in upload directory
        let upload_path = upload_dir().join(&self.cluster_name);
        let ini_path = upload_path.join("Eloqkv.ini");

        if ini_path.exists() {
            info!("Found INI file at {}", ini_path.display());
            let ini_content = fs::read_to_string(&ini_path)
                .with_context(|| format!("Failed to read INI file: {}", ini_path.display()))?;

            let config = self.parse_ini_to_config_json(&ini_content)?;

            // Update all nodes with this configuration
            for entity in entities.iter_mut() {
                entity.ini_config = config.clone();

                match tx_op.put(entity.clone()).await {
                    Ok(_) => {
                        success_count += 1;
                        info!(
                            "Updated INI config for node {}:{} in group {}",
                            entity.host, entity.port, entity.node_group_id
                        );
                    }
                    Err(e) => {
                        failure_count += 1;
                        error!(
                            "Failed to update INI config for node {}:{} in group {}: {}",
                            entity.host, entity.port, entity.node_group_id, e
                        );
                    }
                }
            }

            // Also update the local INI file if needed
            let new_ini_content = self.config_json_to_ini(&config);
            if let Err(e) = fs::write(&ini_path, new_ini_content) {
                error!("Failed to write updated INI file: {}", e);
            } else {
                info!("Updated local INI file at {}", ini_path.display());
            }
        } else {
            info!(
                "No INI file found at {}, skipping update",
                ini_path.display()
            );
            success_count = entities.len();
        }

        Ok((success_count, failure_count))
    }
}

#[async_trait]
impl TaskExecutor for IniConfigUpdateTask {
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
            TaskArgValue::Str("ini-config-update".to_string()),
        );

        info!(
            "Updating INI configuration for cluster: {}",
            self.cluster_name
        );

        let field_updates = self.parse_fields();
        if field_updates.is_empty() && self.fields.is_some() {
            let msg =
                "No valid field updates provided. Format should be field_1:value_1,field_2:value_2"
                    .to_string();
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
            task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(msg));
            return Ok(Some(task_result));
        }

        let tx_op = STATE_MGR.get_state_operation::<TopologyTxOperation>(TOPOLOGY_TX_STATE);

        let mut success_count = 0;
        let mut failure_count = 0;

        // Query nodes from database
        let mut entities = if let Some(specific_nodes) = &self.nodes {
            let mut all_entities = Vec::new();

            // Process each node to be updated
            for node_str in specific_nodes {
                if let Some((host, port_str)) = node_str.split_once(':') {
                    if let Ok(port) = port_str.parse::<i32>() {
                        match tx_op
                            .load(|| {
                                let cond_text =
                                    "cluster_name = ? AND host = ? AND port = ?".to_string();
                                let bind_values = vec![
                                    StateValue::Varchar(self.cluster_name.clone()),
                                    StateValue::Varchar(host.to_string()),
                                    StateValue::Integer(port),
                                ];
                                Some(QueryCondition {
                                    cond_text,
                                    bind_values,
                                })
                            })
                            .await
                        {
                            Ok(entities) => all_entities.extend(entities),
                            Err(e) => {
                                error!("Failed to query node {}:{}: {}", host, port, e);
                                failure_count += 1;
                            }
                        }
                    }
                }
            }
            all_entities
        } else {
            // Query all nodes for the cluster
            match tx_op
                .load(|| {
                    let cond_text = "cluster_name = ?".to_string();
                    let bind_values = vec![StateValue::Varchar(self.cluster_name.clone())];
                    Some(QueryCondition {
                        cond_text,
                        bind_values,
                    })
                })
                .await
            {
                Ok(e) => e,
                Err(e) => {
                    let msg = format!("Failed to query database: {}", e);
                    error!("{}", msg);
                    task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                    task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(msg));
                    return Ok(Some(task_result));
                }
            }
        };

        if entities.is_empty() {
            let msg = format!("No nodes found for cluster {}", self.cluster_name);
            info!("{}", msg);
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
            task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(msg));
            return Ok(Some(task_result));
        }

        // If we're not updating fields but just ensuring INI files are in the database
        if field_updates.is_empty() {
            let (success, failure) = self
                .set_initial_config_from_file(&tx_op, &mut entities)
                .await?;
            success_count = success;
            failure_count = failure;
        } else {
            // Apply field updates to each entity
            for entity in &mut entities {
                match self
                    .update_entity_config(&tx_op, entity, &field_updates)
                    .await
                {
                    Ok(true) => {
                        success_count += 1;
                    }
                    Ok(false) => {
                        // No changes needed
                        success_count += 1;
                    }
                    Err(_) => {
                        failure_count += 1;
                    }
                }
            }

            // Also update the local INI file if it exists
            let upload_path = upload_dir().join(&self.cluster_name);
            let ini_path = upload_path.join("Eloqkv.ini");

            if ini_path.exists() && !entities.is_empty() {
                // Use the first entity's config to update the local file
                let ini_content = self.config_json_to_ini(&entities[0].ini_config);
                if let Err(e) = fs::write(&ini_path, ini_content) {
                    error!("Failed to write updated INI file: {}", e);
                } else {
                    info!("Updated local INI file at {}", ini_path.display());
                }
            }
        }

        let output = format!(
            "INI config update completed for cluster {}. Updated {} nodes, {} failures.",
            self.cluster_name, success_count, failure_count
        );
        info!("{}", output);
        let status = if failure_count > 0 { 1 } else { 0 };
        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(status));
        task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(output));
        Ok(Some(task_result))
    }
}
