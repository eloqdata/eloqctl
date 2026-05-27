use crate::cli::task::cluster_config_utils::parse_cluster_config;
use crate::cli::task::config_fields::field_exists;
use crate::cli::task::redis_op_task::ClusterNodes;
use crate::cli::task::task_base::TaskExecutor;
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskHost, TaskId, TaskInstance};
use crate::cli::task::task_utils::{parse_ng_config, NodeId};
use crate::cli::{upload_dir, CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::{DeployConfig, SCALED_CLUSTER_CONFIG};
use crate::state::state_base::{QueryCondition, StateOperation};
use crate::state::state_mgr::{STATE_MGR, TOPOLOGY_LOG_STATE, TOPOLOGY_TX_STATE};
use crate::state::topology_log_operation::{TopologyLogEntity, TopologyLogOperation};
use crate::state::topology_tx_operation::{ConfigJson, TopologyTxEntity, TopologyTxOperation};
use crate::StateValue;
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tokio::sync::watch;
use tracing::{error, info};

// Update topology in t_topology_tx using live data from RedisOpTask, then update ini files in upload dir
#[derive(Debug, Clone)]
pub struct TopologyUpdateTask {
    task_id: TaskId,
    cluster_name: String,
    config: DeployConfig,
    receiver: watch::Receiver<ClusterNodes>,
    remove_nodes: Option<Vec<String>>, // Optional list of nodes to be removed (format: "host:port")
    tx_node_id: Option<NodeId>,        // Optional node ID to update config for
    field_updates: Vec<String>,        // Field updates in "field:value" format
}

impl TopologyUpdateTask {
    /// Create tasks to update topology using data from a RedisOpTask channel
    pub fn from_redis(
        config: &DeployConfig,
        receiver: watch::Receiver<ClusterNodes>,
        remove_nodes: Option<Vec<String>>,
    ) -> IndexMap<TaskId, TaskInstance> {
        let mut map = IndexMap::new();
        let task_id = TaskId {
            cmd: "topology-update".to_string(),
            task: "redis".to_string(),
            host: "local".to_string(),
        };
        let task = Box::new(TopologyUpdateTask {
            task_id: task_id.clone(),
            cluster_name: config.deployment.cluster_name.clone(),
            config: config.clone(),
            receiver,
            remove_nodes,
            tx_node_id: None,
            field_updates: Vec::new(), // Empty vector for this use case
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

    /// Create tasks to update configuration for a specific node
    pub fn for_config_update(
        config: &DeployConfig,
        receiver: watch::Receiver<ClusterNodes>,
        tx_node_id: NodeId,
        field_updates: Vec<String>,
    ) -> IndexMap<TaskId, TaskInstance> {
        let mut map = IndexMap::new();
        let task_id = TaskId {
            cmd: "topology-update".to_string(),
            task: "config-update".to_string(),
            host: "local".to_string(),
        };
        let task = Box::new(TopologyUpdateTask {
            task_id: task_id.clone(),
            cluster_name: config.deployment.cluster_name.clone(),
            config: config.clone(),
            receiver,
            remove_nodes: None,
            tx_node_id: Some(tx_node_id),
            field_updates,
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

    /// Create tasks to update configuration for all nodes
    pub fn for_all_nodes_config_update(
        config: &DeployConfig,
        field_updates: Vec<String>,
    ) -> IndexMap<TaskId, TaskInstance> {
        let mut map = IndexMap::new();
        let task_id = TaskId {
            cmd: "topology-update".to_string(),
            task: "config-update-all-nodes".to_string(),
            host: "local".to_string(),
        };

        // Create empty receiver channel - not used for full update
        let (_, rx) = watch::channel(ClusterNodes {
            masters: Vec::new(),
            replicas: Vec::new(),
        });

        let task = Box::new(TopologyUpdateTask {
            task_id: task_id.clone(),
            cluster_name: config.deployment.cluster_name.clone(),
            config: config.clone(),
            receiver: rx,
            remove_nodes: None,
            tx_node_id: None, // No specific node ID
            field_updates,
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

    // Update ConfigJson based on field updates
    fn update_config_json(&self, mut config: ConfigJson, field_updates: &[String]) -> ConfigJson {
        for field_update in field_updates {
            if let Some((field, value)) = field_update.split_once(':') {
                match field {
                    // These fields are explicitly defined in ConfigJson
                    "eloq_data_path" => {
                        config.eloq_data_path = value.to_string();
                        info!("Updated eloq_data_path to {}", value);
                    }
                    "enable_data_store" => {
                        if let Ok(enable) = value.parse::<bool>() {
                            config.enable_data_store = enable;
                            info!("Updated enable_data_store to {}", enable);
                        } else {
                            error!("Invalid boolean value for enable_data_store: {}", value);
                        }
                    }
                    "enable_wal" => {
                        if let Ok(enable) = value.parse::<bool>() {
                            config.enable_wal = enable;
                            info!("Updated enable_wal to {}", enable);
                        } else {
                            error!("Invalid boolean value for enable_wal: {}", value);
                        }
                    }
                    "enable_io_uring" => {
                        if let Ok(enable) = value.parse::<bool>() {
                            config.enable_io_uring = enable;
                            info!("Updated enable_io_uring to {}", enable);
                        } else {
                            error!("Invalid boolean value for enable_io_uring: {}", value);
                        }
                    }
                    "checkpoint_interval" | "checkpointer_interval" => {
                        if let Ok(interval) = value.parse::<u32>() {
                            config.checkpoint_interval = Some(interval);
                            info!("Updated checkpointer_interval to {}", interval);
                        } else {
                            error!("Invalid integer value for checkpointer_interval: {}", value);
                        }
                    }
                    "enable_cache_replacement" => {
                        if let Ok(enable) = value.parse::<bool>() {
                            config.enable_cache_replacement = Some(enable);
                            info!("Updated enable_cache_replacement to {}", enable);
                        } else {
                            error!(
                                "Invalid boolean value for enable_cache_replacement: {}",
                                value
                            );
                        }
                    }
                    // All other fields go into additional_settings
                    _ => {
                        if field_exists(field) {
                            config
                                .additional_settings
                                .insert(field.to_string(), value.to_string());
                            info!("Updated {} to {}", field, value);
                        } else {
                            error!("Unknown configuration field: {}", field);
                        }
                    }
                }
            }
        }
        config
    }

    // Parse INI content to ConfigJson
    fn parse_ini_to_config_json(&self, ini_content: &str) -> Result<ConfigJson> {
        let mut config = ConfigJson {
            eloq_data_path: String::new(),
            enable_data_store: false,
            enable_wal: false,
            enable_io_uring: false,
            checkpoint_interval: None,
            enable_cache_replacement: None,
            additional_settings: std::collections::HashMap::new(),
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
                    "enable_io_uring" => {
                        config.enable_io_uring = value.to_lowercase() == "true"
                            || value == "1"
                            || value.to_lowercase() == "yes";
                    }
                    "checkpoint_interval" | "checkpointer_interval" => {
                        if let Ok(interval) = value.parse::<u32>() {
                            config.checkpoint_interval = Some(interval);
                        }
                    }
                    "enable_cache_replacement" => {
                        let enable = value.to_lowercase() == "true"
                            || value == "1"
                            || value.to_lowercase() == "yes";
                        config.enable_cache_replacement = Some(enable);
                    }
                    // Store any other fields in additional_settings
                    _ => {
                        config
                            .additional_settings
                            .insert(key.to_string(), value.to_string());
                    }
                }
            }
        }

        Ok(config)
    }

    // Load ConfigJson for a specific host and port from INI files
    async fn load_config_from_ini_file(&self, host: &str, port: u16) -> Result<ConfigJson> {
        let tx_op = STATE_MGR.get_state_operation::<TopologyTxOperation>(TOPOLOGY_TX_STATE);

        // Try to find existing config in database first
        match tx_op
            .load(|| {
                let cond_text = "cluster_name = ? AND host = ? AND port = ?".to_string();
                let bind_values = vec![
                    StateValue::Varchar(self.cluster_name.clone()),
                    StateValue::Varchar(host.to_string()),
                    StateValue::Integer(port as i32),
                ];
                Some(QueryCondition {
                    cond_text,
                    bind_values,
                })
            })
            .await
        {
            Ok(entities) if !entities.is_empty() => {
                info!(
                    "Found existing configuration for {}:{} in database",
                    host, port
                );
                return Ok(entities[0].ini_config.clone());
            }
            _ => {}
        }

        // Look for node-specific INI file
        let upload_path = upload_dir().join(&self.cluster_name);
        let host_dir = upload_path.join(host);

        if host_dir.exists() {
            // Look for port-specific INI files (EloqKv-node-{port}.ini)
            if let Ok(entries) = fs::read_dir(&host_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_file() && path.extension().is_some_and(|ext| ext == "ini") {
                        if let Some(file_name) = path.file_name() {
                            if let Some(file_name_str) = file_name.to_str() {
                                // Look for a file with the specific port
                                if file_name_str.contains(&format!("node-{}", port)) {
                                    info!("Found port-specific INI file: {}", path.display());
                                    if let Ok(content) = fs::read_to_string(&path) {
                                        return self.parse_ini_to_config_json(&content);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Fall back to default config if no matching file found
        info!(
            "No specific INI file found for {}:{}, using default",
            host, port
        );
        Ok(ConfigJson {
            eloq_data_path: format!("/home/eloq/{}/EloqKV/data/port-{}", self.cluster_name, port),
            enable_data_store: true,
            enable_wal: false,
            enable_io_uring: false,
            checkpoint_interval: None,
            enable_cache_replacement: None,
            additional_settings: std::collections::HashMap::new(),
        })
    }

    // Try to get topology from cluster_config file
    async fn get_topology_from_cluster_config_file(&self) -> Result<Option<Vec<TopologyTxEntity>>> {
        let config_path = upload_dir()
            .join(&self.cluster_name)
            .join(SCALED_CLUSTER_CONFIG);
        let path = Path::new(&config_path);

        if !path.exists() {
            info!("Cluster config file not found at {}", config_path.display());
            return Ok(None);
        }

        info!("Found cluster config file at {}", config_path.display());
        let content = fs::read_to_string(path).with_context(|| {
            format!(
                "Failed to read cluster config file: {}",
                config_path.display()
            )
        })?;

        // Parse the config file using the proper parser from cluster_config_utils
        let cluster_config = parse_cluster_config(&content).with_context(|| {
            format!(
                "Failed to parse cluster config from {}",
                config_path.display()
            )
        })?;

        // Transform ClusterGroupConfig into TopologyTxEntity objects
        let mut tx_entities = Vec::new();
        let now = Utc::now();
        let cluster_name = &self.config.deployment.cluster_name;
        let node_group_count = cluster_config.node_groups.len() as u32;

        for (&ng_id, nodes) in &cluster_config.node_groups {
            for node in nodes {
                // Load INI file from upload directory to store in entities
                let config_json = self
                    .load_config_from_ini_file(&node.host_name, node.port)
                    .await?;

                // Determine the role based on is_candidate flag
                // 1 = Replica, 2 = Voter
                let role = if node.is_candidate { 1 } else { 2 };

                tx_entities.push(TopologyTxEntity {
                    cluster_name: cluster_name.clone(),
                    node_group_count,
                    node_group_id: ng_id,
                    node_id: node.node_id,
                    role,
                    host: node.host_name.clone(),
                    port: node.port,
                    ini_config: config_json,
                    create_timestamp: now,
                    update_timestamp: now,
                });
            }
        }

        info!(
            "Parsed cluster config with {} node groups and {} nodes, version {}",
            node_group_count,
            tx_entities.len(),
            cluster_config.version
        );

        Ok(Some(tx_entities))
    }

    // Extract TX entries from node group config parsed by parse_ng_config
    async fn extract_tx_topology_from_ini_file(&self) -> Result<Vec<TopologyTxEntity>> {
        let mut tx_entities = Vec::new();
        let port_delta: u16 = 10000;
        let cluster_name = &self.config.deployment.cluster_name;
        let now = Utc::now();

        // Get the configuration strings from the deployment config
        let tx_ip_port_list = self.config.deployment.tx_service.tx_host_ports.join(",");
        let standby_ip_port_list = self
            .config
            .deployment
            .tx_service
            .standby_host_ports
            .as_ref()
            .map_or("".to_string(), |hosts| hosts.join(","));
        let voter_ip_port_list = self
            .config
            .deployment
            .tx_service
            .voter_host_ports
            .as_ref()
            .map_or("".to_string(), |hosts| hosts.join(","));

        // Parse node group configuration
        match parse_ng_config(
            &tx_ip_port_list,
            &standby_ip_port_list,
            &voter_ip_port_list,
            Some(port_delta),
        ) {
            Ok(ng_configs) => {
                let node_group_count = ng_configs.len() as u32;

                // Process each node group
                for (ng_id, nodes) in ng_configs.iter() {
                    // Process each node in the group
                    for node in nodes {
                        // Determine the role based on is_candidate flag
                        // 1 = Replica (Standby), 2 = Voter
                        // All is_candidate=true nodes are set as replicas at this step
                        let role = if node.is_candidate { 1 } else { 2 };

                        let port = node.port - port_delta;

                        // Load configuration specific to this host and port
                        let config_json = self.load_config_from_ini_file(&node.ip, port).await?;

                        tx_entities.push(TopologyTxEntity {
                            cluster_name: cluster_name.clone(),
                            node_group_count,
                            node_group_id: *ng_id,
                            node_id: node.node_id,
                            role,
                            host: node.ip.clone(),
                            port,
                            ini_config: config_json,
                            create_timestamp: now,
                            update_timestamp: now,
                        });
                    }
                }

                info!(
                    "Parsed node group configuration with {} groups and {} nodes",
                    node_group_count,
                    tx_entities.len()
                );
            }
            Err(err) => {
                unreachable!("Failed to parse node group configuration: {}", err);
            }
        }

        Ok(tx_entities)
    }

    // Extract log service entries from DeployConfig
    fn extract_log_topology(&self) -> Vec<TopologyLogEntity> {
        let mut log_entities = Vec::new();
        let cluster_name = &self.config.deployment.cluster_name;
        let now = Utc::now();
        if let Some(log_service) = &self.config.deployment.log_service {
            // Get the actual log group members from the log service grouping algorithm
            let log_group_members = log_service.group_member_as_vec();
            let log_count = log_service.nodes.len() as u32;

            // Create a mapping from host:port to log group member info
            let mut host_port_to_member = std::collections::HashMap::new();
            for member in &log_group_members {
                let key = format!("{}:{}", member.member_host, member.port);
                host_port_to_member.insert(key, member);
            }

            // Create topology entities using the actual group IDs
            for node in &log_service.nodes {
                let key = format!("{}:{}", node.host, node.port);
                if let Some(member) = host_port_to_member.get(&key) {
                    log_entities.push(TopologyLogEntity {
                        cluster_name: cluster_name.clone(),
                        node_group_count: log_count,
                        node_group_id: member.group_id as u32, // Use the actual group ID from the grouping algorithm
                        node_id: member.node_id as u32,
                        host: node.host.clone(),
                        port: node.port,
                        data_dirs: Some(node.data_dir.join(",")),
                        create_timestamp: now,
                        update_timestamp: now,
                    });
                }
            }
        }
        log_entities
    }

    // Write ConfigJson back to INI file
    async fn write_config_to_ini_file(
        &self,
        host: &str,
        port: i32,
        config: &ConfigJson,
    ) -> Result<bool> {
        let upload_path = upload_dir().join(&self.cluster_name);
        let host_dir = upload_path.join(host);

        if host_dir.exists() {
            // Look for the specific INI file to update
            if let Ok(entries) = fs::read_dir(&host_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_file() && path.extension().is_some_and(|ext| ext == "ini") {
                        if let Some(file_name) = path.file_name() {
                            if let Some(file_name_str) = file_name.to_str() {
                                // Find the port-specific INI file
                                if file_name_str.contains(&format!("node-{}", port)) {
                                    // Found the file, now update only the specific fields
                                    info!("Updating INI file: {}", path.display());

                                    // Update the file preserving other content
                                    self.update_existing_ini_file(&path, config)?;

                                    info!("Successfully updated INI file at {}", path.display());
                                    return Ok(true);
                                }
                            }
                        }
                    }
                }
            }

            // If we didn't find a specific file but the host directory exists,
            // create a new INI file for this port
            let new_file_path = host_dir.join(format!("EloqKv-node-{}.ini", port));
            info!("Creating new INI file: {}", new_file_path.display());
            let ini_content = self.config_json_to_ini(config);

            fs::write(&new_file_path, ini_content).with_context(|| {
                format!("Failed to create new INI file: {}", new_file_path.display())
            })?;

            info!(
                "Successfully created new INI file at {}",
                new_file_path.display()
            );
            Ok(true)
        } else {
            // Create the host directory if it doesn't exist
            info!("Creating host directory: {}", host_dir.display());
            fs::create_dir_all(&host_dir).with_context(|| {
                format!("Failed to create host directory: {}", host_dir.display())
            })?;

            // Create a new INI file for this port
            let new_file_path = host_dir.join(format!("EloqKv-node-{}.ini", port));
            info!("Creating new INI file: {}", new_file_path.display());
            let ini_content = self.config_json_to_ini(config);

            fs::write(&new_file_path, ini_content).with_context(|| {
                format!("Failed to create new INI file: {}", new_file_path.display())
            })?;

            info!(
                "Successfully created new INI file at {}",
                new_file_path.display()
            );
            Ok(true)
        }
    }

    // Update an existing INI file, preserving all non-modified fields
    fn update_existing_ini_file(&self, file_path: &Path, config: &ConfigJson) -> Result<()> {
        // Read the existing file
        let content = fs::read_to_string(file_path)
            .with_context(|| format!("Failed to read INI file: {}", file_path.display()))?;

        // Track which fields we've updated
        let mut updated_fields = std::collections::HashMap::new();
        updated_fields.insert("eloq_data_path".to_string(), false);
        updated_fields.insert("enable_data_store".to_string(), false);
        updated_fields.insert("enable_wal".to_string(), false);
        updated_fields.insert("enable_io_uring".to_string(), false);
        updated_fields.insert("checkpointer_interval".to_string(), false);
        updated_fields.insert("enable_cache_replacement".to_string(), false);

        // Track additional settings
        for key in config.additional_settings.keys() {
            updated_fields.insert(key.clone(), false);
        }

        // Process line by line, updating our specific fields
        let mut updated_lines = Vec::new();

        for line in content.lines() {
            let line = line.trim();

            if line.is_empty()
                || line.starts_with('#')
                || line.starts_with(';')
                || line.starts_with('[')
            {
                // Keep comments, section headers, and empty lines intact
                updated_lines.push(line.to_string());
                continue;
            }

            // Check if this line is a field we want to update
            if let Some((key, _)) = line.split_once('=') {
                let key = key.trim();

                match key {
                    "eloq_data_path" => {
                        updated_lines.push(format!("eloq_data_path={}", config.eloq_data_path));
                        updated_fields.insert("eloq_data_path".to_string(), true);
                    }
                    "enable_data_store" => {
                        updated_lines
                            .push(format!("enable_data_store={}", config.enable_data_store));
                        updated_fields.insert("enable_data_store".to_string(), true);
                    }
                    "enable_wal" => {
                        updated_lines.push(format!("enable_wal={}", config.enable_wal));
                        updated_fields.insert("enable_wal".to_string(), true);
                    }
                    "enable_io_uring" => {
                        updated_lines.push(format!("enable_io_uring={}", config.enable_io_uring));
                        updated_fields.insert("enable_io_uring".to_string(), true);
                    }
                    "checkpoint_interval" | "checkpointer_interval" => {
                        if let Some(interval) = config.checkpoint_interval {
                            updated_lines.push(format!("checkpointer_interval={}", interval));
                            updated_fields.insert("checkpointer_interval".to_string(), true);
                        } else {
                            // Keep original line if no value provided
                            updated_lines.push(line.to_string());
                        }
                    }
                    "enable_cache_replacement" => {
                        if let Some(enable) = config.enable_cache_replacement {
                            updated_lines.push(format!("enable_cache_replacement={}", enable));
                            updated_fields.insert("enable_cache_replacement".to_string(), true);
                        } else {
                            // Keep original line if no value provided
                            updated_lines.push(line.to_string());
                        }
                    }
                    _ => {
                        // Check if it's in our additional settings
                        if let Some(value) = config.additional_settings.get(key) {
                            updated_lines.push(format!("{}={}", key, value));
                            updated_fields.insert(key.to_string(), true);
                        } else {
                            // Keep other fields unchanged
                            updated_lines.push(line.to_string());
                        }
                    }
                }
            } else {
                // Keep any non key-value lines intact
                updated_lines.push(line.to_string());
            }
        }

        // Add any fields that weren't found in the original file
        let local_section = updated_lines.iter().position(|l| l == "[local]");
        let insert_position = local_section.map(|pos| pos + 1).unwrap_or(0);

        // Check basic fields
        if !updated_fields["eloq_data_path"] {
            updated_lines.insert(
                insert_position,
                format!("eloq_data_path={}", config.eloq_data_path),
            );
        }

        if !updated_fields["enable_data_store"] {
            updated_lines.insert(
                insert_position,
                format!("enable_data_store={}", config.enable_data_store),
            );
        }

        if !updated_fields["enable_wal"] {
            updated_lines.insert(insert_position, format!("enable_wal={}", config.enable_wal));
        }

        // Add enable_io_uring if present and not already updated
        if !updated_fields["enable_io_uring"] {
            updated_lines.insert(
                insert_position,
                format!("enable_io_uring={}", config.enable_io_uring),
            );
        }

        // Add checkpointer_interval if present and not already updated
        if let Some(interval) = config.checkpoint_interval {
            if !updated_fields["checkpointer_interval"] {
                updated_lines.insert(
                    insert_position,
                    format!("checkpointer_interval={}", interval),
                );
            }
        }

        // Add enable_cache_replacement if present and not already updated
        if let Some(enable) = config.enable_cache_replacement {
            if !updated_fields["enable_cache_replacement"] {
                updated_lines.insert(
                    insert_position,
                    format!("enable_cache_replacement={}", enable),
                );
            }
        }

        // Check additional settings
        for (key, value) in &config.additional_settings {
            if !updated_fields.get(key).unwrap_or(&false) {
                updated_lines.insert(insert_position, format!("{}={}", key, value));
            }
        }

        // Write the updated content back to file
        let updated_content = updated_lines.join("\n");
        fs::write(file_path, updated_content).with_context(|| {
            format!("Failed to write updated INI file: {}", file_path.display())
        })?;

        Ok(())
    }

    // Convert ConfigJson to INI format
    fn config_json_to_ini(&self, config: &ConfigJson) -> String {
        let mut result = String::new();

        // Create a basic minimal INI file with just our tracked fields
        result.push_str("[local]\n");
        result.push_str(&format!("eloq_data_path={}\n", config.eloq_data_path));
        result.push_str(&format!("enable_data_store={}\n", config.enable_data_store));
        result.push_str(&format!("enable_wal={}\n", config.enable_wal));
        result.push_str(&format!("enable_io_uring={}\n", config.enable_io_uring));

        // Add checkpointer_interval and enable_cache_replacement if present
        if let Some(interval) = config.checkpoint_interval {
            result.push_str(&format!("checkpointer_interval={}\n", interval));
        }

        if let Some(enable) = config.enable_cache_replacement {
            result.push_str(&format!("enable_cache_replacement={}\n", enable));
        }

        // Add any additional settings
        for (key, value) in &config.additional_settings {
            result.push_str(&format!("{}={}\n", key, value));
        }

        result
    }
}

#[async_trait]
impl TaskExecutor for TopologyUpdateTask {
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

        let now = Utc::now();

        let tx_op = STATE_MGR.get_state_operation::<TopologyTxOperation>(TOPOLOGY_TX_STATE);
        let log_op = STATE_MGR.get_state_operation::<TopologyLogOperation>(TOPOLOGY_LOG_STATE);

        // Handle node-specific configuration update if tx_node_id is provided
        if !self.field_updates.is_empty() {
            if let Some(node_id) = &self.tx_node_id {
                info!(
                    "Updating configuration for node ID {} in cluster {}",
                    node_id, self.cluster_name
                );

                // Query the database for all entities matching this node_id
                match tx_op
                    .load(|| {
                        let cond_text = "cluster_name = ? AND node_id = ?".to_string();
                        let bind_values = vec![
                            StateValue::Varchar(self.cluster_name.clone()),
                            StateValue::Integer(*node_id as i32),
                        ];
                        Some(QueryCondition {
                            cond_text,
                            bind_values,
                        })
                    })
                    .await
                {
                    Ok(entities) => {
                        if entities.is_empty() {
                            let msg = format!(
                                "Node with ID {} not found in cluster {}",
                                node_id, self.cluster_name
                            );
                            error!("{}", msg);
                            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                            task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(msg));
                            return Ok(Some(task_result));
                        }

                        info!(
                            "Found {} entries with node ID {} in cluster {}",
                            entities.len(),
                            node_id,
                            self.cluster_name
                        );

                        let mut success_count = 0;
                        let mut failure_count = 0;

                        // Process all matching entities
                        for mut entity in entities {
                            // Update the configuration based on field updates
                            let updated_config = self
                                .update_config_json(entity.ini_config.clone(), &self.field_updates);
                            entity.ini_config = updated_config.clone();
                            entity.update_timestamp = now;

                            // Save the updated entity back to the database
                            match tx_op.put(entity.clone()).await {
                                Ok(_) => {
                                    // Now also update the INI file in the upload directory
                                    match self
                                        .write_config_to_ini_file(
                                            &entity.host,
                                            entity.port as i32,
                                            &updated_config,
                                        )
                                        .await
                                    {
                                        Ok(_) => {
                                            success_count += 1;
                                            info!(
                                                "Successfully updated configuration for node {}:{} (ID: {}) in group {} in cluster {}",
                                                entity.host, entity.port, entity.node_id, entity.node_group_id, self.cluster_name
                                            );
                                        }
                                        Err(e) => {
                                            failure_count += 1;
                                            error!(
                                                "Updated database but failed to update INI file for node {}:{} (ID: {}) in group {}: {}",
                                                entity.host, entity.port, entity.node_id, entity.node_group_id, e
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    failure_count += 1;
                                    error!(
                                        "Failed to update configuration for node {}:{} (ID: {}) in group {}: {}",
                                        entity.host, entity.port, entity.node_id, entity.node_group_id, e
                                    );
                                }
                            }
                        }

                        // Set the final task result
                        let msg = format!(
                            "Configuration update completed for node ID {} in cluster {}. Successfully updated {} entries, {} failures.",
                            node_id, self.cluster_name, success_count, failure_count
                        );
                        info!("{}", msg);
                        let status = if failure_count > 0 { 1 } else { 0 };
                        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(status));
                        task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(msg));
                        return Ok(Some(task_result));
                    }
                    Err(e) => {
                        let msg = format!(
                            "Error loading node with ID {} from cluster {}: {}",
                            node_id, self.cluster_name, e
                        );
                        error!("{}", msg);
                        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                        task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(msg));
                        return Ok(Some(task_result));
                    }
                }
            } else {
                // Update configuration for all nodes if tx_node_id is None
                info!(
                    "Updating configuration for all nodes in cluster {}",
                    self.cluster_name
                );

                // Load all nodes for this cluster
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
                    Ok(entities) => {
                        if entities.is_empty() {
                            let msg = format!("No nodes found in cluster {}", self.cluster_name);
                            error!("{}", msg);
                            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                            task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(msg));
                            return Ok(Some(task_result));
                        }

                        let mut success_count = 0;
                        let mut failure_count = 0;

                        // Update each node's configuration
                        for mut entity in entities {
                            // Update the configuration based on field updates
                            let updated_config = self
                                .update_config_json(entity.ini_config.clone(), &self.field_updates);
                            entity.ini_config = updated_config.clone();
                            entity.update_timestamp = now;

                            // Save the updated entity back to the database
                            match tx_op.put(entity.clone()).await {
                                Ok(_) => {
                                    // Now also update the INI file in the upload directory
                                    match self
                                        .write_config_to_ini_file(
                                            &entity.host,
                                            entity.port as i32,
                                            &updated_config,
                                        )
                                        .await
                                    {
                                        Ok(_) => {
                                            success_count += 1;
                                            info!(
                                                "Successfully updated configuration for node {}:{} (ID: {}) in cluster {}",
                                                entity.host, entity.port, entity.node_id, self.cluster_name
                                            );
                                        }
                                        Err(e) => {
                                            failure_count += 1;
                                            error!(
                                                "Updated database but failed to update INI file for node {}:{} (ID: {}): {}",
                                                entity.host, entity.port, entity.node_id, e
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    failure_count += 1;
                                    error!(
                                        "Failed to update configuration for node {}:{} (ID: {}): {}",
                                        entity.host, entity.port, entity.node_id, e
                                    );
                                }
                            }
                        }

                        let msg = format!(
                            "Configuration update completed for cluster {}. Successfully updated {} nodes, {} failures.",
                            self.cluster_name, success_count, failure_count
                        );
                        info!("{}", msg);
                        let status = if failure_count > 0 { 1 } else { 0 };
                        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(status));
                        task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(msg));
                        return Ok(Some(task_result));
                    }
                    Err(e) => {
                        let msg = format!(
                            "Error loading nodes from cluster {}: {}",
                            self.cluster_name, e
                        );
                        error!("{}", msg);
                        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                        task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(msg));
                        return Ok(Some(task_result));
                    }
                }
            }
        }

        let mut success_count = 0;
        let mut failure_count = 0;
        let mut deleted_count = 0;

        // If remove_nodes is provided, delete those entries from topology tables
        if let Some(nodes_to_remove) = &self.remove_nodes {
            if !nodes_to_remove.is_empty() {
                info!("Removing nodes from topology tables: {:?}", nodes_to_remove);

                // Process each node to be removed
                for node_str in nodes_to_remove {
                    if let Some((host, port_str)) = node_str.split_once(':') {
                        if let Ok(port) = port_str.parse::<i32>() {
                            // Delete from TX topology table
                            match tx_op
                                .del(|| {
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
                                Ok(count) => {
                                    if count > 0 {
                                        deleted_count += count;
                                        info!(
                                            "Deleted {} entry(s) for TX node {}:{}",
                                            count, host, port
                                        );
                                    } else {
                                        info!(
                                            "No TX topology entries found for node {}:{}",
                                            host, port
                                        );
                                    }
                                }
                                Err(e) => {
                                    failure_count += 1;
                                    error!(
                                        "Failed to delete TX topology entry for node {}:{}: {}",
                                        host, port, e
                                    );
                                }
                            }

                            // Delete from LOG topology table
                            match log_op
                                .del(|| {
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
                                Ok(count) => {
                                    if count > 0 {
                                        deleted_count += count;
                                        info!(
                                            "Deleted {} entry(s) for LOG node {}:{}",
                                            count, host, port
                                        );
                                    } else {
                                        info!(
                                            "No LOG topology entries found for node {}:{}",
                                            host, port
                                        );
                                    }
                                }
                                Err(e) => {
                                    failure_count += 1;
                                    error!(
                                        "Failed to delete LOG topology entry for node {}:{}: {}",
                                        host, port, e
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        // Try to get topology from cluster_config file
        let tx_entries = match self.get_topology_from_cluster_config_file().await {
            Ok(Some(entries)) => {
                info!("Using topology from cluster_config file");
                entries
            }
            Ok(None) => {
                //  Fall back to extract_tx_topology if no cluster_config found (before scale)
                info!("No cluster_config found, using extract_tx_topology method");
                match self.extract_tx_topology_from_ini_file().await {
                    Ok(entries) => entries,
                    Err(e) => {
                        let msg = format!("Failed to extract TX topology: {}", e);
                        error!("{}", msg);
                        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                        task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(msg));
                        return Ok(Some(task_result));
                    }
                }
            }
            Err(e) => {
                bail!("Error reading cluster_config file: {}", e);
            }
        };

        // Update TX entries in database
        for tx_entity in tx_entries {
            match tx_op.put(tx_entity.clone()).await {
                Ok(_) => {
                    success_count += 1;
                    info!(
                        "Updated TX node: {}:{} role {} in group {}",
                        tx_entity.host, tx_entity.port, tx_entity.role, tx_entity.node_group_id
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

        // Update master roles for nodes identified as masters from Redis
        for master in cluster_nodes.masters.iter() {
            // First, query for existing entry with the same host:port (should be a replica)
            let master_ip = master.ip.clone();
            let master_port = master.port as i32;

            // Use load with a closure that returns Option<QueryCondition>
            match tx_op
                .load(|| {
                    let cond_text = "host = ? and port = ? and role = ?".to_string();
                    let bind_values = vec![
                        StateValue::Varchar(master_ip.clone()),
                        StateValue::Integer(master_port),
                        StateValue::Integer(1), // role = 1 (replica)
                    ];
                    Some(QueryCondition {
                        cond_text,
                        bind_values,
                    })
                })
                .await
            {
                Ok(existing_entities) => {
                    if !existing_entities.is_empty() {
                        // Found matching replica(s), update role to master (0)
                        for mut entity in existing_entities {
                            entity.role = 0; // Change role from replica to master
                            entity.update_timestamp = now;

                            match tx_op.put(entity.clone()).await {
                                Ok(_) => {
                                    success_count += 1;
                                    info!(
                                        "Updated role to master for node {}:{} in group {}",
                                        entity.host, entity.port, entity.node_group_id
                                    );
                                }
                                Err(e) => {
                                    failure_count += 1;
                                    error!(
                                        "Failed to update role to master for node {}:{} in group {}: {}",
                                        entity.host, entity.port, entity.node_group_id, e
                                    );
                                }
                            }
                        }
                    } else {
                        info!(
                            "No matching replica found for master {}:{}, skipping role update",
                            master.ip, master.port
                        );
                    }
                }
                Err(e) => {
                    failure_count += 1;
                    error!(
                        "Failed to query topology for master {}:{}: {}",
                        master.ip, master.port, e
                    );
                }
            }
        }

        let output = format!(
            "Topology update from Redis completed for cluster {}. Updated {} entries, deleted {} entries, {} failures.",
            self.cluster_name, success_count, deleted_count, failure_count
        );
        info!("{}", output);
        let status = if failure_count > 0 { 1 } else { 0 };
        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(status));
        task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(output));
        Ok(Some(task_result))
    }
}
