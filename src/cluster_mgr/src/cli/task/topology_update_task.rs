use crate::cli::task::cluster_config_utils::{parse_cluster_config, ClusterGroupConfig};
use crate::cli::task::redis_op_task::ClusterNodes;
use crate::cli::task::task_base::TaskExecutor;
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskHost, TaskId, TaskInstance};
use crate::cli::task::task_utils::parse_ng_config;
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
use serde_json::{json, Value};
use sqlx::{query, Row, SqlitePool};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tokio::sync::watch;
use tracing::{error, info};

// Update topology in t_topology_tx using live data from RedisOpTask
#[derive(Debug, Clone)]
pub struct TopologyUpdateTask {
    task_id: TaskId,
    cluster_name: String,
    config: DeployConfig,
    receiver: watch::Receiver<ClusterNodes>,
    remove_nodes: Option<Vec<String>>, // Optional list of nodes to be removed (format: "host:port")
    tx_node_id: Option<i32>,           // Optional node ID to update config for
    field_updates: Option<Vec<String>>, // Optional field updates in "field:value" format
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
            field_updates: None,
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
        tx_node_id: i32,
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
            field_updates: Some(field_updates),
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

    // Load ConfigJson from t_topology_tx database rather than INI file
    async fn load_config_from_db(&self) -> Result<ConfigJson> {
        let tx_op = STATE_MGR.get_state_operation::<TopologyTxOperation>(TOPOLOGY_TX_STATE);

        // Query for existing entries for this cluster
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
            Ok(existing_entities) => {
                if let Some(entity) = existing_entities.first() {
                    // Return the config from the first entity found
                    info!(
                        "Loaded existing ConfigJson from database for cluster {}",
                        self.cluster_name
                    );
                    Ok(entity.ini_config.clone())
                } else {
                    // During launch, check if the config is in the upload/<cluster-name> directory
                    info!(
                        "No existing config found in database for cluster {}, checking upload directory",
                        self.cluster_name
                    );

                    // Look for INI files in the upload directory for this cluster
                    let upload_path = upload_dir().join(&self.cluster_name);

                    // Look for host-specific INI files first
                    let mut found_ini_files = false;
                    let mut config_json = ConfigJson {
                        eloq_data_path: String::new(),
                        enable_data_store: false,
                        enable_wal: false,
                    };

                    // Get all hosts from the deployment config
                    let all_hosts = self.config.deployment.tx_service.merge_hosts();

                    for host in all_hosts {
                        let host_dir = upload_path.join(&host);
                        if host_dir.exists() {
                            // Look for *node*.ini files in the host directory
                            if let Ok(entries) = fs::read_dir(&host_dir) {
                                for entry in entries {
                                    if let Ok(entry) = entry {
                                        let path = entry.path();
                                        if path.is_file()
                                            && path.extension().map_or(false, |ext| ext == "ini")
                                        {
                                            if let Some(file_name) = path.file_name() {
                                                if let Some(file_name_str) = file_name.to_str() {
                                                    if file_name_str.contains("node") {
                                                        // Found a node INI file, parse it
                                                        info!(
                                                            "Found INI file for host {}: {}",
                                                            host,
                                                            path.display()
                                                        );
                                                        if let Ok(content) =
                                                            fs::read_to_string(&path)
                                                        {
                                                            // Simple INI parsing for known fields
                                                            for line in content.lines() {
                                                                let line = line.trim();
                                                                if line.is_empty()
                                                                    || line.starts_with('#')
                                                                    || line.starts_with(';')
                                                                {
                                                                    continue;
                                                                }

                                                                if let Some((key, value)) =
                                                                    line.split_once('=')
                                                                {
                                                                    let key = key.trim();
                                                                    let value = value.trim();

                                                                    match key {
                                                                        "eloq_data_path" => {
                                                                            config_json
                                                                                .eloq_data_path =
                                                                                value.to_string();
                                                                        }
                                                                        "enable_data_store" => {
                                                                            config_json.enable_data_store = value.to_lowercase() == "true"
                                                                                || value == "1"
                                                                                || value.to_lowercase() == "yes";
                                                                        }
                                                                        "enable_wal" => {
                                                                            config_json
                                                                                .enable_wal = value
                                                                                .to_lowercase()
                                                                                == "true"
                                                                                || value == "1"
                                                                                || value
                                                                                    .to_lowercase()
                                                                                    == "yes";
                                                                        }
                                                                        // Add other fields as needed
                                                                        _ => {}
                                                                    }
                                                                }
                                                            }
                                                            found_ini_files = true;
                                                            break; // Just use the first found INI file for this host
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if found_ini_files {
                                break; // Use the first host's INI file we found
                            }
                        }
                    }

                    // If we found and parsed INI files, use that config
                    if found_ini_files {
                        info!("Using configuration from INI files in upload directory");
                        Ok(config_json)
                    } else {
                        // Fall back to default config if no INI files found
                        info!("No INI files found in upload directory, using default config");
                        Ok(ConfigJson {
                            eloq_data_path: String::new(),
                            enable_data_store: false,
                            enable_wal: false,
                        })
                    }
                }
            }
            Err(e) => {
                error!("Failed to query database for configuration: {}", e);
                // Return default config on error
                Ok(ConfigJson {
                    eloq_data_path: String::new(),
                    enable_data_store: false,
                    enable_wal: false,
                })
            }
        }
    }

    // Load ConfigJson for a specific node ID
    async fn load_config_for_node(&self, node_id: i32) -> Result<Option<TopologyTxEntity>> {
        let tx_op = STATE_MGR.get_state_operation::<TopologyTxOperation>(TOPOLOGY_TX_STATE);

        // Query for specific node entry
        match tx_op
            .load(|| {
                let cond_text = "cluster_name = ? AND node_id = ?".to_string();
                let bind_values = vec![
                    StateValue::Varchar(self.cluster_name.clone()),
                    StateValue::Integer(node_id),
                ];
                Some(QueryCondition {
                    cond_text,
                    bind_values,
                })
            })
            .await
        {
            Ok(entities) => {
                if let Some(entity) = entities.first() {
                    info!(
                        "Found node {}:{} (ID: {}) in group {} for cluster {}",
                        entity.host,
                        entity.port,
                        entity.node_id,
                        entity.node_group_id,
                        self.cluster_name
                    );
                    Ok(Some(entity.clone()))
                } else {
                    info!(
                        "No node with ID {} found for cluster {}",
                        node_id, self.cluster_name
                    );
                    Ok(None)
                }
            }
            Err(e) => {
                error!("Error querying node with ID {}: {}", node_id, e);
                Err(anyhow::anyhow!("Failed to query node: {}", e))
            }
        }
    }

    // Update ConfigJson based on field updates
    fn update_config_json(&self, mut config: ConfigJson, field_updates: &[String]) -> ConfigJson {
        for field_update in field_updates {
            if let Some((field, value)) = field_update.split_once(':') {
                match field {
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
                    _ => {
                        error!("Unknown configuration field: {}", field);
                    }
                }
            }
        }
        config
    }

    // Try to get topology from cluster_config file
    async fn get_topology_from_config_file(&self) -> Result<Option<Vec<TopologyTxEntity>>> {
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
        let node_group_count = cluster_config.node_groups.len() as i32;

        // Load INI file from upload directory to store in entities
        let config_json = self.load_config_from_db().await?;

        for (&ng_id, nodes) in &cluster_config.node_groups {
            for node in nodes {
                // Determine the role based on is_candidate flag
                // 1 = Replica (Standby), 2 = Voter (non-candidate)
                let role = if node.is_candidate {
                    1 // Replica
                } else {
                    2 // Voter
                };

                tx_entities.push(TopologyTxEntity {
                    cluster_name: cluster_name.clone(),
                    node_group_count,
                    node_group_id: ng_id as i32,
                    node_id: node.node_id as i32,
                    role,
                    host: node.host_name.clone(),
                    port: node.port as i32,
                    ini_config: config_json.clone(),
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
    async fn extract_tx_topology(&self) -> Result<Vec<TopologyTxEntity>> {
        let mut tx_entities = Vec::new();
        let port_delta = 10000;
        let cluster_name = &self.config.deployment.cluster_name;
        let now = Utc::now();

        // Load configuration from database instead of INI file
        let config_json = self.load_config_from_db().await?;

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
                let node_group_count = ng_configs.len() as i32;

                // Process each node group
                for (ng_id, nodes) in ng_configs.iter() {
                    // Process each node in the group
                    for node in nodes {
                        // Determine the role based on is_candidate flag
                        // 1 = Replica (Standby), 2 = Voter
                        // All is_candidate=true nodes are set as replicas at this step
                        let role = if node.is_candidate {
                            1 // Set all candidates as replicas initially
                        } else {
                            2 // Voter
                        };

                        tx_entities.push(TopologyTxEntity {
                            cluster_name: cluster_name.clone(),
                            node_group_count,
                            node_group_id: *ng_id as i32,
                            node_id: node.node_id as i32,
                            role,
                            host: node.ip.clone(),
                            port: (node.port as i32 - port_delta as i32),
                            ini_config: config_json.clone(),
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
            let log_nodes = &log_service.nodes;
            let log_count = log_nodes.len() as i32;
            for (i, node) in log_nodes.iter().enumerate() {
                log_entities.push(TopologyLogEntity {
                    cluster_name: cluster_name.clone(),
                    node_group_count: log_count,
                    node_group_id: 0, // set all log nodes to group 0 for the current design
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

        // Handle node-specific configuration update if tx_node_id and field_updates are provided
        if let (Some(node_id), Some(field_updates)) = (&self.tx_node_id, &self.field_updates) {
            if !field_updates.is_empty() {
                info!(
                    "Updating configuration for node ID {} in cluster {}",
                    node_id, self.cluster_name
                );

                // Load the existing entity for the specified node
                match self.load_config_for_node(*node_id).await {
                    Ok(Some(mut entity)) => {
                        // Update the configuration based on field updates
                        let updated_config =
                            self.update_config_json(entity.ini_config.clone(), field_updates);
                        entity.ini_config = updated_config;
                        entity.update_timestamp = now;

                        // Save the updated entity back to the database
                        match tx_op.put(entity.clone()).await {
                            Ok(_) => {
                                let msg = format!(
                                    "Successfully updated configuration for node {}:{} (ID: {}) in cluster {}",
                                    entity.host, entity.port, entity.node_id, self.cluster_name
                                );
                                info!("{}", msg);
                                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
                                task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(msg));
                                return Ok(Some(task_result));
                            }
                            Err(e) => {
                                let msg = format!(
                                    "Failed to update configuration for node {}:{} (ID: {}): {}",
                                    entity.host, entity.port, entity.node_id, e
                                );
                                error!("{}", msg);
                                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                                task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(msg));
                                return Ok(Some(task_result));
                            }
                        }
                    }
                    Ok(None) => {
                        let msg = format!(
                            "Node with ID {} not found in cluster {}",
                            node_id, self.cluster_name
                        );
                        error!("{}", msg);
                        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
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
        let tx_entries = match self.get_topology_from_config_file().await {
            Ok(Some(entries)) => {
                info!("Using topology from cluster_config file");
                entries
            }
            Ok(None) => {
                //  Fall back to extract_tx_topology if no cluster_config found (before scale) #Q? only used on launch?
                info!("No cluster_config found, using extract_tx_topology method");
                match self.extract_tx_topology().await {
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
        let master_count = cluster_nodes.masters.len() as i32;
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
