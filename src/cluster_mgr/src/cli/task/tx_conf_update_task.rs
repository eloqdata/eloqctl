use crate::cli::task::cluster_config_utils::{format_node_lists, parse_cluster_config};
use crate::cli::task::redis_op_task::ClusterNodes;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
};
use crate::cli::task::task_utils::{ClusterNodesWithConfig, ScaleOperationType};
use crate::config::config_base::{DeployConfig, SCALED_CLUSTER_CONFIG};
use anyhow::{anyhow, bail};
use async_trait::async_trait;
use configparser::ini::Ini;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use tokio::sync::watch;
use tracing::{info, warn};

#[derive(Clone, Debug)]
pub struct TxConfUpdateTask {
    task_id: TaskId,
    config: DeployConfig,
    operation_type: ScaleOperationType,
    scale_node_list: Vec<String>,
    cluster_name: String,
    receiver: watch::Receiver<ClusterNodesWithConfig>,
}

impl TxConfUpdateTask {
    pub fn new(
        task_id: TaskId,
        config: DeployConfig,
        operation_type: ScaleOperationType,
        scale_node_list: Vec<String>,
        cluster_name: String,
        receiver: watch::Receiver<ClusterNodesWithConfig>,
    ) -> Self {
        Self {
            task_id,
            config,
            operation_type,
            scale_node_list,
            cluster_name,
            receiver,
        }
    }

    // Generate config files(in .eloqctl/upload dir) for all nodes in scale_node_list

    // Used when resuming an operation to ensure all needed config files exist
    pub async fn update_configs_from_resume(
        &self,
        cluster_nodes_with_config: &ClusterNodesWithConfig,
    ) -> anyhow::Result<()> {
        info!(
            "Generating configuration files for resume operation on cluster {}",
            self.cluster_name
        );

        // Prepare masters, replicas, and voters lists
        let all_masters_str;
        let all_replicas_str;
        let all_voters_str;

        // If we have a cluster configuration from RPC response, use it to generate node group configurations
        if let Some(config_str) = &cluster_nodes_with_config.cluster_config {
            info!("Using cluster configuration from RPC response for resume");

            match parse_cluster_config(config_str) {
                Ok(cluster_config) => {
                    info!(
                        "Using parsed cluster configuration with version {} for resume",
                        cluster_config.version
                    );

                    // Get formatted host:port lists for masters, replicas, and voters
                    let formatted_lists = format_node_lists(&cluster_config);
                    all_masters_str = formatted_lists.masters_str;
                    all_replicas_str = formatted_lists.replicas_str;
                    all_voters_str = formatted_lists.voters_str;
                }
                Err(e) => {
                    bail!("Failed to parse cluster configuration for resume: {}.", e);
                }
            }
        } else {
            bail!("No cluster configuration provided for resume operation.");
        }

        info!("Generated ip_port_list: {}", all_masters_str);
        info!("Generated standby_ip_port_list: {}", all_replicas_str);
        info!("Generated voter_ip_port_list: {}", all_voters_str);

        let upload_dir = crate::cli::upload_dir().join(&self.cluster_name);
        if !upload_dir.exists() {
            fs::create_dir_all(&upload_dir)?;
        }

        // Generate config files for all nodes in scale_node_list
        for node_str in &self.scale_node_list {
            // Parse host:port pair
            let parts: Vec<&str> = node_str.split(':').collect();
            if parts.len() != 2 {
                warn!("Invalid node format in scale_node_list: {}", node_str);
                continue;
            }

            let host = parts[0];
            let port = parts[1];

            // Generate the config file
            let gen_path = self
                .config
                .deployment
                .gen_eloqkv_node_config(Some(host.to_string()), Some(port.to_string()))?;

            info!(
                "Generated config file for node {}: {}",
                node_str,
                gen_path.display()
            );

            // Update its [cluster] section
            let mut ini = Ini::new();
            ini.load(gen_path.to_str().unwrap()).map_err(|e| {
                anyhow!(
                    "Failed to load new config file {}: {}",
                    gen_path.display(),
                    e
                )
            })?;

            ini.set("cluster", "ip_port_list", Some(all_masters_str.clone()));
            if !all_replicas_str.is_empty() {
                ini.set(
                    "cluster",
                    "standby_ip_port_list",
                    Some(all_replicas_str.clone()),
                );
            }
            if !all_voters_str.is_empty() {
                ini.set(
                    "cluster",
                    "voter_ip_port_list",
                    Some(all_voters_str.clone()),
                );
            }

            ini.write(gen_path.as_path()).map_err(|e| {
                anyhow!("Failed to write config file {}: {}", gen_path.display(), e)
            })?;

            info!(
                "Updated cluster section in generated file: {}",
                gen_path.display()
            );
        }

        Ok(())
    }

    /// Update the [cluster] section in all config files with the new topology
    async fn update_configs(
        &self,
        cluster_nodes_with_config: &ClusterNodesWithConfig,
    ) -> anyhow::Result<()> {
        info!(
            "Updating configuration files for cluster {}",
            self.cluster_name
        );

        // Prepare masters, replicas, and voters lists
        let all_masters_str;
        let all_replicas_str;
        let all_voters_str;

        // If we have a cluster configuration from RPC response, use it to generate node group configurations
        if let Some(config_str) = &cluster_nodes_with_config.cluster_config {
            info!("Using cluster configuration from RPC response");

            match parse_cluster_config(config_str) {
                Ok(cluster_config) => {
                    info!(
                        "Using parsed cluster configuration with version {}",
                        cluster_config.version
                    );

                    // Get formatted host:port lists for masters, replicas, and voters
                    let formatted_lists = format_node_lists(&cluster_config);
                    all_masters_str = formatted_lists.masters_str;
                    all_replicas_str = formatted_lists.replicas_str;
                    all_voters_str = formatted_lists.voters_str;
                }
                Err(e) => {
                    bail!("Failed to parse cluster configuration: {}.", e);
                }
            }
        } else {
            bail!("No cluster configuration provided.");
        }

        info!("Generated ip_port_list: {}", all_masters_str);
        info!("Generated standby_ip_port_list: {}", all_replicas_str);
        info!("Generated voter_ip_port_list: {}", all_voters_str);

        let nodes_to_remove = if let ScaleOperationType::RemoveNodes = self.operation_type {
            self.scale_node_list.clone()
        } else {
            Vec::new()
        };

        let ports_to_remove: Vec<i32> = nodes_to_remove
            .iter()
            .map(|p| {
                p.split(':')
                    .next_back()
                    .unwrap_or("0")
                    .parse::<i32>()
                    .unwrap_or(0)
            })
            .collect();

        info!("Nodes to be removed: {:?}", nodes_to_remove);
        info!("Ports to be removed: {:?}", ports_to_remove);

        let upload_dir = crate::cli::upload_dir().join(&self.cluster_name);
        if !upload_dir.exists() {
            fs::create_dir_all(&upload_dir)?;
        }

        // iterate over all dirs in the upload/<cluster-name> to update ini config
        for host_entry in fs::read_dir(&upload_dir)? {
            let host_dir = host_entry?.path();
            if !host_dir.is_dir() {
                continue;
            }

            let host = host_dir.file_name().unwrap().to_string_lossy().to_string();
            info!("Processing host directory: {}", host);

            for file_entry in fs::read_dir(&host_dir)? {
                let file_path = file_entry?.path();
                if !file_path.is_file() || file_path.extension().is_none_or(|ext| ext != "ini") {
                    continue;
                }

                let file_name = file_path.file_name().unwrap().to_string_lossy().to_string();

                // Check if the filename contains the port AND this is a file for a node being removed
                // Only delete files that match both host and port of nodes being removed
                let should_remove = nodes_to_remove.iter().any(|node| {
                    if let Some((node_host, node_port)) = node.split_once(':') {
                        // Check if this file belongs to a node we're removing
                        host == node_host && file_name.contains(&format!("-{}", node_port))
                    } else {
                        false
                    }
                });

                if should_remove {
                    info!(
                        "Deleting config file for removed node: {}",
                        file_path.display()
                    );
                    if let Err(e) = fs::remove_file(&file_path) {
                        warn!(
                            "Failed to delete config file {}: {}",
                            file_path.display(),
                            e
                        );
                    } else {
                        info!("Successfully deleted config file: {}", file_path.display());
                    }
                    continue;
                }

                info!("Updating config file: {}", file_path.display());

                let mut ini = Ini::new();
                match ini.load(file_path.to_str().unwrap()) {
                    Ok(_) => {
                        // Update master nodes (tx_host_ports)
                        ini.set("cluster", "ip_port_list", Some(all_masters_str.clone()));

                        // Update replica nodes (standby_host_ports) if available
                        if !all_replicas_str.is_empty() {
                            ini.set(
                                "cluster",
                                "standby_ip_port_list",
                                Some(all_replicas_str.clone()),
                            );
                        }

                        // Update voter nodes if available
                        if !all_voters_str.is_empty() {
                            ini.set(
                                "cluster",
                                "voter_ip_port_list",
                                Some(all_voters_str.clone()),
                            );
                        }

                        ini.write(&file_path)
                            .map_err(|e| anyhow!("Failed to write config file: {}", e))?;
                        info!("Updated configuration file: {}", file_path.display());
                    }
                    Err(e) => {
                        warn!("Failed to load config file {}: {}", file_path.display(), e);
                        continue;
                    }
                }
            }
        }

        // Generate config files for newly added nodes and update their [cluster] sections
        if let ScaleOperationType::AddNodes = self.operation_type {
            if let Some(config_str) = &cluster_nodes_with_config.cluster_config {
                if let Ok(cluster_config) = parse_cluster_config(config_str) {
                    for (_ng_id, nodes) in cluster_config.node_groups.iter() {
                        for ncfg in nodes.iter() {
                            let node_str = format!("{}:{}", ncfg.host_name, ncfg.port);
                            if self.scale_node_list.contains(&node_str) {
                                // Generate the new config file
                                let gen_path = self.config.deployment.gen_eloqkv_node_config(
                                    Some(ncfg.host_name.clone()),
                                    Some(ncfg.port.to_string()),
                                )?;
                                info!(
                                    "Generated config file for added node {}: {}",
                                    node_str,
                                    gen_path.display()
                                );
                                // Update its [cluster] section
                                let mut ini = Ini::new();
                                ini.load(gen_path.to_str().unwrap()).map_err(|e| {
                                    anyhow!(
                                        "Failed to load new config file {}: {}",
                                        gen_path.display(),
                                        e
                                    )
                                })?;
                                ini.set("cluster", "ip_port_list", Some(all_masters_str.clone()));
                                if !all_replicas_str.is_empty() {
                                    ini.set(
                                        "cluster",
                                        "standby_ip_port_list",
                                        Some(all_replicas_str.clone()),
                                    );
                                }
                                if !all_voters_str.is_empty() {
                                    ini.set(
                                        "cluster",
                                        "voter_ip_port_list",
                                        Some(all_voters_str.clone()),
                                    );
                                }
                                ini.write(gen_path.as_path()).map_err(|e| {
                                    anyhow!(
                                        "Failed to write new config file {}: {}",
                                        gen_path.display(),
                                        e
                                    )
                                })?;
                                info!(
                                    "Updated cluster section in generated file: {}",
                                    gen_path.display()
                                );
                            }
                        }
                    }
                } else {
                    warn!(
                        "Failed to parse cluster configuration for new node generation; skipping."
                    );
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl TaskExecutor for TxConfUpdateTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        _task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        info!(
            "Executing {} to update config files",
            self.task_id.format_string()
        );

        // Create a directory in the cluster's upload dir
        let upload_dir = crate::cli::upload_dir().join(&self.cluster_name);
        if let Err(e) = std::fs::create_dir_all(&upload_dir) {
            return Err(anyhow!(CmdErr::ScaleOpErr(
                "Failed to create upload directory".to_string(),
                e.to_string(),
            )));
        }

        let config_path = upload_dir.join(SCALED_CLUSTER_CONFIG);

        // Check if the receiver has changed, if so use it, otherwise use existing file
        if self.receiver.has_changed().is_ok() {
            let result = self.receiver.borrow().clone();
            info!("Received cluster nodes: {:?}", result.nodes);

            // Write the cluster_config to the file if available
            if let Some(config_content) = &result.cluster_config {
                info!(
                    "Writing cluster configuration to upload directory: {} bytes",
                    config_content.len()
                );

                let mut file = match std::fs::File::create(&config_path) {
                    Ok(f) => f,
                    Err(e) => {
                        return Err(anyhow!(CmdErr::ScaleOpErr(
                            "Failed to create config file".to_string(),
                            e.to_string(),
                        )));
                    }
                };

                // Write the config content to the file
                if let Err(e) = file.write_all(config_content.as_bytes()) {
                    return Err(anyhow!(CmdErr::ScaleOpErr(
                        "Failed to write config content".to_string(),
                        e.to_string(),
                    )));
                }

                info!(
                    "Cluster configuration written to upload directory: {}",
                    config_path.display()
                );
            }

            if let Err(err) = self.update_configs(&result).await {
                return Err(anyhow!(CmdErr::ScaleOpErr(
                    "Failed to update configuration files".to_string(),
                    err.to_string(),
                )));
            }
        } else {
            // Read the existing config file
            info!("Using existing configuration file");
            let content = fs::read_to_string(&config_path).map_err(|e| {
                anyhow!(CmdErr::ScaleOpErr(
                    "Failed to read existing config file".to_string(),
                    e.to_string(),
                ))
            })?;

            // Create a new empty ClusterNodesWithConfig with only the cluster_config field set
            let result = ClusterNodesWithConfig {
                nodes: ClusterNodes {
                    masters: Vec::new(),
                    replicas: Vec::new(),
                },
                cluster_config: Some(content),
            };

            if let Err(err) = self.update_configs_from_resume(&result).await {
                return Err(anyhow!(CmdErr::ScaleOpErr(
                    "Failed to update configuration files".to_string(),
                    err.to_string(),
                )));
            }
        }

        let response = HashMap::from([
            (
                crate::cli::CMD.to_string(),
                TaskArgValue::Str("Update configuration files".to_string()),
            ),
            (crate::cli::CMD_STATUS.to_string(), TaskArgValue::Number(0)),
            (
                crate::cli::CMD_OUTPUT.to_string(),
                TaskArgValue::Str("Configuration files updated successfully".to_string()),
            ),
        ]);

        Ok(Some(response))
    }
}
