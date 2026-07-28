use crate::cli::task::cluster_config_utils::{format_node_lists, parse_cluster_config};
use crate::cli::task::redis_op_task::ClusterNodes;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
};
use crate::cli::task::task_utils::{ClusterNodesWithConfig, ScaleOperationType};
use crate::config::config_base::{DeployConfig, SCALED_CLUSTER_CONFIG};
use anyhow::anyhow;
use async_trait::async_trait;
use std::collections::HashMap;
use std::fs;
use tokio::sync::watch;
use tracing::{info, warn};

/// Task for updating the saved cluster topology after scale operation
#[derive(Clone, Debug)]
pub struct DbDeploymentUpdateTask {
    task_id: TaskId,
    config: DeployConfig,
    operation_type: ScaleOperationType,
    nodes_list: Vec<String>,
    cluster_name: String,
    receiver: watch::Receiver<ClusterNodesWithConfig>,
}

impl DbDeploymentUpdateTask {
    pub fn new(
        task_id: TaskId,
        config: DeployConfig,
        operation_type: ScaleOperationType,
        nodes_list: Vec<String>,
        cluster_name: String,
        receiver: watch::Receiver<ClusterNodesWithConfig>,
    ) -> Self {
        Self {
            task_id,
            config,
            operation_type,
            nodes_list,
            cluster_name,
            receiver,
        }
    }

    /// Update the saved deployment topology.
    async fn update_saved_topology(
        &self,
        cluster_nodes: &ClusterNodesWithConfig,
    ) -> anyhow::Result<()> {
        info!(
            "Updating saved topology for cluster {} after scaling operation",
            self.cluster_name
        );

        // Create a new DeployConfig based on the current config but with updated node information
        let mut updated_config = self.config.clone();

        // If we have a cluster configuration from RPC response, use it to update the config
        if let Some(config_str) = &cluster_nodes.cluster_config {
            match parse_cluster_config(config_str) {
                Ok(cluster_config) => {
                    info!(
                        "Using parsed cluster configuration with version {}",
                        cluster_config.version
                    );

                    // Format the node lists with proper node group separators
                    let formatted_lists = format_node_lists(&cluster_config);

                    // Update the configuration with formatted node lists
                    // Set masters list (tx_host_ports)
                    if !formatted_lists.masters_str.is_empty() {
                        updated_config.deployment.tx_service.tx_host_ports =
                            vec![formatted_lists.masters_str];
                    }

                    // Set replicas list (standby_host_ports)
                    if !formatted_lists.replicas_str.is_empty() {
                        if let Some(ref mut standby_ports) =
                            updated_config.deployment.tx_service.standby_host_ports
                        {
                            *standby_ports = vec![formatted_lists.replicas_str];
                        } else {
                            updated_config.deployment.tx_service.standby_host_ports =
                                Some(vec![formatted_lists.replicas_str]);
                        }
                    } else if matches!(self.operation_type, ScaleOperationType::RemoveNodes) {
                        updated_config.deployment.tx_service.standby_host_ports = Some(vec![]);
                    }

                    // Set voters list (voter_host_ports)
                    if !formatted_lists.voters_str.is_empty() {
                        updated_config.deployment.tx_service.voter_host_ports =
                            Some(vec![formatted_lists.voters_str]);
                    } else if matches!(self.operation_type, ScaleOperationType::RemoveNodes) {
                        updated_config.deployment.tx_service.voter_host_ports = Some(vec![]);
                    }
                }
                Err(e) => {
                    warn!("Failed to parse cluster configuration: {}. Falling back to node info from cluster_nodes.", e);
                    // Fall back to using the cluster_nodes data
                    self.update_config_from_cluster_nodes(&mut updated_config, cluster_nodes);
                }
            }
        } else {
            // No cluster configuration provided, use the cluster_nodes data
            info!("No cluster configuration provided, using cluster nodes data");
            self.update_config_from_cluster_nodes(&mut updated_config, cluster_nodes);
        }

        crate::state::state_mgr::STATE_MGR
            .save_deployment_config(&updated_config, true)
            .await
            .map_err(|e| anyhow!("Failed to update saved topology: {}", e))?;

        info!("Successfully updated saved cluster topology");

        Ok(())
    }

    /// Helper method to update config from cluster nodes when no RPC config is available
    fn update_config_from_cluster_nodes(
        &self,
        updated_config: &mut DeployConfig,
        cluster_nodes: &ClusterNodesWithConfig,
    ) {
        // Extract node information from the cluster_nodes response
        // Format: Extract all master and replica nodes with their IP:port
        let all_masters: Vec<String> = cluster_nodes
            .nodes
            .masters
            .iter()
            .map(|node| format!("{}:{}", node.ip, node.port))
            .collect();

        let all_replicas: Vec<String> = cluster_nodes
            .nodes
            .replicas
            .iter()
            .map(|node| format!("{}:{}", node.ip, node.port))
            .collect();

        info!(
            "Extracted masters: {:?}, replicas: {:?} from cluster nodes",
            all_masters, all_replicas
        );

        // Update configuration based on operation type and nodes list
        match self.operation_type {
            ScaleOperationType::AddNodes => {
                info!(
                    "Handling AddNodes operation with nodes: {:?}",
                    self.nodes_list
                );
                // Update the host_ports in the deployment configuration
                if !all_masters.is_empty() {
                    // Join all master nodes with commas for the tx_host_ports field
                    updated_config.deployment.tx_service.tx_host_ports =
                        vec![all_masters.join(",")];
                }

                if !all_replicas.is_empty() {
                    // Handle the case where standby_host_ports is an Option<Vec<String>>
                    if let Some(ref mut standby_ports) =
                        updated_config.deployment.tx_service.standby_host_ports
                    {
                        *standby_ports = vec![all_replicas.join(",")];
                    } else {
                        // If it's not defined, create a new Option with the replica list
                        updated_config.deployment.tx_service.standby_host_ports =
                            Some(vec![all_replicas.join(",")]);
                    }
                }
            }
            ScaleOperationType::RemoveNodes => {
                info!(
                    "Handling RemoveNodes operation with nodes: {:?}",
                    self.nodes_list
                );
                // For removing nodes, we simply use the new cluster topology from the response
                if !all_masters.is_empty() {
                    updated_config.deployment.tx_service.tx_host_ports =
                        vec![all_masters.join(",")];
                }

                if !all_replicas.is_empty() {
                    if let Some(ref mut standby_ports) =
                        updated_config.deployment.tx_service.standby_host_ports
                    {
                        *standby_ports = vec![all_replicas.join(",")];
                    } else {
                        updated_config.deployment.tx_service.standby_host_ports =
                            Some(vec![all_replicas.join(",")]);
                    }
                } else {
                    // If we removed all replicas, set standby_host_ports to None or empty as appropriate
                    updated_config.deployment.tx_service.standby_host_ports = Some(vec![]);
                }
            }
        }
    }
}

#[async_trait]
impl TaskExecutor for DbDeploymentUpdateTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        _task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        info!(
            "Executing {} to update saved topology",
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

        // Check if the file already exists and has content at the beginning
        let should_use_receiver = if config_path.exists() {
            match fs::read_to_string(&config_path) {
                Ok(existing_content) => {
                    if existing_content.trim().is_empty() {
                        info!("Existing config file is empty, will fetch from receiver");
                        true
                    } else {
                        info!("Using existing config file with content");
                        false
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to read existing config file: {}, will fetch from receiver",
                        e
                    );
                    true
                }
            }
        } else {
            info!("Config file does not exist, will fetch from receiver");
            true
        };

        // Only check the receiver if we need to
        let cluster_nodes_with_config = if should_use_receiver {
            // Get the cluster nodes data from the channel
            if let Err(err) = self.receiver.has_changed() {
                return Err(anyhow!(CmdErr::ScaleOpErr(
                    "Failed to receive cluster nodes information".to_string(),
                    err.to_string(),
                )));
            }
            let cluster_nodes_with_config = self.receiver.borrow().clone();
            info!("Received cluster nodes: {:?}", cluster_nodes_with_config);
            cluster_nodes_with_config
        } else {
            // Read the existing config file and construct a new empty ClusterNodesWithConfig
            info!("Using existing configuration file");
            let content = fs::read_to_string(&config_path).map_err(|e| {
                anyhow!(CmdErr::ScaleOpErr(
                    "Failed to read existing config file".to_string(),
                    e.to_string(),
                ))
            })?;

            // Create a new empty ClusterNodesWithConfig with only the cluster_config field set

            ClusterNodesWithConfig {
                nodes: ClusterNodes {
                    masters: Vec::new(),
                    replicas: Vec::new(),
                    voters: Vec::new(),
                },
                cluster_config: Some(content),
            }
        };

        // Update the saved topology with the new configuration from RPC if available.
        if let Err(err) = self.update_saved_topology(&cluster_nodes_with_config).await {
            return Err(anyhow!(CmdErr::ScaleOpErr(
                "Failed to update saved deployment topology".to_string(),
                err.to_string(),
            )));
        }

        // Return success
        let response = HashMap::from([
            (
                crate::cli::CMD.to_string(),
                TaskArgValue::Str("Update saved topology".to_string()),
            ),
            (crate::cli::CMD_STATUS.to_string(), TaskArgValue::Number(0)),
            (
                crate::cli::CMD_OUTPUT.to_string(),
                TaskArgValue::Str("Database updated successfully".to_string()),
            ),
        ]);

        Ok(Some(response))
    }
}
