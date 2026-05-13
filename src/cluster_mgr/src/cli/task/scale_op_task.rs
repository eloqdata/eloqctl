use crate::cli::task::grpc::cc_request::{
    ClusterRemoveNodeRequest, ClusterScaleWriteLogResult, NodeGroupAddPeersRequest,
};
use crate::cli::task::grpc::GrpcClient;
use crate::cli::task::redis_op_task::ClusterNodes;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
};
use crate::cli::task::task_utils::{ClusterNodesWithConfig, ScaleOperationType};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::task_return_value;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::watch;
use tracing::{error, info, warn};

/// Configuration for scale operation
#[derive(Clone, Debug)]
pub struct ScaleOpConfig {
    pub operation_type: ScaleOperationType,
    pub nodes_list: Vec<String>,
    pub is_candidate: Option<Vec<bool>>,
    pub cluster_name: String,
    pub ng_id: Option<u32>,
}

/// Task for performing cluster scale operations
#[derive(Clone, Debug)]
pub struct ScaleOpTask {
    task_id: TaskId,
    event_id: String,
    scale_op_config: ScaleOpConfig,
    receiver: watch::Receiver<ClusterNodes>,
    sender: watch::Sender<ClusterNodesWithConfig>,
}

impl ScaleOpTask {
    /// Create a new ScaleOpTask
    pub fn new(
        task_id: TaskId,
        event_id: String,
        scale_op_config: ScaleOpConfig,
        receiver: watch::Receiver<ClusterNodes>,
        sender: watch::Sender<ClusterNodesWithConfig>,
    ) -> Self {
        Self {
            task_id,
            event_id,
            scale_op_config,
            receiver,
            sender,
        }
    }

    /// Extract port numbers from host:port strings
    fn extract_ports_from_nodes(&self) -> Vec<i32> {
        self.scale_op_config
            .nodes_list
            .iter()
            .map(|p| {
                p.split(':')
                    .next_back()
                    .unwrap_or("0")
                    .parse::<i32>()
                    .unwrap_or(0)
            })
            .collect()
    }

    /// Get the event_id for this task
    pub fn event_id(&self) -> &String {
        &self.event_id
    }
}

#[async_trait]
impl TaskExecutor for ScaleOpTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        _task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> Result<Option<ExecutionValue>> {
        let mut task_result = HashMap::from([(
            CMD.to_string(),
            TaskArgValue::Str("scale operation".to_string()),
        )]);

        let operation_type_str = match self.scale_op_config.operation_type {
            ScaleOperationType::AddNodes => "add nodes",
            ScaleOperationType::RemoveNodes => "remove nodes",
        };

        // Get the current cluster nodes from the receiver
        let cluster_nodes_with_config = self.receiver.borrow().clone();
        info!("Current cluster topology: {:?}", cluster_nodes_with_config);

        // Check if we have masters in the topology
        if cluster_nodes_with_config.masters.is_empty() {
            let error_msg = "No master nodes found in the cluster topology";
            error!("{}", error_msg);
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
            task_result.insert(
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str(error_msg.to_string()),
            );
            task_return_value!(
                task_result,
                |status_code: i32| -> CmdErr {
                    CmdErr::ScaleOpErr(error_msg.to_string(), status_code.to_string())
                },
                "ScaleOpTask"
            )
        }

        // Get the first leader (master) from the topology
        let leader = &cluster_nodes_with_config.masters[0];
        let tx_host = &leader.ip;
        let tx_port = leader.port + 10001; // Adding 10001 to get the gRPC port
        info!(
            "Using leader node {}:{} for scale operation",
            tx_host, tx_port
        );

        info!(
            "Executing scale operation: {} with event ID {}",
            operation_type_str, self.event_id
        );

        // Stage 0: Initiated
        info!("Starting scale operation (event_id={})", self.event_id);

        // Create RPC client using the leader node's address
        let url = format!("http://{}:{}", tx_host, tx_port);
        info!("Connecting to gRPC service at {}", url);
        let mut client = match GrpcClient::new(&url).await {
            Ok(client) => client,
            Err(e) => {
                let error_msg = format!("Failed to create gRPC client: {}", e);
                error!("{}", error_msg);
                error!("gRPC connection error details: {:?}", e);

                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(error_msg.clone()));
                task_return_value!(
                    task_result,
                    |status_code: i32| -> CmdErr {
                        CmdErr::ScaleOpErr(error_msg, status_code.to_string())
                    },
                    "ScaleOpTask"
                )
            }
        };

        // Extract ports from node list
        let ports = self.extract_ports_from_nodes();
        info!("Extracted ports from node list: {:?}", ports);

        // Determine if we're using ng_id and what its value is
        if let Some(ng_id) = self.scale_op_config.ng_id {
            info!("Using provided ng_id: {} (type: u32)", ng_id);

            // Add some type checking logs for verification
            info!("ng_id value as u32: {}", ng_id);
            info!(
                "ng_id type check - is u32: {}",
                std::any::type_name::<u32>()
            );
        } else {
            info!("No ng_id provided, will use default value 0");
        }

        // Execute RPC based on operation type
        let rpc_result = match self.scale_op_config.operation_type {
            ScaleOperationType::AddNodes => {
                // Use is_candidate setting for all nodes or derive from ports
                let is_candidate: Vec<bool> =
                    if let Some(is_candidate) = &self.scale_op_config.is_candidate {
                        is_candidate.clone()
                    } else {
                        ports.iter().map(|&p| p != 0).collect()
                    };

                // The ports are already i32, ready for the proto
                info!("Using i32 ports for proto: {:?}", ports);

                // Log details before creating the request
                let ng_id_value = match self.scale_op_config.ng_id {
                    Some(id) => {
                        info!("Successfully unwrapped ng_id value: {}", id);
                        id
                    }
                    None => {
                        warn!("No ng_id provided, using default value 0");
                        0
                    }
                };

                let hosts: Vec<String> = self
                    .scale_op_config
                    .nodes_list
                    .iter()
                    .map(|hostport| hostport.split(':').next().unwrap_or(hostport).to_string())
                    .collect();

                info!("Creating NodeGroupAddPeersRequest with:");
                info!(
                    "  - ng_id: {} (from {:?})",
                    ng_id_value, self.scale_op_config.ng_id
                );
                info!("  - ng_id proto field type is uint32 (defined in proto file)");
                info!("  - event_id: {}", self.event_id);
                info!(
                    "  - original nodes_list: {:?}",
                    self.scale_op_config.nodes_list
                );
                info!("  - extracted hosts: {:?}", hosts);
                info!("  - extracted ports: {:?}", ports);
                info!("  - is_candidate: {:?}", is_candidate);

                let request = NodeGroupAddPeersRequest {
                    ng_id: ng_id_value,
                    host_list: hosts,
                    port_list: ports.iter().map(|&port| port + 10000).collect(),
                    is_candidate,
                    id: self.event_id.clone(),
                };

                info!("Sending add_peers request: {:?}", request);
                info!("Add peers request details - ng_id: {}, event_id: {}, hosts: {:?}, ports: {:?}, is_candidate: {:?}", 
                    request.ng_id, request.id, request.host_list, request.port_list, request.is_candidate);
                client.add_peers(request).await
            }
            ScaleOperationType::RemoveNodes => {
                // The ports are already i32, ready for the proto
                info!("Using i32 ports for proto: {:?}", ports);

                let hosts: Vec<String> = self
                    .scale_op_config
                    .nodes_list
                    .iter()
                    .map(|hostport| hostport.split(':').next().unwrap_or(hostport).to_string())
                    .collect();

                info!("Creating ClusterRemoveNodeRequest with:");
                info!("  - event_id: {}", self.event_id);
                info!("  - remove_node_count: {}", 0);
                info!(
                    "  - original nodes_list: {:?}",
                    self.scale_op_config.nodes_list
                );
                info!("  - extracted hosts: {:?}", hosts);
                info!("  - extracted ports: {:?}", ports);

                let request = ClusterRemoveNodeRequest {
                    id: self.event_id.clone(),
                    remove_node_count: 0,
                    host_list: hosts,
                    port_list: ports.iter().map(|&port| port + 10000).collect(),
                };

                info!("Sending remove_node request: {:?}", request);
                info!("Remove nodes request details - event_id: {}, count: {}, hosts: {:?}, ports: {:?}", 
                    request.id, request.remove_node_count, request.host_list, request.port_list);
                client.remove_node(request).await
            }
        };

        // Handle RPC result
        match &rpc_result {
            Ok(response) => {
                info!("Received RPC response: {:?}", response);
                info!("Response result code: {}", response.result);
            }
            Err(e) => {
                error!("RPC call failed with error: {:?}", e);
            }
        }

        // Handle RPC result for task execution
        match rpc_result {
            Ok(response) => {
                // Check if the result is Fail (1)
                if response.result == ClusterScaleWriteLogResult::Fail as i32 {
                    let error_msg =
                        format!("RPC operation failed with result: {}", response.result);
                    error!("{}", error_msg);
                    error!("Response details: {:?}", response);

                    task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                    task_result
                        .insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(error_msg.clone()));
                    task_return_value!(
                        task_result,
                        |status_code: i32| -> CmdErr {
                            CmdErr::ScaleOpErr(error_msg, status_code.to_string())
                        },
                        "ScaleOpTask"
                    )
                }

                // Check if this is not a success result but some other error
                if response.result != ClusterScaleWriteLogResult::Success as i32 {
                    let error_msg =
                        format!("RPC operation failed with result: {}", response.result);
                    error!("{}", error_msg);
                    error!("Response details: {:?}", response);

                    task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                    task_result
                        .insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(error_msg.clone()));
                    task_return_value!(
                        task_result,
                        |status_code: i32| -> CmdErr {
                            CmdErr::ScaleOpErr(error_msg, status_code.to_string())
                        },
                        "ScaleOpTask"
                    )
                }

                info!(
                    "RPC operation successfully initiated with event ID: {}",
                    self.event_id
                );
                info!("Successful response details: {:?}", response);

                // For AddNodes operation, we would normally start the new nodes here
                // but for simplicity, we're just logging that we would do this
                if let ScaleOperationType::AddNodes = self.scale_op_config.operation_type {
                    info!(
                        "New nodes added to cluster: {}. Node startup should be handled separately.",
                        self.scale_op_config.nodes_list.join(", ")
                    );
                }

                // Forward the updated cluster topology to the next task
                // Use the topology returned in the response or update the existing topology
                let mut cluster_nodes = self.receiver.borrow().clone();
                let mut cluster_nodes_with_config = ClusterNodesWithConfig {
                    nodes: cluster_nodes.clone(),
                    cluster_config: None,
                };

                // Add the RPC response configuration to the topology
                if !response.cluster_config.is_empty() {
                    info!(
                        "RPC response contains cluster configuration: \n{}",
                        response.cluster_config
                    );
                    cluster_nodes_with_config.cluster_config =
                        Some(response.cluster_config.clone());
                } else {
                    warn!("RPC response does not contain cluster configuration");
                }

                // For remove operations, remove the nodes from the topology
                if let ScaleOperationType::RemoveNodes = self.scale_op_config.operation_type {
                    // Extract port numbers from the nodes to remove
                    let ports_to_remove = self.extract_ports_from_nodes();

                    // Remove nodes from masters by converting node.port to i32 for comparison
                    cluster_nodes
                        .masters
                        .retain(|node| !ports_to_remove.contains(&(node.port as i32)));

                    // Remove nodes from replicas by converting node.port to i32 for comparison
                    cluster_nodes
                        .replicas
                        .retain(|node| !ports_to_remove.contains(&(node.port as i32)));

                    info!(
                        "Updated topology after removal: masters={:?}, replicas={:?}",
                        cluster_nodes.masters, cluster_nodes.replicas
                    );
                }

                // Forward topology to next task
                if let Err(e) = self.sender.send(cluster_nodes_with_config) {
                    error!("Failed to forward updated topology to next task: {}", e);
                } else {
                    info!("Successfully forwarded updated topology to next task");
                }

                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
                task_result.insert(
                    CMD_OUTPUT.to_string(),
                    TaskArgValue::Str(format!(
                        "Successfully completed {} operation with event ID: {}",
                        operation_type_str, self.event_id
                    )),
                );
            }
            Err(e) => {
                let error_msg = format!("RPC operation failed: {}", e);
                error!("{}", error_msg);

                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(error_msg.clone()));
                task_return_value!(
                    task_result,
                    |status_code: i32| -> CmdErr {
                        CmdErr::ScaleOpErr(error_msg, status_code.to_string())
                    },
                    "ScaleOpTask"
                )
            }
        }

        Ok(Some(task_result))
    }
}
