use crate::cli::task::grpc_log::{
    add_log_peer, remove_log_peer, AddPeerRequest, RemovePeerRequest,
};
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
};
use crate::cli::task::task_utils::ScaleOperationType;
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeployConfig;
use crate::task_return_value;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::watch;
use tracing::{error, info, warn};

#[derive(Clone, Debug)]
pub struct ScaleLogOpTask {
    task_id: TaskId,
    event_id: String,
    nodes: Vec<String>,
    log_group_id: Option<u32>,
    config: DeployConfig,
    operation_type: ScaleOperationType,
    scale_result_tx: watch::Sender<bool>,
}

impl ScaleLogOpTask {
    pub fn new(
        task_id: TaskId,
        event_id: String,
        nodes: Vec<String>,
        log_group_id: Option<u32>,
        config: DeployConfig,
        operation_type: ScaleOperationType,
        scale_result_tx: watch::Sender<bool>,
    ) -> Self {
        Self {
            task_id,
            event_id,
            nodes,
            log_group_id,
            config,
            operation_type,
            scale_result_tx,
        }
    }

    /// Extract hosts and ports from the node list
    fn extract_hosts_and_ports(&self) -> (Vec<String>, Vec<u32>) {
        let mut hosts = Vec::new();
        let mut ports = Vec::new();

        for node in &self.nodes {
            if let Some((host, port_str)) = node.split_once(':') {
                if let Ok(port) = port_str.parse::<u32>() {
                    hosts.push(host.to_string());
                    ports.push(port);
                } else {
                    warn!("Invalid port in node address: {}", node);
                }
            } else {
                warn!("Invalid node address format: {}", node);
            }
        }

        (hosts, ports)
    }
}

#[async_trait]
impl TaskExecutor for ScaleLogOpTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        _task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> Result<Option<ExecutionValue>> {
        // tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let mut task_result = HashMap::from([(
            CMD.to_string(),
            TaskArgValue::Str(match self.operation_type {
                ScaleOperationType::AddNodes => "scale log add operation".to_string(),
                ScaleOperationType::RemoveNodes => "scale log remove operation".to_string(),
            }),
        )]);

        info!(
            "Executing scale log operation (type: {:?}) with event ID: {}",
            self.operation_type, self.event_id
        );

        // Extract hosts and ports from node list
        let (hosts, ports) = self.extract_hosts_and_ports();

        if hosts.is_empty() || ports.is_empty() {
            let error_msg = "No valid node addresses provided".to_string();
            error!("{}", error_msg);
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
            task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(error_msg.clone()));
            task_return_value!(
                task_result,
                |status_code: i32| -> CmdErr {
                    CmdErr::ScaleOpErr(error_msg, status_code.to_string())
                },
                "ScaleLogOpTask"
            )
        }

        // Pick the first log-service node to dial
        let log_nodes = &self.config.deployment.log_service.as_ref().unwrap().nodes;
        let log_leader = &log_nodes[0];
        let url = format!("http://{}:{}", log_leader.host, log_leader.port);
        info!("Using log leader URL for RPC: {}", url);

        let response = match self.operation_type {
            ScaleOperationType::AddNodes => {
                // Create and send the AddPeerRequest
                let request = AddPeerRequest {
                    ip: hosts.clone(),
                    port: ports.clone(),
                    log_group_id: self.log_group_id.unwrap_or(0),
                };
                info!(
                    "AddPeerRequest - ip: {:?}, port: {:?}, log_group_id: {}",
                    request.ip, request.port, request.log_group_id
                );

                info!("Sending add_peer request with {} nodes", hosts.len());

                // Keep sending RPC until success or max retries reached
                let mut retry_count = 0;
                const MAX_RETRIES: u32 = 10;
                loop {
                    match add_log_peer(&url, request.clone()).await {
                        Ok(response) => {
                            info!("AddPeerResponse: {}", response.success);
                            if response.success {
                                break response;
                            }
                            retry_count += 1;
                            info!(
                                "AddPeer operation not successful, retrying (attempt {})",
                                retry_count
                            );

                            // Check if we've exceeded max retries for unsuccessful responses
                            if retry_count >= MAX_RETRIES {
                                let error_msg = format!(
                                    "AddPeer operation failed after {} attempts - response success: {}",
                                    retry_count, response.success
                                );
                                error!("{}", error_msg);

                                // Send failure result through the channel
                                if let Err(e) = self.scale_result_tx.send(false) {
                                    warn!("Failed to send scale result through channel: {}", e);
                                }

                                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                                task_result.insert(
                                    CMD_OUTPUT.to_string(),
                                    TaskArgValue::Str(error_msg.clone()),
                                );
                                return Ok(Some(task_result));
                            }

                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        }
                        Err(e) => {
                            retry_count += 1;
                            error!("Failed to add log peers (attempt {}): {}", retry_count, e);
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

                            // After many retries, give up
                            if retry_count >= MAX_RETRIES {
                                let error_msg = format!(
                                    "Failed to add log peers after {} attempts: {}",
                                    retry_count, e
                                );
                                error!("{}", error_msg);

                                // Send failure result through the channel
                                if let Err(e) = self.scale_result_tx.send(false) {
                                    warn!("Failed to send scale result through channel: {}", e);
                                }

                                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                                task_result.insert(
                                    CMD_OUTPUT.to_string(),
                                    TaskArgValue::Str(error_msg.clone()),
                                );
                                return Ok(Some(task_result));
                            }
                        }
                    }
                }
            }
            ScaleOperationType::RemoveNodes => {
                // Create and send the RemovePeerRequest
                let request = RemovePeerRequest {
                    ip: hosts.clone(),
                    port: ports.clone(),
                    log_group_id: self.log_group_id.unwrap_or(0),
                };
                info!(
                    "RemovePeerRequest - ip: {:?}, port: {:?}, log_group_id: {}",
                    request.ip, request.port, request.log_group_id
                );

                info!("Sending remove_peer request with {} nodes", hosts.len());

                // Keep sending RPC until success or max retries reached
                let mut retry_count = 0;
                const MAX_RETRIES: u32 = 10;
                loop {
                    match remove_log_peer(&url, request.clone()).await {
                        Ok(response) => {
                            info!("RemovePeerResponse: {}", response.success);
                            if response.success {
                                break response;
                            }
                            retry_count += 1;
                            info!(
                                "RemovePeer operation not successful, retrying (attempt {})",
                                retry_count
                            );
                            if retry_count >= MAX_RETRIES {
                                let error_msg = format!(
                                    "RemovePeer operation failed after {} attempts - response success: {}",
                                    retry_count, response.success
                                );
                                error!("{}", error_msg);
                                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                                task_result.insert(
                                    CMD_OUTPUT.to_string(),
                                    TaskArgValue::Str(error_msg.clone()),
                                );
                                task_return_value!(
                                    task_result,
                                    |status_code: i32| -> CmdErr {
                                        CmdErr::ScaleOpErr(error_msg, status_code.to_string())
                                    },
                                    "ScaleLogOpTask"
                                )
                            }
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        }
                        Err(e) => {
                            retry_count += 1;
                            error!(
                                "Failed to remove log peers (attempt {}): {}",
                                retry_count, e
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

                            // After many retries, give up
                            if retry_count >= MAX_RETRIES {
                                let error_msg = format!(
                                    "Failed to remove log peers after {} attempts: {}",
                                    retry_count, e
                                );
                                error!("{}", error_msg);
                                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                                task_result.insert(
                                    CMD_OUTPUT.to_string(),
                                    TaskArgValue::Str(error_msg.clone()),
                                );
                                task_return_value!(
                                    task_result,
                                    |status_code: i32| -> CmdErr {
                                        CmdErr::ScaleOpErr(error_msg, status_code.to_string())
                                    },
                                    "ScaleLogOpTask"
                                )
                            }
                        }
                    }
                }
            }
        };

        // Build execution result
        let operation_name = match self.operation_type {
            ScaleOperationType::AddNodes => "added",
            ScaleOperationType::RemoveNodes => "removed",
        };

        info!(
            "Log node {} operation completed with success: {}",
            operation_name, response.success
        );

        // Send the result through the channel
        if let Err(e) = self.scale_result_tx.send(response.success) {
            warn!("Failed to send scale result through channel: {}", e);
        }

        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
        task_result.insert(
            CMD_OUTPUT.to_string(),
            TaskArgValue::Str(format!(
                "Successfully {} log nodes {}: {}. Response: {}",
                operation_name,
                match self.operation_type {
                    ScaleOperationType::AddNodes => "to",
                    ScaleOperationType::RemoveNodes => "from",
                },
                self.nodes.join(", "),
                response.success
            )),
        );

        Ok(Some(task_result))
    }
}
