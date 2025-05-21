use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::state::scale_operation::{ScaleEntity, ScaleOperation};
use crate::state::state_base::StateOperation;
use crate::state::state_mgr::{SCALE_STATE, STATE_MGR};
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use tokio::sync::watch;
use tokio::time::{timeout, Duration};
use tracing::{error, info, warn};

/// Task for updating the scale operation state in the database for a specific stage
#[derive(Clone, Debug)]
pub struct DbScaleOpUpdateTask {
    task_id: TaskId,
    event_id: String,
    operation_type: i32,
    nodes_list: String,
    is_candidate: Option<String>,
    stage: i32,
    scale_status_rx: Option<watch::Receiver<i32>>,
    cluster_name: String,
}

impl DbScaleOpUpdateTask {
    /// Create a new UpdateScaleStatusTask
    pub fn new(
        task_id: TaskId,
        event_id: String,
        operation_type: i32,
        nodes_list: Vec<String>,
        is_candidate: Option<Vec<bool>>,
        stage: i32,
        cluster_name: String,
    ) -> Self {
        // Convert Vec<bool> to CSV string of booleans
        let is_candidate_str = is_candidate.map(|v| {
            v.iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join(",")
        });

        Self {
            task_id,
            event_id,
            operation_type,
            nodes_list: nodes_list.join(","),
            is_candidate: is_candidate_str,
            stage,
            scale_status_rx: None,
            cluster_name,
        }
    }

    /// Create a new UpdateScaleStatusTask with RPC status channel
    pub fn new_with_status_channel(
        task_id: TaskId,
        event_id: String,
        operation_type: i32,
        nodes_list: Vec<String>,
        is_candidate: Option<Vec<bool>>,
        scale_status_rx: watch::Receiver<i32>,
        cluster_name: String,
    ) -> Self {
        // Convert Vec<bool> to CSV string of booleans
        let is_candidate_str = is_candidate.map(|v| {
            v.iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join(",")
        });

        Self {
            task_id,
            event_id,
            operation_type,
            nodes_list: nodes_list.join(","),
            is_candidate: is_candidate_str,
            stage: 0, // Default stage, will be updated if NOT_STARTED is received
            scale_status_rx: Some(scale_status_rx),
            cluster_name,
        }
    }

    /// Update the scale operation status in the database
    async fn update_scale_status(&self, stage: i32, error_message: Option<String>) -> Result<()> {
        let op = STATE_MGR.get_state_operation::<ScaleOperation>(SCALE_STATE);

        let now = Utc::now();
        let entity = ScaleEntity {
            event_id: self.event_id.clone(),
            cluster_name: self.cluster_name.clone(),
            operation_type: self.operation_type,
            nodes_list: self.nodes_list.clone(),
            is_candidate: self.is_candidate.clone(),
            stage,
            error_message,
            create_timestamp: now,
            update_timestamp: now,
        };

        // Perform the upsert operation
        op.put(entity).await?;

        Ok(())
    }
}

#[async_trait]
impl TaskExecutor for DbScaleOpUpdateTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        _task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> Result<Option<ExecutionValue>> {
        // Initialize result for RPC status channel case
        let mut result = HashMap::new();
        result.insert(
            CMD.to_string(),
            TaskArgValue::Str("update scale status".to_string()),
        );

        // If we have an RPC status channel, check it first
        if let Some(rx) = &self.scale_status_rx {
            info!("Watching for scale status updates from RPC response");

            // Clone the receiver for watching
            let mut status_rx = rx.clone();

            // Try to wait for a status update, with timeout
            match timeout(Duration::from_secs(120), status_rx.changed()).await {
                Ok(Ok(_)) => {
                    // Get the current value
                    let status = *status_rx.borrow();
                    info!("Received status update: {}", status);

                    // 1 = NOT_STARTED
                    if status == 1 {
                        // NOT_STARTED - mark as failed
                        info!("Received NOT_STARTED status, marking scale operation as failed");
                        let error_msg = Some("Scale operation has NOT_STARTED status, indicating failure or operation never began".to_string());

                        if let Err(e) = self.update_scale_status(-1, error_msg.clone()).await {
                            error!("Failed to update scale status: {}", e);
                            return Err(anyhow!("Failed to update scale status: {}", e));
                        }

                        // Return error to abort the operation
                        return Err(anyhow!("Scale operation aborted: NOT_STARTED status detected, indicating the operation failed or never began"));
                    }

                    // For other statuses, continue with normal operation
                    info!("RPC status is not NOT_STARTED, continuing with normal operation (stage {})", self.stage);

                    // Update the scale operation with the current stage
                    if let Err(e) = self.update_scale_status(self.stage, None).await {
                        error!("Failed to update scale status: {}", e);
                        return Err(anyhow!("Failed to update scale status: {}", e));
                    }
                }
                Ok(Err(e)) => {
                    warn!(
                        "Error watching scale status channel: {}, continuing with normal operation",
                        e
                    );
                    // Continue with normal operation anyway
                    if let Err(e) = self.update_scale_status(self.stage, None).await {
                        error!("Failed to update scale status: {}", e);
                        return Err(anyhow!("Failed to update scale status: {}", e));
                    }
                }
                Err(_) => {
                    warn!(
                        "Timeout waiting for scale status update, continuing with normal operation"
                    );
                    // Continue with normal operation anyway
                    if let Err(e) = self.update_scale_status(self.stage, None).await {
                        error!("Failed to update scale status: {}", e);
                        return Err(anyhow!("Failed to update scale status: {}", e));
                    }
                }
            }

            // For cases where we're using RPC status, return result
            result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
            result.insert(
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str(format!(
                    "Successfully updated scale status to stage {}",
                    self.stage
                )),
            );
            return Ok(Some(result));
        } else {
            let op = STATE_MGR.get_state_operation::<ScaleOperation>(SCALE_STATE);

            // Regular update operation
            let now = Utc::now();
            let entity = ScaleEntity {
                event_id: self.event_id.clone(),
                cluster_name: self.cluster_name.clone(),
                operation_type: self.operation_type,
                nodes_list: self.nodes_list.clone(),
                is_candidate: self.is_candidate.clone(),
                stage: self.stage,
                error_message: None,
                create_timestamp: now,
                update_timestamp: now,
            };

            // Perform the upsert operation
            op.put(entity).await?;

            // No additional command output for regular cases
            Ok(None)
        }
    }
}
