use crate::cli::task::grpc::GrpcClient;
use crate::cli::task::redis_op_task::ClusterNodes;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::task_return_value;
use async_trait::async_trait;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use tracing::{error, info};

#[derive(Debug, Clone)]
pub struct CheckTxClusterScaleStatusTask {
    task_id: TaskId,
    event_id: String,
    poll_until_finished: bool,
    scale_status_tx: Option<tokio::sync::watch::Sender<i32>>,
    redis_op_rx: Option<tokio::sync::watch::Receiver<ClusterNodes>>,
    valid_candidate_nodes: Option<Vec<String>>,
}

impl CheckTxClusterScaleStatusTask {
    pub fn new(
        task_id: TaskId,
        event_id: String,
        poll_until_finished: bool,
        scale_status_tx: Option<tokio::sync::watch::Sender<i32>>,
        redis_op_rx: Option<tokio::sync::watch::Receiver<ClusterNodes>>,
        valid_candidate_nodes: Option<Vec<String>>,
    ) -> Self {
        Self {
            task_id,
            event_id,
            poll_until_finished,
            scale_status_tx,
            redis_op_rx,
            valid_candidate_nodes,
        }
    }

    fn get_endpoint(&self) -> String {
        // Try to find the current master from redis_op_rx
        if let Some(ref rx) = self.redis_op_rx {
            let current_nodes = rx.borrow().clone();
            if !current_nodes.masters.is_empty() {
                let leader = &current_nodes.masters[0];
                let tx_host = &leader.ip;
                let tx_port = leader.port + 10001;

                info!(
                    "Using leader node {}:{} from redis_op_rx for scale validation",
                    tx_host, tx_port
                );
                format!("http://{}:{}", tx_host, tx_port)
            } else {
                unreachable!()
            }
        } else {
            let first_candidate_node = self.valid_candidate_nodes.as_ref().unwrap()[0].clone();
            let parts: Vec<&str> = first_candidate_node.split(':').collect();
            if parts.len() != 2 {
                unreachable!()
            }
            let host = parts[0].to_string();
            let port = parts[1].parse::<u16>().unwrap() + 10001;

            info!(
                "Using first candidate node {}:{} from config for scale validation",
                host, port
            );
            format!("http://{}:{}", host, port)
        }
    }
}

#[async_trait]
impl TaskExecutor for CheckTxClusterScaleStatusTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        _task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        let mut task_result = HashMap::from([(
            CMD.to_string(),
            TaskArgValue::Str("validate cluster scale".to_string()),
        )]);

        info!(
            "Validating cluster scale status for event_id: {}",
            self.event_id
        );

        let endpoint = self.get_endpoint();

        info!("Connecting to cc_node_service at {}", endpoint);

        let max_retries = 3;
        let retry_delay = Duration::from_secs(2);
        let mut last_error = None;

        for attempt in 1..=max_retries {
            match GrpcClient::new(&endpoint).await {
                Ok(mut client) => {
                    if self.poll_until_finished {
                        // Poll until status is FINISHED (3), with timeout
                        info!("Polling for cluster scale status until FINISHED");
                        let poll_interval = Duration::from_secs(5);
                        let max_poll_duration = Duration::from_secs(600); // 10 minutes timeout
                        let start_time = std::time::Instant::now();

                        loop {
                            match client
                                .check_cluster_scale_status(self.event_id.clone())
                                .await
                            {
                                Ok(response) => {
                                    info!("Received cluster scale status: {:?}", response.status);

                                    // If status is FINISHED (3), break the loop
                                    if response.status == 3 {
                                        // FINISHED
                                        info!("Cluster scale operation is FINISHED");
                                        let status_value = response.status as i32;

                                        // Send status through channel if available
                                        if let Some(tx) = &self.scale_status_tx {
                                            let _ = tx.send(status_value);
                                            info!("Sent FINISHED status through channel");
                                        }

                                        task_result.insert(
                                            CMD_STATUS.to_string(),
                                            TaskArgValue::Number(0),
                                        );
                                        task_result.insert(
                                            "status".to_string(),
                                            TaskArgValue::Number(status_value),
                                        );
                                        task_result.insert(
                                            CMD_OUTPUT.to_string(),
                                            TaskArgValue::Str(format!(
                                                "Successfully validated cluster scale status: {}",
                                                status_value
                                            )),
                                        );
                                        return Ok(Some(task_result));
                                    }

                                    // If status is NOT_STARTED (1), mark the scale as failed
                                    if response.status == 1 {
                                        info!("Cluster scale status is NOT_STARTED");
                                        let status_value = response.status as i32;

                                        // Send status through channel if available
                                        if let Some(tx) = &self.scale_status_tx {
                                            let _ = tx.send(status_value);
                                            info!("Sent NOT_STARTED status through channel");
                                        }

                                        // Return success instead of failing here
                                        task_result.insert(
                                            CMD_STATUS.to_string(),
                                            TaskArgValue::Number(0),
                                        );
                                        task_result.insert(
                                            "status".to_string(),
                                            TaskArgValue::Number(status_value),
                                        );
                                        task_result.insert(
                                            CMD_OUTPUT.to_string(),
                                            TaskArgValue::Str("Scale operation is in NOT_STARTED state, process will be handled by the UpdateScaleStatusTask".to_string()),
                                        );
                                        return Ok(Some(task_result));
                                    }

                                    // Check if we've exceeded our timeout
                                    if start_time.elapsed() > max_poll_duration {
                                        let status_value = response.status as i32;
                                        if let Some(tx) = &self.scale_status_tx {
                                            let _ = tx.send(status_value);
                                            if status_value == 0 {
                                                info!("Sent UNKNOWN status through channel");
                                            } else {
                                                info!("Sent IN_PROGRESS status through channel");
                                            }
                                        }

                                        let error_msg = format!(
                                            "Timed out waiting for cluster scale to finish. Current status: {:?}",
                                            response.status
                                        );
                                        error!("{}", error_msg);
                                        task_result.insert(
                                            CMD_STATUS.to_string(),
                                            TaskArgValue::Number(1),
                                        );
                                        task_result.insert(
                                            CMD_OUTPUT.to_string(),
                                            TaskArgValue::Str(error_msg.clone()),
                                        );
                                        task_return_value!(
                                            task_result,
                                            |status_code: i32| -> CmdErr {
                                                CmdErr::ValidationErr(
                                                    error_msg,
                                                    status_code.to_string(),
                                                )
                                            },
                                            "CheckTxClusterScaleStatusTask"
                                        );
                                    }

                                    // Wait before next poll
                                    info!(
                                        "Waiting for {} seconds before checking again...",
                                        poll_interval.as_secs()
                                    );
                                    sleep(poll_interval).await;
                                }
                                Err(e) => {
                                    // Log the error but continue polling
                                    error!("Error checking status: {}, will retry...", e);
                                    sleep(poll_interval).await;

                                    // Check if we've exceeded our timeout
                                    if start_time.elapsed() > max_poll_duration {
                                        let error_msg = format!(
                                            "Timed out waiting for cluster scale to finish after error: {}",
                                            e
                                        );
                                        error!("{}", error_msg);
                                        task_result.insert(
                                            CMD_STATUS.to_string(),
                                            TaskArgValue::Number(1),
                                        );
                                        task_result.insert(
                                            CMD_OUTPUT.to_string(),
                                            TaskArgValue::Str(error_msg.clone()),
                                        );
                                        task_return_value!(
                                            task_result,
                                            |status_code: i32| -> CmdErr {
                                                CmdErr::ValidationErr(
                                                    error_msg,
                                                    status_code.to_string(),
                                                )
                                            },
                                            "CheckTxClusterScaleStatusTask"
                                        );
                                    }
                                }
                            }
                        }
                    } else {
                        unreachable!()
                    }
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    error!(
                        "Attempt {}/{}: Failed to connect to cc_node_service: {}",
                        attempt,
                        max_retries,
                        last_error.as_ref().unwrap()
                    );
                    if attempt < max_retries {
                        info!("Retrying in {} seconds...", retry_delay.as_secs());
                        sleep(retry_delay).await;
                    }
                }
            }
        }

        let error_msg = format!(
            "Failed to validate cluster scale status after {} attempts: {}",
            max_retries,
            last_error.unwrap_or_else(|| "Unknown error".to_string())
        );
        error!("{}", error_msg);

        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
        task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(error_msg.clone()));
        task_return_value!(
            task_result,
            |status_code: i32| -> CmdErr {
                CmdErr::ValidationErr(error_msg, status_code.to_string())
            },
            "CheckTxClusterScaleStatusTask"
        )
    }
}
