use crate::cli::task::redis_op_task::ClusterNodes;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::connection::{resolve_service_endpoint, ServiceEndpoint};
use crate::task_return_value;
use anyhow::Result;
use async_trait::async_trait;
use redis::cmd;
use std::collections::HashMap;
use tokio::sync::watch;
use tracing::{debug, error, info};

#[derive(Clone, Debug)]
pub struct FailoverOpTask {
    task_id: TaskId,
    old_leader_host: String,
    old_leader_port: u16,
    new_leader_host: String, // Can be empty, if empty, will be chosen dynamically
    new_leader_port: u16,    // Can be 0, if 0, will be chosen dynamically
    receiver: watch::Receiver<ClusterNodes>,
    password: Option<String>,
    service_endpoints: Option<HashMap<String, ServiceEndpoint>>,
    require_explicit_target: bool,
}

impl FailoverOpTask {
    pub fn new(
        task_id: TaskId,
        old_leader_host: String,
        old_leader_port: u16,
        new_leader_host: String,
        new_leader_port: u16,
        receiver: watch::Receiver<ClusterNodes>,
        password: Option<String>,
    ) -> Self {
        Self {
            task_id,
            old_leader_host,
            old_leader_port,
            new_leader_host,
            new_leader_port,
            receiver,
            password,
            service_endpoints: None,
            require_explicit_target: false,
        }
    }

    pub fn with_service_endpoints(
        mut self,
        service_endpoints: Option<HashMap<String, ServiceEndpoint>>,
    ) -> Self {
        self.service_endpoints = service_endpoints;
        self
    }

    pub fn require_explicit_target(mut self) -> Self {
        self.require_explicit_target = true;
        self
    }

    // Helper function to find the best replica for failover
    fn find_best_replica(&self, cluster_nodes: &ClusterNodes) -> Option<(String, u16)> {
        // If the caller selected a new_leader_host/port, require it to be a valid replica.
        if !self.new_leader_host.is_empty() && self.new_leader_port > 0 {
            let Some(specified_replica) = cluster_nodes
                .replicas
                .iter()
                .find(|r| r.ip == self.new_leader_host && r.port == self.new_leader_port)
            else {
                if self.require_explicit_target {
                    error!(
                        "Selected new leader {}:{} not found as replica",
                        self.new_leader_host, self.new_leader_port
                    );
                    return None;
                }
                info!(
                    "Selected new leader {}:{} not found as replica, will choose another one",
                    self.new_leader_host, self.new_leader_port
                );
                return cluster_nodes
                    .replicas
                    .iter()
                    .find(|replica| replica.connected)
                    .map(|replica| (replica.ip.clone(), replica.port));
            };

            let old_leader = cluster_nodes
                .masters
                .iter()
                .find(|node| node.ip == self.old_leader_host && node.port == self.old_leader_port);
            if let (Some(old_leader), Some(master_id)) =
                (old_leader, specified_replica.master_id.as_ref())
            {
                if old_leader.node_id.as_ref() != Some(master_id) {
                    error!(
                        "Selected new leader {}:{} is not a replica of old leader {}:{}",
                        self.new_leader_host,
                        self.new_leader_port,
                        self.old_leader_host,
                        self.old_leader_port
                    );
                    return None;
                }
            }

            if specified_replica.connected {
                return Some((self.new_leader_host.clone(), self.new_leader_port));
            }

            error!(
                "Selected new leader {}:{} is not connected as replica",
                self.new_leader_host, self.new_leader_port
            );
            return None;
        }

        if self.require_explicit_target {
            error!(
                "Internal failover target is required for {}:{}, but no target was selected",
                self.old_leader_host, self.old_leader_port
            );
            return None;
        }

        // Select first available replica
        if let Some(replica) = cluster_nodes
            .replicas
            .iter()
            .find(|replica| replica.connected)
        {
            return Some((replica.ip.clone(), replica.port));
        }

        None
    }
}

#[async_trait]
impl TaskExecutor for FailoverOpTask {
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
            TaskArgValue::Str("failover operation".to_string()),
        )]);

        // Get the current cluster nodes from the receiver
        let cluster_nodes = self.receiver.borrow().clone();

        debug!(
            "Executing failover check for node {}:{}",
            self.old_leader_host, self.old_leader_port
        );

        // Check if the specified old leader is actually a leader (master) in the cluster
        let old_leader_found = cluster_nodes
            .masters
            .iter()
            .any(|node| node.ip == self.old_leader_host && node.port == self.old_leader_port);

        if !old_leader_found {
            // Node is not a leader, no need for failover, return success
            debug!(
                "Node {}:{} is not a master in the cluster, no failover needed",
                self.old_leader_host, self.old_leader_port
            );
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
            task_result.insert(
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str(format!(
                    "Node {}:{} is not a master, no failover needed",
                    self.old_leader_host, self.old_leader_port
                )),
            );
            return Ok(Some(task_result));
        }

        // Find the best replica for this leader
        let new_leader = self.find_best_replica(&cluster_nodes);

        if new_leader.is_none() {
            error!(
                "No suitable replica found for master {}:{}. Cannot perform failover.",
                self.old_leader_host, self.old_leader_port
            );
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
            task_result.insert(
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str(format!(
                    "No suitable replica found for master {}:{}. Cannot perform failover.",
                    self.old_leader_host, self.old_leader_port
                )),
            );
            task_return_value!(
                task_result,
                |status_code: i32| -> CmdErr {
                    CmdErr::RedisOpErr(
                        "No suitable replica found".to_string(),
                        status_code.to_string(),
                    )
                },
                "FailoverOpTask"
            )
        }

        let (new_leader_host, new_leader_port) = new_leader.unwrap();
        let (old_endpoint_host, old_endpoint_port) = resolve_service_endpoint(
            self.service_endpoints.as_ref(),
            &self.old_leader_host,
            self.old_leader_port,
        );

        info!(
            "Found suitable replica {}:{} for master {}:{}. Initiating failover.",
            new_leader_host, new_leader_port, self.old_leader_host, self.old_leader_port
        );

        // Connect to the old leader (master) to perform the failover operation
        let node_url = if let Some(password) = &self.password {
            format!(
                "redis://:{}@{}:{}",
                password, old_endpoint_host, old_endpoint_port
            )
        } else {
            format!("redis://{}:{}", old_endpoint_host, old_endpoint_port)
        };

        let client = redis::Client::open(node_url.clone())?;
        let mut con = match client.get_connection() {
            Ok(connection) => connection,
            Err(err) => {
                error!(
                    "Failed to connect to the old leader at {}. Error: {}",
                    node_url, err
                );
                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(err.to_string()));
                task_return_value!(
                    task_result,
                    |status_code: i32| -> CmdErr {
                        CmdErr::RedisOpErr(
                            "Connection to old leader failed".to_string(),
                            status_code.to_string(),
                        )
                    },
                    "FailoverOpTask"
                )
            }
        };

        // Execute FAILOVER TO command
        info!(
            "Executing FAILOVER TO {}:{} from old leader {}:{}",
            new_leader_host, new_leader_port, self.old_leader_host, self.old_leader_port
        );

        let failover_result = cmd("FAILOVER")
            .arg("TO")
            .arg(format!("{}:{}", new_leader_host, new_leader_port))
            .query::<String>(&mut con);

        match failover_result {
            Ok(response) => {
                if response == "OK" {
                    info!("Failover initiated successfully: {}", response);
                    task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
                    task_result.insert(
                        CMD_OUTPUT.to_string(),
                        TaskArgValue::Str(format!(
                            "Successfully initiated failover from {}:{} to {}:{}. Response: {}",
                            self.old_leader_host,
                            self.old_leader_port,
                            new_leader_host,
                            new_leader_port,
                            response
                        )),
                    );
                } else {
                    error!(
                        "Failover command returned unexpected response: {}",
                        response
                    );
                    task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                    task_result.insert(
                        CMD_OUTPUT.to_string(),
                        TaskArgValue::Str(format!(
                            "Failover command returned unexpected response: {}",
                            response
                        )),
                    );
                    task_return_value!(
                        task_result,
                        |status_code: i32| -> CmdErr {
                            CmdErr::RedisOpErr(
                                "Failover execution failed with unexpected response".to_string(),
                                status_code.to_string(),
                            )
                        },
                        "FailoverOpTask"
                    )
                }
            }
            Err(err) => {
                error!("Failed to execute failover: {}", err);
                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(err.to_string()));
                task_return_value!(
                    task_result,
                    |status_code: i32| -> CmdErr {
                        CmdErr::RedisOpErr(
                            "Failover execution failed".to_string(),
                            status_code.to_string(),
                        )
                    },
                    "FailoverOpTask"
                )
            }
        }

        Ok(Some(task_result))
    }
}
