use crate::cli::task::redis_op_task::ClusterNodes;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::task_return_value;
use anyhow::Result;
use async_trait::async_trait;
use redis::cmd;
use std::collections::HashMap;
use tokio::sync::watch;
use tracing::{error, info};

#[derive(Clone, Debug)]
pub struct FailoverOpTask {
    task_id: TaskId,
    old_leader_host: String,
    old_leader_port: u16,
    new_leader_host: String,
    new_leader_port: u16,
    receiver: watch::Receiver<ClusterNodes>,
    password: Option<String>,
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
        }
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

        // Check if the specified old leader is actually a leader (master) in the cluster
        let old_leader_found = cluster_nodes
            .masters
            .iter()
            .any(|node| node.ip == self.old_leader_host && node.port == self.old_leader_port);

        if !old_leader_found {
            error!(
                "Specified old leader {}:{} is not a master in the cluster",
                self.old_leader_host, self.old_leader_port
            );
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
            task_result.insert(
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str(format!(
                    "Specified old leader {}:{} is not a master in the cluster",
                    self.old_leader_host, self.old_leader_port
                )),
            );
            task_return_value!(
                task_result,
                |status_code: i32| -> CmdErr {
                    CmdErr::RedisOpErr(
                        "Old leader validation failed".to_string(),
                        status_code.to_string(),
                    )
                },
                "FailoverOpTask"
            )
        }

        // Check if the new leader is a replica in the cluster
        let new_leader_found = cluster_nodes
            .replicas
            .iter()
            .any(|node| node.ip == self.new_leader_host && node.port == self.new_leader_port);

        if !new_leader_found {
            error!(
                "Specified new leader {}:{} is not a replica in the cluster",
                self.new_leader_host, self.new_leader_port
            );
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
            task_result.insert(
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str(format!(
                    "Specified new leader {}:{} is not a replica in the cluster",
                    self.new_leader_host, self.new_leader_port
                )),
            );
            task_return_value!(
                task_result,
                |status_code: i32| -> CmdErr {
                    CmdErr::RedisOpErr(
                        "New leader validation failed".to_string(),
                        status_code.to_string(),
                    )
                },
                "FailoverOpTask"
            )
        }

        // Connect to the old leader (master) to perform the failover operation
        let node_url = if let Some(password) = &self.password {
            format!(
                "redis://:{}@{}:{}",
                password, self.old_leader_host, self.old_leader_port
            )
        } else {
            format!("redis://{}:{}", self.old_leader_host, self.old_leader_port)
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
            self.new_leader_host, self.new_leader_port, self.old_leader_host, self.old_leader_port
        );

        let failover_result = cmd("FAILOVER")
            .arg("TO")
            .arg(format!("{}:{}", self.new_leader_host, self.new_leader_port))
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
                            self.new_leader_host,
                            self.new_leader_port,
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
