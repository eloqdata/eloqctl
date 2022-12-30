use crate::cli::config::{DeploymentConfig, DeploymentService};
use crate::cli::task::ssh_conn::{SSH_EXEC_CMD, SSH_EXEC_CMD_OUTPUT, SSH_EXEC_CMD_STATUS};
use crate::cli::task::task_base::{
    ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use async_trait::async_trait;
use cdrs_tokio::authenticators::NoneAuthenticatorProvider;
use cdrs_tokio::cluster::session::{Session, SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::{NodeAddress, NodeTcpConfigBuilder, TcpConnectionManager};
use cdrs_tokio::frame::message_result::ColType;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::retry::{ConstantReconnectionPolicy, DefaultRetryPolicy};
use cdrs_tokio::transport::TransportTcp;
use cdrs_tokio::types::IntoRustByName;
use itertools::Itertools;
use owo_colors::OwoColorize;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;

pub(crate) const CASS_CQL_STMT: &str = "_CASS_CQL_STMT_";

#[derive(Clone, Debug)]
pub struct CassandraOpTask {
    config: DeploymentConfig,
    task_id: TaskId,
}

impl CassandraOpTask {
    #[allow(dead_code)]
    pub fn from_config(cmd: String, cql: String, config: &DeploymentConfig) -> Vec<TaskInstance> {
        let task_id = TaskId {
            cmd,
            task: "cassandra-op".to_string(),
            host: "_local".to_string(),
        };

        vec![TaskInstance {
            task_input: HashMap::from([(CASS_CQL_STMT.to_string(), TaskArgValue::Str(cql))]),
            task: Box::new(CassandraOpTask::new(config.clone(), task_id)),
            task_host: TaskHost::Local,
        }]
    }

    pub fn new(config: DeploymentConfig, task_id: TaskId) -> Self {
        Self { config, task_id }
    }

    pub async fn cassandra_session(
        &self,
    ) -> anyhow::Result<
        Session<
            TransportTcp,
            TcpConnectionManager,
            RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
        >,
    > {
        let cass_hosts = self.config.get_host_list(DeploymentService::Storage);
        let cass_config = self.config.load_cassandra_config_template()?;
        let client_transport_port = cass_config
            .get("native_transport_port")
            .unwrap()
            .as_i64()
            .unwrap();

        let contact_points = cass_hosts
            .iter()
            .map(|host| NodeAddress::from(format!("{}:{}", host, client_transport_port)))
            .collect_vec();

        let cluster_config = NodeTcpConfigBuilder::new()
            .with_contact_points(contact_points)
            .with_authenticator_provider(Arc::new(NoneAuthenticatorProvider))
            .build()
            .await?;

        let session =
            TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config)
                .with_reconnection_policy(Arc::new(ConstantReconnectionPolicy::default()))
                .with_retry_policy(Box::<DefaultRetryPolicy>::default())
                .build()?;
        Ok(session)
    }
}

#[async_trait]
impl TaskExecutor for CassandraOpTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        _task_host: TaskHost,
        task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        let cql_stmt_input = task_arg.get(CASS_CQL_STMT);
        assert!(cql_stmt_input.is_some());
        let cass_session = self.cassandra_session().await;
        let mut task_result = HashMap::new();
        let cql_stmt = TaskArgValue::into_inner_value::<String>(cql_stmt_input.unwrap().clone());
        task_result.insert(
            SSH_EXEC_CMD.to_string(),
            TaskArgValue::Str(cql_stmt.clone()),
        );
        if let Err(cass_err) = cass_session {
            let err_msg = cass_err.to_string();
            error!("Create new cassandra connection error cause by {}", err_msg);
            task_result.insert(SSH_EXEC_CMD_STATUS.to_string(), TaskArgValue::Number(1));
            task_result.insert(SSH_EXEC_CMD_OUTPUT.to_string(), TaskArgValue::Str(err_msg));
            return Ok(Some(task_result));
        }
        let session = cass_session?;
        let query_rs = session.query(cql_stmt.as_str()).await;
        if let Err(query_err) = query_rs {
            let err_msg = query_err.to_string();
            error!(
                "Error querying cassandra stmt={}, error msg ={}",
                cql_stmt, err_msg
            );
            task_result.insert(SSH_EXEC_CMD_STATUS.to_string(), TaskArgValue::Number(1));
            task_result.insert(SSH_EXEC_CMD_OUTPUT.to_string(), TaskArgValue::Str(err_msg));
            return Ok(Some(task_result));
        }

        let result = query_rs.unwrap().response_body()?;
        let query_result = result.clone().into_rows();
        let metadata = result.as_rows_metadata();
        task_result.insert(SSH_EXEC_CMD_STATUS.to_string(), TaskArgValue::Number(0));
        if let Some(rows) = query_result {
            let row_as_string = rows
                .iter()
                .enumerate()
                .map(|(idx, row)| {
                    let row_str = match metadata {
                        Some(rows_meta) => rows_meta
                            .col_specs
                            .iter()
                            .map(|col_spec| {
                                let col_type = col_spec.clone().col_type.id;
                                let col_value = match col_type {
                                    ColType::Varchar => {
                                        let char_val: String = row
                                            .get_r_by_name(col_spec.name.as_str())
                                            .expect("NONE");
                                        char_val
                                    }
                                    ColType::Boolean => {
                                        let bool_val: bool = row
                                            .get_r_by_name(col_spec.name.as_str())
                                            .expect("NONE");
                                        bool_val.to_string()
                                    }
                                    _ => {
                                        format!("{} un support {}", col_spec.name, col_type)
                                    }
                                };
                                format!("{} {}", idx.green(), col_value)
                            })
                            .join("\t"),
                        None => {
                            format!("{} {:#?}", idx, row)
                        }
                    };
                    row_str
                })
                .join("\n");
            println!("{}", row_as_string);
            task_result.insert(
                SSH_EXEC_CMD_OUTPUT.to_string(),
                TaskArgValue::Str(row_as_string),
            );
        } else {
            println!("CassandraOpTask No data was queried.");
            task_result.insert(
                SSH_EXEC_CMD_OUTPUT.to_string(),
                TaskArgValue::Str("".to_string()),
            );
        }

        Ok(Some(task_result))
    }
}
