use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::{load_yaml_config_template, CASSANDRA_CONF_TEMPLATE};
use async_trait::async_trait;
use cdrs_tokio::authenticators::NoneAuthenticatorProvider;
use cdrs_tokio::cluster::session::{Session, SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::{NodeAddress, NodeTcpConfigBuilder, TcpConnectionManager};
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::retry::{DefaultRetryPolicy, NeverReconnectionPolicy};
use cdrs_tokio::transport::TransportTcp;
use itertools::Itertools;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;

pub(crate) const CASS_CQL_STMT: &str = "_CASS_CQL_STMT_";

#[derive(Clone, Debug)]
pub struct CassandraOpTask {
    task_id: TaskId,
    cass_host: String,
}

impl CassandraOpTask {
    pub fn new(cass_host: String, task_id: TaskId) -> Self {
        Self { cass_host, task_id }
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
        //let cass_hosts = self.config.get_host_list(DeploymentPackage::Storage);
        let cass_config = load_yaml_config_template(CASSANDRA_CONF_TEMPLATE)?;
        let client_transport_port = cass_config
            .get("native_transport_port")
            .unwrap()
            .as_i64()
            .unwrap();

        let contact_point =
            NodeAddress::from(format!("{}:{client_transport_port}", self.cass_host));

        println!("CassandraOpTask contact_points={:#?}", contact_point);

        let cluster_config = NodeTcpConfigBuilder::new()
            .with_authenticator_provider(Arc::new(NoneAuthenticatorProvider {}))
            .with_contact_point(contact_point)
            .build()
            .await?;

        let session =
            TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config)
                .with_reconnection_policy(Arc::new(NeverReconnectionPolicy))
                .with_retry_policy(Box::<DefaultRetryPolicy>::default())
                .build()
                .await?;

        println!("CassandraOpTask create session success!");
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
        let session = self.cassandra_session().await.unwrap();
        let mut task_result = HashMap::new();
        let cql_stmt = TaskArgValue::into_inner_value::<String>(cql_stmt_input.unwrap().clone());
        task_result.insert(CMD.to_string(), TaskArgValue::Str(cql_stmt.clone()));

        let query_rs = session.query(cql_stmt.as_str()).await;
        if let Err(query_err) = query_rs {
            let err_msg = query_err.to_string();
            error!(
                "Error querying cassandra stmt={}, error msg ={}",
                cql_stmt, err_msg
            );
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
            task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(err_msg));
            return Ok(Some(task_result));
        }

        let result = query_rs.unwrap().response_body()?;
        let query_result = result.clone().into_rows();
        let metadata = result.as_rows_metadata();
        task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
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
                                format!("{}\t{}:{}", idx, col_spec.name, col_type)
                            })
                            .join("\t"),
                        None => {
                            format!("{idx} {row:#?}")
                        }
                    };
                    row_str
                })
                .join("\n");
            println!("{row_as_string}");
            task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(row_as_string));
        } else {
            println!("CassandraOpTask No data was queried.");
            task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str("".to_string()));
        }

        Ok(Some(task_result))
    }
}
