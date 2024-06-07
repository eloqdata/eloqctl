use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::cassandra_op_task::CassandraOpTask;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::CMD_OUTPUT;
use crate::config::config_base::{export_asan, DeploymentConfig};
use crate::config::deployment::{Product, Version};
use crate::config::{StorageProvider, MONOGRAPH_INSTALL_SCRIPT};
use crate::task_return_value;
use async_trait::async_trait;
use indexmap::IndexMap;
use owo_colors::OwoColorize;
use std::collections::HashMap;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct MonographInstall {
    config: DeploymentConfig,
    task_id: TaskId,
}

impl MonographInstall {
    pub fn from_config(
        config: &DeploymentConfig,
        task_host: TaskHost,
    ) -> IndexMap<TaskId, TaskInstance> {
        let (_, _, host) = task_host.ssh_conn_tuple();
        let task_id = TaskId {
            cmd: "install".to_string(),
            task: "monograph-install".to_string(),
            host,
        };
        IndexMap::from([(
            task_id.clone(),
            TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(MonographInstall::new(config.clone(), task_id)),
                task_host,
            },
        )])
    }

    pub fn new(config: DeploymentConfig, task_id: TaskId) -> Self {
        Self { config, task_id }
    }

    pub async fn monograph_keyspace_exists(&self) -> anyhow::Result<bool> {
        let keyspace = self.config.deployment.get_keyspace()?;
        let cql = format!(
            r#"select keyspace_name from system_schema.keyspaces where keyspace_name='{keyspace}'"#,
        );
        let cass = self
            .config
            .deployment
            .storage_service
            .cassandra
            .as_ref()
            .unwrap();
        let cass_host = cass.host.first().unwrap().clone();
        let id = TaskId {
            cmd: "install".to_string(),
            task: "cassandra_op".to_string(),
            host: "_local".to_string(),
        };
        let cassandra_op_task =
            CassandraOpTask::new(id, cass_host.clone(), cass.client_port()?, cql);
        let cassandra_op_task_rs = cassandra_op_task
            .execute(TaskHost::Local, HashMap::default())
            .await?
            .unwrap();
        let mono_keyspace_value = TaskArgValue::into_inner_value::<String>(
            cassandra_op_task_rs.get(CMD_OUTPUT).unwrap().clone(),
        );
        Ok(!mono_keyspace_value.is_empty())
    }
}

#[async_trait]
impl TaskExecutor for MonographInstall {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        info!("execute {}", self.task_id.pretty_string());
        let keyspace_exists = match self.config.get_monograph_storage()? {
            StorageProvider::Cassandra => self.monograph_keyspace_exists().await?,
            _ => false,
        };
        if keyspace_exists {
            let keyspace_name = self.config.deployment.get_monograph_keyspace()?;
            let format = r#"MonographDB keyspace already exists."#.red();
            warn!("{} => {}", format, keyspace_name.red());
            return Ok(None);
        }

        let ssh_session =
            SSHSession::from_task_host(task_host, self.config.connection.ssh_auth_key().unwrap())
                .await?;
        let insdir = self.config.install_dir();
        let txsv_dir = self.config.deployment.tx_srv_home();
        let tx_logs = self.config.deployment.tx_srv_logs();
        let bootstarp_sh = match self.config.product() {
            Product::EloqSQL => {
                format!(
                    "mkdir -p {txsv_dir}/logs; /bin/bash {insdir}/{MONOGRAPH_INSTALL_SCRIPT} > {tx_logs}/bootstrap.log 2>&1 ",
                )
            }
            Product::EloqKV => {
                let tx_ini = self.config.deployment.tx_srv_ini();
                let head = if let Version::Debug = self.config.deployment.version() {
                    export_asan(&format!("{tx_logs}/bootstrap-asan"))
                } else {
                    format!("export LD_PRELOAD={txsv_dir}/lib/libmimalloc.so.2")
                };
                format!(
                    r#"mkdir -p {tx_logs}; export LD_LIBRARY_PATH={txsv_dir}/lib:$LD_LIBRARY_PATH; \
                    {head}; {txsv_dir}/bin/eloqkv --config={tx_ini} --bootstrap > {tx_logs}/bootstrap.log 2>&1 "#
                )
            }
        };
        let install_rs = ssh_session.command(&bootstarp_sh, CollectOutput).await?;
        ssh_session.close().await?;
        task_return_value!(
            install_rs,
            |status_code: i32| -> CmdErr {
                CmdErr::MonographInstallErr(bootstarp_sh, status_code.to_string())
            },
            "MonographInstall",
            HashMap::from([(
                "MONOGRAPH_DATA_DIR".to_string(),
                TaskArgValue::Str(format!("{}/datafarm", txsv_dir))
            )])
        );
    }
}
