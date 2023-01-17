use crate::cli::config::{DeploymentConfig, StorageProvider};
use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::cassandra_op_task::{CassandraOpTask, CASS_CQL_STMT};
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::{CMD_OUTPUT, MONOGRAPH_INSTALL_SCRIPT};
use crate::task_return_value;
use async_trait::async_trait;
use indexmap::IndexMap;
use owo_colors::OwoColorize;
use std::collections::HashMap;

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
        let keyspace = self.config.get_monograph_keyspace()?;
        let keyspace_cql = format!(
            r#"select keyspace_name from system_schema.keyspaces where keyspace_name='{}'"#,
            keyspace
        );
        let cassandra_op_task = CassandraOpTask::new(
            self.config.clone(),
            TaskId {
                cmd: "install".to_string(),
                task: "cassandra_op".to_string(),
                host: "_local".to_string(),
            },
        );
        let cassandra_op_task_rs = cassandra_op_task
            .execute(
                TaskHost::Local,
                HashMap::from([(CASS_CQL_STMT.to_string(), TaskArgValue::Str(keyspace_cql))]),
            )
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
        println!("{} execute.\n", self.task_id.pretty_string());

        let storage_service = self.config.get_monograph_storage()?;
        let keyspace_exists = match storage_service {
            StorageProvider::Cassandra => self.monograph_keyspace_exists().await?,
            _ => false,
        };
        if keyspace_exists {
            let keyspace_name = self.config.get_monograph_keyspace()?;
            let format = r#"MonographDB keyspace already exists."#.red();
            println!("{} => {}", format, keyspace_name.red());
            return Ok(None);
        }
        let ssh_session =
            SSHSession::from_task_host(task_host, self.config.connection.ssh_auth_key().unwrap())
                .await?;
        let remote_install_dir = self.config.install_dir();
        let install_db_script = format!(
            r#"mkdir -p {}/monographdb-release/logs; export LD_LIBRARY_PATH={}/monographdb-release/install/lib:$LD_LIBRARY_PATH; /bin/bash {}/{} > {}/monographdb-release/logs/monograph_init.log 2>&1 "#,
            remote_install_dir.as_str(),
            remote_install_dir.as_str(),
            remote_install_dir.as_str(),
            MONOGRAPH_INSTALL_SCRIPT,
            remote_install_dir.as_str(),
        );
        let install_rs = ssh_session
            .command(install_db_script.as_str(), CollectOutput)
            .await?;

        ssh_session.close().await?;
        task_return_value!(
            install_rs,
            |status_code: usize| -> CmdErr {
                CmdErr::MonographInstallErr(install_db_script, status_code.to_string())
            },
            "MonographInstall",
            HashMap::from([(
                "MONOGRAPH_DATA_DIR".to_string(),
                TaskArgValue::Str(format!("{}/datafarm", remote_install_dir))
            )])
        );
    }
}
