use crate::cli::config::DeploymentConfig;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::MONOGRAPH_INSTALL_SCRIPT;
use crate::{ssh_conn_info, task_return_value};
use async_trait::async_trait;
use std::collections::HashMap;
use tracing::info;

#[derive(Debug, Clone)]
pub struct MonographInstall {
    config: DeploymentConfig,
    task_id: TaskId,
}

impl MonographInstall {
    pub fn from_config(config: &DeploymentConfig, task_host: TaskHost) -> Vec<TaskInstance> {
        vec![TaskInstance {
            task_input: HashMap::default(),
            task: Box::new(MonographInstall::new(
                config.clone(),
                TaskId {
                    cmd: "install".to_string(),
                    task: "monograph-install".to_string(),
                },
            )),
            task_host,
        }]
    }

    pub fn new(config: DeploymentConfig, task_id: TaskId) -> Self {
        Self { config, task_id }
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
        ssh_conn_info! {
            self.config.connection.clone(),
            task_host,
            ssh_conn,
            _conn_user,
            _conn_host
        }

        let remote_install_dir = self.config.install_dir();
        let install_db_script = format!(
            r#"export LD_LIBRARY_PATH={}/monographdb-release/install/lib:$LD_LIBRARY_PATH; /bin/bash {}/{}"#,
            remote_install_dir.as_str(),
            remote_install_dir.as_str(),
            MONOGRAPH_INSTALL_SCRIPT
        );
        let install_rs = ssh_conn?.run_cmd(install_db_script.clone(), true)?;

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
