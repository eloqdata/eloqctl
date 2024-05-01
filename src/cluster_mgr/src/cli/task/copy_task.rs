use super::task_base::{TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance};
use crate::cli::task::task_base::ExecutionValue;
use crate::cli::upload_dir;
use crate::config::config_base::DeploymentConfig;
use crate::config::connection::Connection;
use anyhow::bail;
use std::collections::HashMap;
use tracing::info;

#[derive(Debug, Clone)]
pub struct CopyTask {
    id: TaskId,
    conn: Connection,
    src_host: String,
    src_path: String,
    dst_path: String,
}

impl CopyTask {
    pub fn new(
        id: TaskId,
        conn: Connection,
        src_host: String,
        src_path: String,
        dst_path: String,
    ) -> Self {
        Self {
            id,
            conn,
            src_host,
            src_path,
            dst_path,
        }
    }

    pub fn fetch_datafarm(deploy: &DeploymentConfig) -> (TaskId, TaskInstance) {
        let id = TaskId {
            cmd: "install".to_owned(),
            task: "fetch_datafarm".to_owned(),
            host: "_NONE".to_owned(),
        };
        let boot_node = deploy.deployment.bootstrap_host();
        let src_path = format!("{}/datafarm", deploy.install_dir());
        let dst_path = upload_dir().to_string_lossy().to_string();
        let copy = Self::new(
            id.clone(),
            deploy.connection.clone(),
            boot_node,
            src_path,
            dst_path,
        );
        let task = TaskInstance {
            task_input: HashMap::default(),
            task: Box::new(copy),
            task_host: TaskHost::Local,
        };
        (id, task)
    }
}

#[async_trait::async_trait]
impl TaskExecutor for CopyTask {
    fn identifier(&self) -> TaskId {
        self.id.clone()
    }

    async fn execute(
        &self,
        _task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        let source = format!("{}@{}:{}", self.conn.username, self.src_host, self.src_path);
        let mut cmd = tokio::process::Command::new("scp");
        cmd.args(&[
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "StrictHostKeyChecking=no",
            "-i",
            &self.conn.ssh_auth_key().unwrap(),
            "-P",
            &self.conn.ssh_port().to_string(),
            "-r",
            &source,
            &self.dst_path,
        ]);
        let out = cmd.output().await?;
        info!("CopyTask {source} -> {}:\n{:#?}", self.dst_path, out);
        if !out.status.success() {
            bail!(
                "CopyTask {source} -> {}: {:?}",
                self.dst_path,
                out.status.code()
            );
        }
        Ok(None)
    }
}
