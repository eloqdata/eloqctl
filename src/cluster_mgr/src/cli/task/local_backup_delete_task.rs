use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::task::group::Config;
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId};
use crate::cli::{ssh, CMD_OUTPUT, CMD_STATUS};
use crate::state::snapshot_info_operation::SnapshotOperation;
use crate::state::state_base::{QueryCondition, StateOperation};
use crate::state::state_mgr::{SNAPSHOT_STATUS_STATE, STATE_MGR};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use tracing::{error, info};

#[derive(Clone, Debug)]
pub struct LocalBackupDeleteTask {
    task_id: TaskId,
    cluster_name: String,
    snapshot_ts: DateTime<Utc>,
    cmd: String,
    config: Config,
    force: bool,
}

impl LocalBackupDeleteTask {
    pub fn new(
        task_id: TaskId,
        cluster_name: String,
        snapshot_ts: DateTime<Utc>,
        cmd: String,
        config: Config,
        _task_host: TaskHost,
        force: bool,
    ) -> Self {
        Self {
            task_id,
            cluster_name,
            snapshot_ts,
            cmd,
            config,
            force,
        }
    }
}

#[async_trait]
impl TaskExecutor for LocalBackupDeleteTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        info!("execute {}", self.task_id.format_string());

        let auth_key = self.config.conn_ssh_auth_key();
        let ssh_session = ssh::SSHSession::from_task_host(task_host, auth_key).await?;
        let (host, _) = ssh_session.ssh_conn_info();
        let exec_cmd_rs = ssh_session
            .command(self.cmd.clone().as_str(), CollectOutput)
            .await?;

        if let Some(output) = exec_cmd_rs.get(CMD_OUTPUT) {
            info!(
                "Host {host} Cmd {} output {}",
                self.cmd,
                crate::cli::task::task_base::TaskArgValue::into_inner_value::<String>(
                    output.clone()
                )
            );
        }
        ssh_session.close().await?;

        // Check if the command succeeded
        let status_code = exec_cmd_rs
            .get(CMD_STATUS)
            .map(|v| crate::cli::task::task_base::TaskArgValue::into_inner_value::<i32>(v.clone()))
            .unwrap_or(1);

        // Delete from SQLite if force is true OR if filesystem deletion succeeded
        let should_delete_from_sqlite = self.force || status_code == 0;

        if should_delete_from_sqlite {
            let snapshot_operation =
                STATE_MGR.get_state_operation::<SnapshotOperation>(SNAPSHOT_STATUS_STATE);
            if let Err(e) = snapshot_operation
                .del(|| -> Option<QueryCondition> {
                    Some(QueryCondition {
                        cond_text: "cluster_name=$1 AND snapshot_ts=$2".to_string(),
                        bind_values: vec![
                            crate::StateValue::Varchar(self.cluster_name.clone()),
                            crate::StateValue::Timestamp(self.snapshot_ts),
                        ],
                    })
                })
                .await
            {
                error!(
                    "Failed to delete snapshot record from SQLite for {}: {}",
                    self.cluster_name, e
                );
            } else if self.force {
                info!(
                    "Force deleted snapshot record from SQLite: cluster={}, snapshot_ts={}",
                    self.cluster_name, self.snapshot_ts
                );
            } else {
                info!(
                    "Deleted snapshot record from SQLite: cluster={}, snapshot_ts={}",
                    self.cluster_name, self.snapshot_ts
                );
            }
        } else {
            // Do NOT delete from SQLite if filesystem deletion failed and force is false
            error!(
                "Filesystem deletion failed for snapshot: cluster={}, snapshot_ts={}, status={}",
                self.cluster_name, self.snapshot_ts, status_code
            );
        }

        // Build task result based on force flag and deletion status
        let mut task_result = HashMap::new();
        task_result.insert(
            crate::cli::CMD.to_string(),
            TaskArgValue::Str(self.task_id.format_string()),
        );

        if status_code == 0 {
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
            task_result.insert(
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str(format!("Successfully deleted local backup: {}", self.cmd)),
            );
        } else if self.force {
            // With --force, still report success since SQLite record was deleted
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
            task_result.insert(
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str(format!(
                    "Filesystem deletion failed but record removed from database (--force): status={}",
                    status_code
                )),
            );
        } else {
            // Without --force, report failure and keep SQLite record
            let error_output = exec_cmd_rs
                .get(CMD_OUTPUT)
                .map(|v| TaskArgValue::into_inner_value::<String>(v.clone()))
                .unwrap_or_else(|| "Unknown error".to_string());
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
            task_result.insert(
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str(format!("Failed to delete local backup: {}", error_output)),
            );
        }

        Ok(Some(task_result))
    }
}
