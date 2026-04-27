use crate::cli::task::s3_utils::{delete_s3_object, S3ClientBuilder};
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::state::snapshot_info_operation::SnapshotOperation;
use crate::state::state_base::StateOperation;
use crate::state::state_mgr::{SNAPSHOT_STATUS_STATE, STATE_MGR};
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use std::collections::HashMap;
use tracing::{error, info};

#[derive(Clone, Debug)]
pub struct S3DeleteTask {
    task_id: TaskId,
    cluster_name: String,
    snapshot_ts: DateTime<Utc>,
    bucket: String,
    manifest_filename: String,
    aws_id: String,
    aws_secret: String,
    region: String,
    endpoint: Option<String>,
    force: bool,
}

impl S3DeleteTask {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: TaskId,
        cluster_name: String,
        snapshot_ts: DateTime<Utc>,
        bucket: String,
        manifest_filename: String,
        aws_id: String,
        aws_secret: String,
        region: String,
        endpoint: Option<String>,
        force: bool,
    ) -> Self {
        Self {
            task_id,
            cluster_name,
            snapshot_ts,
            bucket,
            manifest_filename,
            aws_id,
            aws_secret,
            region,
            endpoint,
            force,
        }
    }
}

#[async_trait]
impl TaskExecutor for S3DeleteTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        info!("execute {}", self.task_id.format_string());

        match task_host {
            TaskHost::Local => {}
            _ => unreachable!(),
        }

        let mut task_result = HashMap::new();
        task_result.insert(
            CMD.to_string(),
            TaskArgValue::Str(self.task_id.format_string()),
        );

        // Build S3 client
        let s3_client = S3ClientBuilder::build(
            &self.aws_id,
            &self.aws_secret,
            &self.region,
            self.endpoint.as_deref(),
        )
        .await
        .map_err(|e| {
            error!("Failed to create S3 client: {}", e);
            anyhow::anyhow!(e)
        })?;

        // Delete S3 object
        let s3_deletion_result =
            delete_s3_object(&s3_client, &self.bucket, &self.manifest_filename).await;

        // Delete from SQLite if force is true OR if S3 deletion succeeded
        let should_delete_from_sqlite = self.force || s3_deletion_result.is_ok();

        if should_delete_from_sqlite {
            let snapshot_operation =
                STATE_MGR.get_state_operation::<SnapshotOperation>(SNAPSHOT_STATUS_STATE);
            if let Err(e) = snapshot_operation
                .del(|| -> Option<crate::state::state_base::QueryCondition> {
                    Some(crate::state::state_base::QueryCondition {
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
        }

        match s3_deletion_result {
            Ok(_) => {
                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
                task_result.insert(
                    CMD_OUTPUT.to_string(),
                    TaskArgValue::Str(format!(
                        "Successfully deleted s3://{}/{}",
                        self.bucket, self.manifest_filename
                    )),
                );
            }
            Err(e) => {
                error!("Failed to delete S3 object: {}", e);
                if self.force {
                    // With --force, still report success since SQLite record was deleted
                    task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
                    task_result.insert(
                        CMD_OUTPUT.to_string(),
                        TaskArgValue::Str(format!(
                            "S3 deletion failed but record removed from database (--force): {}",
                            e
                        )),
                    );
                } else {
                    // Without --force, report failure and keep SQLite record
                    task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                    task_result.insert(
                        CMD_OUTPUT.to_string(),
                        TaskArgValue::Str(format!("Failed to delete S3 object: {}", e)),
                    );
                }
            }
        }

        Ok(Some(task_result))
    }
}
