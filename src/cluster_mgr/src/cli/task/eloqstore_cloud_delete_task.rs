use crate::cli::task::s3_utils::{delete_s3_object, list_s3_objects, S3ClientBuilder};
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::state::snapshot_info_operation::SnapshotOperation;
use crate::state::state_base::StateOperation;
use crate::state::state_mgr::{SNAPSHOT_STATUS_STATE, STATE_MGR};
use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use tracing::{error, info};

#[derive(Clone, Debug)]
pub struct EloqStoreCloudDeleteTask {
    task_id: TaskId,
    cluster_name: String,
    snapshot_ts: DateTime<Utc>,
    backup_ts: String, // From snapshot.snapshot_path
    bucket: String,
    aws_id: String,
    aws_secret: String,
    region: String,
    endpoint: Option<String>,
    force: bool,
}

impl EloqStoreCloudDeleteTask {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: TaskId,
        cluster_name: String,
        snapshot_ts: DateTime<Utc>,
        backup_ts: String,
        bucket: String,
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
            backup_ts,
            bucket,
            aws_id,
            aws_secret,
            region,
            endpoint,
            force,
        }
    }

    /// Process deletion for a single partition directory
    /// Finds and deletes all manifest files matching the backup_ts pattern: manifest_*_{backup_ts}
    async fn process_partition_delete(
        &self,
        s3_client: &S3Client,
        partition_dir: &str,
        backup_ts: &str,
    ) -> Result<usize> {
        // List all manifest files in this partition directory
        let manifest_prefix = format!("{}/manifest_", partition_dir);
        let all_manifests = list_s3_objects(s3_client, &self.bucket, &manifest_prefix)
            .await
            .context(format!(
                "Failed to list manifests in partition {}",
                partition_dir
            ))?;

        // Find all backup manifest files matching the backup_ts: manifest_<term>_{backup_ts}
        let backup_manifest_pattern = format!("_{}", backup_ts);
        let backup_manifests: Vec<String> = all_manifests
            .iter()
            .filter(|m| m.ends_with(&backup_manifest_pattern))
            .cloned()
            .collect();

        if backup_manifests.is_empty() {
            info!(
                "No backup manifests found for partition {} with backup_ts {}",
                partition_dir, backup_ts
            );
            return Ok(0);
        }

        // Delete all matching backup manifest files
        let mut deleted_count = 0;
        let mut errors = Vec::new();

        for manifest_key in &backup_manifests {
            match delete_s3_object(s3_client, &self.bucket, manifest_key).await {
                Ok(_) => {
                    deleted_count += 1;
                    info!(
                        "Deleted backup manifest: s3://{}/{}",
                        self.bucket, manifest_key
                    );
                }
                Err(e) => {
                    let error_msg = format!("Failed to delete manifest {}: {}", manifest_key, e);
                    error!("{}", error_msg);
                    errors.push(error_msg);
                }
            }
        }

        if !errors.is_empty() {
            return Err(anyhow::anyhow!(
                "Failed to delete some manifests in partition {}: {}",
                partition_dir,
                errors.join("; ")
            ));
        }

        Ok(deleted_count)
    }
}

/// List all table partition directories in bucket
/// Returns directories like: "table1.0", "table1.1", "table2.0", etc.
async fn list_table_partition_dirs(s3_client: &S3Client, bucket: &str) -> Result<Vec<String>> {
    // List all objects with delimiter "/" to get directory structure
    // Filter to get only partition directories (format: <table_name>.<partition_id>/)
    let mut partition_dirs = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut request = s3_client.list_objects_v2().bucket(bucket).delimiter("/");

        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        let response = request
            .send()
            .await
            .context(format!("Failed to list objects in bucket {}", bucket))?;

        // Process common prefixes (directories)
        for prefix in response.common_prefixes() {
            if let Some(prefix_str) = prefix.prefix() {
                // Remove trailing "/" and check if it matches partition directory format
                let dir_name = prefix_str.trim_end_matches('/');
                // Partition directory format: <table_name>.<partition_id>
                // Check if it contains a dot and ends with a number
                if dir_name.contains('.') {
                    // Extract the part after the last dot
                    if let Some(last_dot_pos) = dir_name.rfind('.') {
                        let after_dot = &dir_name[last_dot_pos + 1..];
                        // Check if it's a number (partition_id)
                        if after_dot.parse::<u32>().is_ok() {
                            partition_dirs.push(dir_name.to_string());
                        }
                    }
                }
            }
        }

        // Check if there are more objects to fetch
        if response.is_truncated().unwrap_or(false) {
            continuation_token = response.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    info!(
        "Found {} table partition directory(ies) in bucket {}",
        partition_dirs.len(),
        bucket
    );
    Ok(partition_dirs)
}

#[async_trait]
impl TaskExecutor for EloqStoreCloudDeleteTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> Result<Option<ExecutionValue>> {
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

        // Step 1: Build S3 client
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

        info!("S3 client created successfully for EloqStore cloud delete");

        // Step 2: Validate backup_ts
        let backup_ts = self.backup_ts.trim();
        if backup_ts.is_empty() {
            let error_msg = format!(
                "No backup timestamp found for EloqStore snapshot: cluster={}, snapshot_ts={}",
                self.cluster_name, self.snapshot_ts
            );
            error!("{}", error_msg);
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
            task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(error_msg));
            return Ok(Some(task_result));
        }

        info!("Deleting EloqStore backup with timestamp: {}", backup_ts);
        println!("EloqStore Cloud Delete Task");
        println!("Cluster: {}", self.cluster_name);
        println!(
            "Snapshot timestamp: {}",
            self.snapshot_ts.format("%Y-%m-%d %H:%M:%S UTC")
        );
        println!("Backup timestamp: {}", backup_ts);

        // Step 3: List all table partition directories in bucket
        let partition_dirs = list_table_partition_dirs(&s3_client, &self.bucket)
            .await
            .context("Failed to list table partition directories")?;

        info!(
            "Found {} table partition directory(ies)",
            partition_dirs.len()
        );
        println!(
            "Found {} table partition(s) to process",
            partition_dirs.len()
        );

        // Step 4: Process each partition directory
        let mut total_deleted = 0;
        let mut processed_partitions = 0;
        let mut errors = Vec::new();

        for partition_dir in partition_dirs {
            match self
                .process_partition_delete(&s3_client, &partition_dir, backup_ts)
                .await
            {
                Ok(deleted_count) => {
                    processed_partitions += 1;
                    total_deleted += deleted_count;
                    if deleted_count > 0 {
                        info!(
                            "Deleted {} manifest(s) from partition: {}",
                            deleted_count, partition_dir
                        );
                    }
                }
                Err(e) => {
                    let error_msg = format!("Failed to process partition {}: {}", partition_dir, e);
                    error!("{}", error_msg);
                    errors.push(error_msg);
                }
            }
        }

        // Step 5: Delete from SQLite if force is true OR if S3 deletion succeeded
        let should_delete_from_sqlite = self.force || errors.is_empty();

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

        // Step 6: Return result
        if errors.is_empty() {
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
            task_result.insert(
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str(format!(
                    "Successfully deleted EloqStore backup. Deleted {} manifest(s) from {} partition(s)",
                    total_deleted, processed_partitions
                )),
            );
        } else {
            if self.force {
                // With --force, still report success since SQLite record was deleted
                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
                task_result.insert(
                    CMD_OUTPUT.to_string(),
                    TaskArgValue::Str(format!(
                        "Deletion completed with errors but record removed from database (--force). Deleted: {} manifest(s) from {} partition(s), Errors: {}",
                        total_deleted, processed_partitions, errors.join("; ")
                    )),
                );
            } else {
                // Without --force, report failure and keep SQLite record
                task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
                task_result.insert(
                    CMD_OUTPUT.to_string(),
                    TaskArgValue::Str(format!(
                        "Failed to delete EloqStore backup. Deleted: {} manifest(s) from {} partition(s), Errors: {}",
                        total_deleted, processed_partitions, errors.join("; ")
                    )),
                );
            }
        }

        Ok(Some(task_result))
    }
}
