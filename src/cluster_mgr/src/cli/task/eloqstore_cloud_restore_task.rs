use crate::cli::task::s3_utils::{
    copy_s3_object, delete_s3_object, list_s3_objects, S3ClientBuilder,
};
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::state::snapshot_info_operation::SnapshotEntity;
use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use std::collections::HashMap;
use tracing::{error, info};

#[derive(Clone, Debug)]
pub struct EloqStoreCloudRestoreTask {
    task_id: TaskId,
    cluster_name: String,
    snapshot: SnapshotEntity,
    bucket: String,
    aws_id: String,
    aws_secret: String,
    region: String,
    endpoint: Option<String>,
}

impl EloqStoreCloudRestoreTask {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: TaskId,
        cluster_name: String,
        snapshot: SnapshotEntity,
        bucket: String,
        aws_id: String,
        aws_secret: String,
        region: String,
        endpoint: Option<String>,
    ) -> Self {
        Self {
            task_id,
            cluster_name,
            snapshot,
            bucket,
            aws_id,
            aws_secret,
            region,
            endpoint,
        }
    }
}

enum ProcessResult {
    Restored, // Successfully restored manifest
    Skipped,  // No backup manifest found (should not happen, but handle gracefully)
    Deleted,  // Deleted manifest for post-backup partition
}

impl EloqStoreCloudRestoreTask {
    async fn process_partition_restore(
        &self,
        s3_client: &S3Client,
        partition_dir: &str,
        backup_ts: &str,
    ) -> Result<ProcessResult> {
        // List all manifest files in this partition directory
        let manifest_prefix = format!("{}/manifest_", partition_dir);
        let all_manifests = list_s3_objects(s3_client, &self.bucket, &manifest_prefix)
            .await
            .context(format!(
                "Failed to list manifests in partition {}",
                partition_dir
            ))?;

        // Find backup manifest file: manifest_<term>_<backup_ts>
        let backup_manifest_pattern = format!("_{}", backup_ts);
        let backup_manifest = all_manifests
            .iter()
            .find(|m| m.ends_with(&backup_manifest_pattern));

        if let Some(backup_manifest_key) = backup_manifest {
            // Found backup manifest - restore it
            // Extract term from backup manifest: manifest_<term>_<backup_ts>
            let term = extract_term_from_backup_manifest(backup_manifest_key, backup_ts)
                .context("Failed to extract term from backup manifest")?;

            // Find max term from current database manifests (without backup_ts)
            let current_manifests: Vec<&String> = all_manifests
                .iter()
                .filter(|m| !m.contains(&backup_manifest_pattern))
                .collect();

            let max_term = current_manifests
                .iter()
                .filter_map(|m| extract_term_from_current_manifest(m))
                .max()
                .unwrap_or(term); // Use backup manifest's term if no current manifest found

            // Copy backup manifest to current database manifest: manifest_<max_term>
            let dest_key = format!("{}/manifest_{}", partition_dir, max_term);
            copy_s3_object(s3_client, &self.bucket, backup_manifest_key, &dest_key)
                .await
                .context(format!(
                    "Failed to copy manifest for partition {}",
                    partition_dir
                ))?;

            info!(
                "Restored partition {}: {} -> {}",
                partition_dir, backup_manifest_key, dest_key
            );
            Ok(ProcessResult::Restored)
        } else {
            // No backup manifest found - this partition was created after backup
            // Delete only current database manifests (format: manifest_<term>, without _<timestamp> suffix)
            // Do NOT delete other backup manifests (format: manifest_<term>_<other_ts>)
            let current_manifests: Vec<String> = all_manifests
                .iter()
                .filter(|m| {
                    // Extract filename from key
                    if let Some(filename) = m.split('/').next_back() {
                        // Current database manifest format: manifest_<term> (no _<timestamp> suffix)
                        // Backup manifest format: manifest_<term>_<backup_ts>
                        // Check if filename matches current manifest format (no underscore after term)
                        if let Some(term_part) = filename.strip_prefix("manifest_") {
                            // If term_part contains underscore, it's a backup manifest (format: <term>_<ts>)
                            // If term_part doesn't contain underscore, it's a current manifest (format: <term>)
                            !term_part.contains('_') && term_part.parse::<u64>().is_ok()
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                })
                .cloned()
                .collect();

            for manifest_key in &current_manifests {
                delete_s3_object(s3_client, &self.bucket, manifest_key)
                    .await
                    .context(format!("Failed to delete manifest {}", manifest_key))?;
                info!("Deleted post-backup manifest: {}", manifest_key);
            }

            if !current_manifests.is_empty() {
                Ok(ProcessResult::Deleted)
            } else {
                Ok(ProcessResult::Skipped)
            }
        }
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
        // common_prefixes() returns &[CommonPrefix], not Option
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

/// Extract term from backup manifest: manifest_<term>_<backup_ts>
fn extract_term_from_backup_manifest(manifest_key: &str, backup_ts: &str) -> Result<u64> {
    // Format: {partition_dir}/manifest_<term>_<backup_ts>
    // Extract filename from key
    let filename = manifest_key
        .split('/')
        .next_back()
        .ok_or_else(|| anyhow::anyhow!("Invalid manifest key format: {}", manifest_key))?;

    // Remove "manifest_" prefix
    let without_prefix = filename.strip_prefix("manifest_").ok_or_else(|| {
        anyhow::anyhow!(
            "Manifest filename must start with 'manifest_': {}",
            filename
        )
    })?;

    // Remove "_<backup_ts>" suffix
    let term_str = without_prefix
        .strip_suffix(&format!("_{}", backup_ts))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Manifest filename must end with '_{}': {}",
                backup_ts,
                filename
            )
        })?;

    term_str
        .parse::<u64>()
        .context(format!("Failed to parse term from manifest: {}", filename))
}

/// Extract term from current manifest: manifest_<term>
fn extract_term_from_current_manifest(manifest_key: &str) -> Option<u64> {
    // Format: {partition_dir}/manifest_<term>
    // Extract filename from key
    let filename = manifest_key.split('/').next_back()?;

    // Remove "manifest_" prefix
    let term_str = filename.strip_prefix("manifest_")?;

    term_str.parse::<u64>().ok()
}

#[async_trait]
impl TaskExecutor for EloqStoreCloudRestoreTask {
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

        info!("S3 client created successfully for EloqStore cloud restore");

        // Step 2: Extract backup_ts from snapshot
        // For EloqStore, snapshot_path stores the backup_ts (timestamp), not manifest filenames
        let backup_ts = self.snapshot.snapshot_path.trim();
        if backup_ts.is_empty() {
            let error_msg = format!(
                "No backup timestamp found in snapshot for cluster: {}, snapshot_ts: {}",
                self.cluster_name, self.snapshot.snapshot_ts
            );
            error!("{}", error_msg);
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
            task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(error_msg));
            return Ok(Some(task_result));
        }

        info!("Restoring EloqStore backup with timestamp: {}", backup_ts);
        println!("EloqStore Cloud Restore Task");
        println!("Cluster: {}", self.cluster_name);
        println!(
            "Snapshot timestamp: {}",
            self.snapshot.snapshot_ts.format("%Y-%m-%d %H:%M:%S UTC")
        );
        println!("Backup timestamp: {}", backup_ts);

        // Step 3: List all table partition directories in bucket
        // Partition directories format: <table_name>.<partition_id>/
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
        let mut success_count = 0;
        let mut skipped_count = 0;
        let mut deleted_count = 0;
        let mut errors = Vec::new();

        for partition_dir in partition_dirs {
            match self
                .process_partition_restore(&s3_client, &partition_dir, backup_ts)
                .await
            {
                Ok(ProcessResult::Restored) => {
                    success_count += 1;
                    info!("Successfully restored partition: {}", partition_dir);
                }
                Ok(ProcessResult::Skipped) => {
                    skipped_count += 1;
                    info!(
                        "Skipped partition (no backup manifest found): {}",
                        partition_dir
                    );
                }
                Ok(ProcessResult::Deleted) => {
                    deleted_count += 1;
                    info!(
                        "Deleted manifest for post-backup partition: {}",
                        partition_dir
                    );
                }
                Err(e) => {
                    let error_msg = format!("Failed to process partition {}: {}", partition_dir, e);
                    error!("{}", error_msg);
                    errors.push(error_msg);
                }
            }
        }

        // Step 5: Return result
        if errors.is_empty() {
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
            task_result.insert(
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str(format!(
                    "Successfully restored EloqStore backup. Restored: {}, Skipped: {}, Deleted: {}",
                    success_count, skipped_count, deleted_count
                )),
            );
        } else {
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
            task_result.insert(
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str(format!(
                    "Restore completed with errors. Restored: {}, Skipped: {}, Deleted: {}, Errors: {}",
                    success_count, skipped_count, deleted_count, errors.join("; ")
                )),
            );
        }

        Ok(Some(task_result))
    }
}
