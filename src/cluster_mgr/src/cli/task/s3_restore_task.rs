use crate::cli::task::backup_utils::{
    find_max_epoch_for_ng, parse_database_manifest, parse_snapshot_manifest, split_manifests,
};
use crate::cli::task::s3_utils::{copy_s3_object, list_s3_objects, S3ClientBuilder};
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::state::snapshot_info_operation::SnapshotEntity;
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use tracing::{error, info, warn};

#[derive(Clone, Debug)]
pub struct S3RestoreTask {
    task_id: TaskId,
    cluster_name: String,
    snapshot: SnapshotEntity,
    bucket: String,
    aws_id: String,
    aws_secret: String,
    region: String,
    endpoint: Option<String>,
}

impl S3RestoreTask {
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

#[async_trait]
impl TaskExecutor for S3RestoreTask {
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

        // Parse manifest filenames from snapshot
        let manifest_list = split_manifests(&self.snapshot.snapshot_path);
        if manifest_list.is_empty() {
            let error_msg = format!(
                "No manifest files found in snapshot for cluster: {}, snapshot_ts: {}",
                self.cluster_name, self.snapshot.snapshot_ts
            );
            error!("{}", error_msg);
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
            task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(error_msg));
            return Ok(Some(task_result));
        }

        info!("Found {} manifest(s) to restore", manifest_list.len());

        // List all current database manifests to find max epochs
        let db_manifest_prefix = "rocksdb_cloud/CLOUDMANIFEST-development-";
        let all_manifests = list_s3_objects(&s3_client, &self.bucket, db_manifest_prefix)
            .await
            .context("Failed to list current database manifests")?;

        info!("Found {} current database manifest(s)", all_manifests.len());

        // Process each snapshot manifest
        let mut errors = Vec::new();
        let mut success_count = 0;

        for snapshot_manifest in &manifest_list {
            // Parse snapshot manifest to get ng_id
            let (_snapshot_name, ng_id, _backup_ts) =
                match parse_snapshot_manifest(snapshot_manifest) {
                    Ok(parsed) => parsed,
                    Err(e) => {
                        let error_msg = format!(
                            "Failed to parse snapshot manifest '{}': {}",
                            snapshot_manifest, e
                        );
                        error!("{}", error_msg);
                        errors.push(error_msg);
                        continue;
                    }
                };

            info!(
                "Processing manifest: {} (ng_id: {})",
                snapshot_manifest, ng_id
            );

            // Construct source S3 key
            let source_key = format!("rocksdb_cloud/CLOUDMANIFEST-{}", snapshot_manifest);

            // Check if source file exists
            match s3_client
                .head_object()
                .bucket(&self.bucket)
                .key(&source_key)
                .send()
                .await
            {
                Ok(_) => {
                    info!(
                        "Source manifest exists: s3://{}/{}",
                        self.bucket, source_key
                    );
                }
                Err(_) => {
                    let error_msg = format!(
                        "Snapshot manifest file not found in S3: s3://{}/{}",
                        self.bucket, source_key
                    );
                    error!("{}", error_msg);
                    errors.push(error_msg);
                    continue;
                }
            }

            // Find max epoch for this ng_id
            let max_epoch = find_max_epoch_for_ng(&all_manifests, ng_id)
                .context(format!("Failed to find max epoch for ng_id {}", ng_id))?;

            let new_epoch = max_epoch + 1;
            info!(
                "Max epoch for ng_id {}: {}, using new epoch: {}",
                ng_id, max_epoch, new_epoch
            );

            // Construct destination S3 key
            let dest_key = format!(
                "rocksdb_cloud/CLOUDMANIFEST-development-{}-{}",
                ng_id, new_epoch
            );

            // Copy manifest file
            match copy_s3_object(&s3_client, &self.bucket, &source_key, &dest_key).await {
                Ok(_) => {
                    info!(
                        "Successfully restored manifest for ng_id {}: s3://{}/{}",
                        ng_id, self.bucket, dest_key
                    );
                    success_count += 1;
                }
                Err(e) => {
                    let error_msg = format!("Failed to copy manifest for ng_id {}: {}", ng_id, e);
                    error!("{}", error_msg);
                    errors.push(error_msg);
                }
            }
        }

        // Check for missing ng_ids
        let expected_ng_ids: Vec<u32> = manifest_list
            .iter()
            .filter_map(|m| parse_snapshot_manifest(m).ok().map(|(_, ng_id, _)| ng_id))
            .collect();

        let found_ng_ids: HashSet<u32> = all_manifests
            .iter()
            .filter_map(|m| {
                m.split('/')
                    .next_back()
                    .and_then(|f| parse_database_manifest(f).ok().map(|(ng_id, _)| ng_id))
            })
            .collect();

        // Check if any expected ng_ids are missing from current database
        let missing_ng_ids: Vec<u32> = expected_ng_ids
            .iter()
            .filter(|ng_id| !found_ng_ids.contains(ng_id))
            .copied()
            .collect();

        if !missing_ng_ids.is_empty() {
            warn!(
                "Some ng_ids from snapshot are not present in current database: {:?}",
                missing_ng_ids
            );
            // This is a warning, not an error - the restore can still proceed
        }

        // Return result
        if errors.is_empty() && success_count == manifest_list.len() {
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
            task_result.insert(
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str(format!(
                    "Successfully restored {} manifest(s) for cluster: {}",
                    success_count, self.cluster_name
                )),
            );
        } else {
            let error_summary = if errors.is_empty() {
                format!(
                    "Partial success: restored {}/{} manifest(s)",
                    success_count,
                    manifest_list.len()
                )
            } else {
                format!(
                    "Restore completed with errors. Success: {}/{}. Errors: {}",
                    success_count,
                    manifest_list.len(),
                    errors.join("; ")
                )
            };
            error!("{}", error_summary);
            task_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(1));
            task_result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(error_summary));
        }

        Ok(Some(task_result))
    }
}
