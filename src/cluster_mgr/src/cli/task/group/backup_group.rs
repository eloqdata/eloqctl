use crate::cli::task::backup_task::BackupTask;
use crate::cli::task::backup_utils::{format_snapshots_for_deletion, split_manifests};
use crate::cli::task::eloqstore_cloud_delete_task::EloqStoreCloudDeleteTask;
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{BackupTaskGroup, Config, TaskGroup};
use crate::cli::task::local_backup_delete_task::LocalBackupDeleteTask;
use crate::cli::task::s3_delete_task::S3DeleteTask;
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::util::confirm_action;
use crate::cli::{BackupCommand, SubCommand};
use crate::config::storage_service_config::{DataStoreServiceBackend, StorageService};
use crate::config::DeploymentPackage;
use crate::state::state_mgr::STATE_MGR;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use std::collections::HashMap;

/// Check if EloqStore is configured in cloud mode
fn is_eloqstore_cloud(storage_service: &StorageService) -> bool {
    storage_service
        .eloqdss
        .as_ref()
        .map(|dss| {
            matches!(
                dss.backend_config(),
                DataStoreServiceBackend::EloqStore(config) if config.is_cloud_mode()
            )
        })
        .unwrap_or(false)
}

/// Get EloqStore S3 configuration
/// Returns (bucket, aws_id, aws_secret, region, endpoint)
fn get_eloqstore_s3_config(
    storage_service: &StorageService,
) -> Option<(String, String, String, String, Option<String>)> {
    storage_service
        .eloqdss
        .as_ref()
        .and_then(|dss| match dss.backend_config() {
            DataStoreServiceBackend::EloqStore(config) if config.is_cloud_mode() => {
                let cloud_config = config.get_cloud_config()?;
                let bucket = config.parse_cloud_store_path()?;
                Some((
                    bucket,
                    cloud_config.eloq_store_cloud_access_key.clone(),
                    cloud_config.eloq_store_cloud_secret_key.clone(),
                    cloud_config.eloq_store_cloud_region.clone(),
                    Some(cloud_config.eloq_store_cloud_endpoint.clone()),
                ))
            }
            _ => None,
        })
}

#[async_trait::async_trait]
impl TaskGroup for BackupTaskGroup {
    async fn tasks(
        &self,
        cmd: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => {
                return Err(anyhow::anyhow!(
                    "Expected ClusterConfig for InstallDBTaskGroup"
                ))
            }
        };

        let mut executable = IndexMap::new();
        let mut barrier = vec![];

        match &cmd {
            SubCommand::Backup { cluster, command } => {
                match command {
                    BackupCommand::Start {
                        path,
                        password,
                        dest_host,
                        dest_user,
                    } => {
                        let snapshot_ts = Utc::now();

                        // Check if storage is cloud-based (RocksDB or EloqStore)
                        let is_rocksdb_cloud = cluster_config
                            .deployment
                            .storage_service
                            .as_ref()
                            .map(|s| s.is_rocksdb_cloud())
                            .unwrap_or(false);

                        let is_eloqstore_cloud = cluster_config
                            .deployment
                            .storage_service
                            .as_ref()
                            .map(is_eloqstore_cloud)
                            .unwrap_or(false);

                        let is_cloud = is_rocksdb_cloud || is_eloqstore_cloud;

                        // Validate path: required for local storage, optional for cloud
                        if !is_cloud {
                            let path_value = path.as_ref().ok_or_else(|| {
                                anyhow::anyhow!(
                                    "--path is required for local storage. Please specify --path <PATH>"
                                )
                            })?;
                            if path_value.is_empty() {
                                return Err(anyhow::anyhow!(
                                    "--path cannot be empty for local storage. Please specify --path <PATH>"
                                ));
                            }
                        }

                        let mut mkdir_remote_dir: IndexMap<TaskId, TaskInstance> =
                            Default::default();

                        // Only create directory for LOCAL storage
                        if !is_cloud {
                            let path_value = path.as_ref().unwrap(); // Safe because we validated above
                            let full_path = format!(
                                "{}/{}/{}",
                                path_value,
                                cluster,
                                BackupTask::format_string(snapshot_ts)
                            );
                            let (id, instance) = ExecCustomCommand::from_path(
                                &cmd,
                                format!("mkdir for {}", full_path),
                                format!("mkdir -p {}", full_path),
                                config,
                                dest_host,
                                dest_user,
                            );
                            mkdir_remote_dir.insert(id, instance);
                            barrier.push(mkdir_remote_dir.len());
                        }

                        executable.extend(mkdir_remote_dir);

                        // Collect host ports from both MonographTx and MonographStandby
                        let mut redis_host_ports =
                            cluster_config.get_host_port_list(DeploymentPackage::MonographTx);
                        redis_host_ports.extend(
                            cluster_config.get_host_port_list(DeploymentPackage::MonographStandby),
                        );

                        // Create backup task instance
                        let task_id = TaskId {
                            cmd: "backup".to_string(),
                            task: "snapshot-start".to_string(),
                            host: "_local".to_string(),
                        };

                        // For cloud storage, use empty path and empty dest_host/dest_user
                        // For local storage, use the provided path
                        let (backup_path, backup_dest_host, backup_dest_user) = if is_cloud {
                            (String::new(), None, None)
                        } else {
                            let path_value = path.as_ref().unwrap(); // Safe because we validated above
                            (path_value.clone(), dest_host.clone(), dest_user.clone())
                        };

                        let backup_config = crate::cli::task::backup_task::BackupConfig {
                            path: backup_path,
                            snapshot_ts,
                            password: password.clone(),
                            dest_host: backup_dest_host,
                            dest_user: backup_dest_user,
                        };

                        let backup_task = BackupTask::new(
                            task_id.clone(),
                            redis_host_ports,
                            cluster.clone(),
                            backup_config,
                        );

                        // Insert task instance into the executable map
                        let task_instance = TaskInstance {
                            task_input: HashMap::default(),
                            task: Box::new(backup_task),
                            task_host: TaskHost::Local,
                        };

                        barrier.push(1);
                        executable.insert(task_id, task_instance);
                    }
                    BackupCommand::List {} => {
                        // do the work in CmdExecutor::run()::finishing()
                    }
                    BackupCommand::Remove {
                        until,
                        before,
                        force,
                    } => {
                        let success_task_entity =
                            STATE_MGR.list_snapshots(cluster.to_string()).await?;

                        // Step 1: Determine the cutoff datetime
                        let cutoff_datetime: Option<DateTime<Utc>> =
                            if let Some(until_duration) = until {
                                Some(Utc::now() - *until_duration)
                            } else {
                                before.as_ref().map(|before_datetime| *before_datetime)
                            };

                        // Step 2: Parse snapshot_ts and filter snapshots
                        // Note: We do NOT delete from SQLite upfront - deletion happens only after successful file/S3 deletion
                        let filtered_snapshots: Vec<
                            &crate::state::snapshot_info_operation::SnapshotEntity,
                        > = if let Some(cutoff) = cutoff_datetime {
                            success_task_entity
                                .iter()
                                .filter(|snapshot_info_entity| {
                                    snapshot_info_entity.snapshot_ts < cutoff
                                })
                                .collect()
                        } else {
                            // If no cutoff, proceed with all snapshots
                            success_task_entity.iter().collect()
                        };

                        // Display backups to be deleted and ask for confirmation
                        if !filtered_snapshots.is_empty() {
                            // Load cluster config for display formatting
                            let cluster_config_for_display = STATE_MGR
                                .load_deployment_from_state(cluster)
                                .await?
                                .ok_or_else(|| anyhow::anyhow!("cluster {} not found", cluster))?;

                            println!(
                                "{}",
                                format_snapshots_for_deletion(
                                    &filtered_snapshots,
                                    Some(&cluster_config_for_display)
                                )
                            );

                            // Use blocking I/O for user confirmation
                            // Spawn blocking task to avoid blocking the async runtime
                            let prompt = "Do you want to proceed with deletion?";
                            let confirmation_result =
                                tokio::task::spawn_blocking(|| confirm_action(prompt))
                                    .await
                                    .map_err(|e| {
                                        anyhow::anyhow!("Failed to get user confirmation: {}", e)
                                    })??;

                            if !confirmation_result {
                                println!("Deletion cancelled by user.");
                                // Return early without creating any tasks
                                return Ok(TaskExecutionContext {
                                    task_group: "backup".to_string(),
                                    barrier: Some(vec![]),
                                    executable: IndexMap::default(),
                                });
                            }
                        } else {
                            println!("No backups found to delete.");
                            return Ok(TaskExecutionContext {
                                task_group: "backup".to_string(),
                                barrier: Some(vec![]),
                                executable: IndexMap::default(),
                            });
                        }

                        // Step 3: Separate cloud and local backups
                        let cloud_backups: Vec<
                            &crate::state::snapshot_info_operation::SnapshotEntity,
                        > = filtered_snapshots
                            .iter()
                            .filter(|snapshot| snapshot.dest_host.is_empty())
                            .copied()
                            .collect();
                        let local_backups: Vec<
                            &crate::state::snapshot_info_operation::SnapshotEntity,
                        > = filtered_snapshots
                            .iter()
                            .filter(|snapshot| !snapshot.dest_host.is_empty())
                            .copied()
                            .collect();

                        // Step 4: Handle cloud backups deletion
                        if !cloud_backups.is_empty() {
                            // Check if storage is S3
                            if let Some(storage) = &cluster_config.deployment.storage_service {
                                if storage.is_rocksdb_s3() {
                                    if let Some((bucket, aws_id, aws_secret, region, endpoint)) =
                                        storage.get_s3_config()
                                    {
                                        // Create S3 deletion tasks
                                        let mut s3_delete_tasks: IndexMap<TaskId, TaskInstance> =
                                            Default::default();

                                        for snapshot in &cloud_backups {
                                            // Parse comma-separated manifest list
                                            let manifest_list =
                                                split_manifests(&snapshot.snapshot_path);

                                            if manifest_list.is_empty() {
                                                tracing::warn!(
                                                    "No manifests found for snapshot: cluster={}, snapshot_ts={}",
                                                    cluster,
                                                    snapshot.snapshot_ts
                                                );
                                                continue;
                                            }

                                            // Create deletion task for each manifest
                                            for manifest_filename in manifest_list {
                                                // Construct correct S3 path: {prefix}{bucket_name}/rocksdb_cloud/CLOUDMANIFEST-{manifest_filename}
                                                let s3_bucket = bucket.to_string();
                                                let s3_key = format!(
                                                    "rocksdb_cloud/CLOUDMANIFEST-{}",
                                                    manifest_filename
                                                );
                                                let cluster_name_clone = cluster.clone();
                                                let snapshot_ts_clone = snapshot.snapshot_ts;
                                                let aws_id_clone = aws_id.clone();
                                                let aws_secret_clone = aws_secret.clone();
                                                let region_clone = region.clone();
                                                let endpoint_clone = endpoint.clone();

                                                let task_id = TaskId {
                                                    cmd: "backup".to_string(),
                                                    task: format!("delete-s3-{}", s3_key),
                                                    host: "_local".to_string(),
                                                };

                                                // Create a custom task for S3 deletion
                                                // Task will delete from SQLite only after successful S3 deletion (unless --force)
                                                // Note: Each task will attempt to delete from SQLite, but only the last one will succeed
                                                // This is acceptable since SQLite deletion is idempotent (same condition)
                                                let s3_delete_task = S3DeleteTask::new(
                                                    task_id.clone(),
                                                    cluster_name_clone,
                                                    snapshot_ts_clone,
                                                    s3_bucket,
                                                    s3_key,
                                                    aws_id_clone,
                                                    aws_secret_clone,
                                                    region_clone,
                                                    endpoint_clone,
                                                    *force,
                                                );

                                                let task_instance = TaskInstance {
                                                    task_input: HashMap::default(),
                                                    task: Box::new(s3_delete_task),
                                                    task_host: TaskHost::Local,
                                                };

                                                s3_delete_tasks.insert(task_id, task_instance);
                                            }
                                        }

                                        barrier.push(s3_delete_tasks.len());
                                        executable.extend(s3_delete_tasks);
                                    }
                                }
                                // Check EloqStore cloud storage
                                else if is_eloqstore_cloud(storage) {
                                    if let Some((bucket, aws_id, aws_secret, region, endpoint)) =
                                        get_eloqstore_s3_config(storage)
                                    {
                                        // Create EloqStore deletion tasks
                                        let mut eloqstore_delete_tasks: IndexMap<
                                            TaskId,
                                            TaskInstance,
                                        > = Default::default();

                                        for snapshot in &cloud_backups {
                                            // Extract backup_ts from snapshot_path
                                            let backup_ts = snapshot.snapshot_path.trim();
                                            if backup_ts.is_empty() {
                                                tracing::warn!(
                                                    "No backup timestamp found for EloqStore snapshot: cluster={}, snapshot_ts={}",
                                                    cluster,
                                                    snapshot.snapshot_ts
                                                );
                                                continue;
                                            }

                                            let task_id = TaskId {
                                                cmd: "backup".to_string(),
                                                task: format!("delete-eloqstore-{}", backup_ts),
                                                host: "_local".to_string(),
                                            };

                                            let delete_task = EloqStoreCloudDeleteTask::new(
                                                task_id.clone(),
                                                cluster.clone(),
                                                snapshot.snapshot_ts,
                                                backup_ts.to_string(),
                                                bucket.clone(),
                                                aws_id.clone(),
                                                aws_secret.clone(),
                                                region.clone(),
                                                endpoint.clone(),
                                                *force,
                                            );

                                            let task_instance = TaskInstance {
                                                task_input: HashMap::default(),
                                                task: Box::new(delete_task),
                                                task_host: TaskHost::Local,
                                            };

                                            eloqstore_delete_tasks.insert(task_id, task_instance);
                                        }

                                        barrier.push(eloqstore_delete_tasks.len());
                                        executable.extend(eloqstore_delete_tasks);
                                    }
                                }
                            }
                        }

                        // Step 5: Handle local backups deletion
                        // Use LocalBackupDeleteTask to ensure SQLite deletion only happens after successful filesystem deletion
                        let mut rm_remote_dir: IndexMap<TaskId, TaskInstance> = Default::default();
                        for snapshot in &local_backups {
                            let snapshot_path = snapshot.snapshot_path.clone();
                            let dest_host = snapshot.dest_host.clone();
                            let dest_user = snapshot.dest_user.clone();
                            let cluster_name_clone = cluster.clone();
                            let snapshot_ts_clone = snapshot.snapshot_ts;

                            let conn_user = config.conn_user();
                            let (user, host) = if !dest_host.is_empty() && !dest_user.is_empty() {
                                (dest_user, dest_host)
                            } else {
                                (conn_user.to_string(), "localhost".to_string())
                            };

                            let ssh_port = config.ssh_port() as usize;
                            let task_host = TaskHost::Remote {
                                user,
                                port: ssh_port,
                                host,
                            };

                            let task_id = TaskId {
                                cmd: "backup".to_string(),
                                task: format!("remove-local-{}", snapshot_path),
                                host: "_local".to_string(),
                            };

                            let delete_task = LocalBackupDeleteTask::new(
                                task_id.clone(),
                                cluster_name_clone,
                                snapshot_ts_clone,
                                format!("rm -rf {}", snapshot_path),
                                config.clone(),
                                task_host.clone(),
                                *force,
                            );

                            let task_instance = TaskInstance {
                                task_input: HashMap::default(),
                                task: Box::new(delete_task),
                                task_host,
                            };

                            rm_remote_dir.insert(task_id, task_instance);
                        }

                        barrier.push(rm_remote_dir.len());
                        executable.extend(rm_remote_dir);
                    }
                    BackupCommand::Restore { snapshot_ts } => {
                        // Step 1: Validate storage is cloud-based (S3) - support both RocksDB and EloqStore
                        let is_rocksdb_cloud = cluster_config
                            .deployment
                            .storage_service
                            .as_ref()
                            .map(|s| s.is_rocksdb_cloud())
                            .unwrap_or(false);

                        let is_eloqstore_cloud = cluster_config
                            .deployment
                            .storage_service
                            .as_ref()
                            .map(is_eloqstore_cloud)
                            .unwrap_or(false);

                        let is_cloud = is_rocksdb_cloud || is_eloqstore_cloud;

                        if !is_cloud {
                            return Err(anyhow::anyhow!(
                                "Restore command is only supported for cloud storage (S3). \
                                Current storage type is not cloud-based. \
                                Please use backup commands for local storage."
                            ));
                        }

                        // Step 2: Validate storage is S3 (not GCS) - support both RocksDB S3 and EloqStore cloud
                        let is_rocksdb_s3 = cluster_config
                            .deployment
                            .storage_service
                            .as_ref()
                            .map(|s| s.is_rocksdb_s3())
                            .unwrap_or(false);

                        let is_eloqstore_cloud_s3 = is_eloqstore_cloud; // EloqStore cloud always uses S3-compatible API

                        let is_s3 = is_rocksdb_s3 || is_eloqstore_cloud_s3;

                        if !is_s3 {
                            return Err(anyhow::anyhow!(
                                "Restore command is only supported for S3 storage. \
                                GCS storage restore is not yet supported."
                            ));
                        }

                        // Step 3: Check if cluster is stopped (reusing status command logic) - from Phase 2
                        use crate::cli::task::backup_utils::is_cluster_stopped;
                        match is_cluster_stopped(cluster_config).await {
                            Ok(false) => {
                                return Err(anyhow::anyhow!(
                                    "Cluster is currently running. Restore can only be performed when the cluster is stopped. \
                                    Please stop the cluster first using: eloqctl stop {}",
                                    cluster
                                ));
                            }
                            Ok(true) => {
                                tracing::info!("Cluster is stopped, proceeding with restore...");
                            }
                            Err(e) => {
                                return Err(anyhow::anyhow!(
                                    "Failed to check cluster status: {}. \
                                    Please ensure the cluster is stopped before attempting restore.",
                                    e
                                ));
                            }
                        }

                        // Step 4: Lookup snapshot by timestamp
                        let snapshot = STATE_MGR.get_snapshot_by_ts(cluster, *snapshot_ts).await?;

                        let snapshot = match snapshot {
                            Some(s) => s,
                            None => {
                                return Err(anyhow::anyhow!(
                                    "Snapshot not found for cluster '{}' with timestamp '{}'. \
                                    Please verify the snapshot_ts using: eloqctl backup {} list",
                                    cluster,
                                    snapshot_ts.format("%Y-%m-%d %H:%M:%S UTC"),
                                    cluster
                                ));
                            }
                        };

                        // Step 5: Validate snapshot is for cloud storage
                        if !snapshot.dest_host.is_empty() {
                            return Err(anyhow::anyhow!(
                                "Snapshot is for local storage, not cloud storage. \
                                Restore command only supports cloud storage snapshots."
                            ));
                        }

                        // Step 6: Validate snapshot status is Finished
                        if snapshot.snapshot_status != 0 {
                            return Err(anyhow::anyhow!(
                                "Snapshot status is not 'Finished' (status: {}). \
                                Only finished snapshots can be restored.",
                                snapshot.snapshot_status
                            ));
                        }

                        // Step 7: Display snapshot info and ask for confirmation
                        use crate::cli::task::backup_utils::format_snapshot_for_restore;

                        // Determine storage type for display
                        let is_eloqstore_cloud = cluster_config
                            .deployment
                            .storage_service
                            .as_ref()
                            .map(|s| {
                                s.eloqdss
                                    .as_ref()
                                    .map(|dss| {
                                        matches!(
                                            dss.backend_config(),
                                            DataStoreServiceBackend::EloqStore(config)
                                                if config.is_cloud_mode()
                                        )
                                    })
                                    .unwrap_or(false)
                            })
                            .unwrap_or(false);

                        println!(
                            "{}",
                            format_snapshot_for_restore(&snapshot, is_eloqstore_cloud)
                        );

                        // Use blocking I/O for user confirmation
                        let prompt = "Do you want to proceed with restore?";
                        let confirmation_result =
                            tokio::task::spawn_blocking(|| confirm_action(prompt))
                                .await
                                .map_err(|e| {
                                    anyhow::anyhow!("Failed to get user confirmation: {}", e)
                                })??;

                        if !confirmation_result {
                            println!("Restore cancelled by user.");
                            return Ok(TaskExecutionContext {
                                task_group: "backup".to_string(),
                                barrier: Some(vec![]),
                                executable: IndexMap::default(),
                            });
                        }

                        tracing::info!(
                            "User confirmed restore operation for snapshot: {}",
                            snapshot_ts.format("%Y-%m-%d %H:%M:%S UTC")
                        );

                        // Step 8: Create and execute restore task
                        // Determine storage type and get appropriate configuration
                        let storage_service = cluster_config
                            .deployment
                            .storage_service
                            .as_ref()
                            .ok_or_else(|| {
                                anyhow::anyhow!("Storage service configuration not found")
                            })?;

                        let (bucket, aws_id, aws_secret, region, endpoint, is_eloqstore) =
                            if let Some((b, id, secret, r, e)) = storage_service.get_s3_config() {
                                // RocksDB S3 configuration
                                (b, id, secret, r, e, false)
                            } else if let Some((b, id, secret, r, e)) =
                                get_eloqstore_s3_config(storage_service)
                            {
                                // EloqStore cloud configuration
                                (b, id, secret, r, e, true)
                            } else {
                                return Err(anyhow::anyhow!(
                                    "Failed to get S3 configuration for restore operation. \
                                    Neither RocksDB S3 nor EloqStore cloud configuration found."
                                ));
                            };

                        // Create restore task
                        let task_id = TaskId {
                            cmd: "backup".to_string(),
                            task: format!(
                                "restore-{}",
                                snapshot.snapshot_ts.format("%Y%m%d%H%M%S")
                            ),
                            host: "_local".to_string(),
                        };

                        let task_instance = if is_eloqstore {
                            // Use EloqStore cloud restore task
                            use crate::cli::task::eloqstore_cloud_restore_task::EloqStoreCloudRestoreTask;
                            let restore_task = EloqStoreCloudRestoreTask::new(
                                task_id.clone(),
                                cluster.clone(),
                                snapshot.clone(),
                                bucket,
                                aws_id,
                                aws_secret,
                                region,
                                endpoint,
                            );
                            TaskInstance {
                                task_input: HashMap::default(),
                                task: Box::new(restore_task),
                                task_host: TaskHost::Local,
                            }
                        } else {
                            // Use RocksDB S3 restore task
                            use crate::cli::task::s3_restore_task::S3RestoreTask;
                            let restore_task = S3RestoreTask::new(
                                task_id.clone(),
                                cluster.clone(),
                                snapshot.clone(),
                                bucket,
                                aws_id,
                                aws_secret,
                                region,
                                endpoint,
                            );
                            TaskInstance {
                                task_input: HashMap::default(),
                                task: Box::new(restore_task),
                                task_host: TaskHost::Local,
                            }
                        };

                        let mut executable = IndexMap::new();
                        executable.insert(task_id, task_instance);

                        return Ok(TaskExecutionContext {
                            task_group: "backup".to_string(),
                            barrier: Some(vec![1]), // Single task
                            executable,
                        });
                    }
                    BackupCommand::DumpAOF {
                        rocksdb_path,
                        output_file_dir,
                        thread_count,
                    } => {
                        // Retrieve the snapshot info entities from the state manager
                        let success_task_entity = STATE_MGR
                            .get_from_snapshot_path(rocksdb_path.to_string())
                            .await?;

                        // Get the first snapshot info entity
                        let snapshot_info = match success_task_entity.first() {
                            Some(entity) => entity,
                            None => {
                                return Err(anyhow::anyhow!(
                                    "No snapshot info entities found for path: {}",
                                    rocksdb_path
                                ));
                            }
                        };

                        let dest_host_option = Some(snapshot_info.dest_host.clone());
                        let dest_user_option = Some(snapshot_info.dest_user.clone());

                        let mut dump_task: IndexMap<TaskId, TaskInstance> = Default::default();

                        // Prepare the command parameters
                        let tx_srv_home = cluster_config.deployment.tx_srv_home();
                        let thread_count = thread_count.as_deref().unwrap_or("1");

                        // Construct the command string
                        let command = format!(
                            r#"bash -c 'export LD_LIBRARY_PATH={}/lib; for i in $(ls -1 "{}"); do "{}/bin/eloqkv_to_aof" --rocksdb_path "{}/$i" --output_file_dir "{}/$i" --thread_count "{}"; done'"#,
                            tx_srv_home,
                            rocksdb_path,
                            tx_srv_home,
                            rocksdb_path,
                            output_file_dir,
                            thread_count
                        );

                        // Create the task instance
                        let (id, instance) = ExecCustomCommand::from_path(
                            &cmd,
                            "dump to aof".to_string(),
                            command,
                            config,
                            &dest_host_option,
                            &dest_user_option,
                        );

                        // Insert the task instance into the dump task map
                        dump_task.insert(id, instance);

                        // Update the barrier and executable
                        barrier.push(dump_task.len());
                        executable.extend(dump_task);
                    }
                    BackupCommand::DumpRDB {
                        rocksdb_path,
                        output_file_dir,
                        thread_count,
                    } => {
                        // Retrieve the snapshot info entities from the state manager
                        let success_task_entity = STATE_MGR
                            .get_from_snapshot_path(rocksdb_path.clone())
                            .await?;

                        // Get the first snapshot info entity
                        let snapshot_info = match success_task_entity.first() {
                            Some(entity) => entity,
                            None => {
                                return Err(anyhow::anyhow!(
                                    "No snapshot info entities found for path: {}",
                                    rocksdb_path
                                ));
                            }
                        };

                        let dest_host_option = Some(snapshot_info.dest_host.clone());
                        let dest_user_option = Some(snapshot_info.dest_user.clone());

                        let mut dump_task: IndexMap<TaskId, TaskInstance> = Default::default();

                        // Prepare the command parameters
                        let tx_srv_home = cluster_config.deployment.tx_srv_home();
                        let thread_count = thread_count.as_deref().unwrap_or("1");

                        // Construct the command string
                        let command = format!(
                            r#"bash -c 'export LD_LIBRARY_PATH={}/lib; for i in $(ls -1 "{}"); do "{}/bin/eloqkv_to_rdb" --rocksdb_path "{}/$i" --output_file "{}/$i.rdb" --thread_count "{}"; done'"#,
                            tx_srv_home,
                            rocksdb_path,
                            tx_srv_home,
                            rocksdb_path,
                            output_file_dir,
                            thread_count
                        );

                        // Create the task instance
                        let (id, instance) = ExecCustomCommand::from_path(
                            &cmd,
                            "dump to rdb".to_string(),
                            command,
                            config,
                            &dest_host_option,
                            &dest_user_option,
                        );

                        // Insert the task instance into the dump task map
                        dump_task.insert(id, instance);

                        // Update the barrier and executable
                        barrier.push(dump_task.len());
                        executable.extend(dump_task);
                    }
                }
            }
            _ => unreachable!(),
        }

        // Return the task execution context
        Ok(TaskExecutionContext {
            task_group: "backup".to_string(),
            barrier: Some(barrier),
            executable,
        })
    }
}
