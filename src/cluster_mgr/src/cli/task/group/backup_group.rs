use crate::cli::task::backup_task::BackupTask;
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{BackupTaskGroup, Config, TaskGroup};
use crate::cli::task::local_backup_delete_task::LocalBackupDeleteTask;
use crate::cli::task::s3_delete_task::S3DeleteTask;
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::{BackupCommand, SubCommand};
use crate::config::DeploymentPackage;
use crate::state::state_mgr::STATE_MGR;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use std::collections::HashMap;

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

                        // Check if storage is cloud-based
                        let is_cloud = cluster_config
                            .deployment
                            .storage_service
                            .as_ref()
                            .map(|s| s.is_rocksdb_cloud())
                            .unwrap_or(false);

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
                                            let manifest_filename = snapshot.snapshot_path.clone();
                                            // Construct correct S3 path: eloqkv-{prefix}-{bucket_name}/CLOUDMANIFEST-{manifest_filename}
                                            let s3_bucket = format!("{}", bucket);

                                            let s3_key =
                                                format!("CLOUDMANIFEST-{}", manifest_filename);
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

                                        barrier.push(s3_delete_tasks.len());
                                        executable.extend(s3_delete_tasks);
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
