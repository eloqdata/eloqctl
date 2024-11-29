use crate::cli::task::backup_task::BackupTask;
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{BackupTaskGroup, TaskGroup};
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::{BackupCommand, SubCommand};
use crate::config::config_base::DeployConfig;
use crate::config::DeploymentPackage;
use crate::state::state_mgr::STATE_MGR;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;

#[async_trait::async_trait]
impl TaskGroup for BackupTaskGroup {
    async fn tasks(
        &self,
        cmd: SubCommand,
        config: DeployConfig,
    ) -> anyhow::Result<TaskExecutionContext> {
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

                        let mut mkdir_remote_dir: IndexMap<TaskId, TaskInstance> =
                            Default::default();
                        let full_path = format!(
                            "{}/{}/{}",
                            path,
                            cluster,
                            BackupTask::pretty_string(snapshot_ts)
                        );
                        let (id, instance) = ExecCustomCommand::from_path(
                            &cmd,
                            format!("mkdir for {}", full_path),
                            format!("mkdir -p {}", full_path),
                            &config,
                            dest_host,
                            dest_user,
                        );
                        mkdir_remote_dir.insert(id, instance);
                        barrier.push(mkdir_remote_dir.len());
                        executable.extend(mkdir_remote_dir);

                        // Collect host ports from both MonographTx and MonographStandby
                        let mut redis_host_ports =
                            config.get_host_port_list(DeploymentPackage::MonographTx);
                        redis_host_ports
                            .extend(config.get_host_port_list(DeploymentPackage::MonographStandby));

                        // Create backup task instance
                        let task_id = TaskId {
                            cmd: "backup".to_string(),
                            task: "snapshot-start".to_string(),
                            host: "_local".to_string(),
                        };

                        let backup_task = BackupTask::new(
                            task_id.clone(),
                            redis_host_ports,
                            cluster.clone(),
                            path.clone(),
                            snapshot_ts.clone(),
                            password.clone(),
                            dest_host.clone(),
                            dest_user.clone(),
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
                    BackupCommand::Remove { until, before } => {
                        let success_task_entity =
                            STATE_MGR.list_snapshots(cluster.to_string()).await?;

                        // Step 1: Determine the cutoff datetime
                        let cutoff_datetime: Option<DateTime<Utc>> =
                            if let Some(until_duration) = until {
                                Some(Utc::now() - *until_duration)
                            } else if let Some(before_datetime) = before {
                                Some(*before_datetime)
                            } else {
                                None
                            };

                        STATE_MGR
                            .remove_snapshots(&cluster, &cutoff_datetime)
                            .await?;

                        // Step 2: Parse snapshot_ts and filter snapshots
                        let filtered_snapshots = if let Some(cutoff) = cutoff_datetime {
                            success_task_entity
                                .iter()
                                .filter(|snapshot_info_entity| {
                                    snapshot_info_entity.snapshot_ts < cutoff
                                })
                                .collect::<Vec<_>>()
                        } else {
                            // If no cutoff, proceed with all snapshots
                            success_task_entity.iter().collect::<Vec<_>>()
                        };

                        // Step 3: Proceed to delete the filtered snapshots
                        let snapshot_vec = filtered_snapshots
                            .iter()
                            .map(|snapshot_info_entity| {
                                (
                                    snapshot_info_entity.snapshot_path.clone(),
                                    snapshot_info_entity.dest_host.clone(),
                                    snapshot_info_entity.dest_user.clone(),
                                )
                            })
                            .collect_vec();

                        let mut rm_remote_dir: IndexMap<TaskId, TaskInstance> = Default::default();
                        for (snapshot_path, dest_host, dest_user) in snapshot_vec {
                            let (id, instance) = ExecCustomCommand::from_path(
                                &cmd,
                                format!("remove snapshot for {}", snapshot_path),
                                format!("rm -rf {}", snapshot_path),
                                &config,
                                &Some(dest_host),
                                &Some(dest_user),
                            );
                            rm_remote_dir.insert(id, instance);
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
                        let tx_srv_home = config.deployment.tx_srv_home();
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
                            format!("dump to aof"),
                            command,
                            &config,
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
                        let tx_srv_home = config.deployment.tx_srv_home();
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
                            format!("dump to rdb"),
                            command,
                            &config,
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
