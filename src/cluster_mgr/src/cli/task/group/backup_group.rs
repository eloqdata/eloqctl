use crate::cli::task::backup_task::BackupTask;
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{BackupTaskGroup, TaskGroup};
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::SubCommand;
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
                    crate::cli::BackupCommand::Start {
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
                    crate::cli::BackupCommand::List {} => {
                        // do the work in CmdExecutor::run()::finishing()
                    }
                    crate::cli::BackupCommand::Remove { until, before } => {
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
