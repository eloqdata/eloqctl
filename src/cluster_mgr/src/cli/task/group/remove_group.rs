use std::collections::HashMap;

use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{
    BackupTaskGroup, Config, CtrlDBTaskGroup, RemoveTaskGroup, TaskGroup,
};
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::{BackupCommand, SubCommand};
use indexmap::IndexMap;
use itertools::Itertools;

#[async_trait::async_trait]
impl TaskGroup for RemoveTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => {
                return Err(anyhow::anyhow!(
                    "Expected ClusterConfig for RemoveTaskGroup"
                ))
            }
        };

        let (cluster, force_remove) = match cmd_arg.clone() {
            SubCommand::Remove { cluster, force } => (cluster.clone(), force),
            _ => {
                unreachable!()
            }
        };
        let mut barrier = vec![];
        let mut executable;
        // terminate all process
        if force_remove {
            executable = IndexMap::new();
        } else {
            let remove_stop = CtrlDBTaskGroup
                .tasks(
                    SubCommand::Remove {
                        cluster: cluster.clone(),
                        force: false,
                    },
                    config,
                )
                .await?;
            if let Some(ba) = remove_stop.barrier {
                barrier.extend(ba);
            } else {
                barrier.push(remove_stop.executable.len());
            }
            executable = remove_stop.executable;
        }

        if !force_remove {
            if let Some(logsv) = &cluster_config.deployment.log_service {
                // clean log service data
                let clean_tasks = logsv
                    .log_directories()
                    .into_iter()
                    .map(|(host, dirs)| {
                        let content = dirs
                            .into_iter()
                            .map(|d| format!("rm -rf {}", d))
                            .join(" && ");

                        let task_host = TaskHost::remote(&cluster_config.connection, &host);
                        let task_id = TaskId {
                            cmd: cmd_arg.as_ref().to_string(),
                            task: format!("clean_log@{host}"),
                            host: host.clone(),
                        };
                        (
                            task_id.clone(),
                            TaskInstance {
                                task_input: HashMap::default(),
                                task: Box::new(ExecCustomCommand::new(
                                    content,
                                    task_id,
                                    config.clone(),
                                )),
                                task_host,
                            },
                        )
                    })
                    .collect::<IndexMap<TaskId, TaskInstance>>();
                barrier.push(clean_tasks.len());
                executable.extend(clean_tasks);
            }
        }
        // remove cluster directory
        if !force_remove {
            let clean_tasks = ExecCustomCommand::from_config(
                &cmd_arg,
                "clean",
                format!("rm -rf {}", cluster_config.install_dir()),
                config,
            );
            barrier.push(clean_tasks.len());
            executable.extend(clean_tasks);
        }
        // remove keyspace in external dynamo
        if let Some(store) = &cluster_config.deployment.storage_service {
            match store.provider().unwrap() {
                crate::config::StorageProvider::EloqDSS => {
                    // Handle DataStoreService storage provider cleanup
                    if let Some(dss) = &store.eloqdss {
                        use crate::config::storage_service_config::DataStoreServiceBackend;
                        match dss.backend_config() {
                            DataStoreServiceBackend::EloqStore(_eloq_store_config) => {
                                // EloqStore backend: no rclone cleanup needed
                            } // Future backends can be handled here, e.g.:
                              // DataStoreServiceBackend::BigTable(_) => { ... }
                        }
                    }
                }
                _ => {}
            }
        }

        if !force_remove {
            let remove_backup_task = BackupTaskGroup
                .tasks(
                    SubCommand::Backup {
                        cluster: cluster.clone(),
                        command: BackupCommand::Remove {
                            until: None,
                            before: None,
                            force: true,
                        },
                    },
                    config,
                )
                .await?
                .executable;
            barrier.push(remove_backup_task.len());
            executable.extend(remove_backup_task);
        }

        Ok(TaskExecutionContext {
            task_group: cmd_arg.as_ref().to_string(),
            barrier: Some(barrier),
            executable,
        })
    }
}
