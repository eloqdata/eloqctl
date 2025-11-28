use std::collections::HashMap;

use crate::cli::task::cassandra_op_task::CassandraOpTask;
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{
    BackupTaskGroup, Config, CtrlDBTaskGroup, RemoveTaskGroup, TaskGroup,
};
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::{BackupCommand, SubCommand};
use crate::config::StorageProvider;
use anyhow::bail;
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

        let cluster = match cmd_arg.clone() {
            SubCommand::Remove { cluster } => cluster.clone(),
            _ => {
                unreachable!()
            }
        };
        let mut barrier = vec![];
        let mut executable;
        // terminate all process
        let remove_stop = CtrlDBTaskGroup
            .tasks(
                SubCommand::Remove {
                    cluster: cluster.clone(),
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

        if let Some(logsv) = &cluster_config.deployment.log_service {
            // clean log service data
            let conn_user = &cluster_config.connection.username;
            let ssh_port = cluster_config.connection.ssh_port();
            let clean_tasks = logsv
                .log_directories()
                .into_iter()
                .map(|(host, dirs)| {
                    let content = dirs
                        .into_iter()
                        .map(|d| format!("rm -r {}", d))
                        .join(" && ");

                    let task_host = TaskHost::Remote {
                        user: conn_user.clone(),
                        port: ssh_port as usize,
                        host: host.clone(),
                    };
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
        // remove cluster directory
        let clean_tasks = ExecCustomCommand::from_config(
            &cmd_arg,
            "clean",
            format!("rm -r {}", cluster_config.install_dir()),
            config,
        );
        barrier.push(clean_tasks.len());
        executable.extend(clean_tasks);
        // remove keyspace in external cassandra/scylla/dynamo
        if let Some(store) = &cluster_config.deployment.storage_service {
            match store.provider().unwrap() {
                StorageProvider::Cassandra => {
                    let cass = store.cassandra.as_ref().unwrap();
                    if cass.external().is_some() {
                        let host = cass.host.first().unwrap().clone();
                        let task_id = TaskId {
                            cmd: "remove".to_string(),
                            task: "drop-keyspace".to_string(),
                            host: "_local".to_string(),
                        };
                        let cql = format!(
                            "DROP KEYSPACE {}",
                            cluster_config.deployment.get_keyspace()?
                        );
                        let task =
                            CassandraOpTask::new(task_id.clone(), host, cass.client_port()?, cql);
                        let inst = TaskInstance {
                            task_input: HashMap::default(),
                            task: Box::new(task),
                            task_host: TaskHost::Local,
                        };
                        barrier.push(1);
                        executable.insert(task_id, inst);
                    }
                }
                StorageProvider::Dynamodb => {
                    bail!("drop dynamodb keyspace is not implemented")
                }
                StorageProvider::Rocksdb => {}
                StorageProvider::EloqDSS => {
                    // Handle DataStoreService storage provider cleanup
                    if let Some(dss) = &store.eloqdss {
                        use crate::config::storage_service_config::DataStoreServiceBackend;
                        match dss.backend_config() {
                            DataStoreServiceBackend::EloqStore(eloq_store_config) => {
                                // EloqStore backend: clean up rclone resources if cloud mode is enabled
                                if eloq_store_config.is_cloud_mode() {
                                    use crate::cli::task::rclone_config_task::RcloneConfigTask;
                                    use crate::cli::task::rclone_dir_task::RcloneDirTask;

                                    // 1. Rclone directory purge (rclone purge remote:path)
                                    // 2. Rclone directory remove (rclone rmdir remote:path)
                                    let rclone_dir_tasks =
                                        RcloneDirTask::build_tasks(cmd_arg.clone(), config);
                                    if !rclone_dir_tasks.is_empty() {
                                        barrier.push(rclone_dir_tasks.len());
                                        executable.extend(rclone_dir_tasks);
                                    }

                                    // 3. Rclone config delete (rclone config delete {remote_name})
                                    let rclone_config_tasks =
                                        RcloneConfigTask::build_tasks(cmd_arg.clone(), config);
                                    if !rclone_config_tasks.is_empty() {
                                        barrier.push(rclone_config_tasks.len());
                                        executable.extend(rclone_config_tasks);
                                    }
                                }
                            } // Future backends can be handled here, e.g.:
                              // DataStoreServiceBackend::BigTable(_) => { ... }
                        }
                    }
                }
            }
        }

        let remove_backup_task = BackupTaskGroup
            .tasks(
                SubCommand::Backup {
                    cluster: cluster.clone(),
                    command: BackupCommand::Remove {
                        until: None,
                        before: None,
                        force: false,
                    },
                },
                config,
            )
            .await?
            .executable;
        barrier.push(remove_backup_task.len());
        executable.extend(remove_backup_task);

        Ok(TaskExecutionContext {
            task_group: cmd_arg.as_ref().to_string(),
            barrier: Some(barrier),
            executable,
        })
    }
}
