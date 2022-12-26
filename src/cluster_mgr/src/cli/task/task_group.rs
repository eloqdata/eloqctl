use crate::cli::config::{DeploymentConfig, DeploymentService, StorageProvider};
use crate::cli::task::cassandra_ctl_task::CassandraCtlTask;
use crate::cli::task::download_task::{DownloadTask, ALL_DOWNLOAD_TASKS};
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::monograph_ctl_task::MonographCtlTask;
use crate::cli::task::monograph_install_task::MonographInstall;
use crate::cli::task::task_base::{TaskHost, TaskId, TaskInstance};
use crate::cli::task::unpack_file_task::UnpackFileTask;
use crate::cli::task::upload_task::{UploadTask, ALL_UPLOAD_TASKS};
use crate::cli::CommandArgs;
use crate::state::task_status_operation::TaskStatusEntity;
use dyn_clone::DynClone;
use itertools::Itertools;
use std::collections::HashMap;
use std::sync::LazyLock;
use tracing::info;

#[derive(Clone)]
pub struct TaskExecutionContext {
    pub cmd_args: CommandArgs,
    pub barrier: Option<Vec<usize>>,
    pub executable: Vec<TaskInstance>,
}

/// `TaskGroup` base on different business logic, multiple tasks are organized into task groups,
/// and barriers are inserted between task lists according to dependencies.
pub trait TaskGroup: Send + Sync + DynClone {
    fn tasks(
        &self,
        cmd_arg: CommandArgs,
        config: DeploymentConfig,
        successful_tasks: Option<Vec<TaskStatusEntity>>,
    ) -> anyhow::Result<TaskExecutionContext>;
}

dyn_clone::clone_trait_object!(TaskGroup);

#[macro_export]
macro_rules! task_group_boxed {
    ($({$task_group:ident}),*) => {
        $(
        #[derive(Clone)]
        struct $task_group;

        impl $task_group {
            fn boxed() -> Box<dyn TaskGroup> {
                Box::new(Self {})
            }
        }
        )*
    };
}

task_group_boxed! {
    {DeploymentTaskGroup},
    {InstallDBTaskGroup},
    {CtrlDBTaskGroup},
    {CustomCmdTaskGroup}
}

pub static TASK_GROUP: LazyLock<HashMap<String, Box<dyn TaskGroup>>> = LazyLock::new(|| {
    HashMap::from([
        ("deploy".to_string(), DeploymentTaskGroup::boxed()),
        ("install".to_string(), InstallDBTaskGroup::boxed()),
        ("start".to_string(), CtrlDBTaskGroup::boxed()),
        ("stop".to_string(), CtrlDBTaskGroup::boxed()),
        ("restart".to_string(), CtrlDBTaskGroup::boxed()),
        ("status".to_string(), CtrlDBTaskGroup::boxed()),
        ("exec_cmd".to_string(), CustomCmdTaskGroup::boxed()),
    ])
});

#[macro_export]
macro_rules! deploy_task_match {
    ($task_impl:ident, $all_task_name:expr, $all_success_tasks:expr, $config:expr) => {{
        let all_task_execution_context_list = $task_impl::from_config($config)?;
        let task_execution_vec = DeploymentTaskGroup::skip_success_task_execution(
            $all_task_name,
            all_task_execution_context_list,
            $all_success_tasks,
        );
        task_execution_vec
    }};
}

impl DeploymentTaskGroup {
    fn get_task_entity_by_name(
        task_name_vec: Vec<String>,
        success_task_entity: Vec<TaskStatusEntity>,
    ) -> Vec<TaskStatusEntity> {
        success_task_entity
            .iter()
            .filter(|task_status_entity| {
                let task_id_string = task_status_entity.task.clone();
                let task_name = TaskId::from_json_string(task_id_string).task;
                task_name_vec.contains(&task_name)
            })
            .cloned()
            .collect_vec()
    }

    fn skip_success_task_execution(
        task_name_vec: Vec<String>,
        task_list: Vec<TaskInstance>,
        all_task_entity: Vec<TaskStatusEntity>,
    ) -> Vec<TaskInstance> {
        let success_tasks =
            DeploymentTaskGroup::get_task_entity_by_name(task_name_vec, all_task_entity);

        let mut result = vec![];
        for task_execution in task_list.iter() {
            let (_, _, host) = task_execution.task_host.ssh_conn_tuple();
            let include_host = success_tasks
                .iter()
                .filter(|task| task.task_host == host)
                .count()
                > 0;
            if !include_host {
                result.push(task_execution.clone());
            }
        }
        result
    }
}

impl TaskGroup for DeploymentTaskGroup {
    fn tasks(
        &self,
        cmd_args: CommandArgs,
        config: DeploymentConfig,
        successful_tasks: Option<Vec<TaskStatusEntity>>,
    ) -> anyhow::Result<TaskExecutionContext> {
        let all_success_tasks = if let Some(task_status_entity) = successful_tasks {
            task_status_entity
        } else {
            vec![]
        };

        let download_tasks = ALL_DOWNLOAD_TASKS
            .iter()
            .copied()
            .map(|task| task.to_string())
            .collect_vec();

        let download_execution = deploy_task_match!(
            DownloadTask,
            download_tasks,
            all_success_tasks.clone(),
            &config
        );

        let upload_tasks = ALL_UPLOAD_TASKS
            .iter()
            .copied()
            .map(|task| task.to_string())
            .collect_vec();

        let upload_execution =
            deploy_task_match!(UploadTask, upload_tasks, all_success_tasks.clone(), &config);

        let unpack_tasks = config
            .unpack_files_map()
            .keys()
            .into_iter()
            .map(|key| format!("{}_unpack", key))
            .collect_vec();

        let unpack_execution =
            deploy_task_match!(UnpackFileTask, unpack_tasks, all_success_tasks, &config);

        let barrier = vec![
            download_execution.len(),
            upload_execution.len(),
            unpack_execution.len(),
        ];
        let executable = [download_execution, upload_execution, unpack_execution].concat();

        Ok(TaskExecutionContext {
            cmd_args,
            barrier: Some(barrier),
            executable,
        })
    }
}

impl TaskGroup for InstallDBTaskGroup {
    fn tasks(
        &self,
        cmd_args: CommandArgs,
        config: DeploymentConfig,
        _successful_tasks: Option<Vec<TaskStatusEntity>>,
    ) -> anyhow::Result<TaskExecutionContext> {
        let monograph_hosts = config.get_host_list(DeploymentService::Monograph);
        let monograph_hosts_len = monograph_hosts.len();
        assert!(monograph_hosts_len >= 1);
        let conn_user = &config.connection.username;
        let ssh_port = config.connection.ssh_port();
        let install_db_host_string = monograph_hosts.first().unwrap();
        let install_db_host = TaskHost::Remote {
            user: conn_user.clone(),
            port: ssh_port as usize,
            hosts: install_db_host_string.clone(),
        };
        info!(
            "InstallDBTaskGroup The list of MonographDB node is: {:?}, install_db_host={:?}",
            monograph_hosts, install_db_host
        );
        let install_cmd = CommandArgs::Install {
            cluster: config.clone().deployment.cluster_name,
        };
        let storage_provider = config.get_monograph_storage()?;

        let mut execution_context_tuple = match storage_provider {
            StorageProvider::Cassandra => {
                let cassandra_start = CassandraCtlTask::from_config(install_cmd, &config);
                let monograph_install =
                    MonographInstall::from_config(&config, install_db_host);
                TaskExecutionContext {
                    cmd_args,
                    barrier: Some(vec![cassandra_start.len(), monograph_install.len()]),
                    executable: [cassandra_start, monograph_install].concat(),
                }
            }
            StorageProvider::DynamoDB => {
                let monograph_is_multi_node = monograph_hosts.len() > 1;
                let monograph_install =
                    MonographInstall::from_config(&config, install_db_host);
                TaskExecutionContext {
                    cmd_args,
                    barrier: if monograph_is_multi_node {
                        Some(vec![monograph_install.len()])
                    } else {
                        None
                    },
                    executable: monograph_install,
                }
            }
        };
        if monograph_hosts.len() > 1 {
            let dest_hosts = monograph_hosts[0..=monograph_hosts_len - 1]
                .iter()
                .map(|host| TaskHost::Remote {
                    user: conn_user.clone(),
                    port: ssh_port as usize,
                    hosts: host.to_string(),
                })
                .collect_vec();

            let upload_task =
                UploadTask::build_datafarm_tasks(&config,  dest_hosts);

            let mut barrier = execution_context_tuple.barrier.unwrap();
            let mut executable = execution_context_tuple.executable;
            barrier.push(upload_task.len());
            executable.extend(upload_task.into_iter());

            execution_context_tuple.barrier = Some(barrier);
            execution_context_tuple.executable = executable;
        }

        Ok(execution_context_tuple)
    }
}

impl TaskGroup for CtrlDBTaskGroup {
    fn tasks(
        &self,
        cmd_arg: CommandArgs,
        config: DeploymentConfig,
        _successful_tasks: Option<Vec<TaskStatusEntity>>,
    ) -> anyhow::Result<TaskExecutionContext> {
        let task_execution_context_tuple = match cmd_arg.as_ref() {
            "start" | "restart" => {
                let storage_provider = config.get_monograph_storage()?;
                match storage_provider {
                    StorageProvider::Cassandra => {
                        let cassandra_execution_context =
                            CassandraCtlTask::from_config(cmd_arg.clone(), &config);
                        let mono_ctl_task_execution =
                            MonographCtlTask::from_config(cmd_arg.clone(), &config);

                        TaskExecutionContext {
                            cmd_args: cmd_arg.clone(),
                            barrier: Some(vec![
                                cassandra_execution_context.len(),
                                mono_ctl_task_execution.len(),
                            ]),
                            executable: [cassandra_execution_context, mono_ctl_task_execution]
                                .concat(),
                        }
                    }
                    StorageProvider::DynamoDB => TaskExecutionContext {
                        cmd_args: cmd_arg.clone(),
                        barrier: None,
                        executable: MonographCtlTask::from_config(cmd_arg.clone(), &config),
                    },
                }
            }
            _ => TaskExecutionContext {
                cmd_args: cmd_arg.clone(),
                barrier: None,
                executable: MonographCtlTask::from_config(cmd_arg.clone(), &config),
            },
        };

        Ok(task_execution_context_tuple)
    }
}

impl TaskGroup for CustomCmdTaskGroup {
    fn tasks(
        &self,
        cmd_arg: CommandArgs,
        config: DeploymentConfig,
        _successful_tasks: Option<Vec<TaskStatusEntity>>,
    ) -> anyhow::Result<TaskExecutionContext> {
        let user_command = match cmd_arg.clone() {
            CommandArgs::Exec {
                command,
                cluster: _,
            } => command,
            _ => {
                unreachable!()
            }
        };
        let exec_cmd_task_execution = ExecCustomCommand::from_config(user_command, &config);

        Ok(TaskExecutionContext {
            cmd_args: cmd_arg,
            barrier: None,
            executable: exec_cmd_task_execution,
        })
    }
}
