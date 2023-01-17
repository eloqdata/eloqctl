use crate::cli::config::{DeploymentConfig, DeploymentService, StorageProvider};
use crate::cli::task::cassandra_ctl_task::CassandraCtlTask;
use crate::cli::task::download_task::{DownloadFromRemoteTask, ALL_DOWNLOAD_TASKS};
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::local_copy_task::LocalCopyTask;
use crate::cli::task::monograph_ctl_task::MonographCtlTask;
use crate::cli::task::monograph_install_task::MonographInstall;
use crate::cli::task::runtime_deps_install::RuntimeDepsInstallation;
use crate::cli::task::task_base::{TaskHost, TaskId, TaskInstance};
use crate::cli::task::unpack_file_task::UnpackFileTask;
use crate::cli::task::upload_task::{UploadTask, ALL_UPLOAD_TASKS};
use crate::cli::CommandArgs;
use crate::state::task_status_operation::TaskStatusEntity;
use dyn_clone::DynClone;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;
use std::sync::LazyLock;
use tracing::info;

#[derive(Clone)]
pub struct TaskExecutionContext {
    pub cmd_args: CommandArgs,
    pub barrier: Option<Vec<usize>>,
    pub executable: IndexMap<TaskId, TaskInstance>,
}

impl TaskExecutionContext {
    pub fn list_task_ids(&self) -> Vec<TaskId> {
        self.executable.keys().cloned().collect_vec()
    }
}

/// `TaskGroup` base on different business logic, multiple tasks are organized into task groups,
/// and barriers are inserted between task lists according to dependencies.
pub trait TaskGroup: Send + Sync + DynClone {
    fn tasks(
        &self,
        cmd_arg: CommandArgs,
        config: DeploymentConfig,
        already_successful: Option<Vec<TaskStatusEntity>>,
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
    {CustomCmdTaskGroup},
    {InstallRuntimeDepsTaskGroup}
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
        ("run-deps".to_string(), InstallRuntimeDepsTaskGroup::boxed()),
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
        task_list: IndexMap<TaskId, TaskInstance>,
        all_task_entity: Vec<TaskStatusEntity>,
    ) -> IndexMap<TaskId, TaskInstance> {
        let success_tasks =
            DeploymentTaskGroup::get_task_entity_by_name(task_name_vec, all_task_entity);

        let mut result = IndexMap::new();
        for task_entry in task_list.iter() {
            let task_instance = task_entry.1;
            let (_, _, host) = task_instance.task_host.ssh_conn_tuple();
            let include_host = success_tasks
                .iter()
                .filter(|task| task.task_host == host)
                .count()
                > 0;
            if !include_host {
                result.insert(task_entry.0.clone(), task_instance.clone());
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
            DownloadFromRemoteTask,
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

        let mut copy_or_download_task_instances = LocalCopyTask::form_config(&config)?;
        copy_or_download_task_instances.extend(download_execution.into_iter());

        let barrier = vec![
            copy_or_download_task_instances.len(),
            upload_execution.len(),
            unpack_execution.len(),
        ];
        let mut executable = IndexMap::new();
        executable.extend(copy_or_download_task_instances.into_iter());
        executable.extend(upload_execution.into_iter());
        executable.extend(unpack_execution.into_iter());

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
                let upload_cass_config_task = UploadTask::build_upload_cass_conf_task(&config)?;
                let cassandra_start = CassandraCtlTask::from_config(install_cmd, &config);
                let monograph_install = MonographInstall::from_config(&config, install_db_host);
                let barrier = vec![
                    upload_cass_config_task.len(),
                    cassandra_start.len(),
                    monograph_install.len(),
                ];
                let mut executable = IndexMap::new();
                executable.extend(upload_cass_config_task.into_iter());
                executable.extend(cassandra_start.into_iter());
                executable.extend(monograph_install.into_iter());
                TaskExecutionContext {
                    cmd_args,
                    barrier: Some(barrier),
                    executable,
                }
            }
            StorageProvider::DynamoDB => {
                let monograph_is_multi_node = monograph_hosts.len() > 1;
                let monograph_install = MonographInstall::from_config(&config, install_db_host);
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
        let mut barrier = execution_context_tuple.clone().barrier.unwrap();
        let mut executable = execution_context_tuple.executable;
        if monograph_hosts.len() > 1 {
            let dest_hosts = monograph_hosts[1..=monograph_hosts_len - 1]
                .iter()
                .map(|host| TaskHost::Remote {
                    user: conn_user.clone(),
                    port: ssh_port as usize,
                    hosts: host.to_string(),
                })
                .collect_vec();
            info!(
                "InstallDBTaskGroup MonographDB multiple installation hosts are configured {:?}",
                dest_hosts
            );
            let upload_task = UploadTask::build_upload_data_dir_tasks(&config, dest_hosts);

            barrier.push(upload_task.len());
            executable.extend(upload_task.into_iter());

            execution_context_tuple.barrier = Some(barrier.clone());
            execution_context_tuple.executable = executable.clone();
        }

        // rm -rf cc_ng/ tx_log/
        let remote_install_dir = config.install_dir();
        let rm_log_data_cmd = format!(
            "rm -rf {}/datafarm/cc_ng {}/datafarm/tx_log",
            remote_install_dir, remote_install_dir
        );

        let rm_log_data_task_instance = ExecCustomCommand::from_config(rm_log_data_cmd, &config);
        barrier.push(rm_log_data_task_instance.len());
        executable.extend(rm_log_data_task_instance.into_iter());
        execution_context_tuple.barrier = Some(barrier);
        execution_context_tuple.executable = executable;

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
        let cmd_ref = cmd_arg.as_ref();
        let storage_provider = config.get_monograph_storage()?;

        let start_cass_if_need = (cmd_ref == "start" || cmd_ref == "restart")
            && storage_provider == StorageProvider::Cassandra;

        let mut mut_executable = if start_cass_if_need {
            CassandraCtlTask::from_config(cmd_arg.clone(), &config)
        } else {
            IndexMap::default()
        };

        let mut barrier = if !mut_executable.is_empty() {
            vec![mut_executable.len()]
        } else {
            vec![]
        };

        let batch_cmd = match cmd_arg {
            CommandArgs::Restart {
                cluster: ref cluster_name,
            } => {
                vec![
                    CommandArgs::Stop {
                        cluster: cluster_name.clone(),
                        force: Some("false".to_string()),
                    },
                    CommandArgs::Start {
                        cluster: cluster_name.to_string(),
                    },
                ]
            }
            _ => {
                vec![cmd_arg.clone()]
            }
        };

        for cmd in batch_cmd {
            let crl_task_instance = MonographCtlTask::from_config(cmd.clone(), &config);
            barrier.push(crl_task_instance.len());
            mut_executable.extend(crl_task_instance.into_iter());
        }

        let final_barrier = if start_cass_if_need {
            Some(barrier)
        } else {
            None
        };
        Ok(TaskExecutionContext {
            cmd_args: cmd_arg.clone(),
            barrier: final_barrier,
            executable: mut_executable,
        })
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

impl TaskGroup for InstallRuntimeDepsTaskGroup {
    fn tasks(
        &self,
        cmd_arg: CommandArgs,
        config: DeploymentConfig,
        _already_successful: Option<Vec<TaskStatusEntity>>,
    ) -> anyhow::Result<TaskExecutionContext> {
        let install_runtime_deps = RuntimeDepsInstallation::from_config(&config)?;
        Ok(TaskExecutionContext {
            cmd_args: cmd_arg,
            barrier: None,
            executable: install_runtime_deps,
        })
    }
}
