use crate::cli::config::{DeploymentConfig, DeploymentService, StorageProvider};
use crate::cli::download_dir;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::{ssh_conn_info, task_return_value};
use async_trait::async_trait;
use itertools::Itertools;
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::info;

pub(crate) const SOURCE_PATH: &str = "source_file";
pub(crate) const DB_CONFIG_UPLOAD_TASK: &str = "db_config_upload";
pub(crate) const INSTALL_MONOGRAPH_UPLOAD_TASK: &str = "install_monograph_script_upload";
pub(crate) const MONOGRAPH_CONFIG_UPLOAD_TASK: &str = "monograph_config_upload";
pub(crate) const MONOGRAPH_INSTALL_CONFIG_UPLOAD_TASK: &str = "monograph_install_db_conf_upload";

pub(crate) const ALL_UPLOAD_TASKS: [&str; 4] = [
    DB_CONFIG_UPLOAD_TASK,
    INSTALL_MONOGRAPH_UPLOAD_TASK,
    MONOGRAPH_CONFIG_UPLOAD_TASK,
    MONOGRAPH_INSTALL_CONFIG_UPLOAD_TASK,
];

#[derive(Debug, Clone)]
pub struct UploadTask {
    config: DeploymentConfig,
    task_host: TaskHost,
    task_id: TaskId,
}
#[macro_export]
macro_rules! monograph_config_task_execution {
    ( $({$execution_vec:expr, $task_name:expr, $config:expr, $source_task_host:expr,$task_host:expr, $source_path:expr}),*) => {
        $(
        $execution_vec.push(
           TaskInstance {
               task_input: HashMap::from([(SOURCE_PATH.to_string(),TaskArgValue::Str($source_path))]),
               task: Box::new(UploadTask::new(
                   $config.clone(),
                   $source_task_host.clone(),
                   TaskId {
                       cmd: "deploy".to_string(),
                       task: $task_name,
                   },)
               ),
               task_host: $task_host.clone(),
           }
        );
        )*
    };
}

impl UploadTask {
    pub fn build_datafarm_tasks(
        config: &DeploymentConfig,
        source_host: TaskHost,
        dest_hosts: Vec<TaskHost>,
    ) -> Vec<TaskInstance> {
        let datafarm = format!("{}/datafarm", config.install_dir());
        dest_hosts
            .iter()
            .map(|dest_host| TaskInstance {
                task_input: HashMap::from([(
                    SOURCE_PATH.to_string(),
                    TaskArgValue::Str(datafarm.clone()),
                )]),
                task: Box::new(UploadTask::new(
                    config.clone(),
                    source_host.clone(),
                    TaskId {
                        cmd: "install".to_string(),
                        task: "upload_datafarm".to_string(),
                    },
                )),
                task_host: dest_host.clone(),
            })
            .collect_vec()
    }

    fn tasks_from_host_list(
        service: DeploymentService,
        host_vec: Vec<String>,
        config: &DeploymentConfig,
    ) -> anyhow::Result<Vec<TaskInstance>> {
        let download_files = config.download_file_as_map()?;

        let install_db_script_opt = if service == DeploymentService::Monograph {
            let db_config_pair = config.gen_install_db_script()?;
            Some(db_config_pair)
        } else {
            None
        };

        // let db_start_script_path =
        let install_db_config_path = config.clone().gen_monograph_config(None)?;

        let install_db_config = install_db_config_path.to_str().unwrap().to_string();
        let conn_user = config.connection.clone().username;
        let ssh_port = config.connection.ssh_port();
        let source_task_host = TaskHost::Remote {
            user: conn_user.clone(),
            port: ssh_port as usize,
            hosts: "127.0.0.1".to_string(),
        };
        let storage_provider = config.get_monograph_storage()?;
        let execution_context_vec = host_vec
            .into_iter()
            .map(|remote_host| {
                let task_host = TaskHost::Remote {
                    user: conn_user.clone(),
                    hosts: remote_host.clone(),
                    port: ssh_port as usize,
                };
                match service {
                    DeploymentService::Storage => {
                        let mut upload_storage_task_execution= vec![];
                        if storage_provider == StorageProvider::Cassandra {
                            let cassandra_download_file =
                                download_files.get(&DeploymentService::Storage).unwrap();

                            let cassandra_download_path = download_dir()
                                .join(cassandra_download_file)
                                .to_str()
                                .unwrap()
                                .to_string();

                            let upload_cassandra = UploadTask::new(
                                config.clone(),
                                task_host.clone(),
                                TaskId {
                                    cmd: "deploy".to_string(),
                                    task: format!("{}_upload", "cassandra"),
                                },
                            );
                            upload_storage_task_execution.push(TaskInstance {
                                task_input: HashMap::from([(
                                    SOURCE_PATH.to_string(),
                                    TaskArgValue::Str(cassandra_download_path),
                                )]),
                                task: Box::new(upload_cassandra),
                                task_host,
                            });
                        }
                        upload_storage_task_execution
                    }
                    DeploymentService::Monograph => {
                        let mut task_execution_vec = vec![];
                        let db_config_path =
                            config.clone().gen_monograph_config(Some(remote_host)).unwrap();

                        let install_db_script = install_db_script_opt.as_ref().unwrap().clone();
                        let db_config_path_str = db_config_path.to_str().unwrap().to_string();
                        // $execution_vec:expr, $task_name:expr, $config:expr, $task_host:expr, $source_path:expr
                        let install_db_path_string = install_db_script.to_str().unwrap().to_string();

                        let monograph_download_file =
                             download_files.get(&DeploymentService::Monograph).unwrap();

                        let monograph_download_location = download_dir().
                            join(monograph_download_file).to_str().
                            unwrap().to_string();
                        monograph_config_task_execution! {
                            {task_execution_vec, DB_CONFIG_UPLOAD_TASK.to_string(), config, source_task_host, task_host, db_config_path_str},
                            {task_execution_vec, INSTALL_MONOGRAPH_UPLOAD_TASK.to_string(), config, source_task_host, task_host, install_db_path_string},
                            {task_execution_vec, MONOGRAPH_CONFIG_UPLOAD_TASK.to_string(), config, source_task_host, task_host, monograph_download_location},
                            {task_execution_vec, MONOGRAPH_INSTALL_CONFIG_UPLOAD_TASK.to_string(), config, source_task_host, task_host, install_db_config.clone()}
                        }
                        task_execution_vec
                    }
                }
            })
            .into_iter()
            .flatten()
            .collect_vec();
        Ok(execution_context_vec)
    }

    pub fn from_config(config: &DeploymentConfig) -> anyhow::Result<Vec<TaskInstance>> {
        let all_hosts = config.get_host_as_map();
        let execution_context_vec = all_hosts
            .into_iter()
            .map(|entry| {
                let service = entry.0;
                let hosts = entry.1;
                UploadTask::tasks_from_host_list(service, hosts, config).unwrap()
            })
            .into_iter()
            .flatten()
            .collect_vec();

        Ok(execution_context_vec)
    }

    pub fn new(config: DeploymentConfig, task_host: TaskHost, task_id: TaskId) -> Self {
        Self {
            config,
            task_host,
            task_id,
        }
    }
}

#[async_trait]
impl TaskExecutor for UploadTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        task_input: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        ssh_conn_info! {
            self.config.connection.clone(),
            self.task_host.clone(),
            ssh_conn,
            _conn_user,
            _conn_host
        }
        let (remote_user, port, remote_host) = task_host.ssh_conn_tuple();
        let source_path_str =
            TaskArgValue::into_inner_value::<String>(task_input.get(SOURCE_PATH).unwrap().clone());

        let source_path_buf = PathBuf::from(source_path_str.as_str());
        // scp /xxx/local_file user@remote_host:remote_dir/file
        let remote_install_dir = self.config.install_dir();
        //let local_file = self.source_path.to_str().unwrap();
        info!(
            "UploadFileTask will be start local_file={}",
            source_path_str
        );
        let source_file_name = source_path_buf.file_name().unwrap().to_str().unwrap();
        let scp_cmd = format!(
            // dir port, usr host remote_dir file_name
            r#"mkdir -p {} && scp -P {} {} {}@{}:{}/{}"#,
            remote_install_dir,
            port,
            source_path_str,
            remote_user,
            remote_host,
            remote_install_dir,
            source_file_name
        );
        let task_rs = ssh_conn?.run_cmd(scp_cmd, false)?;
        task_return_value!(
            task_rs,
            |status_code: usize| -> CmdErr {
                CmdErr::UploadErr(source_path_str, status_code.to_string())
            },
            "UploadTask"
        );
    }
}
