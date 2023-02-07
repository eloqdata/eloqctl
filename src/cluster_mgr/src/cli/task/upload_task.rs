use crate::cli::download_dir;
use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::config::config_base::DeploymentConfig;
use crate::config::{DeploymentService, StorageProvider};
use crate::task_return_value;
use async_trait::async_trait;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::info;

pub(crate) const SOURCE_PATH: &str = "source_file";
pub(crate) const DEST_PATH: &str = "dest_file";
pub(crate) const COPY_DIR: &str = "copy_dir";
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
    task_id: TaskId,
}
#[macro_export]
macro_rules! monograph_config_task_execution {
    ( $({$execution_vec:expr, $task_name:expr, $config:expr, $task_host:expr, $source_path:expr}),*) => {
        $(
        let (_,_,remote_host) =  $task_host.clone().ssh_conn_tuple();
        let task_id = TaskId {
           cmd: "deploy".to_string(),
           task: $task_name,
           host: remote_host,
        };
        $execution_vec.insert(
           task_id.clone(),
           TaskInstance {
               task_input: HashMap::from([(SOURCE_PATH.to_string(),TaskArgValue::Str($source_path))]),
               task: Box::new(UploadTask::new(
                   $config.clone(),task_id)
               ),
               task_host: $task_host.clone(),
           }
        );
        )*
    };
}

impl UploadTask {
    /// Upload the cassandra.yaml file to the remote host (remote host list from deployment.yaml).
    pub fn build_upload_cass_conf_task(
        config: &DeploymentConfig,
    ) -> anyhow::Result<IndexMap<TaskId, TaskInstance>> {
        let cass_config = config.gen_cassandra_config()?;
        let ssh_port = config.connection.ssh_port();
        let conn_user = config.clone().connection.username;
        let upload_cass_config_task = cass_config
            .into_iter()
            .map(|(host, cass_config)| {
                let cass_config_path_str = cass_config.to_str().unwrap().to_string();
                let task_id = TaskId {
                    cmd: "install".to_string(),
                    task: "cassandra_config_upload".to_string(),
                    host: host.clone(),
                };
                (
                    task_id.clone(),
                    TaskInstance {
                        task_input: HashMap::from([
                            (
                                SOURCE_PATH.to_string(),
                                TaskArgValue::Str(cass_config_path_str),
                            ),
                            (
                                DEST_PATH.to_string(),
                                TaskArgValue::Str(
                                    "apache-cassandra/conf/cassandra.yaml".to_string(),
                                ),
                            ),
                        ]),
                        task: Box::new(UploadTask::new(config.clone(), task_id)),
                        task_host: TaskHost::Remote {
                            user: conn_user.clone(),
                            port: ssh_port as usize,
                            hosts: host,
                        },
                    },
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>();
        Ok(upload_cass_config_task)
    }

    /// Upload the MonographDB data_dir to the remote host.
    pub fn build_upload_data_dir_tasks(
        config: &DeploymentConfig,
        dest_hosts: Vec<TaskHost>,
    ) -> IndexMap<TaskId, TaskInstance> {
        let datafarm = format!("{}/datafarm", config.install_dir());
        dest_hosts
            .iter()
            .map(|dest_host| {
                let (_, _, host) = dest_host.ssh_conn_tuple();
                let task_id = TaskId {
                    cmd: "install".to_string(),
                    task: "upload_datafarm".to_string(),
                    host,
                };
                (
                    task_id.clone(),
                    TaskInstance {
                        task_input: HashMap::from([
                            (SOURCE_PATH.to_string(), TaskArgValue::Str(datafarm.clone())),
                            (COPY_DIR.to_string(), TaskArgValue::Str("-r".to_string())),
                        ]),
                        task: Box::new(UploadTask::new(config.clone(), task_id)),
                        task_host: dest_host.clone(),
                    },
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>()
    }

    fn task_install_from_hosts(
        service: DeploymentService,
        host_vec: Vec<String>,
        config: &DeploymentConfig,
    ) -> anyhow::Result<IndexMap<TaskId, TaskInstance>> {
        let download_files = config.download_file_as_map()?;

        let install_db_script_opt = if service == DeploymentService::Monograph {
            let db_config_pair = config.gen_install_db_script()?;
            Some(db_config_pair)
        } else {
            None
        };

        let install_db_config_path = config.clone().gen_monograph_config(None)?;

        let install_db_config = install_db_config_path.to_str().unwrap().to_string();
        let conn_user = config.connection.clone().username;
        let ssh_port = config.connection.ssh_port();
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
                        let mut task_instance = HashMap::new();
                        if storage_provider == StorageProvider::Cassandra {
                            let cassandra_download_file =
                                download_files.get(&DeploymentService::Storage).unwrap();

                            let cassandra_download_path = download_dir()
                                .join(cassandra_download_file)
                                .to_str()
                                .unwrap()
                                .to_string();

                            let task_id = TaskId {
                                cmd: "deploy".to_string(),
                                task: format!("{}_upload", "cassandra"),
                                host: remote_host,
                            };
                            let upload_cassandra = UploadTask::new(
                                config.clone(),
                                task_id.clone()
                            );
                            task_instance.insert(task_id, TaskInstance {
                                task_input: HashMap::from([(
                                    SOURCE_PATH.to_string(),
                                    TaskArgValue::Str(cassandra_download_path),
                                )]),
                                task: Box::new(upload_cassandra),
                                task_host,
                            });
                        }
                        task_instance
                    }
                    DeploymentService::Monograph => {
                        info!("UploadTask upload file to remote={:?}", task_host);
                        let mut task_execution_vec:HashMap<TaskId, TaskInstance> = HashMap::new();
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
                            {task_execution_vec, DB_CONFIG_UPLOAD_TASK.to_string(), config, task_host, db_config_path_str},
                            {task_execution_vec, INSTALL_MONOGRAPH_UPLOAD_TASK.to_string(), config, task_host, install_db_path_string},
                            {task_execution_vec, MONOGRAPH_CONFIG_UPLOAD_TASK.to_string(), config, task_host, monograph_download_location},
                            {task_execution_vec, MONOGRAPH_INSTALL_CONFIG_UPLOAD_TASK.to_string(), config, task_host, install_db_config.clone()}
                        }
                        task_execution_vec
                    }
                }
            })
            .into_iter()
            .flatten()
            .collect::<IndexMap<TaskId, TaskInstance>>();
        Ok(execution_context_vec)
    }

    /// Upload installation package, MonographDB configuration file (my.cnf),
    /// MonographDB install script, install config to remote host.
    pub fn from_config(
        config: &DeploymentConfig,
    ) -> anyhow::Result<IndexMap<TaskId, TaskInstance>> {
        let all_hosts = config.get_host_as_map();
        let upload_task_instance = all_hosts
            .into_iter()
            .map(|entry| {
                let service = entry.0;
                let hosts = entry.1;
                UploadTask::task_install_from_hosts(service, hosts, config).unwrap()
            })
            .into_iter()
            .flatten()
            .collect::<IndexMap<TaskId, TaskInstance>>();

        Ok(upload_task_instance)
    }

    pub fn new(config: DeploymentConfig, task_id: TaskId) -> Self {
        Self { config, task_id }
    }

    pub async fn create_remote_directory(&self, remote_task_host: TaskHost) -> anyhow::Result<()> {
        let ssh_session = SSHSession::from_task_host(
            remote_task_host,
            self.config.connection.ssh_auth_key().unwrap(),
        )
        .await?;
        let mkdir = format!("mkdir -p {}", self.config.install_dir());
        let mkdir_output = ssh_session.command(mkdir.as_str(), CollectOutput).await?;
        info!("UploadTask create remote dir complete={:?}", mkdir_output);
        Ok(())
    }
}

#[async_trait]
impl TaskExecutor for UploadTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        remote_task_host: TaskHost,
        task_input: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        println!("{} execute.\n", self.task_id.pretty_string());
        self.create_remote_directory(remote_task_host.clone())
            .await?;

        let source_ip_rs = local_ip_address::local_ip()?;
        let local_ip_addr = source_ip_rs.to_string();
        let ssh_port = self.config.connection.ssh_port();
        let ssh_user = self.config.connection.clone().username;
        let source_task_host = TaskHost::Remote {
            user: ssh_user,
            port: ssh_port as usize,
            hosts: local_ip_addr,
        };

        let ssh_session = SSHSession::from_task_host(
            source_task_host,
            self.config.connection.ssh_auth_key().unwrap(),
        )
        .await?;

        let remote_install_dir = self.config.install_dir();

        let (remote_user, port, remote_host) = remote_task_host.ssh_conn_tuple();
        let source_path_str =
            TaskArgValue::into_inner_value::<String>(task_input.get(SOURCE_PATH).unwrap().clone());

        let source_path_buf = PathBuf::from(source_path_str.as_str());

        let dest_file_name = if let Some(dest_file_str) = task_input.get(DEST_PATH) {
            TaskArgValue::into_inner_value::<String>(dest_file_str.clone())
        } else {
            source_path_buf
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        };

        let copy_dir = if let Some(scp_dir) = task_input.get(COPY_DIR) {
            TaskArgValue::into_inner_value::<String>(scp_dir.clone())
        } else {
            "".to_string()
        };

        let scp_auth_key = format!("-i {}", self.config.connection.ssh_auth_key().unwrap());
        // scp /xxx/local_file user@remote_host:remote_dir/file
        let scp_cmd = format!(
            // dir port, usr host remote_dir file_name
            r#"scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no {copy_dir} {scp_auth_key} -P {port} {source_path_str} {remote_user}@{remote_host}:{remote_install_dir}/{dest_file_name}"#,
        );
        info!("UploadTask cmd={}", scp_cmd);
        let err_msg = format!("cmd={scp_cmd},source_path={source_path_str}");
        let task_rs = ssh_session.command(scp_cmd.as_str(), CollectOutput).await?;
        ssh_session.close().await?;
        task_return_value!(
            task_rs,
            |status_code: usize| -> CmdErr { CmdErr::UploadErr(err_msg, status_code.to_string()) },
            "UploadTask"
        );
    }
}
