use crate::config::config_base::DeploymentConfig;
use crate::cli::ssh::{SSHCommandOption, SSHSession};
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::task_return_value;
use async_trait::async_trait;
use indexmap::IndexMap;
use std::collections::HashMap;
use tracing::info;

pub(crate) const REMOTE_TAR: &str = "remote_tar";

#[derive(Debug, Clone)]
pub struct UnpackFileTask {
    config: DeploymentConfig,
    task_id: TaskId,
}

impl UnpackFileTask {
    pub fn from_config(
        config: &DeploymentConfig,
    ) -> anyhow::Result<IndexMap<TaskId, TaskInstance>> {
        let remote_install_dir = config.install_dir();
        let conn_usr = config.connection.clone().username;
        let ssh_port = config.connection.ssh_port();
        // key is file name , value is host list
        let all_hosts = config.unpack_files_map();
        let unpack_task_instance = all_hosts
            .into_iter()
            .map(|entry| {
                let unpack_file = entry.0;
                let hosts = entry.1;
                hosts
                    .into_iter()
                    .map(|remote_host| {
                        let remote_tarball =
                            format!("{}/{}", remote_install_dir, unpack_file.as_str());
                        let task_host = TaskHost::Remote {
                            user: conn_usr.clone(),
                            port: ssh_port as usize,
                            hosts: remote_host.clone(),
                        };
                        let task_id = TaskId {
                            cmd: "deploy".to_string(),
                            task: format!("{unpack_file}_unpack"),
                            host: remote_host,
                        };
                        (
                            task_id.clone(),
                            TaskInstance {
                                task_input: HashMap::from([(
                                    REMOTE_TAR.to_string(),
                                    TaskArgValue::Str(remote_tarball),
                                )]),
                                task: Box::new(UnpackFileTask {
                                    config: config.clone(),
                                    task_id,
                                }),
                                task_host,
                            },
                        )
                    })
                    .collect::<IndexMap<TaskId, TaskInstance>>()
            })
            .into_iter()
            .flatten()
            .collect::<IndexMap<TaskId, TaskInstance>>();
        Ok(unpack_task_instance)
    }
}

#[async_trait]
impl TaskExecutor for UnpackFileTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        task_input: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        println!("{} execute.\n", self.task_id.pretty_string());
        let ssh_session = SSHSession::from_task_host(
            task_host,
            self.config.connection.ssh_auth_key().unwrap().to_string(),
        )
        .await?;
        let remote_tar =
            TaskArgValue::into_inner_value::<String>(task_input.get(REMOTE_TAR).unwrap().clone());
        let install_dir = self.config.install_dir();
        let unpack_pair = if remote_tar.contains("monograph") {
            let target_dir = format!("{install_dir}/monographdb-release");
            (
                format!(
                    r#"mkdir -p {install_dir}/monographdb-release && tar -zxvf {remote_tar} -C {target_dir}"#,
                ),
                target_dir,
            )
        } else {
            let extract_file_name = remote_tar.replace("-bin.tar.gz", "");
            let target_dir = format!("{install_dir}/apache-cassandra");
            (
                format!(
                    r#"rm -rf {target_dir} > /dev/null; mkdir -p {install_dir} && tar -zxvf {remote_tar} -C {install_dir}; mv {extract_file_name} {target_dir}"#,
                ),
                target_dir,
            )
        };
        let unpack_cmd = unpack_pair.0;
        info!("UnpackFileTask cmd={}", unpack_cmd.as_str());
        let task_rs = ssh_session
            .command(unpack_cmd.clone().as_str(), SSHCommandOption::None)
            .await?;

        ssh_session.close().await?;
        task_return_value!(
            task_rs,
            |status_code: usize| -> CmdErr {
                CmdErr::UnpackErr(unpack_cmd, status_code.to_string())
            },
            "UnpackFileTask",
            HashMap::from([(
                "UNPACK_TARGET_DIR".to_string(),
                TaskArgValue::Str(unpack_pair.1)
            )])
        )
    }
}
