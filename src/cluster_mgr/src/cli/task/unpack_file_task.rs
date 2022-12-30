use crate::cli::config::DeploymentConfig;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::{ssh_conn_info, task_return_value};
use async_trait::async_trait;
use itertools::Itertools;
use std::collections::HashMap;
use tracing::info;

pub(crate) const REMOTE_TAR: &str = "remote_tar";

#[derive(Debug, Clone)]
pub struct UnpackFileTask {
    config: DeploymentConfig,
    task_id: TaskId,
}

impl UnpackFileTask {
    pub fn from_config(config: &DeploymentConfig) -> anyhow::Result<Vec<TaskInstance>> {
        let remote_install_dir = config.install_dir();
        let conn_usr = config.connection.clone().username;
        let ssh_port = config.connection.ssh_port();
        // key is file name , value is host list
        let all_hosts = config.unpack_files_map();
        let unpack_execution_vec = all_hosts
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
                        TaskInstance {
                            task_input: HashMap::from([(
                                REMOTE_TAR.to_string(),
                                TaskArgValue::Str(remote_tarball),
                            )]),
                            task: Box::new(UnpackFileTask {
                                config: config.clone(),
                                task_id: TaskId {
                                    cmd: "deploy".to_string(),
                                    task: format!("{}_unpack", unpack_file),
                                    host: remote_host,
                                },
                            }),
                            task_host,
                        }
                    })
                    .collect_vec()
            })
            .into_iter()
            .flatten()
            .collect_vec();
        Ok(unpack_execution_vec)
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
        ssh_conn_info! {
            self.config.connection.clone(),
            task_host,
            ssh_conn,
            _conn_user,
            _conn_host
        }
        let remote_tar =
            TaskArgValue::into_inner_value::<String>(task_input.get(REMOTE_TAR).unwrap().clone());
        let install_dir = self.config.install_dir();
        let unpack_pair = if remote_tar.contains("monograph") {
            let target_dir = format!("{}/monographdb-release", install_dir);
            (
                format!(
                    r#"mkdir -p {}/monographdb-release && tar -zxvf {} -C {}"#,
                    install_dir, remote_tar, target_dir
                ),
                target_dir,
            )
        } else {
            let target_dir = format!("{}/apache-cassandra", install_dir);
            (
                format!(
                    r#"mkdir -p {} && tar -zxvf {} -C {} && mv {} {}"#,
                    install_dir,
                    remote_tar,
                    install_dir,
                    remote_tar.replace("-bin.tar.gz", ""),
                    target_dir
                ),
                target_dir,
            )
        };
        let unpack_cmd = unpack_pair.0;
        info!("UnpackFileTask cmd={}", unpack_cmd.as_str());
        let task_rs = ssh_conn?.run_cmd(unpack_cmd.clone(), false)?;

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
