use crate::cli::ssh::{SSHCommandOption, SSHSession};
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::config::config_base::{
    DeploymentConfig, MONOGRAPH_LOG_SERVICE_DIR, MONOGRAPH_TX_SERVICE_DIR,
};
use crate::task_return_value;
use async_trait::async_trait;
use indexmap::IndexMap;
use std::collections::HashMap;
use tracing::info;

pub(crate) const REMOTE_TAR: &str = "remote_tar";
pub(crate) const UNPACKED_NAME: &str = "unpacked_name";
pub(crate) const REMOTE_UNPACKED_NAMES: [&str; 7] = [
    "apache-cassandra",
    "prometheus",
    "grafana",
    "node_exporter",
    "mysqld_exporter",
    "datastax-mcac-agent",
    "monograph-logserver",
];

#[derive(Debug, Clone)]
pub struct UnpackFileTask {
    config: DeploymentConfig,
    task_id: TaskId,
}

fn extract_unpacked_name(raw_file_name: &str) -> String {
    for unpacked in REMOTE_UNPACKED_NAMES {
        if !raw_file_name.contains(unpacked) {
            continue;
        }
        return unpacked.to_string();
    }
    unreachable!()
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
                let packed_file = entry.0.to_lowercase();
                let hosts = entry.1;
                let unpacked_file = if packed_file.contains("monograph-tx") {
                    MONOGRAPH_TX_SERVICE_DIR.to_string()
                } else if packed_file.contains("monograph-log") {
                    MONOGRAPH_LOG_SERVICE_DIR.to_string()
                } else {
                    extract_unpacked_name(packed_file.as_str())
                };
                hosts
                    .into_iter()
                    .map(|remote_host| {
                        let remote_tarball = format!("{remote_install_dir}/{packed_file}");
                        let task_host = TaskHost::Remote {
                            user: conn_usr.clone(),
                            port: ssh_port as usize,
                            hosts: remote_host.clone(),
                        };
                        let task_id = TaskId {
                            cmd: "deploy".to_string(),
                            task: format!("{packed_file}_unpack"),
                            host: remote_host,
                        };
                        (
                            task_id.clone(),
                            TaskInstance {
                                task_input: HashMap::from([
                                    (REMOTE_TAR.to_string(), TaskArgValue::Str(remote_tarball)),
                                    (
                                        UNPACKED_NAME.to_string(),
                                        TaskArgValue::Str(unpacked_file.clone()),
                                    ),
                                ]),
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
        let unpacked_name = TaskArgValue::into_inner_value::<String>(
            task_input.get(UNPACKED_NAME).unwrap().clone(),
        );
        let install_dir = self.config.install_dir();
        let unpack_pair = if unpacked_name.contains(MONOGRAPH_TX_SERVICE_DIR) {
            let target_dir = format!("{install_dir}/{unpacked_name}");
            (
                format!(r#"mkdir -p {target_dir} && tar -zxvf {remote_tar} -C {target_dir}"#,),
                target_dir,
            )
        } else {
            let target_dir = format!("{install_dir}/{unpacked_name}");
            (
                format!(
                    r#"mkdir -p {target_dir}; tar -zxvf {remote_tar} -C {target_dir} --strip-components 1 --overwrite"#,
                ),
                target_dir,
            )
        };
        let unpack_cmd = unpack_pair.0;
        let unpack_and_remove_raw_file = format!("{unpack_cmd};rm -rf {remote_tar}");
        info!("UnpackFileTask cmd={}", unpack_and_remove_raw_file);
        let task_rs = ssh_session
            .command(unpack_and_remove_raw_file.as_str(), SSHCommandOption::None)
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

#[cfg(test)]
mod tests {
    use crate::cli::task::unpack_file_task::extract_unpacked_name;

    #[test]
    pub fn test_extract_unpacked_name() {
        let unpacked_name = extract_unpacked_name("monographdb-ubuntu20-release-bin.tar.gz");
        println!("unpacked fil name={unpacked_name}")
    }
}
