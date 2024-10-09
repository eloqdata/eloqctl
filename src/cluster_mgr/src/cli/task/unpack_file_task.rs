use crate::cli::ssh::{SSHCommandOption, SSHSession};
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::config::config_base::{DeployConfig, LOG_SERVICE_HOME};
use crate::config::storage_service_config::CassKind;
use crate::config::DownloadUrl;
use crate::task_return_value;
use async_trait::async_trait;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;
use tracing::info;

pub(crate) const REMOTE_UNPACKED_NAMES: [&str; 8] = [
    "cassandra",
    "prometheus",
    "grafana",
    "node_exporter",
    "mysqld_exporter",
    "datastax-mcac-agent",
    "monograph-logserver",
    "codis",
];

#[derive(Debug, Clone)]
pub struct UnpackFileTask {
    config: DeployConfig,
    task_id: TaskId,
    tarball: String,
    unpack_dest: String,
    exclude: Vec<String>,
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
    pub fn from_config(config: &DeployConfig) -> anyhow::Result<IndexMap<TaskId, TaskInstance>> {
        let deployment_ref = &config.deployment;

        let tx_image = DownloadUrl::from_url_str(deployment_ref.tx_image())
            .unwrap()
            .file_name();
        let log_image = if let Some(img) = deployment_ref.log_image() {
            DownloadUrl::from_url_str(img).unwrap().file_name()
        } else {
            "".to_string()
        };
        let remote_install_dir = config.install_dir();
        let conn_usr = config.connection.clone().username;
        let ssh_port = config.connection.ssh_port();
        let unpack_file_location = config.unpack_files_map();
        let unpack_task_instance = unpack_file_location
            .iter()
            .map(|unpack_location| {
                let packed_file = &unpack_location.file;
                let curr_file_name = packed_file.file_name();
                let remote_host = &unpack_location.host;

                let unpack_dest = if curr_file_name.eq(&log_image) {
                    LOG_SERVICE_HOME.to_string()
                } else if curr_file_name.eq(&tx_image) {
                    config.product().home().to_owned()
                } else {
                    extract_unpacked_name(curr_file_name.as_str())
                };

                let tarball = format!("{remote_install_dir}/{curr_file_name}");
                let task_host = TaskHost::Remote {
                    user: conn_usr.clone(),
                    port: ssh_port as usize,
                    host: remote_host.clone(),
                };
                let task_id = TaskId {
                    cmd: "deploy".to_string(),
                    task: format!("{curr_file_name}_unpack"),
                    host: remote_host.clone(),
                };
                (
                    task_id.clone(),
                    TaskInstance {
                        task_input: HashMap::default(),
                        task: Box::new(UnpackFileTask {
                            config: config.clone(),
                            task_id,
                            tarball,
                            unpack_dest,
                            exclude: vec![],
                        }),
                        task_host,
                    },
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>();

        Ok(unpack_task_instance)
    }

    pub fn unpack_eloqservers(config: &DeployConfig) -> IndexMap<TaskId, TaskInstance> {
        let deploy_ref = &config.deployment;
        let image = deploy_ref.tx_image().split('/').last().unwrap();
        let tx_home = config.product().home().to_owned();
        let mut tasks = deploy_ref
            .tx_service
            .tx_host_ports
            .iter()
            .map(|host_port| {
                //
                let host = host_port.split(':').next().unwrap();
                Self::make_task_pair(config, host, image, &tx_home, vec![])
            })
            .collect::<IndexMap<TaskId, TaskInstance>>();
        if let Some(srv) = &deploy_ref.log_service {
            let image = srv.image.as_ref().unwrap().split('/').last().unwrap();
            let ret = srv
                .log_host_unique()
                .iter()
                .map(|host| Self::make_task_pair(config, host, image, LOG_SERVICE_HOME, vec![]))
                .collect::<IndexMap<TaskId, TaskInstance>>();
            tasks.extend(ret);
        }
        tasks
    }

    pub fn unpack_cassandra(config: &DeployConfig, ex_cnf: bool) -> IndexMap<TaskId, TaskInstance> {
        let deploy_ref = &config.deployment;
        if let Some(cass) = &deploy_ref.storage_service.cassandra {
            if let CassKind::Internal(cdp) = &cass.kind {
                let image = cdp.image_file();
                let home = extract_unpacked_name(&image);
                let exclude = if ex_cnf {
                    vec![
                        "conf/cassandra.yaml".to_owned(),
                        "conf/jvm11-server.options".to_owned(),
                        "conf/cassandra-env.sh".to_owned(),
                    ]
                } else {
                    vec![]
                };
                return cass
                    .host
                    .iter()
                    .map(|host| Self::make_task_pair(config, host, &image, &home, exclude.clone()))
                    .collect();
            }
        }
        IndexMap::new()
    }

    fn make_task_pair(
        config: &DeployConfig,
        host: &str,
        image: &str,
        home: &str,
        exclude: Vec<String>,
    ) -> (TaskId, TaskInstance) {
        let tarball = format!("{}/{image}", config.deployment.install_dir());
        let task_host = TaskHost::Remote {
            user: config.connection.username.clone(),
            port: config.connection.ssh_port() as usize,
            host: host.to_owned(),
        };
        let task_id = TaskId {
            cmd: "update".to_string(),
            task: format!("{image}_unpack"),
            host: host.to_owned(),
        };
        let task = UnpackFileTask {
            config: config.clone(),
            task_id: task_id.clone(),
            tarball,
            unpack_dest: home.to_owned(),
            exclude,
        };
        let inst = TaskInstance {
            task_input: HashMap::default(),
            task: Box::new(task),
            task_host,
        };
        (task_id, inst)
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
        _task_input: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        info!("execute {}", self.task_id.pretty_string());
        let ssh_session = SSHSession::from_task_host(
            task_host,
            self.config.connection.ssh_auth_key().unwrap().to_string(),
        )
        .await?;
        let tarball = &self.tarball;
        let target = format!("{}/{}", self.config.install_dir(), self.unpack_dest);
        let exclude = self
            .exclude
            .iter()
            .map(|ex| format!("--exclude='{ex}'"))
            .join(" ");

        let mut cmds = vec![format!("mkdir -p {target}")];
        cmds.push(format!(
            "tar -zxvf {tarball} -C {target} --strip-components 1 --overwrite {exclude}"
        ));
        // TODO(ZX) temp, redis_server code has bug to fix, delete the command below later
        // cmds.push("cp /home/mono/workspace/monograph_redis_bin/Debug/bin/eloqkv /home/mono/eloqkv_with_hot_standby/EloqKV/bin/eloqkv".to_string());
        cmds.push(format!("rm {tarball}"));
        let unpack_cmd = cmds.join(" && ");
        info!("UnpackFileTask cmd={unpack_cmd}");

        let task_rs = ssh_session
            .command(unpack_cmd.as_str(), SSHCommandOption::None)
            .await?;
        ssh_session.close().await?;
        task_return_value!(
            task_rs,
            |status_code: i32| -> CmdErr { CmdErr::UnpackErr(unpack_cmd, status_code.to_string()) },
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
