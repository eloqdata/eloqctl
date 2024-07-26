use crate::cli::ssh::SSHSession;
use crate::cli::task::task_base::{
    ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::util::{os_id, os_major_version};
use crate::config::config_base::DeployConfig;
use anyhow::{bail, Result};
use async_trait::async_trait;
use indexmap::IndexMap;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use itertools::Itertools;
use std::collections::HashMap;
use std::vec;
use tracing::info;

#[derive(Clone, Debug)]
pub struct DepPkgTask {
    task_id: TaskId,
    config: DeployConfig,
    prepare: Vec<String>,
    head: String,
    pkgs: Vec<String>,
    pg_bar: ProgressBar,
}

impl DepPkgTask {
    pub fn from_config(config: &DeployConfig) -> Result<IndexMap<TaskId, TaskInstance>> {
        let os_name = os_id();
        let version = os_major_version();
        info!("RuntimeDep from_config = {os_name} {version}");
        let (prepare, head);
        match os_name.as_str() {
            "ubuntu" => {
                prepare = vec!["sudo apt update"];
                head = "sudo DEBIAN_FRONTEND=noninteractive apt install -y --no-install-recommends";
            }
            "rhel" => match version.as_str() {
                "7" => {
                    prepare = vec!["sudo yum install -y epel-release", "sudo yum update -y"];
                    head = "sudo yum install -y";
                }
                "8" => {
                    prepare = vec![
                        "sudo dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm", 
                        "sudo /usr/bin/crb enable", 
                        "sudo dnf install -y epel-release", 
                        "sudo dnf update -y"];
                    head = "sudo dnf install -y";
                }
                "9" => {
                    prepare = vec![
                        "sudo dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm", 
                        "sudo /usr/bin/crb enable", 
                        "sudo dnf install -y epel-release", 
                        "sudo dnf update -y"
                        ];
                    head = "sudo dnf install -y";
                }
                _ => unreachable!(),
            },
            _ => {
                bail!("for now only support ubuntu/rhel linux");
            }
        };
        let is_root = config.connection.username == "root";
        let prepare = prepare
            .into_iter()
            .map(|s| {
                if &s[..6] == "sudo " && is_root {
                    &s[5..]
                } else {
                    s
                }
                .to_owned()
            })
            .collect_vec();
        let head = if &head[..6] == "sudo " && is_root {
            &head[5..]
        } else {
            head
        };
        let pkgs = DeployConfig::load_runtime_deps_by_os(&os_name)?;

        let mpg_bar = MultiProgress::new();
        let conn_user = config.connection.clone().username;
        let ssh_port = config.connection.ssh_port();
        let host_values = config.get_unique_host_list();
        let install_dep_task = host_values
            .iter()
            .map(|host_name| {
                let task_id = TaskId {
                    cmd: "run_deps".to_string(),
                    task: format!("{os_name}_install_deps"),
                    host: host_name.clone(),
                };
                let pg_bar = mpg_bar.add(ProgressBar::hidden());
                let task = DepPkgTask {
                    task_id: task_id.clone(),
                    config: config.clone(),
                    prepare: prepare.clone(),
                    head: head.to_owned(),
                    pkgs: pkgs.clone(),
                    pg_bar,
                };
                let task_host = TaskHost::Remote {
                    user: conn_user.clone(),
                    port: ssh_port as usize,
                    hosts: host_name.to_string(),
                };
                (
                    task_id,
                    TaskInstance {
                        task_input: HashMap::new(),
                        task: Box::new(task),
                        task_host,
                    },
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>();

        Ok(install_dep_task)
    }
}

#[async_trait]
impl TaskExecutor for DepPkgTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> Result<Option<ExecutionValue>> {
        info!("execute {}", self.task_id.pretty_string());
        let (_, _, host) = task_host.ssh_conn_tuple();
        let session = SSHSession::from_task_host(
            task_host.clone(),
            self.config.connection.ssh_auth_key().unwrap(),
        )
        .await?;
        let pkg_cmds = self
            .pkgs
            .iter()
            .map(|s| format!("{} {s}", self.head))
            .collect_vec();
        let temp = "[{pos}/{len}] {elapsed} {bar:40.cyan/grey} {wide_msg}";
        let style = ProgressStyle::default_bar().template(temp)?;
        self.pg_bar.set_style(style);
        self.pg_bar
            .set_length((self.prepare.len() + pkg_cmds.len()) as u64);
        for cmd in self.prepare.iter().chain(pkg_cmds.iter()) {
            let msg = format!("{host} $ {cmd}");
            info!("install package {msg}");
            self.pg_bar.set_message(msg);
            let (code, out) = session.execute(cmd).await?;
            if code != 0 {
                bail!("install package failed on {host}: '{cmd}' :{code}, {out}")
            }
            self.pg_bar.inc(1);
        }
        let msg = format!("{host} Finished");
        self.pg_bar.finish_with_message(msg);
        session.close().await?;
        Ok(None)
    }
}
