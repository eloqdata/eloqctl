#![allow(dead_code)]

use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::task::group::Config;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::{ssh, SubCommand, CMD_OUTPUT};
use crate::config::config_base::DeployConfig;
use crate::config::storage_service_config::DataStoreServiceBackend;
use crate::task_return_value;
use async_trait::async_trait;
use indexmap::IndexMap;
use std::collections::HashMap;
use tracing::{debug, info};

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RcloneDirCmd {
    Mkdir(String),
    Purge(String),
    Rmdir(String),
}

impl RcloneDirCmd {
    pub fn cmd_value(&self) -> String {
        match self.clone() {
            RcloneDirCmd::Mkdir(cmd) => cmd,
            RcloneDirCmd::Purge(cmd) => cmd,
            RcloneDirCmd::Rmdir(cmd) => cmd,
        }
    }
}

impl RcloneDirCmd {
    fn build_cmd(config: &DeployConfig, cmd_arg: SubCommand) -> Vec<(String, RcloneDirCmd)> {
        let Some(storage_service) = &config.deployment.storage_service else {
            return vec![];
        };

        let Some(dss) = &storage_service.eloqdss else {
            return vec![];
        };

        // Only process if backend is EloqStore
        let eloq_store_config = match dss.backend_config() {
            DataStoreServiceBackend::EloqStore(cfg) => cfg,
        };

        // Only process if cloud mode is enabled
        if !eloq_store_config.is_cloud_mode() {
            return vec![];
        }

        // Parse remote:path from cloud_store_path
        let (remote_name, path) = match eloq_store_config.parse_cloud_store_path() {
            Some((remote, path)) => (remote, path),
            None => return vec![],
        };

        let mut tasks = vec![];

        match cmd_arg {
            SubCommand::Launch { .. } | SubCommand::Install { .. } => {
                // Build mkdir command
                let mkdir_cmd = format!("rclone mkdir {remote_name}:{path}");
                tasks.push((
                    format!("{remote_name}:{path}"),
                    RcloneDirCmd::Mkdir(mkdir_cmd),
                ));
            }
            SubCommand::Remove { .. } => {
                // Build purge and rmdir commands
                let purge_cmd = format!("rclone purge {remote_name}:{path}");
                let rmdir_cmd = format!("rclone rmdir {remote_name}:{path}");
                tasks.push((
                    format!("{remote_name}:{path}"),
                    RcloneDirCmd::Purge(purge_cmd),
                ));
                tasks.push((
                    format!("{remote_name}:{path}"),
                    RcloneDirCmd::Rmdir(rmdir_cmd),
                ));
            }
            _ => {}
        }

        tasks
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct RcloneDirTask {
    cmd: RcloneDirCmd,
    task_id: TaskId,
    config: Config,
}

impl RcloneDirTask {
    pub fn build_tasks(cmd_arg: SubCommand, config: &Config) -> IndexMap<TaskId, TaskInstance> {
        let mut task_map = IndexMap::new();

        let deploy_config = match config {
            Config::Cluster(cfg) => cfg,
            Config::Proxy(_) => return task_map,
        };

        let tasks = RcloneDirCmd::build_cmd(deploy_config, cmd_arg);

        if tasks.is_empty() {
            return task_map;
        }

        // Get unique hosts - for rclone dir operations, we need to run on the node where rclone will be used
        // For Local mode: txservice nodes
        // For Remote Internal mode: DSS nodes
        let Some(storage_service) = deploy_config.deployment.storage_service.as_ref() else {
            return task_map;
        };
        let Some(dss) = storage_service.eloqdss.as_ref() else {
            return task_map;
        };

        let target_hosts = if dss.is_local_mode() {
            // Local mode: use txservice nodes (just use first node for dir operations)
            let hosts = deploy_config.get_unique_host_list();
            if hosts.is_empty() {
                return task_map;
            }
            vec![hosts[0].clone()]
        } else if dss.is_remote_mode() && !dss.is_external() {
            // Remote Internal mode: use first DSS node for dir operations
            if let Some(peer_host_ports) = dss.peer_host_ports.as_ref() {
                if peer_host_ports.is_empty() {
                    return task_map;
                }
                if let Some(first_host) = peer_host_ports[0].split(':').next() {
                    vec![first_host.to_string()]
                } else {
                    return task_map;
                }
            } else {
                return task_map;
            }
        } else {
            // Remote External mode: don't manage rclone
            return task_map;
        };

        let conn_user = config.conn_user();
        let ssh_port = config.ssh_port();

        for (path_key, cmd) in tasks {
            for host in &target_hosts {
                let task_host = TaskHost::Remote {
                    user: conn_user.to_string(),
                    port: ssh_port as usize,
                    host: host.clone(),
                };

                let cmd_type = match &cmd {
                    RcloneDirCmd::Mkdir(_) => "mkdir",
                    RcloneDirCmd::Purge(_) => "purge",
                    RcloneDirCmd::Rmdir(_) => "rmdir",
                };

                let task_id = TaskId {
                    cmd: "rclone_dir".to_string(),
                    task: format!("{cmd_type}@{path_key}@{host}"),
                    host: host.clone(),
                };

                let task_instance = TaskInstance {
                    task_input: HashMap::default(),
                    task: Box::new(RcloneDirTask {
                        cmd: cmd.clone(),
                        task_id: task_id.clone(),
                        config: config.clone(),
                    }),
                    task_host,
                };

                task_map.insert(task_id, task_instance);
            }
        }

        task_map
    }
}

#[async_trait]
impl TaskExecutor for RcloneDirTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        info!("execute {}", self.task_id.format_string());

        let auth_key = self.config.conn_ssh_auth_key();
        let ssh_session = ssh::SSHSession::from_task_host(task_host, auth_key).await?;
        let (host, _) = ssh_session.ssh_conn_info();

        let cmd_str = self.cmd.cmd_value();
        let exec_cmd_rs = ssh_session.command(cmd_str.as_str(), CollectOutput).await?;

        if let Some(output) = exec_cmd_rs.get(CMD_OUTPUT) {
            debug!(
                "Host {host} Cmd {} output {}",
                cmd_str,
                TaskArgValue::into_inner_value::<String>(output.clone())
            );
        }

        ssh_session.close().await?;
        task_return_value!(
            exec_cmd_rs,
            |status_code: i32| -> CmdErr {
                CmdErr::ExecUserCmdErr(cmd_str, status_code.to_string())
            },
            "RcloneDirTask"
        )
    }
}
