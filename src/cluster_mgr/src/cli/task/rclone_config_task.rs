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
pub enum RcloneConfigCmd {
    Create(String),
    Delete(String),
}

impl RcloneConfigCmd {
    pub fn cmd_value(&self) -> String {
        match self.clone() {
            RcloneConfigCmd::Create(cmd) => cmd,
            RcloneConfigCmd::Delete(cmd) => cmd,
        }
    }
}

impl RcloneConfigCmd {
    fn build_cmd(config: &DeployConfig, cmd_arg: SubCommand) -> Vec<(String, RcloneConfigCmd)> {
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

        // Parse remote name from cloud_store_path
        let (remote_name, _) = match eloq_store_config.parse_cloud_store_path() {
            Some((remote, _)) => (remote, ()),
            None => return vec![],
        };

        // Get cloud config
        let cloud_config = match eloq_store_config.get_cloud_config() {
            Some(cfg) => cfg,
            None => return vec![],
        };

        let mut tasks = vec![];

        match cmd_arg {
            SubCommand::Launch { .. } | SubCommand::Install { .. } => {
                // Build create command
                let cloud_type = &cloud_config.cloud_type;
                let cloud_provider = &cloud_config.cloud_provider;
                let access_key_id = &cloud_config.access_key_id;
                let secret_access_key = &cloud_config.secret_access_key;
                let endpoint = &cloud_config.endpoint;

                let create_cmd = format!(
                    "rclone config create {remote_name} {cloud_type} provider {cloud_provider} env_auth false access_key_id {access_key_id} secret_access_key {secret_access_key} endpoint {endpoint} acl private"
                );

                tasks.push((remote_name.clone(), RcloneConfigCmd::Create(create_cmd)));
            }
            SubCommand::Remove { .. } => {
                // Build delete command
                let delete_cmd = format!("rclone config delete {remote_name}");

                tasks.push((remote_name.clone(), RcloneConfigCmd::Delete(delete_cmd)));
            }
            _ => {}
        }

        tasks
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct RcloneConfigTask {
    cmd: RcloneConfigCmd,
    task_id: TaskId,
    config: Config,
}

impl RcloneConfigTask {
    pub fn build_tasks(cmd_arg: SubCommand, config: &Config) -> IndexMap<TaskId, TaskInstance> {
        let mut task_map = IndexMap::new();

        let deploy_config = match config {
            Config::Cluster(cfg) => cfg,
            Config::Proxy(_) => return task_map,
        };

        let tasks = RcloneConfigCmd::build_cmd(deploy_config, cmd_arg);

        if tasks.is_empty() {
            return task_map;
        }

        // Get unique hosts - for rclone config, we need to run on the node where rclone will be used
        // For Local mode: txservice nodes
        // For Remote Internal mode: DSS nodes
        let Some(storage_service) = deploy_config.deployment.storage_service.as_ref() else {
            return task_map;
        };
        let Some(dss) = storage_service.eloqdss.as_ref() else {
            return task_map;
        };

        let target_hosts = if dss.is_local_mode() {
            // Local mode: use txservice nodes
            deploy_config.get_unique_host_list()
        } else if dss.is_remote_mode() && !dss.is_external() {
            // Remote Internal mode: use DSS nodes
            if let Some(peer_host_ports) = dss.peer_host_ports.as_ref() {
                peer_host_ports
                    .iter()
                    .filter_map(|hp| hp.split(':').next())
                    .map(|h| h.to_string())
                    .collect::<Vec<_>>()
            } else {
                return task_map;
            }
        } else {
            // Remote External mode: don't manage rclone
            return task_map;
        };

        let conn_user = config.conn_user();
        let ssh_port = config.ssh_port();

        for (remote_name, cmd) in tasks {
            for host in &target_hosts {
                let task_host = TaskHost::Remote {
                    user: conn_user.to_string(),
                    port: ssh_port as usize,
                    host: host.to_string(),
                };

                let task_id = TaskId {
                    cmd: "rclone_config".to_string(),
                    task: format!("{remote_name}@{host}"),
                    host: host.clone(),
                };

                let task_instance = TaskInstance {
                    task_input: HashMap::default(),
                    task: Box::new(RcloneConfigTask {
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
impl TaskExecutor for RcloneConfigTask {
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
            "RcloneConfigTask"
        )
    }
}
