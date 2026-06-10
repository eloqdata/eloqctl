#![allow(dead_code)]

use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::task::group::Config;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::{ssh, SubCommand, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeployConfig;
use crate::config::storage_service_config::{DataStoreServiceBackend, EloqStoreConfig};
use crate::config::DeploymentPackage;
use crate::task_return_value;
use async_trait::async_trait;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;
use tracing::{debug, info};

#[derive(Clone, Debug)]
pub struct EloqStoreDataCleanTask {
    clean_cmd: String,
    task_id: TaskId,
    config: Config,
    // For Remote mode, need to store DSS process check info
    dss_check_info: Option<DssCheckInfo>,
}

#[derive(Clone, Debug)]
struct DssCheckInfo {
    ps_cmd: String, // The ps command to check if DSS server is running
    host: String,   // The DSS server host
}

/// Compute default data paths based on DataStoreService mode
fn compute_default_data_paths(
    dss: &crate::config::storage_service_config::DataStoreService,
    deploy_config: &DeployConfig,
) -> Vec<String> {
    // Backend is already verified to be EloqStore in build_tasks
    let tx_home = deploy_config.deployment.tx_srv_home();

    if dss.is_local_mode() {
        // Local mode: compute default path for each EloqKV node
        deploy_config
            .get_host_port_list(DeploymentPackage::EloqTx)
            .iter()
            .filter_map(|hp| {
                hp.split_once(':').map(|(_, port)| {
                    let eloq_data_path = format!("{}/data/port-{}", tx_home, port);
                    EloqStoreConfig::compute_default_eloq_store_data_path(&eloq_data_path)
                })
            })
            .unique()
            .collect()
    } else if dss.is_remote_mode() && !dss.is_external() {
        // Remote Internal mode: compute default path for each DSS node
        // Use DSS port as EloqKV port to compute default path
        if let Some(peer_host_ports) = dss.peer_host_ports.as_ref() {
            peer_host_ports
                .iter()
                .filter_map(|hp| {
                    hp.split_once(':').map(|(_, port)| {
                        let eloq_data_path = format!("{}/data/port-{}", tx_home, port);
                        EloqStoreConfig::compute_default_eloq_store_data_path(&eloq_data_path)
                    })
                })
                .unique()
                .collect()
        } else {
            Vec::new()
        }
    } else {
        // Remote External mode: don't compute default paths
        Vec::new()
    }
}

impl EloqStoreDataCleanTask {
    /// Build tasks for cleaning EloqStore data directories
    /// Only generates tasks if EloqStore Cloud mode is enabled
    ///
    /// # Arguments
    /// * `cmd_arg` - The command argument
    /// * `config` - The cluster configuration
    /// * `target_hosts_filter` - Optional filter to only include specific hosts.
    ///   If None, includes all hosts based on DataStoreService mode.
    pub fn build_tasks(
        cmd_arg: SubCommand,
        config: &Config,
        target_hosts_filter: Option<&[String]>,
    ) -> IndexMap<TaskId, TaskInstance> {
        let mut task_map = IndexMap::new();

        let deploy_config = match config {
            Config::Cluster(cfg) => cfg,
        };

        // Check if EloqStore Cloud mode is enabled
        let Some(storage_service) = deploy_config.deployment.storage_service.as_ref() else {
            return task_map;
        };
        let Some(dss) = storage_service.eloqdss.as_ref() else {
            return task_map;
        };

        // Only process if backend is EloqStore
        let DataStoreServiceBackend::EloqStore(eloq_store_config) = dss.backend_config();

        // Only process if cloud mode is enabled
        if !eloq_store_config.is_cloud_mode() {
            return task_map;
        }

        // Parse data path list (comma-separated)
        // If None or empty, compute default values based on mode
        let data_paths = if let Some(path_list) = &eloq_store_config.eloq_store_data_path_list {
            let paths: Vec<String> = path_list
                .split(',')
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect();
            if paths.is_empty() {
                // Path list is empty string, compute default values
                compute_default_data_paths(dss, deploy_config)
            } else {
                paths
            }
        } else {
            // Path list is None, compute default values
            compute_default_data_paths(dss, deploy_config)
        };

        if data_paths.is_empty() {
            return task_map;
        }

        // Determine target hosts and DSS check info based on DataStoreService mode
        let (all_target_hosts, dss_check_info_map) = if dss.is_local_mode() {
            // Local mode: use EloqKV nodes (txservice nodes)
            // No DSS process check needed for Local mode
            let hosts = deploy_config
                .get_host_port_list(DeploymentPackage::EloqTx)
                .iter()
                .filter_map(|hp| hp.split(':').next())
                .map(|h| h.to_string())
                .unique()
                .collect::<Vec<_>>();
            (hosts, HashMap::new())
        } else if dss.is_remote_mode() && !dss.is_external() {
            // Remote Internal mode: use DSS nodes
            // Need to collect DSS process check info for each node
            if let Some(peer_host_ports) = dss.peer_host_ports.as_ref() {
                let mut hosts = Vec::new();
                let mut dss_check_map = HashMap::new();

                let tx_home = deploy_config.deployment.tx_srv_home();
                let user = deploy_config.connection.username.as_str();

                for peer_host_port in peer_host_ports {
                    let (host, port) = peer_host_port
                        .split_once(':')
                        .expect("invalid dss host:port");
                    let host = host.to_string();

                    // Build DSS process check command
                    let ini_file = deploy_config.deployment.dss_srv_ini(port);
                    let ps_cmd = format!(
                        "ps uxwe -u {} | grep '{}/bin/dss_server' | grep ' --config={}' | grep -v grep | awk '{{print $2}}'",
                        user, tx_home, ini_file
                    );

                    dss_check_map.insert(
                        host.clone(),
                        DssCheckInfo {
                            ps_cmd,
                            host: host.clone(),
                        },
                    );
                    hosts.push(host);
                }

                (hosts.into_iter().unique().collect(), dss_check_map)
            } else {
                return task_map;
            }
        } else {
            // Remote External mode: don't manage data cleanup
            return task_map;
        };

        // Apply filter if provided
        let target_hosts: Vec<String> = if let Some(filter) = target_hosts_filter {
            all_target_hosts
                .into_iter()
                .filter(|host| filter.contains(host))
                .collect()
        } else {
            all_target_hosts
        };

        if target_hosts.is_empty() {
            return task_map;
        }

        // Build clean command for all paths
        // Use `find {path} -mindepth 1 -delete` to clear directory contents without removing the directory itself
        let clean_commands: Vec<String> = data_paths
            .iter()
            .map(|path| format!("find {path} -mindepth 1 -delete 2>/dev/null || true"))
            .collect();
        let clean_cmd = clean_commands.join(" && ");

        // Create one task per host
        for host in target_hosts {
            let task_host = TaskHost::remote(&deploy_config.connection, &host);

            let task_id = TaskId {
                cmd: cmd_arg.as_ref().to_string(),
                task: format!("eloq_store_data_clean@{host}"),
                host: host.clone(),
            };

            // Get DSS check info for this host (only for Remote mode)
            let dss_check_info = dss_check_info_map.get(&host).cloned();

            let task_instance = TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(EloqStoreDataCleanTask {
                    clean_cmd: clean_cmd.clone(),
                    task_id: task_id.clone(),
                    config: config.clone(),
                    dss_check_info,
                }),
                task_host,
            };

            task_map.insert(task_id, task_instance);
        }

        task_map
    }
}

#[async_trait]
impl TaskExecutor for EloqStoreDataCleanTask {
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

        // For Remote mode, check if DSS server is running first
        if let Some(ref dss_check) = self.dss_check_info {
            use crate::cli::task::task_utils::{
                check_pid, parse_process_pid, PID_NOT_FOUND, PROCESS_PID,
            };

            // Check if DSS server is running
            let pid_result = check_pid(
                dss_check.ps_cmd.clone(),
                ssh_session.clone(),
                parse_process_pid,
            )
            .await?;

            if let Some(pid_value) = pid_result.get(PROCESS_PID) {
                let pid = TaskArgValue::into_inner_value::<String>(pid_value.clone());
                if !pid.is_empty() && pid != PID_NOT_FOUND {
                    // DSS server is running, skip data directory cleanup
                    info!(
                        "DSS server is running on {} (PID: {}), skipping data directory cleanup",
                        host, pid
                    );
                    ssh_session.close().await?;
                    let mut result = HashMap::new();
                    result.insert(
                        CMD_OUTPUT.to_string(),
                        TaskArgValue::Str(format!(
                            "DSS server is running (PID: {}), skipped data directory cleanup",
                            pid
                        )),
                    );
                    result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
                    return Ok(Some(result));
                }
            }
            // DSS server is not running, proceed with data directory cleanup
            info!(
                "DSS server is not running on {}, proceeding with data directory cleanup",
                host
            );
        }

        // Execute data directory cleanup
        let exec_cmd_rs = ssh_session
            .command(self.clean_cmd.as_str(), CollectOutput)
            .await?;

        if let Some(output) = exec_cmd_rs.get(CMD_OUTPUT) {
            debug!(
                "Host {host} Cmd {} output {}",
                self.clean_cmd,
                TaskArgValue::into_inner_value::<String>(output.clone())
            );
        }

        ssh_session.close().await?;
        task_return_value!(
            exec_cmd_rs,
            |status_code: i32| -> CmdErr {
                CmdErr::ExecUserCmdErr(
                    format!("eloq_store_data_clean: {}", self.clean_cmd),
                    status_code.to_string(),
                )
            },
            "EloqStoreDataCleanTask"
        )
    }
}
