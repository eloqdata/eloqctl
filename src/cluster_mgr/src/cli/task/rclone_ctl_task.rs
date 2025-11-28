#![allow(dead_code)]

use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::group::Config;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::{SubCommand, CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeployConfig;
use crate::config::storage_service_config::{DataStoreServiceBackend, EloqStoreConfig};
use crate::task_return_value;
use async_trait::async_trait;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use tracing::{debug, info};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RcloneCtlCmd {
    Start(String),
    Stop(String),
    ForceStop(String),
    Status(String),
}

impl RcloneCtlCmd {
    pub fn cmd_value(&self) -> String {
        match self.clone() {
            RcloneCtlCmd::Start(cmd) => cmd,
            RcloneCtlCmd::Stop(cmd) => cmd,
            RcloneCtlCmd::ForceStop(cmd) => cmd,
            RcloneCtlCmd::Status(cmd) => cmd,
        }
    }
}

impl RcloneCtlCmd {
    fn build_cmd(config: &DeployConfig, cmd_arg: SubCommand) -> Vec<(String, RcloneCtlCmd)> {
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

        // Get daemon ports
        let ports = eloq_store_config.get_daemon_ports();
        if ports.is_empty() {
            return vec![];
        }

        // Determine target hosts
        let target_hosts = if dss.is_local_mode() {
            // Local mode: use txservice nodes
            config.get_unique_host_list()
        } else if dss.is_remote_mode() && !dss.is_external() {
            // Remote Internal mode: use DSS nodes
            if let Some(peer_host_ports) = dss.peer_host_ports.as_ref() {
                peer_host_ports
                    .iter()
                    .filter_map(|hp| hp.split(':').next())
                    .map(|h| h.to_string())
                    .collect::<Vec<_>>()
            } else {
                return vec![];
            }
        } else {
            // Remote External mode: don't manage rclone
            return vec![];
        };

        let user = &config.connection.username;
        let tx_home = config.deployment.tx_srv_home();
        let logs_dir = format!("{}/logs/rclone", tx_home);

        let mut tasks = vec![];

        for host in target_hosts {
            for port in &ports {
                let ps_cmd = format!(
                    "ps uxwe -u {} | grep 'rclone rcd' | grep -e '--rc-addr=127.0.0.1:{}' | grep -v grep | awk '{{print $2}}'",
                    user, port
                );

                match cmd_arg {
                    SubCommand::Start { .. }
                    | SubCommand::Launch { .. }
                    | SubCommand::Install { .. } => {
                        let start_cmd = format!(
                            "cd {}; mkdir -p {}; rclone rcd --rc-no-auth --rc-addr=127.0.0.1:{} --transfers=16 --checkers=16 --s3-upload-concurrency=8 --s3-chunk-size=8M --fast-list -v > {}/rclone-{}.log 2>&1 &",
                            tx_home, logs_dir, port, logs_dir, port
                        );
                        tasks.push((host.clone(), RcloneCtlCmd::Start(start_cmd)));
                        tasks.push((host.clone(), RcloneCtlCmd::Status(ps_cmd)));
                    }
                    SubCommand::Stop { force, .. } => {
                        if force {
                            tasks.push((
                                host.clone(),
                                RcloneCtlCmd::ForceStop(format!("{} | xargs -r kill -9", ps_cmd)),
                            ));
                        } else {
                            tasks.push((
                                host.clone(),
                                RcloneCtlCmd::Stop(format!("{} | xargs -r kill", ps_cmd)),
                            ));
                        }
                        tasks.push((host.clone(), RcloneCtlCmd::Status(ps_cmd)));
                    }
                    SubCommand::Status { .. } => {
                        tasks.push((host.clone(), RcloneCtlCmd::Status(ps_cmd)));
                    }
                    SubCommand::Remove { .. } => {
                        tasks.push((
                            host.clone(),
                            RcloneCtlCmd::ForceStop(format!("{} | xargs -r kill -9", ps_cmd)),
                        ));
                        tasks.push((host.clone(), RcloneCtlCmd::Status(ps_cmd)));
                    }
                    _ => {}
                }
            }
        }

        tasks
    }
}

#[derive(Clone, Debug)]
pub struct RcloneCtlTask {
    config: DeployConfig,
    task_id: TaskId,
    ctl_cmd_by_host: Vec<(String, RcloneCtlCmd)>,
}

impl RcloneCtlTask {
    pub fn new(
        config: DeployConfig,
        task_id: TaskId,
        ctl_cmd_by_host: Vec<(String, RcloneCtlCmd)>,
    ) -> Self {
        Self {
            config,
            task_id,
            ctl_cmd_by_host,
        }
    }

    pub fn from_config(cmd_arg: SubCommand, config: &Config) -> IndexMap<TaskId, TaskInstance> {
        let deploy_config = match config {
            Config::Cluster(cfg) => cfg,
            Config::Proxy(_) => return IndexMap::new(),
        };

        let ctl_cmd_by_host = RcloneCtlCmd::build_cmd(deploy_config, cmd_arg.clone());
        if ctl_cmd_by_host.is_empty() {
            return IndexMap::new();
        }

        let user = &deploy_config.connection.username;
        let ssh_port = deploy_config.connection.ssh_port() as usize;

        // Group by host to create one task per host
        let hosts: Vec<String> = ctl_cmd_by_host
            .iter()
            .map(|(h, _)| h.clone())
            .unique()
            .collect();

        hosts
            .iter()
            .map(|host| {
                let task_host = TaskHost::Remote {
                    user: user.clone(),
                    port: ssh_port,
                    host: host.clone(),
                };
                let task_id = TaskId {
                    cmd: format!("rclone_{}", cmd_arg.as_ref()),
                    task: cmd_arg.as_ref().to_string(),
                    host: host.clone(),
                };
                (
                    task_id.clone(),
                    TaskInstance {
                        task_input: HashMap::from([(
                            CMD.to_string(),
                            TaskArgValue::Str(cmd_arg.as_ref().to_string()),
                        )]),
                        task: Box::new(RcloneCtlTask::new(
                            deploy_config.clone(),
                            task_id,
                            ctl_cmd_by_host.clone(),
                        )),
                        task_host,
                    },
                )
            })
            .collect()
    }
}

#[async_trait]
impl TaskExecutor for RcloneCtlTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        info!("execute {}", self.task_id.format_string());
        let ssh_session =
            SSHSession::from_task_host(task_host, self.config.connection.ssh_auth_key().unwrap())
                .await?;

        // Filter commands for this host
        let host = self.task_id.host.clone();
        let cmds_for_host = self
            .ctl_cmd_by_host
            .iter()
            .filter(|(h, _)| h == &host)
            .map(|(_, c)| c.clone())
            .collect_vec();

        if cmds_for_host.is_empty() {
            ssh_session.close().await?;
            return Ok(Some(HashMap::from([
                (CMD_STATUS.to_string(), TaskArgValue::Number(0)),
                (
                    CMD_OUTPUT.to_string(),
                    TaskArgValue::Str("no rclone command for host".to_string()),
                ),
            ])));
        }

        // Build control entries
        #[derive(Clone)]
        struct CtlEntry {
            status_cmd: String,
            exec_cmd: Option<String>,
            kind: &'static str, // "start" | "stop" | "forcestop" | "status"
        }

        fn status_from_stop(stop_cmd: &str) -> String {
            if let Some(idx) = stop_cmd.find(" | xargs") {
                stop_cmd[..idx].to_string()
            } else {
                stop_cmd.to_string()
            }
        }

        fn status_from_start(start_cmd: &str, user: &str) -> Option<String> {
            // Extract port from "--rc-addr=127.0.0.1:PORT" in start command
            if let Some(pos) = start_cmd.find("--rc-addr=127.0.0.1:") {
                let port_start = pos + "--rc-addr=127.0.0.1:".len();
                let tail = &start_cmd[port_start..];
                let end = tail
                    .find(|c: char| c.is_whitespace() || c == '-' || c == '>')
                    .unwrap_or(tail.len());
                let port = &tail[..end];
                let ps_cmd = format!(
                    "ps uxwe -u {} | grep 'rclone rcd' | grep -e '--rc-addr=127.0.0.1:{}' | grep -v grep | awk '{{print $2}}'",
                    user, port
                );
                Some(ps_cmd)
            } else {
                None
            }
        }

        let user = &self.config.connection.username;
        let mut ctl_entries: Vec<CtlEntry> = Vec::new();
        for c in cmds_for_host.iter() {
            match c {
                RcloneCtlCmd::Status(s) => ctl_entries.push(CtlEntry {
                    status_cmd: s.clone(),
                    exec_cmd: None,
                    kind: "status",
                }),
                RcloneCtlCmd::Stop(s) => ctl_entries.push(CtlEntry {
                    status_cmd: status_from_stop(s),
                    exec_cmd: Some(s.clone()),
                    kind: "stop",
                }),
                RcloneCtlCmd::ForceStop(s) => ctl_entries.push(CtlEntry {
                    status_cmd: status_from_stop(s),
                    exec_cmd: Some(s.clone()),
                    kind: "forcestop",
                }),
                RcloneCtlCmd::Start(s) => {
                    if let Some(ps) = status_from_start(s, user) {
                        ctl_entries.push(CtlEntry {
                            status_cmd: ps,
                            exec_cmd: Some(s.clone()),
                            kind: "start",
                        })
                    }
                }
            }
        }

        // Query pids for all entries
        let mut pid_values: Vec<(CtlEntry, ExecutionValue)> = Vec::new();
        for entry in ctl_entries.into_iter() {
            let rs = crate::cli::task::task_utils::check_pid(
                entry.status_cmd.clone(),
                ssh_session.clone(),
                crate::cli::task::task_utils::parse_process_pid,
            )
            .await?;
            pid_values.push((entry, rs));
        }

        // Aggregate for status output
        let any_pid_running = pid_values.iter().any(|(_, rs)| {
            rs.get(crate::cli::task::task_utils::PROCESS_PID)
                .map(|v| {
                    let p = TaskArgValue::into_inner_value::<String>(v.clone());
                    !p.is_empty() && p != crate::cli::task::task_utils::PID_NOT_FOUND
                })
                .unwrap_or(false)
        });

        let mut pid_exec_value: Option<ExecutionValue> = None;
        if let Some((_, rs)) = pid_values.first() {
            pid_exec_value = Some(rs.clone());
        }

        // If this task is a pure status check, return rclone status and pid info
        if self.task_id.task == "status" {
            let mut result = pid_exec_value.unwrap_or_default();
            let output = if any_pid_running {
                // Extract PID from result if available
                if let Some(pid_value) = result.get(crate::cli::task::task_utils::PROCESS_PID) {
                    let pid = TaskArgValue::into_inner_value::<String>(pid_value.clone());
                    if pid != crate::cli::task::task_utils::PID_NOT_FOUND && !pid.is_empty() {
                        format!("\nrclone is running, pid: {}.", pid)
                    } else {
                        "\nrclone is down.".to_string()
                    }
                } else {
                    "\nrclone is running.".to_string()
                }
            } else {
                "\nrclone is down.".to_string()
            };
            result.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(output));
            if !result.contains_key(CMD_STATUS) {
                result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
            }
            ssh_session.close().await?;
            return Ok(Some(result));
        }

        // Decide actions based on computed PIDs
        let current_task_kind = self.task_id.task.clone();
        let mut commands_to_run: Vec<(String, bool)> = Vec::new(); // (cmd, is_start)
        match current_task_kind.as_str() {
            "stop" | "remove" => {
                for (entry, rs) in pid_values.iter() {
                    if (entry.kind == "stop" || entry.kind == "forcestop")
                        && entry.exec_cmd.is_some()
                    {
                        let pid = rs
                            .get(crate::cli::task::task_utils::PROCESS_PID)
                            .map(|v| TaskArgValue::into_inner_value::<String>(v.clone()))
                            .unwrap_or_default();
                        if !pid.is_empty() && pid != crate::cli::task::task_utils::PID_NOT_FOUND {
                            commands_to_run.push((entry.exec_cmd.clone().unwrap(), false));
                        }
                    }
                }
            }
            "start" | "launch" | "install" => {
                for (entry, rs) in pid_values.iter() {
                    if entry.kind == "start" && entry.exec_cmd.is_some() {
                        let pid = rs
                            .get(crate::cli::task::task_utils::PROCESS_PID)
                            .map(|v| TaskArgValue::into_inner_value::<String>(v.clone()))
                            .unwrap_or_default();
                        if pid.is_empty() || pid == crate::cli::task::task_utils::PID_NOT_FOUND {
                            commands_to_run.push((entry.exec_cmd.clone().unwrap(), true));
                        }
                    }
                }
            }
            _ => {}
        }

        let mut result = HashMap::new();
        for (cmd_to_run, is_start) in commands_to_run.into_iter() {
            debug!("RcloneCtlTask cmd={}", cmd_to_run);
            let exec_rs = ssh_session
                .command(cmd_to_run.as_str(), CollectOutput)
                .await?;
            result.extend(exec_rs);

            // If we just started rclone, wait for readiness (pid exists) up to 30s
            if is_start {
                let status_cmd_opt = cmds_for_host
                    .iter()
                    .filter_map(|c| match c {
                        RcloneCtlCmd::Status(s) => Some(s.clone()),
                        RcloneCtlCmd::Stop(s) | RcloneCtlCmd::ForceStop(s) => {
                            Some(status_from_stop(s))
                        }
                        RcloneCtlCmd::Start(s) => status_from_start(s, user),
                    })
                    .next();

                if let Some(status_cmd) = status_cmd_opt {
                    let mut ready = false;
                    for _ in 0..30 {
                        let rs = crate::cli::task::task_utils::check_pid(
                            status_cmd.clone(),
                            ssh_session.clone(),
                            crate::cli::task::task_utils::parse_process_pid,
                        )
                        .await?;
                        if let Some(v) = rs.get(crate::cli::task::task_utils::PROCESS_PID) {
                            let p = TaskArgValue::into_inner_value::<String>(v.clone());
                            if !p.is_empty() && p != crate::cli::task::task_utils::PID_NOT_FOUND {
                                ready = true;
                                break;
                            }
                        }
                        sleep(Duration::from_secs(1)).await;
                    }
                    if !ready {
                        // Mark non-fatal readiness wait timeout to surface in logs
                        result.insert(
                            CMD_OUTPUT.to_string(),
                            TaskArgValue::Str(
                                "rclone readiness wait timeout (continuing)".to_string(),
                            ),
                        );
                    }
                }
            }
        }
        if !result.contains_key(CMD_STATUS) {
            result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
        }
        ssh_session.close().await?;
        task_return_value!(
            result,
            |status_code: i32| -> CmdErr {
                CmdErr::ExecUserCmdErr("rclone_ctl".to_string(), status_code.to_string())
            },
            "RcloneCtlTask"
        )
    }
}
