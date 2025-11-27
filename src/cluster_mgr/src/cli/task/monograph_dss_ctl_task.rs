use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::{SubCommand, CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeployConfig;
use crate::{get_ctl_cmd_string, task_return_value};
use anyhow::anyhow;
use async_trait::async_trait;
use indexmap::IndexMap;
use itertools::Itertools;
use regex::Regex;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use tracing::{debug, info};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DssCtlCmd {
    Start(String),
    Stop(String),
    ForceStop(String),
    Status(String),
}

get_ctl_cmd_string!(DssCtlCmd, Start, Stop, ForceStop, Status);

fn parse_host_port(hp: &str) -> anyhow::Result<(String, String)> {
    let re = Regex::new(r"^([^:]+):(\d+)$").unwrap();
    if let Some(caps) = re.captures(hp) {
        Ok((
            caps.get(1).unwrap().as_str().to_string(),
            caps.get(2).unwrap().as_str().to_string(),
        ))
    } else {
        Err(anyhow!(format!(
            "invalid host:port '{}', expected host:port",
            hp
        )))
    }
}

impl DssCtlCmd {
    fn build_cmd(config: &DeployConfig, cmd_arg: SubCommand) -> Vec<(String, DssCtlCmd)> {
        let Some(storage_service) = &config.deployment.storage_service else {
            return vec![];
        };

        // Get peer_host_ports from either EloqDssRocksdb or DataStoreService Remote mode
        let peer_host_ports =
            if let Some(crate::config::storage_service_config::RocksDB::EloqDssRocksdb(eloq_dss)) =
                &storage_service.rocksdb
            {
                eloq_dss.peer_host_ports.clone()
            } else if let Some(dss) = &storage_service.eloqdss {
                if dss.is_remote_mode() {
                    dss.peer_host_ports.clone().unwrap_or_default()
                } else {
                    return vec![];
                }
            } else {
                return vec![];
            };

        if peer_host_ports.is_empty() {
            return vec![];
        }

        let tx_home = config.deployment.tx_srv_home();
        let dss_bin = format!("{}/bin/dss_server", tx_home);

        peer_host_ports
            .iter()
            .map(|peer| {
                let (host, port) = parse_host_port(peer).expect("invalid dss host:port");
                let logs_dir = format!("{}/logs/dss", tx_home);
                let ini_file = config.deployment.dss_srv_ini(&port);

                let ps_cmd = format!(
                    "ps uxwe -u {} | grep '{}/bin/dss_server' | grep ' --config={}' | grep -v grep | awk '{{print $2}}'",
                    &config.connection.username, tx_home, ini_file
                );
                let ctl = match &cmd_arg {
                    SubCommand::Start { .. } | SubCommand::Launch { .. } => {
                        // Ensure dirs exist, then start with the uploaded config
                        let start_cmd = format!(
                            "cd {tx_home}; mkdir -p {logs_dir}; {dss_bin} --config={ini_file} \
> {logs_dir}/std-{port}.log 2>&1 &"
                        );
                        DssCtlCmd::Start(start_cmd)
                    }
                    SubCommand::Status { .. } => DssCtlCmd::Status(ps_cmd),
                    SubCommand::Stop { force, .. } => {
                        if *force {
                            DssCtlCmd::ForceStop(format!("{ps_cmd} | xargs -r kill -9"))
                        } else {
                            DssCtlCmd::Stop(format!("{ps_cmd} | xargs -r kill"))
                        }
                    }
                    SubCommand::Remove { .. } => {
                        DssCtlCmd::ForceStop(format!("{ps_cmd} | xargs -r kill -9"))
                    }
                    _ => unreachable!(),
                };
                (host, ctl)
            })
            .collect_vec()
    }
}

#[derive(Clone, Debug)]
pub struct MonographDssCtlTask {
    config: DeployConfig,
    task_id: TaskId,
    ctl_cmd_by_host: Vec<(String, DssCtlCmd)>,
}

impl MonographDssCtlTask {
    pub fn new(
        config: DeployConfig,
        task_id: TaskId,
        ctl_cmd_by_host: Vec<(String, DssCtlCmd)>,
    ) -> Self {
        Self {
            config,
            task_id,
            ctl_cmd_by_host,
        }
    }

    pub fn from_config(
        cmd_arg: SubCommand,
        config: &DeployConfig,
    ) -> IndexMap<TaskId, TaskInstance> {
        let ctl_cmd_by_host = DssCtlCmd::build_cmd(config, cmd_arg.clone());
        if ctl_cmd_by_host.is_empty() {
            return IndexMap::new();
        }
        let user = &config.connection.username;
        let ssh_port = config.connection.ssh_port() as usize;

        ctl_cmd_by_host
            .iter()
            .map(|(host, _)| {
                let task_host = TaskHost::Remote {
                    user: user.clone(),
                    port: ssh_port,
                    host: host.clone(),
                };
                let task_id = TaskId {
                    cmd: format!("dss_{}", cmd_arg.as_ref()),
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
                        task: Box::new(MonographDssCtlTask::new(
                            config.clone(),
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
impl TaskExecutor for MonographDssCtlTask {
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
                    TaskArgValue::Str("no dss command for host".to_string()),
                ),
            ])));
        }

        // Always compute PIDs first by deriving status commands on-the-fly
        // for each entry belonging to this host.
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

        fn status_from_start(start_cmd: &str, user: &str, tx_home: &str) -> Option<String> {
            // Extract ini path from "--config=..." in start command
            if let Some(pos) = start_cmd.find("--config=") {
                let ini_start = pos + "--config=".len();
                let tail = &start_cmd[ini_start..];
                let end = tail
                    .find(|c: char| c.is_whitespace() || c == '>' || c == '&')
                    .unwrap_or(tail.len());
                let ini_file = &tail[..end];
                let ps_cmd = format!(
                    "ps uxwe -u {} | grep '{}/bin/dss_server' | grep ' --config={}' | grep -v grep | awk '{{print $2}}'",
                    user, tx_home, ini_file
                );
                Some(ps_cmd)
            } else {
                None
            }
        }

        let user = &self.config.connection.username;
        let tx_home = self.config.deployment.tx_srv_home();
        let mut ctl_entries: Vec<CtlEntry> = Vec::new();
        for c in cmds_for_host.iter() {
            match c {
                DssCtlCmd::Status(s) => ctl_entries.push(CtlEntry {
                    status_cmd: s.clone(),
                    exec_cmd: None,
                    kind: "status",
                }),
                DssCtlCmd::Stop(s) => ctl_entries.push(CtlEntry {
                    status_cmd: status_from_stop(s),
                    exec_cmd: Some(s.clone()),
                    kind: "stop",
                }),
                DssCtlCmd::ForceStop(s) => ctl_entries.push(CtlEntry {
                    status_cmd: status_from_stop(s),
                    exec_cmd: Some(s.clone()),
                    kind: "forcestop",
                }),
                DssCtlCmd::Start(s) => {
                    if let Some(ps) = status_from_start(s, user, tx_home.as_str()) {
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

        // If this task is a pure status check, return DSS status and pid info
        if self.task_id.task == "status" {
            let mut result = pid_exec_value.unwrap_or_default();
            let output = if any_pid_running {
                "\ndss_server is running.".to_string()
            } else {
                "\ndss_server is down.".to_string()
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
            "start" | "launch" => {
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
            debug!("MonographDssCtlTask cmd={}", cmd_to_run);
            let exec_rs = ssh_session
                .command(cmd_to_run.as_str(), CollectOutput)
                .await?;
            result.extend(exec_rs);

            // If we just started DSS, wait for readiness (pid exists) up to 30s
            if is_start {
                let status_cmd_opt = cmds_for_host
                    .iter()
                    .filter_map(|c| match c {
                        DssCtlCmd::Status(s) => Some(s.clone()),
                        DssCtlCmd::Stop(s) | DssCtlCmd::ForceStop(s) => Some(status_from_stop(s)),
                        DssCtlCmd::Start(s) => status_from_start(s, user, tx_home.as_str()),
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
                                "dss_server readiness wait timeout (continuing)".to_string(),
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
                CmdErr::ExecUserCmdErr("dss_ctl".to_string(), status_code.to_string())
            },
            "MonographDssCtlTask"
        )
    }
}
