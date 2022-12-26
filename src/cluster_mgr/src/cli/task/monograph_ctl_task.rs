use crate::cli::config::{DeploymentConfig, DeploymentService};
use crate::cli::task::ssh_conn::SSHConn;
use crate::cli::task::task_base::CmdErr::MonographCtlErr;
use crate::cli::task::task_base::{
    ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::task::task_utils::{check_process_pid, start_service, stop_service};
use crate::cli::CommandArgs;
use crate::{get_ctl_cmd_string, ssh_conn_info};
use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use std::collections::HashMap;
use strum_macros::AsRefStr;
use tracing::{error, info};

const START_MONOGRAPH: &str = "start";
const STOP_MONOGRAPH: &str = "stop";
const RESTART_MONOGRAPH: &str = "restart";
const MONOGRAPH_STATUS: &str = "status";

#[derive(Clone, Debug, Eq, PartialEq, AsRefStr)]
pub enum MonographCtlCmd {
    Start(String),
    Stop(String),
    Status(String),
}

get_ctl_cmd_string!(MonographCtlCmd, Start, Stop, Status);

macro_rules! monograph_cmd {
    ($ctl_cmd:ty,$remote_install_home:expr $(, $cmd_arg:expr)? $(,)?) => {{
        let ctl_cmd = stringify!($ctl_cmd);
        match ctl_cmd {
           $(
            "MonographCtlCmd::Start" => MonographCtlCmd::Start(format!(r"mkdir -p {}/monographdb-release/logs && cd {}/monographdb-release/install &&  export LD_LIBRARY_PATH={}/monographdb-release/install/lib:$LD_LIBRARY_PATH; {}/monographdb-release/install/bin/mysqld --defaults-file={}/my_{}.cnf > {}/monographdb-release/logs/mysqld_start.log 2>&1 &",
               $remote_install_home, $remote_install_home, $remote_install_home,
               $remote_install_home,$remote_install_home,$cmd_arg, $remote_install_home)),
            "MonographCtlCmd::Stop" => {
               MonographCtlCmd::Stop(format!("kill {}", $cmd_arg))
            },
            )*
            "MonographCtlCmd::Status" => {
               let mysqld_pid = format!(r#"echo `ps uxwe | grep {}/monographdb-release/install/bin/mysqld | grep -v grep | "#, $remote_install_home);
               let output_pid = r#"awk '{print $2}'`"#;
               let ps_cmd = format!(r#"{} {} "#, mysqld_pid, output_pid);
               MonographCtlCmd::Status(ps_cmd)
            },
            _=> {
                unreachable!()
            }
        }
    }};
}

#[derive(Debug, Clone)]
pub struct MonographCtlTask {
    config: DeploymentConfig,
    task_id: TaskId,
}

impl MonographCtlTask {
    pub fn from_config(cmd: CommandArgs, config: &DeploymentConfig) -> Vec<TaskInstance> {
        let task_id = match cmd {
            CommandArgs::Start { cluster: _ } => TaskId {
                cmd: START_MONOGRAPH.to_string(),
                task: "monographdb-start".to_string(),
                host: "".to_string(),
            },
            CommandArgs::Stop { cluster: _ } => TaskId {
                cmd: STOP_MONOGRAPH.to_string(),
                task: "monographdb-stop".to_string(),
                host: "".to_string(),
            },
            CommandArgs::Status { cluster: _ } => TaskId {
                cmd: MONOGRAPH_STATUS.to_string(),
                task: "monographdb-status".to_string(),
                host: "".to_string(),
            },
            CommandArgs::Restart { cluster: _ } => TaskId {
                cmd: RESTART_MONOGRAPH.to_string(),
                task: "monographdb-restart".to_string(),
                host: "".to_string(),
            },
            _ => {
                unreachable!()
            }
        };
        let conn_user = &config.connection.username;
        let ssh_port = config.connection.ssh_port();
        let hosts = config.get_host_list(DeploymentService::Monograph);
        hosts
            .iter()
            .map(|host| {
                let remote_host = TaskHost::Remote {
                    user: conn_user.clone(),
                    port: ssh_port as usize,
                    hosts: host.clone(),
                };

                let mut special_task_id = task_id.clone();
                special_task_id.host = host.clone();
                TaskInstance {
                    task_input: HashMap::default(),
                    task: Box::new(MonographCtlTask::new(config.clone(), special_task_id)),
                    task_host: remote_host,
                }
            })
            .collect_vec()
    }

    pub fn new(config: DeploymentConfig, task_id: TaskId) -> Self {
        Self { config, task_id }
    }

    fn parse_monograph_pid(output: String) -> Option<i32> {
        if output.is_empty() {
            None
        } else {
            let mut pid = None;
            let output_normal = output.trim();
            info!("MonographCtlTask parse output_normal={}", output_normal);
            for line in output_normal.lines() {
                let line_normal = line.trim();
                if line_normal.is_empty() {
                    continue;
                }
                info!("MonographCtlTask parse_check_status_output line = {}", line);
                if !line_normal.chars().all(char::is_numeric) {
                    continue;
                }
                let parse_rs = line_normal.parse::<i32>().unwrap();
                pid = Some(parse_rs);
                break;
            }
            info!("MonographCtlTask found MonographDB PID={:?}", pid);
            pid
        }
    }

    fn monograph_pid(&self, ssh_conn: &SSHConn) -> anyhow::Result<Option<i32>> {
        let remote_install_dir = self.config.install_dir();
        let check_status = monograph_cmd!(MonographCtlCmd::Status, remote_install_dir);
        check_process_pid(
            check_status.cmd_value(),
            ssh_conn,
            MonographCtlTask::parse_monograph_pid,
        )
    }
}

#[async_trait]
impl TaskExecutor for MonographCtlTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        let cmd_str = self.task_id.cmd.as_str();
        let remote_install_dir = self.config.install_dir();
        ssh_conn_info! {
            self.config.connection.clone(),
            task_host,
            ssh_conn_rs,
            _conn_user,
            conn_host
        }
        let ssh_conn = &ssh_conn_rs?;
        let check_process_status = self.monograph_pid(ssh_conn);
        let check_status = monograph_cmd!(MonographCtlCmd::Status, remote_install_dir);
        let start_cmd = monograph_cmd!(MonographCtlCmd::Start, remote_install_dir, conn_host);
        info!(
            "MonographCtlTask receive cmd={}, remote_host={:?}",
            cmd_str, conn_host
        );
        let execute_rs = match cmd_str {
            "start" => {
                if let Ok(pid_opt) = check_process_status {
                    if pid_opt.is_none() {
                        start_service(
                            start_cmd.cmd_value(),
                            check_status.cmd_value(),
                            ssh_conn,
                            |output| -> bool {
                                MonographCtlTask::parse_monograph_pid(output).is_some()
                            },
                        )
                    } else {
                        info!(
                            "MonographCtlTask found MonographDB process already running. PID={:?}",
                            pid_opt
                        );
                        Ok(true)
                    }
                } else {
                    error!(
                        "MonographCtlTask current cmd is Start.Check process status failed. check_status_cmd={:?}",
                        check_status
                    );
                    Err(anyhow!(MonographCtlErr(
                        check_status.cmd_value(),
                        check_process_status.err().unwrap().to_string()
                    )))
                }
            }
            "stop" => {
                if let Ok(pid_opt) = check_process_status {
                    if let Some(pid) = pid_opt {
                        let stop_cmd =
                            monograph_cmd!(MonographCtlCmd::Stop, remote_install_dir, pid);
                        stop_service(stop_cmd.cmd_value(), ssh_conn)
                    } else {
                        info!("MonographCtlTask no running database processes found.");
                        Ok(true)
                    }
                } else {
                    error!(
                        "MonographCtlTask current cmd is Stop.Check process status failed. check_status_cmd={:?}",
                        check_status
                    );
                    Err(anyhow!(MonographCtlErr(
                        check_status.cmd_value(),
                        check_process_status.err().unwrap().to_string()
                    )))
                }
            }
            "restart" => {
                if let Ok(pid_opt) = check_process_status {
                    if let Some(pid) = pid_opt {
                        let stop_cmd =
                            monograph_cmd!(MonographCtlCmd::Stop, remote_install_dir, pid);
                        stop_service(stop_cmd.cmd_value(), ssh_conn)?;

                        start_service(
                            start_cmd.cmd_value(),
                            check_status.cmd_value(),
                            ssh_conn,
                            |output| -> bool {
                                MonographCtlTask::parse_monograph_pid(output).is_some()
                            },
                        )
                    } else {
                        start_service(
                            start_cmd.cmd_value(),
                            check_status.cmd_value(),
                            ssh_conn,
                            |output| -> bool {
                                MonographCtlTask::parse_monograph_pid(output).is_some()
                            },
                        )
                    }
                } else {
                    error!(
                        "MonographCtlTask current cmd is Restart. check process status failed. check_status_cmd={:?}",
                        check_status
                    );
                    Err(anyhow!(MonographCtlErr(
                        check_status.cmd_value(),
                        check_process_status.err().unwrap().to_string()
                    )))
                }
            }
            "status" => {
                if let Ok(pid_opt) = check_process_status {
                    println!(
                        "MonographCtlTask found MonographDB process={:?} in {:?}",
                        pid_opt, conn_host
                    );
                    Ok(true)
                } else {
                    error!(
                        "MonographCtlTask current cmd is Status. check process status failed. check_status_cmd={:?}",
                        check_status
                    );
                    Err(anyhow!(MonographCtlErr(
                        check_status.cmd_value(),
                        check_process_status.err().unwrap().to_string()
                    )))
                }
            }
            _ => unreachable!(),
        };

        if let Ok(status) = execute_rs {
            println!(
                "MonographCtlTask execute cmd={} success status={}",
                cmd_str, status
            );
            Ok(None)
        } else {
            let err_msg = execute_rs.err().unwrap().to_string();
            error!(
                "MonographCtlTask execute cmd {} failed. {}",
                cmd_str, err_msg
            );
            Err(anyhow!(MonographCtlErr(cmd_str.to_string(), err_msg)))
        }
    }
}

#[cfg(test)]
mod test {

    use crate::cli::task::monograph_ctl_task::MonographCtlCmd;

    #[test]
    pub fn test_monograph_cmd_macro() {
        let start_cmd = monograph_cmd!(
            MonographCtlCmd::Start,
            "/data/opt/mono-moc".to_string(),
            "localhost"
        );
        println!("monograph start cmd = {:?}", start_cmd);
    }
}
