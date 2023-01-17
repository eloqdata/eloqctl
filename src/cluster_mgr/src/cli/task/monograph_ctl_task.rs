use crate::cli::config::{DeploymentConfig, DeploymentService};
use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::task_base::{
    CmdErr, CmdErr::MonographCtlErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
    TaskInstance,
};
use crate::cli::task::task_utils::{
    check_process_pid, ctl_action_wait_complete, ctl_cmd, PROCESS_PID,
};
use crate::cli::CommandArgs;
use crate::{get_ctl_cmd_string, task_return_value};
use anyhow::anyhow;
use async_trait::async_trait;
use indexmap::IndexMap;
use std::collections::HashMap;
use strum_macros::AsRefStr;
use tracing::{error, info};

#[derive(Clone, Debug, Eq, PartialEq, AsRefStr)]
pub enum MonographCtlCmd {
    #[strum(serialize = "start")]
    Start(String),
    #[strum(serialize = "stop")]
    Stop(String),
    #[strum(serialize = "force_stop")]
    ForceStop(String),
    #[strum(serialize = "status")]
    Status(String),
}

get_ctl_cmd_string!(MonographCtlCmd, Start, Stop, ForceStop, Status);

macro_rules! monograph_cmd {
    ($ctl_cmd:ty,$remote_install_home:expr $(, $cmd_arg:expr)? $(,)?) => {{
        let ctl_cmd = stringify!($ctl_cmd);
        let mysqld_pid = format!(r#"ps uxwe | grep {}/monographdb-release/install/bin/mysqld | grep -v grep | "#, $remote_install_home);
        let output_pid = r#"awk '{print $2}'"#;
        match ctl_cmd {
            $(
            "MonographCtlCmd::Start" => MonographCtlCmd::Start(format!(r"mkdir -p {}/monographdb-release/logs && cd {}/monographdb-release/install &&  export LD_LIBRARY_PATH={}/monographdb-release/install/lib:$LD_LIBRARY_PATH; {}/monographdb-release/install/bin/mysqld --defaults-file={}/my_{}.cnf > {}/monographdb-release/logs/mysqld_start.log 2>&1 &",
               $remote_install_home, $remote_install_home, $remote_install_home,
               $remote_install_home,$remote_install_home,$cmd_arg, $remote_install_home)),
            )*
            "MonographCtlCmd::ForceStop" => MonographCtlCmd::ForceStop(format!("{} {} | xargs kill -9", mysqld_pid, output_pid)),
            "MonographCtlCmd::Stop" => {
               MonographCtlCmd::Stop(format!("{} {} | xargs kill", mysqld_pid, output_pid))
            },
            "MonographCtlCmd::Status" => {
               let ps_cmd = format!(r#"{} {} "#, mysqld_pid, output_pid);
               MonographCtlCmd::Status(ps_cmd)
            },
            _=> {
                unreachable!()
            }
        }
    }};
}

macro_rules! monograph_ctl {
    ($self:ident, $mono_process_status:expr, {$op:tt, $pid_check_expr:expr}, $ctl_func:expr) => {{
        if let Ok(ref process_info) = $mono_process_status {
            let pid = TaskArgValue::into_inner_value::<String>(
                process_info.get(PROCESS_PID).unwrap().clone(),
            );
            if pid $op $pid_check_expr {
                $ctl_func().await
            } else {
               Ok($mono_process_status?)
            }
        } else {
            error!(
                "MonographCtlTask process status failed. check_status_cmd={:?}",
                $mono_process_status
            );
            Err(anyhow!(MonographCtlErr(
                $self.ctl_cmd.cmd_value(),
                $mono_process_status.err().unwrap().to_string()
            )))
        }
    }};
}

#[derive(Debug, Clone)]
pub struct MonographCtlTask {
    config: DeploymentConfig,
    task_id: TaskId,
    ctl_cmd: MonographCtlCmd,
}

impl MonographCtlTask {
    pub fn from_config(
        cmd_arg: CommandArgs,
        config: &DeploymentConfig,
    ) -> IndexMap<TaskId, TaskInstance> {
        let conn_user = &config.connection.username;
        let ssh_port = config.connection.ssh_port();
        let remote_install_dir = config.install_dir();
        let mono_hosts = config.get_host_list(DeploymentService::Monograph);

        let is_force_stop = match cmd_arg {
            CommandArgs::Stop {
                cluster: _,
                ref force,
            } => force.is_some() && force.as_ref().unwrap().as_str() == "true",
            _ => false,
        };
        let cmd_str_ref = cmd_arg.as_ref();
        mono_hosts
            .iter()
            .map(|host| {
                let task_id = TaskId {
                    cmd: cmd_str_ref.to_string(),
                    task: format!("monographdb-{}", cmd_str_ref),
                    host: host.to_string(),
                };

                let ctl_cmd = match cmd_str_ref {
                    "start" => {
                        monograph_cmd!(MonographCtlCmd::Start, remote_install_dir, host.clone())
                    }
                    "status" => {
                        monograph_cmd!(MonographCtlCmd::Status, remote_install_dir)
                    }
                    "stop" => {
                        if is_force_stop {
                            monograph_cmd!(MonographCtlCmd::ForceStop, remote_install_dir)
                        } else {
                            monograph_cmd!(MonographCtlCmd::Stop, remote_install_dir)
                        }
                    }
                    _ => {
                        unreachable!()
                    }
                };
                (
                    task_id.clone(),
                    TaskInstance {
                        task_input: HashMap::default(),
                        task: Box::new(MonographCtlTask::new(config.clone(), task_id, ctl_cmd)),
                        task_host: TaskHost::Remote {
                            user: conn_user.clone(),
                            port: ssh_port as usize,
                            hosts: host.to_string(),
                        },
                    },
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>()
    }

    pub fn new(config: DeploymentConfig, task_id: TaskId, ctl_cmd: MonographCtlCmd) -> Self {
        Self {
            config,
            task_id,
            ctl_cmd,
        }
    }

    fn parse_monograph_pid(output: String) -> Option<i32> {
        if output.is_empty() {
            None
        } else {
            let mut pid = None;
            let output_normal = output.trim();
            for line in output_normal.lines() {
                let line_normal = line.trim();
                if line_normal.is_empty() {
                    continue;
                }
                if !line_normal.chars().all(char::is_numeric) {
                    continue;
                }
                let parse_rs = line_normal.parse::<i32>().unwrap();
                info!("MonographCtlTask found MonographDB PID={:?}", parse_rs);
                pid = Some(parse_rs);
                break;
            }
            println!("MonographCtlTask found MonographDB PID={:?}", pid);
            pid
        }
    }

    async fn monograph_pid(&self, ssh_conn: SSHSession) -> anyhow::Result<ExecutionValue> {
        let remote_install_dir = self.config.install_dir();
        let check_status = monograph_cmd!(MonographCtlCmd::Status, remote_install_dir);
        check_process_pid(
            check_status.cmd_value(),
            ssh_conn,
            MonographCtlTask::parse_monograph_pid,
        )
        .await
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
        println!("{} execute.\n", self.task_id.pretty_string());
        let ssh_session =
            SSHSession::from_task_host(task_host, self.config.connection.ssh_auth_key().unwrap())
                .await?;
        let remote_install_dir = self.config.install_dir();
        let check_status_cmd =
            monograph_cmd!(MonographCtlCmd::Status, remote_install_dir).cmd_value();
        let check_process_status = self.monograph_pid(ssh_session.clone()).await;
        let ctl_cmd_ref = self.ctl_cmd.as_ref();
        let mono_ctl_rs = match ctl_cmd_ref {
            "status" => {
                monograph_ctl!(self, check_process_status, {==, "NONE"}, async || -> anyhow::Result<ExecutionValue> {
                    self.monograph_pid(ssh_session.clone()).await
                })
            }
            "stop" | "force_stop" => {
                let stop_cmd = self.ctl_cmd.cmd_value();
                monograph_ctl!(self, check_process_status, {!=, "NONE"}, async || -> anyhow::Result<ExecutionValue> {
                    ctl_action_wait_complete(stop_cmd, check_status_cmd, ssh_session.clone(),
                        async move |stop_cmd, ssh_conn| { ssh_conn.command(stop_cmd.as_str(), CollectOutput).await},
                        |output| -> bool {
                           MonographCtlTask::parse_monograph_pid(output).is_none()
                        },
                    ).await
                })
            }
            "start" => {
                let start_cmd = self.ctl_cmd.cmd_value();
                monograph_ctl!(self, check_process_status, {==, "NONE"}, async || -> anyhow::Result<ExecutionValue> {
                    ctl_action_wait_complete(start_cmd,check_status_cmd,ssh_session.clone(),
                         async move |start_cmd, ssh_conn| { ctl_cmd(start_cmd, ssh_conn).await},
                         |output| -> bool { MonographCtlTask::parse_monograph_pid(output).is_some() },

                    ).await
                })
            }
            _ => {
                unreachable!()
            }
        };

        ssh_session.close().await?;
        let ctl_rtn_value = mono_ctl_rs?;
        task_return_value!(
            ctl_rtn_value,
            |status_code: usize| -> CmdErr {
                CmdErr::MonographCtlErr(self.ctl_cmd.cmd_value(), status_code.to_string())
            },
            "MonographCtlTask"
        )
    }
}
