use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::task_base::{
    CmdErr, CmdErr::MonographCtlErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
    TaskInstance,
};
use crate::cli::task::task_utils::{
    check_process_pid, ctl_action_wait_complete, parse_process_pid, PROCESS_PID,
};
use crate::cli::{CommandArgs, CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeploymentConfig;
use crate::config::DeploymentService;
use crate::{get_ctl_cmd_string, task_return_value, wait_command_complete};
use anyhow::anyhow;
use async_trait::async_trait;
use indexmap::IndexMap;
use sqlx::{Connection, Executor, Row};
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

pub(crate) static MONO_DB_USER: &str = "mono_user";
pub(crate) static MONO_DB_PWD: &str = "mono_pwd";

#[derive(Clone, Debug)]
pub struct MonographDetector {
    host: String,
    mysql_port: u16,
    user: String,
    password: String,
}

impl MonographDetector {
    pub fn new(host: String, mysql_port: u16, user: String, password: String) -> Self {
        Self {
            host,
            mysql_port,
            user,
            password,
        }
    }

    pub async fn detect(&self) -> anyhow::Result<ExecutionValue> {
        let user = &self.user;
        let pwd = &self.password;
        let host = &self.host;
        let port = self.mysql_port;
        let mysql_conn_url = format!("mysql://{user}:{pwd}@{host}:{port}/test");
        let mono_conn_rs = sqlx::mysql::MySqlConnection::connect(mysql_conn_url.as_str()).await;
        if let Err(mono_conn_err) = mono_conn_rs {
            error!(
                "established database connection failure url={},err_msg={:?}",
                mysql_conn_url, mono_conn_err
            );
            return Ok(HashMap::from([
                (CMD.to_string(), TaskArgValue::Str(mysql_conn_url)),
                (CMD_STATUS.to_string(), TaskArgValue::Number(usize::MAX)),
                (
                    CMD_OUTPUT.to_string(),
                    TaskArgValue::Str(mono_conn_err.to_string()),
                ),
            ]));
        }
        let mut mono_conn = mono_conn_rs.unwrap();
        let query_cmd = "select date_format(now(), '%Y-%m-%d %T') as now_date";
        info!(
            "MonographDetector established database connection successfully user={},host={}",
            user, pwd
        );
        let query_rs = mono_conn.fetch_one(query_cmd).await;
        let result = if let Ok(row) = query_rs {
            let now_date: String = row.get(0);
            info!("MonographDB status is normal {}", now_date);
            Ok(now_date)
        } else {
            let err_msg = query_rs.err().unwrap().to_string();
            error!(
                "Cannot connect to MonographDB on {}, err_msg={}",
                host, err_msg
            );
            Err(anyhow!(err_msg))
        };
        mono_conn.close().await?;
        match result {
            Ok(now_date) => Ok(HashMap::from([
                (CMD.to_string(), TaskArgValue::Str(query_cmd.to_string())),
                (CMD_STATUS.to_string(), TaskArgValue::Number(0)),
                (CMD_OUTPUT.to_string(), TaskArgValue::Str(now_date)),
            ])),
            Err(err) => Ok(HashMap::from([
                (CMD.to_string(), TaskArgValue::Str(query_cmd.to_string())),
                (CMD_STATUS.to_string(), TaskArgValue::Number(usize::MAX)),
                (CMD_OUTPUT.to_string(), TaskArgValue::Str(err.to_string())),
            ])),
        }
    }
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

        let mut db_user = "_NONE".to_string();
        let mut db_pwd = "_NONE".to_string();
        let mut is_force_stop = false;
        match cmd_arg.clone() {
            CommandArgs::Stop {
                cluster: _,
                ref force,
            } => is_force_stop = force.is_some() && force.as_ref().unwrap().as_str() == "true",
            CommandArgs::Status {
                cluster: _,
                user,
                password,
            } => {
                if let Some(user_val) = user {
                    db_user = user_val;
                }
                if let Some(password_val) = password {
                    db_pwd = password_val;
                }
            }
            _ => {}
        };
        let cmd_str_ref = cmd_arg.as_ref();
        mono_hosts
            .iter()
            .map(|host| {
                let task_id = TaskId {
                    cmd: cmd_str_ref.to_string(),
                    task: format!("monographdb-{cmd_str_ref}"),
                    host: host.to_string(),
                };
                let cmd_task_input_tuple = match cmd_str_ref {
                    "start" => (
                        monograph_cmd!(MonographCtlCmd::Start, remote_install_dir, host.clone()),
                        HashMap::default(),
                    ),
                    "status" => (
                        monograph_cmd!(MonographCtlCmd::Status, remote_install_dir),
                        HashMap::from([
                            (MONO_DB_USER.to_string(), TaskArgValue::Str(db_user.clone())),
                            (MONO_DB_PWD.to_string(), TaskArgValue::Str(db_pwd.clone())),
                        ]),
                    ),
                    "stop" => {
                        if is_force_stop {
                            (
                                monograph_cmd!(MonographCtlCmd::ForceStop, remote_install_dir),
                                HashMap::default(),
                            )
                        } else {
                            (
                                monograph_cmd!(MonographCtlCmd::Stop, remote_install_dir),
                                HashMap::default(),
                            )
                        }
                    }
                    _ => {
                        unreachable!()
                    }
                };
                let ctl_cmd = cmd_task_input_tuple.0;
                let task_input = cmd_task_input_tuple.1;
                (
                    task_id.clone(),
                    TaskInstance {
                        task_input,
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

    async fn monograph_pid(&self, ssh_conn: SSHSession) -> anyhow::Result<ExecutionValue> {
        let remote_install_dir = self.config.install_dir();
        let check_status = monograph_cmd!(MonographCtlCmd::Status, remote_install_dir);
        let cmd_val = check_status.cmd_value();
        println!("MonographDB process_info pid = {cmd_val}");
        check_process_pid(cmd_val, ssh_conn, parse_process_pid).await
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
        task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        println!("{} execute.\n", self.task_id.pretty_string());
        let ssh_session =
            SSHSession::from_task_host(task_host, self.config.connection.ssh_auth_key().unwrap())
                .await?;
        let remote_install_dir = self.config.install_dir();
        let (host_value, _) = ssh_session.ssh_conn_info();

        let check_status_cmd =
            monograph_cmd!(MonographCtlCmd::Status, remote_install_dir).cmd_value();
        let check_process_status = self.monograph_pid(ssh_session.clone()).await;
        let ctl_cmd_ref = self.ctl_cmd.as_ref();
        let mono_ctl_rs = match ctl_cmd_ref {
            "status" => {
                let db_user = TaskArgValue::into_inner_value::<String>(
                    task_arg.get(MONO_DB_USER).unwrap().clone(),
                );
                let db_pwd = TaskArgValue::into_inner_value::<String>(
                    task_arg.get(MONO_DB_PWD).unwrap().clone(),
                );
                let mysql_port = self.config.deployment.port.mysql_port;
                if !db_user.eq("_NONE") && db_pwd.eq("_NONE") {
                    let db_detector =
                        MonographDetector::new(host_value, mysql_port, db_user, db_pwd);
                    db_detector.detect().await
                } else {
                    check_process_status
                }
            }
            "stop" | "force_stop" => {
                let stop_cmd = self.ctl_cmd.cmd_value();
                monograph_ctl!(self, check_process_status, {!=, "NONE"}, async || -> anyhow::Result<ExecutionValue> {
                     wait_command_complete!(stop_cmd, check_status_cmd, ssh_session.clone(), is_none)
                })
            }
            "start" => {
                let start_cmd = self.ctl_cmd.cmd_value();
                monograph_ctl!(self, check_process_status, {==, "NONE"}, async || -> anyhow::Result<ExecutionValue> {
                    wait_command_complete!(start_cmd, check_status_cmd, ssh_session.clone(), is_some)
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
