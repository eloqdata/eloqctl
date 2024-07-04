use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::task_base::{
    CmdErr, CmdErr::MonographCtlErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
    TaskInstance,
};
use crate::cli::task::task_utils::{
    check_pid, ctl_action_wait_complete, parse_process_pid, PROCESS_PID,
};
use crate::cli::{SubCommand, CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeploymentConfig;
use crate::config::deployment::Product;
use crate::config::DeploymentPackage;
use crate::{get_ctl_cmd_string, task_return_value, wait_command_complete};
use anyhow::anyhow;
use async_trait::async_trait;
use indexmap::IndexMap;
use sqlx::{Connection, Executor, Row};
use std::collections::HashMap;
use std::io;
use std::time::Duration;
use strum_macros::AsRefStr;
use tracing::{debug, error, info};

#[derive(Clone, Debug, Eq, PartialEq, AsRefStr)]
pub enum TxCtlCmd {
    #[strum(serialize = "start")]
    Start(String),
    #[strum(serialize = "stop")]
    Stop(String),
    #[strum(serialize = "force_stop")]
    ForceStop(String),
    #[strum(serialize = "status")]
    Status(String),
}

get_ctl_cmd_string!(TxCtlCmd, Start, Stop, ForceStop, Status);

macro_rules! monograph_cmd {
    ($ctl_cmd:ty,$tx_srv_bin:expr, $user:expr) => {{
        let ctl_cmd = stringify!($ctl_cmd);
        let pid_cmd = format!(
            "ps uxwe -u {} | grep {} | grep -v grep | ",
            $user, $tx_srv_bin
        );
        let output_pid = r#"awk '{print $2}'"#;
        match ctl_cmd {
            "TxCtlCmd::ForceStop" => {
                TxCtlCmd::ForceStop(format!("{} {} | xargs kill -9", pid_cmd, output_pid))
            }
            "TxCtlCmd::Stop" => TxCtlCmd::Stop(format!("{} {} | xargs kill", pid_cmd, output_pid)),
            "TxCtlCmd::Status" => {
                let ps_cmd = format!(r#"{} {} "#, pid_cmd, output_pid);
                TxCtlCmd::Status(ps_cmd)
            }
            _ => {
                unreachable!()
            }
        }
    }};
}

macro_rules! tx_ctl {
    ($self:ident, $mono_process_status:expr, {$op:tt, $pid_check_expr:expr}, $ctl_func:expr) => {{
        if let Ok(ref process_info) = $mono_process_status {
            debug!("tx_ctl process_info={process_info:#?}");
            let pid = TaskArgValue::into_inner_value::<String>(
                process_info.get(PROCESS_PID).unwrap().clone(),
            );
            let ctl_f = $ctl_func;
            if pid $op $pid_check_expr {
                ctl_f().await
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

pub(crate) static WAIT_SECS: &str = "wait_ready_seconds";
pub(crate) static MONO_DB_USER: &str = "mono_user";
pub(crate) static MONO_DB_PWD: &str = "mono_pwd";

macro_rules! maybe_continue_probe {
    ($wait_secs:expr) => {
        if $wait_secs > 0 {
            info!("TxService probe failed, retrying. {}", $wait_secs);
            $wait_secs -= 1;
            let sleep_duration = Duration::from_secs(1);
            tokio::time::sleep(sleep_duration).await;
            continue;
        }
    };
}

#[derive(Clone, Debug)]
pub struct MySQLProbe {
    host: String,
    mysql_port: u16,
    user: String,
    password: String,
}

impl MySQLProbe {
    pub fn new(host: String, mysql_port: u16, user: String, password: String) -> Self {
        Self {
            host,
            mysql_port,
            user,
            password,
        }
    }

    pub async fn probe(&self, mut wait_secs: i32) -> anyhow::Result<ExecutionValue> {
        info!("Probe whether EloqSQL is ready to be connected");
        let user_pwd = if self.password.eq("_NONE") {
            self.user.clone()
        } else {
            format!("{}:{}", self.user, self.password)
        };
        let host = &self.host;
        let port = self.mysql_port;
        let url = format!("mysql://{user_pwd}@{host}:{port}/mysql");
        loop {
            let mono_conn_rs = sqlx::mysql::MySqlConnection::connect(url.as_str()).await;
            if let Err(err) = mono_conn_rs {
                if let sqlx::Error::Io(ref e) = err {
                    if io::ErrorKind::ConnectionRefused == e.kind() {
                        maybe_continue_probe!(wait_secs);
                    }
                }
                error!("EloqSQL connect failed {}: {:?}", url, err);
                return Ok(HashMap::from([
                    (
                        CMD.to_string(),
                        TaskArgValue::Str(format!("Dial MonographDB={url}")),
                    ),
                    (CMD_STATUS.to_string(), TaskArgValue::Number(-1)),
                    (CMD_OUTPUT.to_string(), TaskArgValue::Str(err.to_string())),
                ]));
            }
            let mut conn = mono_conn_rs.unwrap();
            let query_cmd = "SHOW DATABASES";
            let query_rs = conn.fetch_one(query_cmd).await;
            if let Ok(row) = query_rs {
                let now_date: String = row.get(0);
                info!("MonographDB status is normal {}", now_date);
                conn.close().await?;
                return Ok(HashMap::from([
                    (CMD.to_string(), TaskArgValue::Str(query_cmd.to_string())),
                    (CMD_STATUS.to_string(), TaskArgValue::Number(0)),
                    (CMD_OUTPUT.to_string(), TaskArgValue::Str(now_date)),
                ]));
            }
            let err_msg = query_rs.err().unwrap().to_string();
            error!("Cannot connect to EloqSQL @{}: {}", host, err_msg);
            maybe_continue_probe!(wait_secs);
            conn.close().await?;
            return Ok(HashMap::from([
                (CMD.to_string(), TaskArgValue::Str(query_cmd.to_string())),
                (CMD_STATUS.to_string(), TaskArgValue::Number(-1)),
                (
                    CMD_OUTPUT.to_string(),
                    TaskArgValue::Str(err_msg.to_string()),
                ),
            ]));
        }
    }

    pub async fn ssh_probe(
        config: &DeploymentConfig,
        ssh_sess: &SSHSession,
        mut wait_secs: i32,
    ) -> anyhow::Result<ExecutionValue> {
        info!("Probe whether EloqSQL is ready to be connected locally");
        let mut cmd = config.client_conn();
        cmd.push_str(" --execute 'SHOW DATABASES'");
        loop {
            let ret = ssh_sess.command(&cmd, CollectOutput).await?;
            let code = TaskArgValue::into_inner_value::<i32>(ret.get(CMD_STATUS).unwrap().clone());
            if code != 0 {
                maybe_continue_probe!(wait_secs);
            }
            return Ok(ret);
        }
    }
}

#[derive(Clone, Debug)]
pub struct RedisProbe {
    host: String,
    port: u16,
}

impl RedisProbe {
    pub fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }
    pub async fn probe(&self, mut wait_secs: i32) -> anyhow::Result<ExecutionValue> {
        info!("Probe whether Redis is ready to be connected");
        let url = format!("redis://{}:{}/", self.host, self.port);
        let client = redis::Client::open(url.clone())?;
        loop {
            match client.get_connection() {
                Ok(_) => {
                    return Ok(HashMap::from([
                        (CMD.to_string(), TaskArgValue::Str("GET probe".to_string())),
                        (CMD_STATUS.to_string(), TaskArgValue::Number(0)),
                        (CMD_OUTPUT.to_string(), TaskArgValue::Str("nil".to_owned())),
                    ]));
                }
                Err(err) => {
                    if err.is_connection_refusal() {
                        maybe_continue_probe!(wait_secs);
                    }
                    return Ok(HashMap::from([
                        (
                            CMD.to_string(),
                            TaskArgValue::Str(format!("Dial Redis={url}")),
                        ),
                        (CMD_STATUS.to_string(), TaskArgValue::Number(-1)),
                        (CMD_OUTPUT.to_string(), TaskArgValue::Str(err.to_string())),
                    ]));
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct MonographTxCtlTask {
    config: DeploymentConfig,
    task_id: TaskId,
    ctl_cmd: TxCtlCmd,
}

impl MonographTxCtlTask {
    pub fn from_config(
        cmd_arg: SubCommand,
        config: &DeploymentConfig,
    ) -> IndexMap<TaskId, TaskInstance> {
        let conn_user = &config.connection.username;
        let ssh_port = config.connection.ssh_port();
        let tx_bin = config.deployment.tx_srv_bin();
        let mono_hosts = config.get_host_list(DeploymentPackage::MonographTx);

        let mut wait_secs = -1;
        let mut db_user = "_NONE".to_string();
        let mut db_pwd = "_NONE".to_string();
        let mut is_force_stop = false;
        match cmd_arg.clone() {
            SubCommand::Stop {
                cluster: _,
                force,
                all: _,
            } => is_force_stop = force,
            SubCommand::Status {
                cluster: _,
                user,
                password,
                wait,
            } => {
                if let Some(user_val) = user {
                    db_user = user_val;
                }
                if let Some(password_val) = password {
                    db_pwd = password_val;
                }
                if let Some(w) = wait {
                    wait_secs = w as i32;
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
                    task: format!("txservice-{cmd_str_ref}"),
                    host: host.to_string(),
                };
                let cmd_task_input_tuple = match cmd_str_ref {
                    "start" => (
                        TxCtlCmd::Start(config.deployment.tx_srv_start_cmd()),
                        HashMap::default(),
                    ),
                    "status" => (
                        monograph_cmd!(TxCtlCmd::Status, tx_bin, conn_user.clone()),
                        HashMap::from([
                            (WAIT_SECS.to_string(), TaskArgValue::Number(wait_secs)),
                            (MONO_DB_USER.to_string(), TaskArgValue::Str(db_user.clone())),
                            (MONO_DB_PWD.to_string(), TaskArgValue::Str(db_pwd.clone())),
                        ]),
                    ),
                    "stop" => {
                        if is_force_stop {
                            (
                                monograph_cmd!(TxCtlCmd::ForceStop, tx_bin, conn_user.clone()),
                                HashMap::default(),
                            )
                        } else {
                            (
                                monograph_cmd!(TxCtlCmd::Stop, tx_bin, conn_user.clone()),
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
                        task: Box::new(MonographTxCtlTask::new(config.clone(), task_id, ctl_cmd)),
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

    pub fn new(config: DeploymentConfig, task_id: TaskId, ctl_cmd: TxCtlCmd) -> Self {
        Self {
            config,
            task_id,
            ctl_cmd,
        }
    }

    async fn monograph_pid(
        &self,
        ssh_conn: SSHSession,
        user: &str,
    ) -> anyhow::Result<ExecutionValue> {
        let tx_bin = self.config.deployment.tx_srv_bin();
        let check_status = monograph_cmd!(TxCtlCmd::Status, tx_bin, user.to_string());
        let cmd_val = check_status.cmd_value();
        check_pid(cmd_val, ssh_conn, parse_process_pid).await
        // check_process_pid(cmd_val, ssh_conn, parse_process_pid).await
    }
}

#[async_trait]
impl TaskExecutor for MonographTxCtlTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        info!("execute {}", self.task_id.pretty_string());
        let ssh_session =
            SSHSession::from_task_host(task_host, self.config.connection.ssh_auth_key().unwrap())
                .await?;
        let tx_bin = self.config.deployment.tx_srv_bin();
        let (host_value, user) = ssh_session.ssh_conn_info();
        let check_status_cmd = monograph_cmd!(TxCtlCmd::Status, tx_bin, user).cmd_value();
        let check_process_status = self.monograph_pid(ssh_session.clone(), user.as_str()).await;
        let ctl_cmd_ref = self.ctl_cmd.as_ref();
        let mono_ctl_rs = match ctl_cmd_ref {
            "status" => {
                let wait_secs =
                    TaskArgValue::into_inner_value::<i32>(task_arg.get(WAIT_SECS).unwrap().clone());
                match self.config.product() {
                    Product::EloqSQL => {
                        let db_user = TaskArgValue::into_inner_value::<String>(
                            task_arg.get(MONO_DB_USER).unwrap().clone(),
                        );
                        if !db_user.eq("_NONE") {
                            let db_pwd = TaskArgValue::into_inner_value::<String>(
                                task_arg.get(MONO_DB_PWD).unwrap().clone(),
                            );
                            let mysql_port = self.config.deployment.client_port();
                            MySQLProbe::new(host_value, mysql_port, db_user, db_pwd)
                                .probe(wait_secs)
                                .await
                        } else if wait_secs >= 0 {
                            MySQLProbe::ssh_probe(&self.config, &ssh_session, wait_secs).await
                        } else {
                            check_process_status
                        }
                    }
                    Product::EloqKV => {
                        if wait_secs >= 0 {
                            let cs_port = self.config.deployment.client_port();
                            RedisProbe::new(host_value, cs_port).probe(wait_secs).await
                        } else {
                            check_process_status
                        }
                    }
                }
            }
            "stop" | "force_stop" => {
                let stop_cmd = self.ctl_cmd.cmd_value();
                tx_ctl!(self, check_process_status, {!=, "NONE"}, async || -> anyhow::Result<ExecutionValue> {
                     wait_command_complete!(stop_cmd, check_status_cmd, ssh_session.clone(), is_none)
                })
            }
            "start" => {
                let start_cmd = self.ctl_cmd.cmd_value();
                tx_ctl!(self, check_process_status, {==, "NONE"}, async || -> anyhow::Result<ExecutionValue> {
                    wait_command_complete!(start_cmd, check_status_cmd, ssh_session.clone(), is_some)
                })
            }
            _ => {
                unreachable!()
            }
        };

        ssh_session.close().await?;
        let ctl_rtn_value = mono_ctl_rs?;
        let exec_cmd = if let Some(cmd) = ctl_rtn_value.get(CMD) {
            cmd.clone().into_inner_value()
        } else {
            self.ctl_cmd.cmd_value()
        };
        task_return_value!(
            ctl_rtn_value,
            |status_code: i32| -> CmdErr {
                CmdErr::MonographCtlErr(exec_cmd, status_code.to_string())
            },
            "MonographCtlTask"
        )
    }
}
