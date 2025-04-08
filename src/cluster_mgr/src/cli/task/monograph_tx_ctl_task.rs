use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::redis_op_task::ClusterNodes;
use crate::cli::task::task_base::{
    CmdErr, CmdErr::MonographCtlErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
    TaskInstance,
};
use crate::cli::task::task_utils::{
    check_pid, ctl_action_wait_complete, parse_process_pid, PID_NOT_FOUND, PROCESS_PID,
};
use crate::cli::{SubCommand, CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeployConfig;
use crate::config::deployment::Product;
use crate::config::DeploymentPackage;
use crate::{get_ctl_cmd_string, task_return_value, wait_command_complete};
use anyhow::anyhow;
use async_trait::async_trait;
use indexmap::IndexMap;
use regex::Regex;
use sqlx::{Connection, Executor, Row};
use std::collections::HashMap;
use std::io;
use std::thread;
use std::time::Duration;
use strum_macros::AsRefStr;
use tokio::sync::watch;
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

#[derive(Debug, PartialEq, Clone)]
pub enum ServerType {
    Tx,
    Standby,
    Voter,
    Node,
}

impl std::fmt::Display for ServerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ServerType::Tx => "txservice",
                ServerType::Standby => "standby",
                ServerType::Voter => "voter",
                ServerType::Node => "node",
            }
        )
    }
}

get_ctl_cmd_string!(TxCtlCmd, Start, Stop, ForceStop, Status);

macro_rules! monograph_cmd_with_port {
    ($ctl_cmd:ty, $tx_srv_bin:expr, $user:expr, $port:expr) => {{
        let ctl_cmd = stringify!($ctl_cmd);
        let pid_cmd = format!(
            "ps uxwe -u {} | grep {} | grep {} | grep -v grep | ",
            $user, $tx_srv_bin, $port
        );
        let output_pid = r#"awk '{print $2}'"#;
        match ctl_cmd {
            "TxCtlCmd::ForceStop" => {
                TxCtlCmd::ForceStop(format!("{} {} | xargs -r kill -9", pid_cmd, output_pid))
            }
            "TxCtlCmd::Stop" => {
                TxCtlCmd::Stop(format!("{} {} | xargs -r kill", pid_cmd, output_pid))
            }
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
        config: &DeployConfig,
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
                        (CMD.to_string(), TaskArgValue::Str(url)),
                        (CMD_STATUS.to_string(), TaskArgValue::Number(0)),
                        (CMD_OUTPUT.to_string(), TaskArgValue::Str("OK".to_owned())),
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
    config: DeployConfig,
    task_id: TaskId,
    ctl_cmd: TxCtlCmd,
    receiver: Option<watch::Receiver<ClusterNodes>>,
}

// Define the context struct
struct TaskGenerationContext<'a> {
    cmd_arg: &'a SubCommand,
    config: &'a DeployConfig,
    conn_user: &'a str,
    ssh_port: usize,
    tx_bin: &'a str,
    wait_secs: i32,
    db_user: &'a str,
    db_pwd: &'a str,
    is_force_stop: bool,
    receiver: Option<watch::Receiver<ClusterNodes>>,
}

fn generate_tasks_for_host_ports(
    context: &TaskGenerationContext,
    host_ports: &[String],
    server_type: ServerType,
) -> IndexMap<TaskId, TaskInstance> {
    let cmd_str_ref = context.cmd_arg.as_ref();
    host_ports
        .iter()
        .map(|host_port| {
            let (host, port) = host_port.split_once(':').unwrap_or_else(|| {
                panic!(
                    "Error: Invalid host_port format '{}'. Expected 'host:port'.",
                    host_port
                )
            });

            let task_id = TaskId {
                cmd: cmd_str_ref.to_string(),
                task: format!("{}-{}-{}", server_type, cmd_str_ref, port),
                host: host.to_string(),
            };

            let cmd_task_input_tuple = match cmd_str_ref {
                "start" => (
                    TxCtlCmd::Start(
                        context
                            .config
                            .deployment
                            .srv_start_cmd(port, server_type.clone()),
                    ),
                    HashMap::default(),
                ),
                "status" => (
                    monograph_cmd_with_port!(
                        TxCtlCmd::Status,
                        context.tx_bin,
                        context.conn_user.to_string(),
                        port.to_string()
                    ),
                    HashMap::from([
                        (
                            WAIT_SECS.to_string(),
                            TaskArgValue::Number(context.wait_secs),
                        ),
                        (
                            MONO_DB_USER.to_string(),
                            TaskArgValue::Str(context.db_user.to_string()),
                        ),
                        (
                            MONO_DB_PWD.to_string(),
                            TaskArgValue::Str(context.db_pwd.to_string()),
                        ),
                    ]),
                ),
                "stop" | "remove" => {
                    if context.is_force_stop {
                        (
                            monograph_cmd_with_port!(
                                TxCtlCmd::ForceStop,
                                context.tx_bin,
                                context.conn_user.to_string(),
                                port.to_string()
                            ),
                            HashMap::default(),
                        )
                    } else {
                        (
                            monograph_cmd_with_port!(
                                TxCtlCmd::Stop,
                                context.tx_bin,
                                context.conn_user.to_string(),
                                port.to_string()
                            ),
                            HashMap::default(),
                        )
                    }
                }
                _ => unreachable!(),
            };

            let ctl_cmd = cmd_task_input_tuple.0;
            let task_input = cmd_task_input_tuple.1;
            let task = if context.receiver.is_some() {
                Box::new(MonographTxCtlTask::new(
                    context.config.clone(),
                    task_id.clone(),
                    ctl_cmd,
                    context.receiver.clone(),
                ))
            } else {
                Box::new(MonographTxCtlTask::new(
                    context.config.clone(),
                    task_id.clone(),
                    ctl_cmd,
                    None,
                ))
            };

            (
                task_id.clone(),
                TaskInstance {
                    task_input,
                    task,
                    task_host: TaskHost::Remote {
                        user: context.conn_user.to_string(),
                        port: context.ssh_port,
                        host: host.to_string(),
                    },
                },
            )
        })
        .collect::<IndexMap<TaskId, TaskInstance>>()
}

impl MonographTxCtlTask {
    pub fn from_config(
        cmd_arg: SubCommand,
        config: &DeployConfig,
        server_type: ServerType,
    ) -> IndexMap<TaskId, TaskInstance> {
        let conn_user = &config.connection.username;
        let ssh_port = config.connection.ssh_port() as usize;
        let tx_bin = config.deployment.tx_srv_bin();
        let mut node_host_ports = vec![];

        let (mut wait_secs, mut db_user, mut db_pwd, mut is_force_stop) =
            (-1, "_NONE".to_string(), "_NONE".to_string(), false);

        match &cmd_arg {
            SubCommand::Start { nodes, .. } => {
                if server_type == ServerType::Node {
                    node_host_ports = nodes.clone();
                }
            }
            SubCommand::Stop { force, .. } => {
                is_force_stop = *force;
                if server_type == ServerType::Node {
                    unreachable!("stop --nodes should not use this function");
                }
            }
            SubCommand::Remove { .. } => is_force_stop = true,
            SubCommand::Status {
                user,
                password,
                wait,
                ..
            } => {
                if let Some(user_val) = user {
                    db_user = user_val.clone();
                }
                if let Some(password_val) = password {
                    db_pwd = password_val.clone();
                }
                if let Some(w) = wait {
                    wait_secs = *w as i32;
                }
            }
            _ => {}
        }

        let context = TaskGenerationContext {
            cmd_arg: &cmd_arg,
            config,
            conn_user,
            ssh_port,
            tx_bin: &tx_bin,
            wait_secs,
            db_user: &db_user,
            db_pwd: &db_pwd,
            is_force_stop,
            receiver: None,
        };

        let host_ports = match server_type {
            ServerType::Tx => config.get_host_port_list(DeploymentPackage::MonographTx),
            ServerType::Standby => config.get_host_port_list(DeploymentPackage::MonographStandby),
            ServerType::Voter => config.get_host_port_list(DeploymentPackage::MonographVoter),
            ServerType::Node => node_host_ports,
        };

        generate_tasks_for_host_ports(&context, &host_ports, server_type)
    }

    // currently only use in stop process
    pub fn from_config_with_channel(
        cmd_arg: SubCommand,
        config: &DeployConfig,
        server_type: ServerType,
        receiver: Option<watch::Receiver<ClusterNodes>>,
    ) -> anyhow::Result<IndexMap<TaskId, TaskInstance>> {
        let conn_user = &config.connection.username;
        let ssh_port = config.connection.ssh_port() as usize;
        let tx_bin = config.deployment.tx_srv_bin();

        let (mut wait_secs, mut db_user, mut db_pwd, mut is_force_stop) =
            (-1, "_NONE".to_string(), "_NONE".to_string(), false);

        let mut node_host_ports = Vec::new();

        match &cmd_arg {
            SubCommand::Stop { nodes, force, .. } => {
                is_force_stop = *force;
                if server_type == ServerType::Node {
                    node_host_ports = nodes.clone();
                }
            }
            SubCommand::Remove { .. } => is_force_stop = true,
            SubCommand::Status {
                user,
                password,
                wait,
                ..
            } => {
                if let Some(user_val) = user {
                    db_user = user_val.clone();
                }
                if let Some(password_val) = password {
                    db_pwd = password_val.clone();
                }
                if let Some(w) = wait {
                    wait_secs = *w as i32;
                }
            }
            _ => {}
        }

        let context = TaskGenerationContext {
            cmd_arg: &cmd_arg,
            config,
            conn_user,
            ssh_port,
            tx_bin: &tx_bin,
            wait_secs,
            db_user: &db_user,
            db_pwd: &db_pwd,
            is_force_stop,
            receiver,
        };

        let host_ports = match server_type {
            ServerType::Tx => config.get_host_port_list(DeploymentPackage::MonographTx),
            ServerType::Standby => config.get_host_port_list(DeploymentPackage::MonographStandby),
            ServerType::Voter => config.get_host_port_list(DeploymentPackage::MonographVoter),
            ServerType::Node => node_host_ports,
        };

        Ok(generate_tasks_for_host_ports(
            &context,
            &host_ports,
            server_type,
        ))
    }

    pub fn new(
        config: DeployConfig,
        task_id: TaskId,
        ctl_cmd: TxCtlCmd,
        receiver: Option<watch::Receiver<ClusterNodes>>,
    ) -> Self {
        Self {
            config,
            task_id,
            ctl_cmd,
            receiver,
        }
    }
}

fn extract_server_type_and_port(input: &str) -> (&str, &str) {
    if let Some((server_type, port)) = input.split_once("-start-") {
        return (server_type, port);
    }
    if let Some((server_type, port)) = input.split_once("-stop-") {
        return (server_type, port);
    }
    if let Some((server_type, port)) = input.split_once("-status-") {
        return (server_type, port);
    }
    if let Some((server_type, port)) = input.split_once("-remove-") {
        return (server_type, port);
    }

    panic!("format error!!!");
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
        info!("execute {}", self.task_id.format_string());

        let mut master_host_ports = Vec::new();
        let mut standby_host_ports = Vec::new();
        // Receive the cluster nodes in execute
        if let Some(receiver) = &self.receiver {
            let timeout = Duration::from_secs(5);
            let start_time = tokio::time::Instant::now();

            loop {
                if receiver.has_changed().unwrap_or(false) {
                    // The receiver has changed; get the data
                    let cluster_nodes = receiver.borrow();
                    debug!(
                        "Received cluster nodes: {:?}, Current thread ID: {:?}",
                        cluster_nodes,
                        thread::current().id()
                    );

                    // Process the cluster_nodes
                    for node in &cluster_nodes.masters {
                        master_host_ports.push(format!("{}:{}", node.ip, node.port));
                    }
                    for node in &cluster_nodes.replicas {
                        standby_host_ports.push(format!("{}:{}", node.ip, node.port));
                    }
                    break;
                }

                if start_time.elapsed() >= timeout {
                    let redis_cmd_result = HashMap::from([
                        (
                            CMD.to_string(),
                            TaskArgValue::Str("cluster nodes".to_string()),
                        ),
                        (CMD_STATUS.to_string(), TaskArgValue::Number(1)),
                        (
                            CMD_OUTPUT.to_string(),
                            TaskArgValue::Str("timeout".to_string()),
                        ),
                    ]);
                    task_return_value!(
                        redis_cmd_result,
                        |status_code: i32| -> CmdErr {
                            CmdErr::RedisOpErr(
                                "timeout, fail to receive cluster nodes response".to_string(),
                                status_code.to_string(),
                            )
                        },
                        "MonographCtlTask"
                    )
                }

                // Avoid busy looping
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        debug!(
            "task host: {:?}, Current thread ID: {:?}, master_host_ports: {:?}, standby_host_ports: {:?}",
            task_host,
            thread::current().id(),
            master_host_ports,
            standby_host_ports
        );
        let task_str = self.task_id.task.as_str();
        let (server_type, port) = extract_server_type_and_port(task_str);

        let ssh_session = SSHSession::from_task_host(
            task_host.clone(),
            self.config.connection.ssh_auth_key().unwrap(),
        )
        .await?;
        let tx_bin = self.config.deployment.tx_srv_bin();
        let (host_value, user) = ssh_session.ssh_conn_info();
        let check_status_cmd =
            monograph_cmd_with_port!(TxCtlCmd::Status, tx_bin, user, port).cmd_value();

        let check_status =
            monograph_cmd_with_port!(TxCtlCmd::Status, tx_bin, user.to_string(), port);
        let cmd_val = check_status.cmd_value();
        let check_process_status = check_pid(cmd_val, ssh_session.clone(), parse_process_pid).await;

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
                            let cs_port: u16 = port.parse().unwrap();
                            let _ = RedisProbe::new(host_value, cs_port).probe(wait_secs).await;
                            check_process_status
                        } else {
                            check_process_status
                        }
                    }
                }
            }
            "stop" | "force_stop" => {
                let stop_cmd = self.ctl_cmd.cmd_value();
                let mut matching_ports: Vec<String> = Vec::new();
                match server_type {
                    "txservice" => {
                        matching_ports = master_host_ports
                            .iter()
                            .filter_map(|host_port| {
                                let parts: Vec<&str> = host_port.split(':').collect();
                                if parts.len() == 2 && parts[0] == self.task_id.host {
                                    Some(parts[1].to_string())
                                } else {
                                    None
                                }
                            })
                            .collect();
                    }
                    "standby" => {
                        matching_ports = standby_host_ports
                            .iter()
                            .filter_map(|host_port| {
                                let parts: Vec<&str> = host_port.split(':').collect();
                                if parts.len() == 2 && parts[0] == self.task_id.host {
                                    Some(parts[1].to_string())
                                } else {
                                    None
                                }
                            })
                            .collect();
                    }
                    "voter" => (),
                    "node" => {
                        matching_ports.push(port.to_string());
                    }
                    _ => {
                        unreachable!("Unknown server type: {}", server_type);
                    }
                }

                // Modify stop_cmd to use grep with the matched ports
                if !matching_ports.is_empty() {
                    let port_pattern = matching_ports.join("|");
                    let re = Regex::new(r"grep \d+").unwrap();
                    let modified_stop_cmd =
                        re.replace(&stop_cmd, &format!("grep -E '{}'", port_pattern));
                    debug!("Modified stop_cmd: {}", modified_stop_cmd.to_string());

                    let modified_check_status_cmd =
                        re.replace(&check_status_cmd, &format!("grep -E '{}'", port_pattern));
                    debug!(
                        "Modified check_status_cmd: {}",
                        modified_check_status_cmd.to_string()
                    );

                    // should not check using port from config
                    wait_command_complete!(
                        modified_stop_cmd.to_string(),
                        modified_check_status_cmd.to_string(),
                        ssh_session.clone(),
                        is_none
                    )
                } else {
                    debug!("No matching ports found for the given host.");
                    tx_ctl!(self, check_process_status, {!=, PID_NOT_FOUND}, async || -> anyhow::Result<ExecutionValue> {
                        wait_command_complete!(stop_cmd, check_status_cmd, ssh_session.clone(), is_none)
                    })
                }
            }
            "start" => {
                let start_cmd = self.ctl_cmd.cmd_value();
                let rs = tx_ctl!(self, check_process_status, {==, PID_NOT_FOUND}, async || -> anyhow::Result<ExecutionValue> {
                    wait_command_complete!(start_cmd, check_status_cmd, ssh_session.clone(), is_some)
                });
                rs
            }
            _ => {
                unreachable!("Unrecognized command: {}", ctl_cmd_ref);
            }
        };

        ssh_session.close().await?;
        let mut ctl_rtn_value = mono_ctl_rs?;
        match ctl_cmd_ref {
            "status" => {
                if ctl_rtn_value.get(PROCESS_PID).is_some() {
                    let pid = TaskArgValue::into_inner_value::<String>(
                        ctl_rtn_value.get(PROCESS_PID).unwrap().clone(),
                    );
                    if pid == PID_NOT_FOUND {
                        let output = format!("\neloqkv service is down.");
                        ctl_rtn_value.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(output));
                    } else {
                        let output = format!("\neloqkv service is running, pid: {}.", pid);
                        ctl_rtn_value.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(output));
                    }
                }
            }
            _ => {}
        }
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
