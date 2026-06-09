use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::redis_op_task::{parse_cluster_nodes, ClusterNodes};
use crate::cli::task::task_base::{
    CmdErr, CmdErr::EloqCtlErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
    TaskInstance,
};
use crate::cli::task::task_utils::{
    check_pid, critical_runtime_ports, ctl_action_wait_complete, parse_process_pid,
    wait_tcp_ports_state, PID_NOT_FOUND, PROCESS_PID,
};
use crate::cli::{SubCommand, CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeployConfig;
use crate::config::DeploymentPackage;
use crate::{get_ctl_cmd_string, task_return_value, wait_command_complete};
use anyhow::anyhow;
use async_trait::async_trait;
use indexmap::IndexMap;
use itertools::Itertools;
use redis::{Client, RedisResult, Value};
use regex::Regex;
use std::collections::HashMap;
use std::thread;
use std::time::Duration;
use strum_macros::AsRefStr;
use tokio::sync::watch;
use tracing::{error, info};

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

macro_rules! eloq_cmd_with_port {
    ($ctl_cmd:ty, $tx_srv_bin:expr, $user:expr, $port:expr) => {{
        let ctl_cmd = stringify!($ctl_cmd);
        let pid_cmd = format!(
            "ps -e -o pid,cmd --no-headers -u {} | grep {} | grep {}.ini | grep -v grep",
            $user, $tx_srv_bin, $port
        );
        match ctl_cmd {
            "TxCtlCmd::ForceStop" => {
                let pid_output = format!("{} | awk '{{print $1}}'", pid_cmd);
                TxCtlCmd::ForceStop(format!("{} | xargs -r kill -9", pid_output))
            }
            "TxCtlCmd::Stop" => {
                let pid_output = format!("{} | awk '{{print $1}}'", pid_cmd);
                TxCtlCmd::Stop(format!("{} | xargs -r kill", pid_output))
            }
            "TxCtlCmd::Status" => TxCtlCmd::Status(pid_cmd),
            _ => {
                unreachable!()
            }
        }
    }};
}

macro_rules! tx_ctl {
    ($self:ident, $eloq_process_status:expr, {$op:tt, $pid_check_expr:expr}, $ctl_func:expr) => {{
        if let Ok(ref process_info) = $eloq_process_status {
            info!("tx_ctl process_info={process_info:#?}");
            let pid = TaskArgValue::into_inner_value::<String>(
                process_info.get(PROCESS_PID).unwrap().clone(),
            );
            let ctl_f = $ctl_func;
            if pid $op $pid_check_expr {
                ctl_f().await
            } else {
               Ok($eloq_process_status?)
            }
        } else {
            error!(
                "EloqCtlTask process status failed. check_status_cmd={:?}",
                $eloq_process_status
            );
            Err(anyhow!(EloqCtlErr(
                $self.ctl_cmd.cmd_value(),
                $eloq_process_status.err().unwrap().to_string()
            )))
        }
    }};
}

pub(crate) static WAIT_SECS: &str = "wait_ready_seconds";
const REDIS_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const REDIS_IO_TIMEOUT: Duration = Duration::from_secs(3);

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
pub struct RedisProbe {
    host: String,
    port: u16,
    password: Option<String>,
    tls_enabled: bool,
}

impl RedisProbe {
    pub fn new(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            password: None,
            tls_enabled: false,
        }
    }

    pub fn with_password(host: String, port: u16, password: Option<String>) -> Self {
        Self {
            host,
            port,
            password,
            tls_enabled: false,
        }
    }

    pub fn with_password_and_tls(
        host: String,
        port: u16,
        password: Option<String>,
        tls_enabled: bool,
    ) -> Self {
        Self {
            host,
            port,
            password,
            tls_enabled,
        }
    }

    pub async fn probe(&self, mut wait_secs: i32) -> anyhow::Result<ExecutionValue> {
        info!("Probe whether Redis is ready to serve requests");
        let scheme = if self.tls_enabled { "rediss" } else { "redis" };
        let url = if let Some(ref pass) = self.password {
            format!("{scheme}://:{pass}@{}:{}/", self.host, self.port)
        } else {
            format!("{scheme}://{}:{}/", self.host, self.port)
        };
        let client_url = if self.tls_enabled {
            format!("{url}#insecure")
        } else {
            url.clone()
        };
        let client = redis::Client::open(client_url)?;
        loop {
            match client.get_connection_with_timeout(REDIS_CONNECT_TIMEOUT) {
                Ok(mut con) => {
                    let _ = con.set_read_timeout(Some(REDIS_IO_TIMEOUT));
                    let _ = con.set_write_timeout(Some(REDIS_IO_TIMEOUT));
                    // Send PING to verify Redis is actually serving commands,
                    // not just accepting TCP connections.
                    match redis::cmd("PING").query::<String>(&mut con) {
                        Ok(ref pong) if pong == "PONG" => {
                            return Ok(HashMap::from([
                                (CMD.to_string(), TaskArgValue::Str(url)),
                                (CMD_STATUS.to_string(), TaskArgValue::Number(0)),
                                (CMD_OUTPUT.to_string(), TaskArgValue::Str("OK".to_owned())),
                            ]));
                        }
                        Ok(resp) => {
                            info!(
                                "Redis PING returned unexpected response '{}', retrying",
                                resp
                            );
                            maybe_continue_probe!(wait_secs);
                        }
                        Err(err) => {
                            info!("Redis PING to {} failed: {}, retrying", url, err);
                            if err.is_connection_refusal() {
                                maybe_continue_probe!(wait_secs);
                            }
                            maybe_continue_probe!(wait_secs);
                        }
                    }
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

    pub async fn probe_cluster(
        startup_nodes: Vec<String>,
        password: Option<String>,
        mut wait_secs: i32,
    ) -> anyhow::Result<ExecutionValue> {
        let redis_urls: Vec<String> = startup_nodes
            .iter()
            .map(|host_port| {
                if let Some(pass) = &password {
                    format!("redis://:{pass}@{host_port}/")
                } else {
                    format!("redis://{host_port}/")
                }
            })
            .collect();
        let cmd = format!("Redis cluster startup nodes={}", startup_nodes.join(","));
        loop {
            let mut last_error: Option<String> = None;
            for url in &redis_urls {
                let query_result: RedisResult<Value> = match Client::open(url.clone()) {
                    Ok(client) => match client.get_connection_with_timeout(REDIS_CONNECT_TIMEOUT) {
                        Ok(mut con) => {
                            let _ = con.set_read_timeout(Some(REDIS_IO_TIMEOUT));
                            let _ = con.set_write_timeout(Some(REDIS_IO_TIMEOUT));
                            redis::cmd("CLUSTER").arg("NODES").query(&mut con)
                        }
                        Err(err) => {
                            last_error = Some(err.to_string());
                            continue;
                        }
                    },
                    Err(err) => {
                        last_error = Some(err.to_string());
                        continue;
                    }
                };

                match query_result {
                    Ok(value) => {
                        let nodes = parse_cluster_nodes(value)?;
                        let has_master = nodes.iter().any(|slot| !slot.masters.is_empty());
                        if has_master {
                            return Ok(HashMap::from([
                                (CMD.to_string(), TaskArgValue::Str(cmd)),
                                (CMD_STATUS.to_string(), TaskArgValue::Number(0)),
                                (
                                    CMD_OUTPUT.to_string(),
                                    TaskArgValue::Str("cluster has a serving master".to_string()),
                                ),
                            ]));
                        }
                        last_error = Some("cluster topology did not report a master".to_string());
                    }
                    Err(err) => last_error = Some(err.to_string()),
                }
            }

            if wait_secs <= 0 {
                return Ok(HashMap::from([
                    (CMD.to_string(), TaskArgValue::Str(cmd)),
                    (CMD_STATUS.to_string(), TaskArgValue::Number(-1)),
                    (
                        CMD_OUTPUT.to_string(),
                        TaskArgValue::Str(
                            last_error.unwrap_or_else(|| "cluster is not ready".to_string()),
                        ),
                    ),
                ]));
            }
            wait_secs -= 1;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

#[derive(Clone, Debug)]
pub struct EloqTxCtlTask {
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
    tx_bin: &'a str,
    wait_secs: i32,
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
                    TxCtlCmd::Start(if matches!(server_type, ServerType::Node) {
                        // For nodes, we need to use both host and port
                        context
                            .config
                            .deployment
                            .srv_start_cmd_with_host(port, server_type.clone())
                    } else {
                        context
                            .config
                            .deployment
                            .srv_start_cmd(port, server_type.clone())
                    }),
                    HashMap::default(),
                ),
                "status" => (
                    eloq_cmd_with_port!(
                        TxCtlCmd::Status,
                        context.tx_bin,
                        context.conn_user.to_string(),
                        port.to_string()
                    ),
                    HashMap::from([(
                        WAIT_SECS.to_string(),
                        TaskArgValue::Number(context.wait_secs),
                    )]),
                ),
                "stop" | "remove" => {
                    if context.is_force_stop {
                        (
                            eloq_cmd_with_port!(
                                TxCtlCmd::ForceStop,
                                context.tx_bin,
                                context.conn_user.to_string(),
                                port.to_string()
                            ),
                            HashMap::default(),
                        )
                    } else {
                        (
                            eloq_cmd_with_port!(
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
                Box::new(EloqTxCtlTask::new(
                    context.config.clone(),
                    task_id.clone(),
                    ctl_cmd,
                    context.receiver.clone(),
                ))
            } else {
                Box::new(EloqTxCtlTask::new(
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
                    task_host: TaskHost::remote(&context.config.connection, host),
                },
            )
        })
        .collect::<IndexMap<TaskId, TaskInstance>>()
}

impl EloqTxCtlTask {
    pub fn from_config(
        cmd_arg: SubCommand,
        config: &DeployConfig,
        server_type: ServerType,
    ) -> IndexMap<TaskId, TaskInstance> {
        let conn_user = &config.connection.username;
        let tx_bin = config.deployment.tx_srv_bin();
        let mut node_host_ports = vec![];

        let (mut wait_secs, mut is_force_stop) = (-1, false);

        match &cmd_arg {
            SubCommand::Start { nodes, .. } if server_type == ServerType::Node => {
                node_host_ports = nodes.clone();
            }
            SubCommand::Stop { force, .. } => {
                is_force_stop = *force;
                if server_type == ServerType::Node {
                    unreachable!("stop --nodes should not use this function");
                }
            }
            SubCommand::Remove { .. } => is_force_stop = true,
            SubCommand::Status { wait, .. } => {
                wait_secs = wait.map_or(-1, |w| w as i32);
            }
            _ => {}
        }

        let context = TaskGenerationContext {
            cmd_arg: &cmd_arg,
            config,
            conn_user,
            tx_bin: &tx_bin,
            wait_secs,
            is_force_stop,
            receiver: None,
        };

        let host_ports = match server_type {
            ServerType::Tx => config.get_host_port_list(DeploymentPackage::EloqTx),
            ServerType::Standby => config.get_host_port_list(DeploymentPackage::EloqStandby),
            ServerType::Voter => config.get_host_port_list(DeploymentPackage::EloqVoter),
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
        let tx_bin = config.deployment.tx_srv_bin();

        let (mut wait_secs, mut is_force_stop) = (-1, false);

        let mut node_host_ports = Vec::new();

        match &cmd_arg {
            SubCommand::Stop { nodes, force, .. } => {
                is_force_stop = *force;
                if server_type == ServerType::Node {
                    node_host_ports = nodes.clone();
                }
            }
            SubCommand::Remove { .. } => is_force_stop = true,
            SubCommand::Status { wait, .. } => {
                wait_secs = wait.map_or(-1, |w| w as i32);
            }
            _ => {}
        }

        let context = TaskGenerationContext {
            cmd_arg: &cmd_arg,
            config,
            conn_user,
            tx_bin: &tx_bin,
            wait_secs,
            is_force_stop,
            receiver,
        };

        let host_ports = match server_type {
            ServerType::Tx => config.get_host_port_list(DeploymentPackage::EloqTx),
            ServerType::Standby => config.get_host_port_list(DeploymentPackage::EloqStandby),
            ServerType::Voter => config.get_host_port_list(DeploymentPackage::EloqVoter),
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

    fn redis_probe_endpoint(&self, host: &str, port: u16) -> (String, u16, bool) {
        let tls_enabled = self.config.deployment.tls_enabled();
        let (endpoint_host, endpoint_port) = self.config.service_endpoint(host, port);
        (endpoint_host, endpoint_port, tls_enabled)
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
impl TaskExecutor for EloqTxCtlTask {
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
            let allow_missing_topology = matches!(self.ctl_cmd.as_ref(), "stop" | "force_stop");

            loop {
                if receiver.has_changed().unwrap_or(false) {
                    // The receiver has changed; get the data
                    let cluster_nodes = receiver.borrow();
                    info!(
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
                    if allow_missing_topology {
                        info!(
                            "Timeout waiting for cluster topology for {}. Falling back to direct process control.",
                            self.task_id.format_string()
                        );
                        break;
                    }
                    let redis_cmd_result = HashMap::from([
                        (
                            CMD.to_string(),
                            TaskArgValue::Str("cluster topology".to_string()),
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
                        "EloqCtlTask"
                    )
                }

                // Avoid busy looping
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        info!(
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
            eloq_cmd_with_port!(TxCtlCmd::Status, tx_bin, user, port).cmd_value();
        let status_error_cmd = check_status_cmd.clone();

        let check_status = eloq_cmd_with_port!(TxCtlCmd::Status, tx_bin, user.to_string(), port);
        let cmd_val = check_status.cmd_value();
        let check_process_status = check_pid(cmd_val, ssh_session.clone(), parse_process_pid).await;

        let ctl_cmd_ref = self.ctl_cmd.as_ref();
        let eloq_ctl_rs = match ctl_cmd_ref {
            "status" => {
                let wait_secs =
                    TaskArgValue::into_inner_value::<i32>(task_arg.get(WAIT_SECS).unwrap().clone());
                if wait_secs >= 0 && matches!(server_type, "txservice" | "standby") {
                    let rp = self.config.redis_password(None);
                    let cs_port: u16 = port.parse().unwrap();
                    let (endpoint_host, endpoint_port, tls_enabled) =
                        self.redis_probe_endpoint(&self.task_id.host, cs_port);
                    let probe_host = if endpoint_host == self.task_id.host {
                        host_value
                    } else {
                        endpoint_host
                    };
                    let probe_result = RedisProbe::with_password_and_tls(
                        probe_host,
                        endpoint_port,
                        rp,
                        tls_enabled,
                    )
                    .probe(wait_secs)
                    .await?;
                    let status_code = probe_result
                        .get(CMD_STATUS)
                        .map(|v| TaskArgValue::into_inner_value::<i32>(v.clone()))
                        .unwrap_or(0);
                    if status_code == 0 {
                        Ok(probe_result)
                    } else {
                        let detail = probe_result
                            .get(CMD_OUTPUT)
                            .map(|v| TaskArgValue::into_inner_value::<String>(v.clone()))
                            .unwrap_or_else(|| "probe failed".to_string());
                        Ok(HashMap::from([
                            (
                                CMD.to_string(),
                                TaskArgValue::Str(format!(
                                    "Dial Redis={}://{}:{}",
                                    if tls_enabled { "rediss" } else { "redis" },
                                    self.task_id.host,
                                    endpoint_port
                                )),
                            ),
                            (CMD_STATUS.to_string(), TaskArgValue::Number(0)),
                            (
                                CMD_OUTPUT.to_string(),
                                TaskArgValue::Str(format!("eloqkv service is down: {detail}")),
                            ),
                        ]))
                    }
                } else {
                    check_process_status
                }
            }
            "stop" | "force_stop" => {
                let stop_cmd = self.ctl_cmd.cmd_value();
                let mut target_ports: Vec<String> = Vec::new();
                match server_type {
                    "txservice" => {
                        target_ports = master_host_ports
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
                        target_ports = standby_host_ports
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
                        target_ports.push(port.to_string());
                    }
                    _ => {
                        unreachable!("Unknown server type: {}", server_type);
                    }
                }
                if target_ports.is_empty() {
                    target_ports.push(port.to_string());
                }

                let runtime_ports = target_ports
                    .iter()
                    .filter_map(|p| p.parse::<u16>().ok())
                    .flat_map(|p| critical_runtime_ports(&self.config, p))
                    .unique()
                    .collect::<Vec<_>>();

                // Modify stop_cmd to use grep with the matched ports
                if !target_ports.is_empty() {
                    let port_pattern = target_ports.join("|");
                    let re = Regex::new(r"grep \d+").unwrap();
                    let modified_stop_cmd =
                        re.replace(&stop_cmd, &format!("grep -E '{}'", port_pattern));
                    info!("Modified stop_cmd: {}", modified_stop_cmd.to_string());

                    let modified_check_status_cmd =
                        re.replace(&check_status_cmd, &format!("grep -E '{}'", port_pattern));
                    info!(
                        "Modified check_status_cmd: {}",
                        modified_check_status_cmd.to_string()
                    );

                    // should not check using port from config
                    let mut stop_result = wait_command_complete!(
                        modified_stop_cmd.to_string(),
                        modified_check_status_cmd.to_string(),
                        ssh_session.clone(),
                        is_none
                    )?;
                    if !wait_tcp_ports_state(
                        &ssh_session,
                        &runtime_ports,
                        false,
                        Duration::from_secs(120),
                    )
                    .await?
                    {
                        return Err(anyhow!(
                            "timed out waiting for runtime ports to be released on {}: {:?}",
                            self.task_id.host,
                            runtime_ports
                        ));
                    }
                    if let Some(output) = stop_result.get(CMD_OUTPUT) {
                        let detail = TaskArgValue::into_inner_value::<String>(output.clone());
                        stop_result.insert(
                            CMD_OUTPUT.to_string(),
                            TaskArgValue::Str(format!(
                                "{detail}, runtime ports released={runtime_ports:?}"
                            )),
                        );
                    }
                    Ok(stop_result)
                } else {
                    info!("No matching ports found for the given host.");
                    let mut stop_result = tx_ctl!(self, check_process_status, {!=, PID_NOT_FOUND}, async || -> anyhow::Result<ExecutionValue> {
                        wait_command_complete!(stop_cmd, check_status_cmd, ssh_session.clone(), is_none)
                    })?;
                    if !wait_tcp_ports_state(
                        &ssh_session,
                        &runtime_ports,
                        false,
                        Duration::from_secs(120),
                    )
                    .await?
                    {
                        return Err(anyhow!(
                            "timed out waiting for runtime ports to be released on {}: {:?}",
                            self.task_id.host,
                            runtime_ports
                        ));
                    }
                    if let Some(output) = stop_result.get(CMD_OUTPUT) {
                        let detail = TaskArgValue::into_inner_value::<String>(output.clone());
                        stop_result.insert(
                            CMD_OUTPUT.to_string(),
                            TaskArgValue::Str(format!(
                                "{detail}, runtime ports released={runtime_ports:?}"
                            )),
                        );
                    }
                    Ok(stop_result)
                }
            }
            "start" => {
                let start_cmd = self.ctl_cmd.cmd_value();
                info!("start_cmd: {}", start_cmd);
                info!("check_status_cmd: {}", check_status_cmd);
                let start_cmd_for_wait = start_cmd.clone();
                let mut process_started = tx_ctl!(self, check_process_status, {==, PID_NOT_FOUND}, async || -> anyhow::Result<ExecutionValue> {
                     wait_command_complete!(start_cmd_for_wait, check_status_cmd, ssh_session.clone(), is_some)
                })?;
                let base_port: u16 = port
                    .parse()
                    .map_err(|e| anyhow!("invalid service port {port}: {e}"))?;
                let runtime_ports = critical_runtime_ports(&self.config, base_port);
                if !wait_tcp_ports_state(
                    &ssh_session,
                    &runtime_ports,
                    true,
                    Duration::from_secs(120),
                )
                .await?
                {
                    return Err(anyhow!(
                        "timed out waiting for runtime ports to listen on {}: {:?}",
                        self.task_id.host,
                        runtime_ports
                    ));
                }
                let (endpoint_host, endpoint_port, tls_enabled) =
                    self.redis_probe_endpoint(&self.task_id.host, base_port);
                let probe = RedisProbe::with_password_and_tls(
                    endpoint_host,
                    endpoint_port,
                    self.config.redis_password(None),
                    tls_enabled,
                );
                probe.probe(120).await.map_err(|e| {
                    anyhow!(
                        "service started but Redis is not ready on {}:{} after waiting: {e}",
                        self.task_id.host,
                        endpoint_port
                    )
                })?;
                if let Some(output) = process_started.get(CMD_OUTPUT) {
                    let start_output = TaskArgValue::into_inner_value::<String>(output.clone());
                    process_started.insert(CMD.to_string(), TaskArgValue::Str(start_cmd));
                    process_started.insert(
                        CMD_OUTPUT.to_string(),
                        TaskArgValue::Str(format!(
                            "start process check={start_output}, runtime ports listening={runtime_ports:?}, redis probe succeeded"
                        )),
                    );
                }
                Ok(process_started)
            }
            _ => {
                unreachable!("Unrecognized command: {}", ctl_cmd_ref);
            }
        };

        ssh_session.close().await?;
        let mut ctl_rtn_value = match eloq_ctl_rs {
            Ok(value) => value,
            Err(err) if ctl_cmd_ref == "status" => HashMap::from([
                (CMD.to_string(), TaskArgValue::Str(status_error_cmd)),
                (CMD_STATUS.to_string(), TaskArgValue::Number(0)),
                (
                    CMD_OUTPUT.to_string(),
                    TaskArgValue::Str(format!("eloqkv service is down: {err}")),
                ),
            ]),
            Err(err) => return Err(err),
        };
        if ctl_cmd_ref == "status" {
            ctl_rtn_value.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
        }
        if ctl_cmd_ref == "status" && ctl_rtn_value.contains_key(PROCESS_PID) {
            let pid = TaskArgValue::into_inner_value::<String>(
                ctl_rtn_value.get(PROCESS_PID).unwrap().clone(),
            );
            if pid == PID_NOT_FOUND {
                let output = "\neloqkv service is down.".to_string();
                ctl_rtn_value.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(output));
            } else {
                let output = format!("\neloqkv service is running, pid: {}.", pid);
                ctl_rtn_value.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(output));
            }
        }
        let exec_cmd = if let Some(cmd) = ctl_rtn_value.get(CMD) {
            cmd.clone().into_inner_value()
        } else {
            self.ctl_cmd.cmd_value()
        };
        task_return_value!(
            ctl_rtn_value,
            |status_code: i32| -> CmdErr { CmdErr::EloqCtlErr(exec_cmd, status_code.to_string()) },
            "EloqCtlTask"
        )
    }
}
