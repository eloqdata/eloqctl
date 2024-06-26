use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::cassandra_op_task::CassandraOpTask;
use crate::cli::task::task_base::{
    CmdErr::CassandraCtlErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
    TaskInstance,
};
use crate::cli::task::task_utils::{check_pid, PROCESS_PID};
use crate::cli::{CommandArgs, CMD_STATUS};
use crate::config::config_base::DeploymentConfig;
use crate::config::storage_service_config::Cassandra;
use crate::config::DeploymentPackage;
use crate::get_ctl_cmd_string;
use anyhow::anyhow;
use async_trait::async_trait;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;
use std::time::Duration;
use strum_macros::AsRefStr;
use tracing::{debug, error, info, warn};
use users::{get_current_gid, get_current_uid};

pub(crate) const CASSANDRA_CMD_STR: &str = "cassandra_cmd";
const JAVA_HOME: &str = "dirname $(dirname `readlink -f /etc/alternatives/java`)";

#[macro_export]
macro_rules! cassandra_cmd {
    ($cmd:ty, $cassandra_home:expr, $conn_user:expr) => {{
        let cmd_var = stringify!($cmd);
        let cmd_user = $conn_user;
        let echo_cmd=format!("ps uxwe -u {} | grep {} | grep -v grep", cmd_user, $cassandra_home);
        match cmd_var {
            "CassandraCmd::Start" => {
                let mut opts = "-f".to_string();
                if get_current_uid() == 0 || get_current_gid() == 0 {
                    opts.push_str(" -R");
                }
                CassandraCmd::Start(format!(
                    r#"mkdir -p {}/logs && cd {} && export JAVA_HOME=$({}); {}/bin/cassandra {} > {}/logs/cassandra_start.log 2>&1 &"#,
                    $cassandra_home, $cassandra_home, JAVA_HOME, $cassandra_home, opts, $cassandra_home
                ))
            }
            "CassandraCmd::Status" => CassandraCmd::Status("select keyspace_name,durable_writes from system_schema.keyspaces".to_string()),
            "CassandraCmd::Stop" => {
                let kill_cass = r#"| awk '{print $2}' | xargs kill"#;
                CassandraCmd::Stop(format!("{echo_cmd} {kill_cass}"))
            },
            "CassandraCmd::ProcessInfo" => {
                let print_pid = r#"| awk '{print $2,$11}'"#;
                let pid_cmd = format!("{} {}", echo_cmd, print_pid);
                let pid_cwd = r#" awk '{printf "%s", sep $0; sep = "+"}; END {if (NR) print ""}'"#;
                let final_cmd = format!("{} | {}", pid_cmd, pid_cwd);
                CassandraCmd::ProcessInfo(final_cmd)
            },
            _=> {
               unreachable!()
            }
        }
    }};
}

#[macro_export]
macro_rules! cassandra_ctl {
    ($task_host:expr,$cmd:expr, $cmd_var:ident, $ssh_conn:expr, $self:ident, $check_fn:ident) => {{
        let cmd_rs = match $cmd.clone() {
            CassandraCmd::$cmd_var(_cmd) => {
                let exec_rs = $self.cassandra_pid($ssh_conn.clone(), $task_host).await;
                if let Ok(ref cmd_exec_rs) = exec_rs {
                    let pid_rs_value = cmd_exec_rs.get(PROCESS_PID).unwrap();
                    let cassandra_pid =
                        TaskArgValue::into_inner_value::<String>(pid_rs_value.clone());
                    let pid_opt = if cassandra_pid == "NONE" {
                        None
                    } else {
                        Some(cassandra_pid)
                    };
                    if pid_opt.$check_fn() {
                        $self.execute_cassandra_cmd($ssh_conn, $cmd.clone()).await
                    } else {
                        exec_rs
                    }
                } else {
                    Err(anyhow!(exec_rs.err().unwrap().to_string()))
                }
            }
            _ => {
                unreachable!()
            }
        };
        cmd_rs
    }};
}

#[derive(Clone, Debug, Eq, PartialEq, AsRefStr)]
pub enum CassandraCmd {
    #[strum(serialize = "start")]
    Start(String),
    #[strum(serialize = "stop")]
    Stop(String),
    #[strum(serialize = "status")]
    Status(String),
    #[strum(serialize = "processInfo")]
    ProcessInfo(String),
}

impl CassandraCmd {
    pub fn from_string(cmd_str: &str, home: String, user: String) -> Self {
        let echo_cmd = format!("ps uxwe -u {user} | grep {home} | grep -v grep");
        match cmd_str {
            "start" => {
                let mut opts = "-f".to_string();
                if get_current_uid() == 0 || get_current_gid() == 0 {
                    opts.push_str(" -R");
                }
                let cmd = format!(
                    "mkdir -p {home}/logs && cd {home} && export JAVA_HOME=$({JAVA_HOME}); {home}/bin/cassandra {opts} > {home}/logs/start.log 2>&1 &",
                );
                CassandraCmd::Start(cmd)
            }
            "stop" => {
                let kill_cass = "awk '{print $2}' | xargs kill";
                CassandraCmd::Stop(format!("{echo_cmd} | {kill_cass}"))
            }
            "status" => CassandraCmd::Status(
                "select keyspace_name,durable_writes from system_schema.keyspaces".to_owned(),
            ),
            "processinfo" => {
                let print_pid = "awk '{print $2,$11}'";
                // let pid_cwd = r#" awk '{printf "%s", sep $0; sep = "+"}; END {if (NR) print ""}' "#;
                let final_cmd = format!("{echo_cmd} | {print_pid}");
                CassandraCmd::ProcessInfo(final_cmd)
            }
            _ => {
                unreachable!()
            }
        }
    }
}

get_ctl_cmd_string!(CassandraCmd, ProcessInfo, Start, Status, Stop);

#[derive(Clone, Debug)]
pub struct CassandraCtlTask {
    config: DeploymentConfig,
    task_id: TaskId,
}

impl CassandraCtlTask {
    pub fn from_config(
        cmd: CommandArgs,
        config: &DeploymentConfig,
    ) -> IndexMap<TaskId, TaskInstance> {
        let cassandra_task_ctrl_attr = match cmd {
            CommandArgs::Start { cluster: _ } | CommandArgs::Restart { cluster: _ } => (
                "start",
                TaskId {
                    cmd: "start".to_string(),
                    task: "cassandra-start".to_string(),
                    host: "_NONE".to_string(),
                },
            ),
            CommandArgs::Install { cluster: _ } => (
                "start",
                TaskId {
                    cmd: "start".to_string(),
                    task: "cassandra-bootstarp".to_string(),
                    host: "_NONE".to_string(),
                },
            ),
            CommandArgs::Stop {
                cluster: _,
                force: _,
                all: _,
            } => (
                "stop",
                TaskId {
                    cmd: "stop".to_string(),
                    task: "cassandra-stop".to_string(),
                    host: "_NONE".to_string(),
                },
            ),
            _ => {
                unreachable!()
            }
        };
        let cmd_str = cassandra_task_ctrl_attr.0;
        let task_id = cassandra_task_ctrl_attr.1;
        let conn_user = config.connection.clone().username;
        let ssh_port = config.connection.ssh_port();
        let cassandra_hosts = config.get_host_list(DeploymentPackage::Storage);
        cassandra_hosts
            .iter()
            .map(|host| {
                let mut task_id_final = task_id.clone();
                task_id_final.host = host.clone();
                (
                    task_id_final.clone(),
                    TaskInstance {
                        task_input: HashMap::from([(
                            CASSANDRA_CMD_STR.to_string(),
                            TaskArgValue::Str(cmd_str.to_string()),
                        )]),
                        task: Box::new(CassandraCtlTask::new(config.clone(), task_id_final)),
                        task_host: TaskHost::Remote {
                            user: conn_user.clone(),
                            port: ssh_port as usize,
                            hosts: host.clone(),
                        },
                    },
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>()
    }

    // DataStax recommends starting the seed nodes one at a time, and then starting the rest of the nodes.
    pub fn barrier(size: usize) -> Vec<usize> {
        let mut barrier = vec![];
        for i in 1..=size {
            if i <= Cassandra::MAX_SEED {
                barrier.push(1);
            } else {
                barrier.push(size - Cassandra::MAX_SEED);
                break;
            }
        }
        barrier
    }

    pub fn new(config: DeploymentConfig, task_id: TaskId) -> Self {
        Self { config, task_id }
    }

    async fn cassandra_pid(
        &self,
        ssh_conn: SSHSession,
        task_host: TaskHost,
    ) -> anyhow::Result<ExecutionValue> {
        let java_home = ssh_conn.execute(JAVA_HOME).await?.1;
        let conn_user = task_host.ssh_conn_tuple().0;
        let cassandra_home = self.config.deployment.cassandra_home();
        let cassandra_process = CassandraCmd::from_string("processinfo", cassandra_home, conn_user);
        let process_info = cassandra_process.cmd_value();
        check_pid(process_info, ssh_conn, |output| -> Option<i32> {
            let pids = output
                .lines()
                .filter_map(|line| {
                    let p_info = line.split_whitespace().collect_vec();
                    info!("PID={} JAVA_HOME={}", p_info[0], p_info[1]);
                    if p_info[1].contains(&java_home) {
                        Some(p_info[0].parse::<i32>().unwrap())
                    } else {
                        None
                    }
                })
                .collect_vec();
            if pids.len() > 1 {
                warn!(
                    "too many java process so that can't tell cassandra {:?}",
                    pids
                )
            }
            pids.first().copied()
        })
        .await
    }

    async fn cassandra_start(
        &self,
        start_cmd: String,
        ssh_conn: &SSHSession,
    ) -> anyhow::Result<ExecutionValue> {
        let ssh_info = ssh_conn.ssh_conn_info();
        let check_status = CassandraCmd::from_string(
            "status",
            self.config.deployment.cassandra_home(),
            ssh_info.1,
        );
        debug!(
            "CassandraCtlTask check_node_status_cmd={}, start={}",
            check_status.cmd_value(),
            start_cmd
        );
        let start_rs = ssh_conn.command(start_cmd.as_str(), CollectOutput).await?;
        let curr_cass_host = ssh_info.0;
        let cass_port = self
            .config
            .deployment
            .storage_service
            .cassandra
            .as_ref()
            .unwrap()
            .client_port()?;
        let sleep_duration = Duration::from_secs(2);
        let mut timeout_remaining = Duration::from_secs(5 * 60);
        let id = TaskId {
            cmd: "start".to_string(),
            task: "check-cassandra-status".to_string(),
            host: "_local".to_string(),
        };
        let cql = check_status.cmd_value();
        let cassandra_op = CassandraOpTask::new(id, curr_cass_host.clone(), cass_port, cql);
        loop {
            let op_status = cassandra_op
                .execute(TaskHost::Local, HashMap::default())
                .await?
                .unwrap();
            let status_value = op_status.get(CMD_STATUS).unwrap();
            let status_code = TaskArgValue::into_inner_value::<i32>(status_value.clone());
            if status_code == 0 {
                info!("Cassandra instance={:?} UP now", curr_cass_host.clone());
                break;
            } else {
                tokio::time::sleep(sleep_duration).await;
                timeout_remaining -= sleep_duration;
            }
            if timeout_remaining.as_secs() == 0 {
                warn!("Cassandra instance={:?} startup timeout", curr_cass_host);
                return Ok(op_status);
            }
        }
        Ok(start_rs)
    }
}

#[async_trait]
impl TaskExecutor for CassandraCtlTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        let ssh_session = SSHSession::from_task_host(
            task_host.clone(),
            self.config.connection.ssh_auth_key().unwrap(),
        )
        .await?;
        info!("execute {}", self.task_id.pretty_string());
        let cmd_str = TaskArgValue::into_inner_value::<String>(
            task_arg.get(CASSANDRA_CMD_STR).unwrap().clone(),
        );
        let cassandra_home = self.config.deployment.cassandra_home();
        info!(
            "CassandraCtlTask will be run. Cmd={:?}, cassandra_home={:?}",
            cmd_str, cassandra_home
        );
        let conn_user = &self.config.connection.username;
        let cass_cmd = CassandraCmd::from_string(
            &cmd_str.to_ascii_lowercase(),
            cassandra_home,
            conn_user.clone(),
        );

        let exec_rs = self
            .cassandra_pid(ssh_session.clone(), task_host.clone())
            .await?;
        let pid_rs_value = exec_rs.get(PROCESS_PID).unwrap();
        let cassandra_pid = TaskArgValue::into_inner_value::<String>(pid_rs_value.clone());
        let exec_rs = match cass_cmd.clone() {
            CassandraCmd::Start(cmd) => {
                if cassandra_pid == "NONE" {
                    self.cassandra_start(cmd, &ssh_session).await
                } else {
                    info!("cassandra has already started");
                    Ok(exec_rs)
                }
            }
            CassandraCmd::Stop(cmd) => {
                if cassandra_pid == "NONE" {
                    info!("cassandra is not running");
                    Ok(exec_rs)
                } else {
                    ssh_session.command(&cmd, CollectOutput).await
                }
            }
            _ => unreachable!(),
        };

        ssh_session.close().await?;
        if let Err(err) = exec_rs {
            error!(
                "CassandraCtl execute cmd {} failed. cause by {}",
                cass_cmd.as_ref(),
                err.to_string()
            );
            Err(anyhow!(CassandraCtlErr(
                cass_cmd.as_ref().to_string(),
                err.to_string()
            )))
        } else {
            let response = exec_rs.unwrap();
            Ok(Some(response))
        }
    }
}
