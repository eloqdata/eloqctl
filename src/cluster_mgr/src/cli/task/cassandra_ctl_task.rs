use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::cassandra_op_task::{CassandraOpTask, CASS_CQL_STMT};
use crate::cli::task::task_base::{
    CmdErr::CassandraCtlErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
    TaskInstance, REMOTE_ENV_PROPS,
};
use crate::cli::task::task_utils::{check_pid, PROCESS_PID};
use crate::cli::{CommandArgs, CMD_STATUS};
use crate::config::config_base::DeploymentConfig;
use crate::config::DeploymentPackage;
use crate::get_ctl_cmd_string;
use anyhow::anyhow;
use async_trait::async_trait;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;
use std::time::Duration;
use strum_macros::AsRefStr;
use tracing::{error, info, warn};

pub(crate) const CASSANDRA_CMD_STR: &str = "cassandra_cmd";

#[macro_export]
macro_rules! cassandra_cmd {
    ($cmd:ty, $cassandra_home:expr, $conn_user:expr) => {{
        use $crate::cli::task::task_base::REMOTE_ENV_PROPS;
        let cmd_var = stringify!($cmd);
        let remote_env_props = REMOTE_ENV_PROPS.as_ref().unwrap();
        let java_home = remote_env_props.get("JAVA_HOME").unwrap();
        let cmd_user = $conn_user;
        let echo_cmd=format!("ps uxwe -u {} | grep {} | grep -v grep", cmd_user, $cassandra_home);
        match cmd_var {
            "CassandraCmd::Start" => CassandraCmd::Start(format!(
                r#"mkdir -p {}/logs && cd {} && export JAVA_HOME={}; {}/bin/cassandra -f > {}/logs/cassandra_start.log 2>&1 &"#,
                $cassandra_home, $cassandra_home, java_home, $cassandra_home, $cassandra_home
            )),
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
    pub fn from_string(cmd_str: String, cassandra_home: String, conn_user: String) -> Self {
        match cmd_str.to_lowercase().as_str() {
            "start" => {
                cassandra_cmd!(CassandraCmd::Start, cassandra_home, conn_user)
            }
            "stop" => {
                cassandra_cmd!(CassandraCmd::Stop, cassandra_home, conn_user)
            }
            "status" => {
                cassandra_cmd!(CassandraCmd::Status, cassandra_home, conn_user)
            }
            "processinfo" => {
                cassandra_cmd!(CassandraCmd::ProcessInfo, cassandra_home, conn_user)
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

    pub fn new(config: DeploymentConfig, task_id: TaskId) -> Self {
        Self { config, task_id }
    }

    fn cassandra_home(&self) -> String {
        format!("{}/apache-cassandra", self.config.install_dir())
    }

    async fn cassandra_pid(
        &self,
        ssh_conn: SSHSession,
        task_host: TaskHost,
    ) -> anyhow::Result<ExecutionValue> {
        let conn_user = task_host.ssh_conn_tuple().0;
        let cassandra_home = self.cassandra_home();
        let remote_env_props = REMOTE_ENV_PROPS.as_ref().unwrap();
        let java_home = remote_env_props.get("JAVA_HOME").unwrap();
        let cassandra_process =
            cassandra_cmd!(CassandraCmd::ProcessInfo, cassandra_home, conn_user);

        let process_info = cassandra_process.cmd_value();
        check_pid(process_info, ssh_conn, |output| -> Option<i32> {
            let mut pid = None;
            for line in output.lines() {
                let p_info_pair = line.split('+').collect_vec();
                println!(
                    "CassandraCtlTask JAVA_HOME={java_home} check_process_info={p_info_pair:#?}"
                );
                if p_info_pair.is_empty() || p_info_pair.len() == 1 {
                    continue;
                }
                let collect_pid = p_info_pair
                    .into_iter()
                    .filter(|split| {
                        let p_info = split.split_whitespace().collect_vec();
                        p_info[1].contains(java_home)
                    })
                    .map(|p_info| p_info.split_whitespace().collect_vec()[0])
                    .collect_vec();
                if !collect_pid.is_empty() {
                    pid = Some(collect_pid[0].parse::<i32>().unwrap());
                    println!(
                        "CassandraCtlTask found cassandra process is already running PID={pid:?}"
                    );
                    break;
                }
            }
            pid
        })
        .await
    }

    async fn cassandra_start(
        &self,
        start_cmd: String,
        ssh_conn: &SSHSession,
    ) -> anyhow::Result<ExecutionValue> {
        let ssh_info = ssh_conn.ssh_conn_info();
        let check_status = cassandra_cmd!(CassandraCmd::Status, self.cassandra_home(), ssh_info.1);
        println!(
            "CassandraCtlTask check_node_status_cmd={}, start={}",
            check_status.cmd_value(),
            start_cmd
        );
        let start_rs = ssh_conn.command(start_cmd.as_str(), CollectOutput).await?;
        let curr_cass_host = ssh_info.0;
        let sleep_duration = Duration::from_secs(2);
        let mut timeout_remaining = Duration::from_secs(5 * 60);
        loop {
            let cassandra_op = CassandraOpTask::new(
                curr_cass_host.clone(),
                TaskId {
                    cmd: "start".to_string(),
                    task: "check-cassandra-status".to_string(),
                    host: "_local".to_string(),
                },
            );
            let op_status = cassandra_op
                .execute(
                    TaskHost::Local,
                    HashMap::from([(
                        CASS_CQL_STMT.to_string(),
                        TaskArgValue::Str(check_status.cmd_value()),
                    )]),
                )
                .await?
                .unwrap();

            let status_value = op_status.get(CMD_STATUS).unwrap();
            let status_code = TaskArgValue::into_inner_value::<i32>(status_value.clone());
            if status_code == 0 {
                println!("Cassandra instance={:?} UP now", curr_cass_host.clone());
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

    pub async fn execute_cassandra_cmd(
        &self,
        ssh_conn: &SSHSession,
        cmd: CassandraCmd,
    ) -> anyhow::Result<ExecutionValue> {
        let ctl_rsp = match cmd {
            CassandraCmd::Stop(stop_cmd) => {
                ssh_conn.command(stop_cmd.as_str(), CollectOutput).await?
            }
            CassandraCmd::Start(start_cmd) => self.cassandra_start(start_cmd, ssh_conn).await?,
            _ => {
                unreachable!()
            }
        };
        Ok(ctl_rsp)
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
        println!("{} execute.\n", self.task_id.pretty_string());
        let cmd_str = TaskArgValue::into_inner_value::<String>(
            task_arg.get(CASSANDRA_CMD_STR).unwrap().clone(),
        );
        let cassandra_home = self.cassandra_home();
        info!(
            "CassandraCtlTask will be run. Cmd={:?}, cassandra_home={:?}",
            cmd_str, cassandra_home
        );
        let conn_user = &self.config.connection.username;
        let cmd = CassandraCmd::from_string(cmd_str, cassandra_home, conn_user.clone());
        let exec_rs = if cmd.as_ref() == "start" {
            cassandra_ctl!(task_host, cmd, Start, &ssh_session, self, is_none)
        } else if cmd.as_ref() == "stop" {
            cassandra_ctl!(task_host, cmd, Stop, &ssh_session, self, is_some)
        } else {
            unreachable!()
        };

        ssh_session.close().await?;
        if let Err(err) = exec_rs {
            error!(
                "CassandraCtl execute cmd {} failed. cause by {}",
                cmd.as_ref(),
                err.to_string()
            );
            Err(anyhow!(CassandraCtlErr(
                cmd.as_ref().to_string(),
                err.to_string()
            )))
        } else {
            let response = exec_rs.unwrap();
            Ok(Some(response))
        }
    }
}
