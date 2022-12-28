use crate::cli::config::{DeploymentConfig, DeploymentService};
use crate::cli::task::ssh_conn::SSHConn;
use crate::cli::task::task_base::{
    CmdErr::CassandraCtlErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
    TaskInstance,
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

pub(crate) const CASSANDRA_CMD_STR: &str = "cassandra_cmd";

#[macro_export]
macro_rules! cassandra_cmd {
    ( $cmd:ty, $cassandra_home:expr $(, $cmd_arg:expr)? $(,)?) => {{
        use $crate::cli::task::task_base::REMOTE_ENV_PROPS;
        let cmd_var = stringify!($cmd);
        let remote_env_props = REMOTE_ENV_PROPS.as_ref().unwrap();
        let java_home = remote_env_props.get("JAVA_HOME").unwrap();
        match cmd_var {
            "CassandraCmd::Start" => CassandraCmd::Start(format!(
                r#"mkdir -p {}/logs && cd {} && export JAVA_HOME={}; {}/bin/cassandra -f > {}/logs/cassandra_start.log 2>&1 &"#,
                //r#"bash {}/start_cassandra.bash"#,
                $cassandra_home, $cassandra_home, java_home, $cassandra_home, $cassandra_home
            )),
            "CassandraCmd::Status" => CassandraCmd::Status(format!("export JAVA_HOME={}; {}/bin/nodetool status | grep -v ^$ | tail -n 1", java_home, $cassandra_home)),
            $("CassandraCmd::Stop" => {
                let pid = $cmd_arg;
                CassandraCmd::Stop(format!("kill {}", pid))
            },
            "CassandraCmd::ProcessInfo" => {
                let cmd_user = $cmd_arg;
                let echo_cmd=format!(" echo `ps uxwe -u {} | grep {} | grep -v grep", cmd_user, $cassandra_home);
                let print_pid = r#"| awk '{print $2}'`"#;
                let pid_cmd = format!("{} {}", echo_cmd, print_pid);
                let pid_cwd = r#"{ read pid; cmd="readlink /proc/$pid/cwd"; output=`eval $cmd`; echo "$pid:$output";}"#;
                let final_cmd = format!("{} | {}", pid_cmd, pid_cwd);
                CassandraCmd::ProcessInfo(final_cmd)
            },)*
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
                let running_rs = $self.cassandra_pid($ssh_conn.clone(), $task_host);
                if let Ok(pid_opt) = running_rs {
                    if pid_opt.$check_fn() {
                        $self.execute_cassandra_cmd($ssh_conn, $cmd.clone())
                    } else {
                        Ok(true)
                    }
                } else {
                    Err(anyhow!(running_rs.err().unwrap().to_string()))
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
    #[strum(serialize = "Start")]
    Start(String),
    #[strum(serialize = "Stop")]
    Stop(String),
    #[strum(serialize = "Status")]
    Status(String),
    #[strum(serialize = "ProcessInfo")]
    ProcessInfo(String),
}

impl CassandraCmd {
    pub fn from_string(cmd_str: String, cassandra_home: String, conn_user: Option<String>) -> Self {
        match cmd_str.to_lowercase().as_str() {
            "start" => {
                cassandra_cmd!(CassandraCmd::Start, cassandra_home)
            }
            "stop" => {
                cassandra_cmd!(CassandraCmd::Stop, cassandra_home)
            }
            "status" => {
                cassandra_cmd!(CassandraCmd::Status, cassandra_home)
            }
            "processinfo" => {
                let user = conn_user.unwrap();
                cassandra_cmd!(CassandraCmd::ProcessInfo, cassandra_home, user)
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
    pub fn from_config(cmd: CommandArgs, config: &DeploymentConfig) -> Vec<TaskInstance> {
        let cassandra_task_ctrl_attr = match cmd {
            CommandArgs::Start { cluster: _ }
            | CommandArgs::Install { cluster: _ }
            | CommandArgs::Restart { cluster: _ } => (
                "start",
                TaskId {
                    cmd: "start".to_string(),
                    task: "cassandra-start".to_string(),
                    host: "_NONE".to_string(),
                },
            ),
            CommandArgs::Stop {
                cluster: _,
                force: _,
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
        let cassandra_hosts = config.get_host_list(DeploymentService::Storage);
        cassandra_hosts
            .iter()
            .map(|host| {
                let mut task_id_final = task_id.clone();
                task_id_final.host = host.clone();
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
                }
            })
            .collect_vec()
    }

    pub fn new(config: DeploymentConfig, task_id: TaskId) -> Self {
        Self { config, task_id }
    }

    fn cassandra_home(&self) -> String {
        format!("{}/apache-cassandra", self.config.install_dir())
    }

    fn cassandra_pid(&self, ssh_conn: SSHConn, task_host: TaskHost) -> anyhow::Result<Option<i32>> {
        let conn_user = task_host.ssh_conn_tuple().0;
        let cassandra_home = self.cassandra_home();
        let cassandra_process =
            cassandra_cmd!(CassandraCmd::ProcessInfo, cassandra_home, conn_user);

        let process_info = cassandra_process.cmd_value();

        check_process_pid(process_info, &ssh_conn, |output| -> Option<i32> {
            let mut pid = None;
            for line in output.lines() {
                let splits = line.split(':').collect_vec();
                if splits.is_empty() || splits.len() == 1 {
                    continue;
                }
                assert_eq!(splits.len(), 2);
                let process_cmd = splits[1];
                if cassandra_home.as_str() == process_cmd {
                    let pid_num = splits[0].parse::<i32>().unwrap();
                    pid = Some(pid_num);
                    info!(
                        "CassandraCtlTask found cassandra process is already running PID={}",
                        pid_num
                    );
                    break;
                }
            }
            pid
        })
    }

    fn cassandra_start(&self, start_cmd: String, ssh_conn: &SSHConn) -> anyhow::Result<bool> {
        let check_status = cassandra_cmd!(CassandraCmd::Status, self.cassandra_home());
        start_service(
            start_cmd,
            check_status.cmd_value(),
            ssh_conn,
            |output| -> bool {
                let mut process_ready = false;
                info!(r#"CassandraCtlTask check_status {:?}"#, output);
                for line in output.lines() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    if !line.contains(char::is_whitespace) {
                        continue;
                    }
                    if line.starts_with("UN") {
                        process_ready = true;
                    }
                }
                process_ready
            },
        )
    }

    pub fn execute_cassandra_cmd(
        &self,
        ssh_conn: &SSHConn,
        cmd: CassandraCmd,
    ) -> anyhow::Result<bool> {
        let ctl_rsp = match cmd {
            CassandraCmd::Stop(stop_cmd) => stop_service(stop_cmd, ssh_conn)?,
            CassandraCmd::Start(start_cmd) => self.cassandra_start(start_cmd, ssh_conn)?,
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
        ssh_conn_info! {
            self.config.connection.clone(),
            task_host.clone(),
            ssh_conn_rs,
            _conn_user,
            _conn_host
        }
        let cmd_str = TaskArgValue::into_inner_value::<String>(
            task_arg.get(CASSANDRA_CMD_STR).unwrap().clone(),
        );
        let cassandra_home = self.cassandra_home();
        info!(
            "CassandraCtlTask will be run. Cmd={:?}, cassandra_home={:?}",
            cmd_str, cassandra_home
        );
        let cmd = CassandraCmd::from_string(cmd_str.clone(), cassandra_home, None);
        let ssh_conn = ssh_conn_rs?;
        let exec_rs = if cmd.as_ref() == "Start" {
            cassandra_ctl!(task_host.clone(), cmd, Start, &ssh_conn, self, is_none)
        } else if cmd.as_ref() == "Stop" {
            cassandra_ctl!(task_host.clone(), cmd, Stop, &ssh_conn, self, is_some)
        } else {
            unreachable!()
        };
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
            println!("CassandraCtl cmd={} success", cmd_str);
            let pid_opt = self.cassandra_pid(ssh_conn, task_host)?;
            let pid = if let Some(pid_val) = pid_opt {
                TaskArgValue::Str(pid_val.to_string())
            } else {
                TaskArgValue::Str("".to_string())
            };
            Ok(Some(HashMap::from([("CASSANDRA_PID".to_string(), pid)])))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cli::task::cassandra_ctl_task::CassandraCmd;

    #[test]
    pub fn test_build_cassandra_cmd() {
        let cassandra_bin = "/data1/opt/mono-poc";
        let cassandra_process = cassandra_cmd!(CassandraCmd::ProcessInfo, cassandra_bin, "mono");
        println!("start = {:#?}", cassandra_process);
    }

    #[test]
    pub fn test_string_compare() {
        let str_value = ":".to_string();
        println!("eq={}", str_value.is_empty() || str_value == ":")
    }
}
