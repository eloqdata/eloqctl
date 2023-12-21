use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::task_base::CmdErr;
use crate::cli::task::task_base::{
    ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::task::task_utils::{check_pid, parse_process_pid, PROCESS_PID};
use crate::cli::CommandArgs;
use crate::cli::CMD_STATUS;
use crate::config::config_base::DeploymentConfig;
use crate::config::log_service::LogProcessKey;
use crate::{get_ctl_cmd_string, task_return_value};
use futures::future;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;
use std::future::Future;
use tracing::info;

const CLUSTER_COMMAND_STR: &str = "cluster_cmd";
const FIND_LOG_PROCESS_CMD: &str = r"ps uxwe -u _USER | grep '_LOG_BIN_CMD' | grep '_STORAGE_PATH' \
 ~ | grep -v grep | awk '{print _COLUMN}'";

// const AWK_PRINT_PID: &str =
//     r#"awk '{printf "%s", sep $0; sep = "_SEP"}; END {if (NR) print ""}'"#;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum LogCtlCmd {
    Start(String),
    Stop(String),
    Status(String),
}

get_ctl_cmd_string!(LogCtlCmd, Start, Stop, Status);

impl LogCtlCmd {
    pub fn build_cmd(
        config: &DeploymentConfig,
        cmd_arg: CommandArgs,
    ) -> HashMap<LogProcessKey, LogCtlCmd> {
        LogCtlCmd::build_cmd_with_predicate(
            config,
            cmd_arg,
            None::<Box<dyn Fn(String, u16) -> bool>>,
        )
    }

    fn build_cmd_with_predicate<F>(
        config: &DeploymentConfig,
        cmd_arg: CommandArgs,
        test: Option<Box<F>>,
    ) -> HashMap<LogProcessKey, LogCtlCmd>
    where
        F: Fn(String, u16) -> bool + ?Sized,
    {
        let log_home_dir_binding = config.log_home_dir();
        let log_home = log_home_dir_binding.as_str();

        let home_dir = config.install_dir();
        let user = &config.connection.username;
        let log_srv = config.deployment.log_service.as_ref().unwrap();
        let log_cmd_binding = log_srv.log_start_cmd();
        let log_cmds = log_cmd_binding.values().flatten().collect_vec();

        log_cmds
            .iter()
            .filter(|log_item| {
                let log_port = log_item.log_member.port;
                let host = &log_item.log_member.member_host;
                if let Some(predicate) = &test {
                    predicate(host.clone(), log_port)
                } else {
                    true
                }
            })
            .map(|cmd_items| {
                let log_port = cmd_items.log_member.port;
                let host = &cmd_items.log_member.member_host;

                let ps_cmd_part = FIND_LOG_PROCESS_CMD
                    .replace("_USER", user)
                    .replace("_LOG_BIN_CMD", format!("{log_home}/bin/launch_sv").as_str())
                    .replace("_STORAGE_PATH", cmd_items.log_member.storage_path.as_str())
                    .replace("_COLUMN", "$2");
                let log_start_cmd = format!(
                    "export LD_LIBRARY_PATH={log_home}/lib:$LD_LIBRARY_PATH;\
                    /bin/bash {home_dir}/start_tx_log_{host}_{log_port}.bash"
                );
                let log_cmd = match &cmd_arg {
                    CommandArgs::Start { cluster: _ } => LogCtlCmd::Start(log_start_cmd),
                    CommandArgs::Status {
                        cluster: _,
                        user: _,
                        password: _,
                    } => LogCtlCmd::Status(ps_cmd_part),
                    CommandArgs::Stop {
                        cluster: _,
                        force,
                        all: _,
                    } => {
                        let ps_log_info = ps_cmd_part;
                        let is_force = force.is_some();
                        let stop_cmd_string = if is_force {
                            format!("{ps_log_info} | xargs kill -9")
                        } else {
                            format!("{ps_log_info} | xargs kill")
                        };
                        LogCtlCmd::Stop(stop_cmd_string)
                    }
                    CommandArgs::LogService {
                        cluster: _,
                        command: log_cmd,
                    } => {
                        let log_cmd_str = log_cmd.as_str();
                        if log_cmd_str.is_empty() {
                            panic!("LogService command only support start | stop");
                        }
                        match log_cmd_str.to_lowercase().as_str() {
                            "start" => LogCtlCmd::Start(log_start_cmd),
                            "stop" => LogCtlCmd::Stop(format!("{ps_cmd_part} | xargs kill")),
                            "status" => LogCtlCmd::Status(ps_cmd_part),
                            _ => unreachable!(),
                        }
                    }
                    _ => unreachable!(),
                };
                let process_key = LogProcessKey {
                    host: host.clone(),
                    port: log_port,
                };
                (process_key, log_cmd)
            })
            .collect::<HashMap<LogProcessKey, LogCtlCmd>>()
    }
}

#[derive(Clone, Debug)]
pub struct MonographLogCtlTask {
    config: DeploymentConfig,
    task_id: TaskId,
    log_cmd: HashMap<LogProcessKey, LogCtlCmd>,
}

impl MonographLogCtlTask {
    pub fn new(
        config: DeploymentConfig,
        task_id: TaskId,
        log_cmd: HashMap<LogProcessKey, LogCtlCmd>,
    ) -> Self {
        Self {
            config,
            task_id,
            log_cmd,
        }
    }

    fn cluster_cmd_string(cmd_arg: CommandArgs) -> String {
        let cmd_ref = cmd_arg.as_ref();
        match cmd_ref {
            "start" | "stop" | "status" => cmd_ref.to_string(),
            "log-srv" => match cmd_arg {
                CommandArgs::LogService {
                    cluster: _,
                    command: log_cmd,
                } => log_cmd,
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    pub fn from_config(
        cmd_arg: CommandArgs,
        config: &DeploymentConfig,
    ) -> IndexMap<TaskId, TaskInstance> {
        let deployment_ref = &config.deployment;
        let has_log_srv = deployment_ref.log_service.is_some();
        if !has_log_srv {
            return IndexMap::new();
        }
        let log_cmd_by_key = LogCtlCmd::build_cmd(config, cmd_arg.clone());
        let user = &config.connection.username;
        let port = config.connection.ssh_port() as usize;

        let cluster_arg_ref = MonographLogCtlTask::cluster_cmd_string(cmd_arg.clone());
        log_cmd_by_key
            .iter()
            .into_group_map_by(|(process_key, _cmd)| process_key.host.clone())
            .into_iter()
            .map(|(host, key_cmd_pair)| {
                let task_host = TaskHost::Remote {
                    user: user.to_string(),
                    port,
                    hosts: host.to_string(),
                };
                let task_id = TaskId {
                    cmd: format!("monograph_log_{cluster_arg_ref}"),
                    task: cmd_arg.as_ref().to_string(),
                    host,
                };

                let log_cmd = key_cmd_pair
                    .iter()
                    .map(|pair| (pair.0.clone(), pair.1.clone()))
                    .collect::<HashMap<LogProcessKey, LogCtlCmd>>();

                (
                    task_id.clone(),
                    TaskInstance {
                        task_input: HashMap::from([(
                            CLUSTER_COMMAND_STR.to_string(),
                            TaskArgValue::Str(cluster_arg_ref.to_string()),
                        )]),
                        task: Box::new(MonographLogCtlTask::new(config.clone(), task_id, log_cmd)),
                        task_host,
                    },
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>()
    }

    // If there are multiple processes on a node, the execution results of these processes are merged.
    // 1. cmd/cmd_output value1; value2
    // 2. cmd_status v1+v2
    // Note: that the merged return code does not change the original semantics.
    fn merge_execution_value(
        &self,
        input_execution_value: HashMap<LogProcessKey, ExecutionValue>,
    ) -> ExecutionValue {
        input_execution_value
            .iter()
            .flat_map(|(_key, cmd_result)| {
                cmd_result
                    .iter()
                    .map(|(cmd_key, cmd_rs)| (cmd_key.clone(), cmd_rs.clone()))
                    .collect_vec()
            })
            .into_group_map_by(|pair| pair.0.clone())
            .into_iter()
            .map(|(key, record)| {
                let key_str = key.as_str();
                let merged_task_value = match key_str {
                    CMD_STATUS => {
                        let status_acc = record
                            .into_iter()
                            .map(|(_key, task_val)| task_val.into_inner_value::<i32>())
                            .sum::<i32>();
                        //.fold(0_usize, |acc, x| acc + x);
                        TaskArgValue::Number(status_acc)
                    }
                    _ => TaskArgValue::Str(
                        record
                            .into_iter()
                            .map(|(_key, cmd_rtn_value)| cmd_rtn_value.to_string())
                            .join(";"),
                    ),
                };
                (key.to_string(), merged_task_value)
            })
            .collect::<ExecutionValue>()
    }

    async fn join_all_command_result<Fut>(
        cmd_result: Vec<Fut>,
    ) -> HashMap<LogProcessKey, ExecutionValue>
    where
        Fut: Future<Output = (LogProcessKey, anyhow::Result<ExecutionValue>)> + Sized,
    {
        let join_result = future::join_all(cmd_result).await;
        join_result
            .iter()
            .filter(|(_, rs)| rs.is_ok())
            .map(|(key, rs)| {
                let value = rs.as_ref().unwrap();
                (key.clone(), value.clone())
            })
            .collect::<HashMap<LogProcessKey, ExecutionValue>>()
    }

    async fn log_service_pid(
        &self,
        ssh_session: &SSHSession,
    ) -> anyhow::Result<HashMap<LogProcessKey, ExecutionValue>> {
        let cluster_status_cmd = CommandArgs::Status {
            cluster: self.config.deployment.cluster_name.to_string(),
            user: None,
            password: None,
        };
        // key is host:port,value is ps log command.
        let check_status_cmd_by_key = self
            .log_cmd
            .iter()
            .flat_map(|(process_key, _log_cmd)| {
                LogCtlCmd::build_cmd_with_predicate(
                    &self.config,
                    cluster_status_cmd.clone(),
                    Some(Box::new(|host: String, port| -> bool {
                        process_key.host.eq(host.as_str()) && port == process_key.port
                    })),
                )
            })
            .collect::<HashMap<LogProcessKey, LogCtlCmd>>();

        let cmd_result = check_status_cmd_by_key
            .iter()
            .map(|(key, status_cmd)| async {
                let cmd_as_string = status_cmd.cmd_value();
                let status_rs =
                    check_pid(cmd_as_string, ssh_session.clone(), parse_process_pid).await;
                (key.clone(), status_rs)
            })
            .collect_vec();

        Ok(MonographLogCtlTask::join_all_command_result(cmd_result).await)
    }
}

#[async_trait::async_trait]
impl TaskExecutor for MonographLogCtlTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        let cluster_mgr_cmd = task_arg.get(CLUSTER_COMMAND_STR).unwrap();
        let cluster_cmd_string = cluster_mgr_cmd.clone().into_inner_value::<String>();
        println!("{} execute.\n", self.task_id.pretty_string());
        let ssh_session =
            SSHSession::from_task_host(task_host, self.config.connection.ssh_auth_key().unwrap())
                .await?;
        let pid_cmd_value = self.log_service_pid(&ssh_session).await?;
        let cmd_execution_result = if cluster_cmd_string.eq("status") {
            self.merge_execution_value(pid_cmd_value)
        } else {
            let execution_cmd_vec = self
                .log_cmd
                .iter()
                .filter(|(key, _ctrl_cmd)| {
                    let execution_value = pid_cmd_value.get(key).unwrap();
                    let pid = TaskArgValue::into_inner_value::<String>(
                        execution_value.get(PROCESS_PID).unwrap().clone(),
                    );
                    println!(
                        "MonographLogCtlTask found pid={pid}, command={cluster_cmd_string} {key:#?}"
                    );
                    if cluster_cmd_string.eq("stop") {
                        //stop and There are still log process alive
                        !pid.eq("NONE")
                    } else if cluster_cmd_string.eq("start") {
                        //start and There are still unstarted log processes
                        pid.eq("NONE")
                    } else {
                        unreachable!()
                    }
                })
                .map(|(key, ctl_cmd)| (key.clone(), ctl_cmd.clone()))
                .collect::<HashMap<LogProcessKey, LogCtlCmd>>();

            if execution_cmd_vec.is_empty() {
                self.merge_execution_value(pid_cmd_value)
            } else {
                let cmd_result = execution_cmd_vec
                    .iter()
                    .map(|(key, ctl_cmd)| async {
                        let cmd_value_string = ctl_cmd.cmd_value();
                        println!("MonographLogCtlTask send command={cmd_value_string}");
                        let cmd_result = ssh_session
                            .command(cmd_value_string.as_str(), CollectOutput)
                            .await;

                        (key.clone(), cmd_result)
                    })
                    .collect_vec();
                let all_cmd_result = MonographLogCtlTask::join_all_command_result(cmd_result).await;
                self.merge_execution_value(all_cmd_result)
            }
        };
        ssh_session.close().await?;
        task_return_value!(
            cmd_execution_result,
            |status_code: i32| -> CmdErr {
                CmdErr::ExecUserCmdErr(cluster_cmd_string.clone(), status_code.to_string())
            },
            "MonographLogCtlTask"
        )
    }
}
