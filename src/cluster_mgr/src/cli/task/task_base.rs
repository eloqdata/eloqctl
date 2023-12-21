use crate::cli::cmd_printer::{CmdPrinter, Printable};
use crate::cli::task::group::init_task_group;
use crate::cli::task::task_controller::TaskController;
use crate::cli::{CommandArgs, CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeploymentConfig;
use crate::config::load_remote_env;
use crate::enum_into_trait;
use async_trait::async_trait;
use dyn_clone::DynClone;
use futures::StreamExt;
use indexmap::IndexMap;
use itertools::Itertools;
use once_cell::sync::{Lazy, OnceCell};
use owo_colors::OwoColorize;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::string::ToString;
use tabled::{display::ExpandedDisplay, Tabled};
use thiserror::Error;
use tracing::{error, info};
use ExecutionValue as LastResult;

pub type EnvProps = HashMap<String, String>;

pub(crate) static REMOTE_ENV_PROPS: Lazy<anyhow::Result<EnvProps>> =
    Lazy::new(|| load_remote_env(None));

#[derive(Clone)]
pub struct TaskExecutionContext {
    pub task_group: String,
    pub barrier: Option<Vec<usize>>,
    pub executable: IndexMap<TaskId, TaskInstance>,
}

impl TaskExecutionContext {
    pub fn list_task_ids(&self) -> Vec<TaskId> {
        self.executable.keys().cloned().collect_vec()
    }
}

#[macro_export]
macro_rules! get_ctl_cmd_string {
    ( $clt_cmd_type:ident, $($cmd_var:ident),* $(,)? ) => {
        impl $clt_cmd_type {
            pub fn cmd_value(&self) -> String {
                 match self.clone() {
                 $(
                   $clt_cmd_type::$cmd_var(cmd) => cmd,
                  )*
                 }
            }
        }
    };
}

enum_into_trait! {TaskValueInto, task_value_into, TaskArgValue}

#[macro_export]
macro_rules! post_task_execute {
    ($execution_rs:expr, $cluster:expr, $task_mame:expr, $command:expr, $task_host:expr) => {{
        let status_tuple = if let Ok(execution) = $execution_rs.as_ref() {
            (0, execution.clone())
        } else {
            (1, None)
        };

        let now: DateTime<Utc> = chrono::Utc::now();
        //let datetime_now = default_utc.format("%Y-%m-%d %H:%M:%S");
        let task_status_entity = TaskStatusEntity {
            cluster_name: $cluster,
            task: String::from($task_mame),
            command: String::from($command),
            task_host: String::from($task_host),
            task_status: status_tuple.0,
            create_timestamp: now,
            update_timestamp: now,
        };
        $crate::cli::task::task_utils::save_task_status(task_status_entity, status_tuple.1).await
    }};
}

#[macro_export]
macro_rules! task_value_into_impl {
    ($({$type_var:ident, $task_type:ty}),*) => {
       $(impl TaskValueInto for $task_type {
           fn task_value_into(task_type: TaskArgValue) -> Self {
              match task_type {
                 TaskArgValue::$type_var(value) => value,
                 _ => unreachable!(),
              }
           }
        })*
    };
}

task_value_into_impl! {
    {Str, String},
    {Number, i32},
    {List, Vec<String>}
}

#[macro_export]
macro_rules! task_return_value {
    ($task_result:expr, $task_err_closure:expr, $task_name:expr $(,$return_value:expr)? ) => {{
        use tracing::info;
        use $crate::cli::CMD_STATUS;
        let task_rs = $task_result.clone();
        let task_status = task_rs.get(CMD_STATUS).unwrap();
        let status_code = TaskArgValue::into_inner_value::<i32>(task_status.clone());
        if status_code != 0 {
            let invoke_closure = $task_err_closure;
            let cmd_err = invoke_closure(status_code);
            info!(
                "task_return_value current status_code != 0, task_name={:?},cmd_err={:?}",
                $task_name, cmd_err
            );
            return Err(anyhow::anyhow!(cmd_err));
        } else {
            return Ok(Some(task_rs));
        }
    }};
}

#[derive(PartialEq, Eq, Clone, Error, Debug)]
pub enum CmdErr {
    #[error("Remote cmd [{0}] execution failed. status_code={1}")]
    ExecUserCmdErr(String, String),
    #[error("Download file failed, download URL {0} , error causes {1}")]
    DownloadErr(String, String),
    #[error("Upload file failed, local file {0}, error causes {1}")]
    UploadErr(String, String),
    #[error("Error establishing ssh connection, user@host={0}, error causes {1}")]
    SSHConnErr(String, String),
    #[error("SSHConn execute remote cmd {0} failed, error causes {1}")]
    SSHRemoteCmdErr(String, String),
    #[error("Error executing apache-cassandra control command {0} failed, error causes {1}")]
    CassandraCtlErr(String, String),
    #[error("MonographDB installation database error. command {0} , error causes {1}")]
    MonographInstallErr(String, String),
    #[error("Failed to execute the MonographDB control command. command {0} , error causes {1}")]
    MonographCtlErr(String, String),
    #[error("The cluster name  must be unique and the current cluster [{0}] already exists.")]
    ClusterAlreadyExists(String),
    #[error("Unpacking file errors. command {0}, error causes {1}")]
    UnpackErr(String, String),
    #[error("Error interacting with cassandra. error causes {0}")]
    CassandraOpErr(String),
    #[error("Error executing LocalCopyTask; please check if the source path exists path {0}")]
    CopyTaskErr(String),
    #[error("Error executing MonographDB monitor component task {0}, error causes {1}")]
    MonitorCtlCmdErr(String, String),
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum TaskArgValue {
    Str(String),
    Number(i32),
    List(Vec<String>),
}

impl ToString for TaskArgValue {
    fn to_string(&self) -> String {
        match self {
            TaskArgValue::Str(string_value) => string_value.to_string(),
            TaskArgValue::Number(num_value) => num_value.to_string(),
            TaskArgValue::List(list_value) => list_value.join(","),
        }
    }
}

impl TaskArgValue {
    pub fn into_inner_value<T: TaskValueInto>(self) -> T {
        TaskValueInto::task_value_into(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskHost {
    Local,
    Remote {
        user: String,
        port: usize,
        hosts: String,
    },
}

impl TaskHost {
    pub fn ssh_conn_tuple(&self) -> (String, usize, String) {
        match self {
            TaskHost::Local => ("_local".to_string(), 22, "localhost".to_string()),
            TaskHost::Remote { user, port, hosts } => (user.clone(), *port, hosts.clone()),
        }
    }
}

pub type ExecutionValue = HashMap<String, TaskArgValue>;
pub type TaskStatusRecord = Vec<HashMap<String, TaskArgValue>>;
// ::new(|| {
//     HashMap::from([(
//         "_FINISH_SIGNAL".to_string(),
//         TaskArgValue::Str("".to_string()),
//     )])
// });
pub(crate) static FINISH_: OnceCell<LastResult> = OnceCell::new();

pub(crate) fn init_finish_signal() -> &'static LastResult {
    FINISH_.get_or_init(|| {
        HashMap::from([(
            "_FINISH_SIGNAL".to_string(),
            TaskArgValue::Str("".to_string()),
        )])
    })
}

#[derive(Clone, Debug, Tabled, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId {
    pub cmd: String,
    pub task: String,
    pub host: String,
}

impl TaskId {
    pub fn format_string(&self) -> String {
        format!("host={},cmd={},task={}", self.host, self.cmd, self.task)
    }

    pub fn pretty_string(&self) -> ExpandedDisplay {
        ExpandedDisplay::new([self.clone()])
    }

    pub fn as_json_string(&self) -> String {
        let task_id_string = serde_json::to_string(self);
        task_id_string.unwrap()
    }

    pub fn from_json_string(task_id_string: String) -> Self {
        let task_id: TaskId = serde_json::from_str(task_id_string.as_str()).unwrap();
        task_id
    }
}

/// The `TaskExecutor` is the smallest business unit part of the command
/// and the smallest parallel execution unit; for example, when executing the deploy command,
/// depending on the configuration, deploy may consist of download, upload, or unpack.
/// And these tasks have sequential dependencies.
#[async_trait]
pub trait TaskExecutor: 'static + Send + Sync + DynClone + Debug {
    fn identifier(&self) -> TaskId;

    /// Execute the task asynchronously and return the result, with `TaskHost` as Remote means
    /// that the task is executed on a remote host via ssh. and task_arg is the input parameter
    /// for the task execution.
    async fn execute(
        &self,
        task_host: TaskHost,
        task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>>;
}

dyn_clone::clone_trait_object!(TaskExecutor);

#[derive(Debug, Clone)]
pub struct TaskInstance {
    pub(crate) task_input: HashMap<String, TaskArgValue>,
    pub(crate) task: Box<dyn TaskExecutor>,
    pub(crate) task_host: TaskHost,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskResultEnum {
    Success(Option<ExecutionValue>),
    Error(String),
}

impl TaskResultEnum {
    pub fn is_success(&self) -> bool {
        match self {
            TaskResultEnum::Success(_) => true,
            TaskResultEnum::Error(_) => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskResultPair {
    pub task_id: String,
    pub result: TaskResultEnum,
}

/// `TaskMgr` is the entry point for task invocation, calling `TaskGroup` and  `TaskController`
/// based on command input.
#[derive(Clone)]
pub struct TaskMgr {
    task_controller: TaskController,
}

impl Default for TaskMgr {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskMgr {
    pub fn new() -> Self {
        Self {
            task_controller: TaskController::new(),
        }
    }
}

impl TaskMgr {
    pub async fn recv_task_result<F, Fut>(&self, f: F)
    where
        F: Fn(TaskResultPair) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut result_reader = self.task_controller.clone().try_stream();
        while let Some(Ok(task_result_pair)) = result_reader.next().await {
            f(task_result_pair).await;
        }
    }

    pub async fn print_task_result(&self) {
        self.recv_task_result(|task_result_pair| async {
            let task_id: String = task_result_pair.task_id;
            let result: TaskResultEnum = task_result_pair.result;
            match result {
                TaskResultEnum::Success(opt_rs) => {
                    let table_printer = CmdPrinter::new();
                    if let Some(execution_value) = opt_rs {
                        table_printer.add_row(
                            task_id,
                            execution_value,
                            |task_id, execution_value| -> Printable {
                                Printable {
                                    task_id,
                                    cmd: TaskArgValue::into_inner_value::<String>(
                                        execution_value.get(CMD).unwrap().clone(),
                                    ),

                                    cmd_status: if TaskArgValue::into_inner_value::<i32>(
                                        execution_value.get(CMD_STATUS).unwrap().clone(),
                                    ) == 0
                                    {
                                        "Success".green().to_string()
                                    } else {
                                        "Failure".red().to_string()
                                    },
                                    cmd_output: TaskArgValue::into_inner_value::<String>(
                                        execution_value.get(CMD_OUTPUT).unwrap().clone(),
                                    ),
                                }
                            },
                        );
                        table_printer.table_print();
                    }
                }
                TaskResultEnum::Error(err_msg) => {
                    let err_msg = format!(r#"task {task_id} failed. cause by {err_msg}"#);
                    error!("{}", err_msg.red());
                }
            }
        })
        .await
    }

    pub async fn task_context(
        &self,
        cmd_args: CommandArgs,
        config: &DeploymentConfig,
    ) -> anyhow::Result<TaskExecutionContext> {
        let group_key = cmd_args.as_ref();

        let task_groups = init_task_group();
        let run_group = task_groups.get(group_key).unwrap();
        run_group.tasks(cmd_args, config.clone()).await
    }

    pub async fn run_tasks(
        &'static self,
        cmd_args: CommandArgs,
        config: DeploymentConfig,
    ) -> anyhow::Result<Vec<TaskResultPair>> {
        let tasks_execution = self.task_context(cmd_args, &config).await?;
        info!(
            "TaskMgr start current barrier={:?}",
            tasks_execution.barrier
        );
        self.task_controller
            .run_all_tasks(tasks_execution, config)
            .await
    }
}
