use crate::cli::config::{load_remote_env, DeploymentConfig};
use crate::cli::task::ssh_conn::{SSH_EXEC_CMD, SSH_EXEC_CMD_OUTPUT, SSH_EXEC_CMD_STATUS};
use crate::cli::task::task_controller::TaskController;
use crate::cli::task::task_group::TASK_GROUP;
use crate::cli::CommandArgs;
use crate::enum_into_trait;
use crate::state::task_status_operation::TaskStatusEntity;
use async_trait::async_trait;
use dyn_clone::DynClone;
use futures::StreamExt;
use owo_colors::OwoColorize;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::string::ToString;
use std::sync::LazyLock;
use tabled::display::ExpandedDisplay;
use tabled::object::{Columns, Rows, Segment};
use tabled::{Alignment, Modify, ModifyObject, Table, Tabled, Width};
use thiserror::Error;
use tracing::error;
use ExecutionValue as LastResult;

pub type EnvProps = HashMap<String, String>;

pub(crate) static REMOTE_ENV_PROPS: LazyLock<anyhow::Result<EnvProps>> =
    LazyLock::new(|| load_remote_env(None));

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

        let task_status_entity = TaskStatusEntity {
            cluster_name: $cluster,
            task: String::from($task_mame),
            command: String::from($command),
            task_host: String::from($task_host),
            task_status: status_tuple.0,
            create_timestamp: Default::default(),
            update_timestamp: Default::default(),
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
    {Number, usize},
    {List, Vec<String>}
}

#[macro_export]
macro_rules! task_return_value {
    ($task_result:expr, $task_err_closure:expr, $task_name:expr $(,$return_value:expr)? ) => {{
        use $crate::cli::task::ssh_conn::SSH_EXEC_CMD_STATUS;
        let task_rs = $task_result.clone();
        let task_status = task_rs.get(SSH_EXEC_CMD_STATUS).unwrap();
        let status_code = TaskArgValue::into_inner_value::<usize>(task_status.clone());
        if status_code != 0 {
            println!(
                "{} execution failure status_code={}",
                $task_name, status_code
            );
            return Err(anyhow::anyhow!($task_err_closure(status_code)));
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
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TaskArgValue {
    Str(String),
    Number(usize),
    List(Vec<String>),
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

pub(crate) static FINISH_: LazyLock<LastResult> = LazyLock::new(|| {
    HashMap::from([(
        "_FINISH_SIGNAL".to_string(),
        TaskArgValue::Str("".to_string()),
    )])
});

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
        ExpandedDisplay::new(&[self.clone()])
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskResultPair {
    pub(crate) task_id: String,
    pub(crate) result: TaskResultEnum,
}

#[derive(Tabled, Clone, Debug)]
pub struct PrintableTaskResult {
    task_id: String,
    cmd: String,
    cmd_status: String,
    cmd_output: String,
}

#[derive(Debug)]
struct TablePrinter {
    data: RefCell<Vec<PrintableTaskResult>>,
}

impl TablePrinter {
    pub(crate) fn new() -> Self {
        Self {
            data: RefCell::new(vec![]),
        }
    }

    pub(crate) fn add_row(&self, task_id: String, execution_value: ExecutionValue) {
        let row = PrintableTaskResult {
            task_id,
            cmd: TaskArgValue::into_inner_value::<String>(
                execution_value.get(SSH_EXEC_CMD).unwrap().clone(),
            ),

            cmd_status: if TaskArgValue::into_inner_value::<usize>(
                execution_value.get(SSH_EXEC_CMD_STATUS).unwrap().clone(),
            ) == 0
            {
                "Success".to_string()
            } else {
                "Failure".to_string()
            },
            cmd_output: TaskArgValue::into_inner_value::<String>(
                execution_value.get(SSH_EXEC_CMD_OUTPUT).unwrap().clone(),
            ),
        };
        self.data.borrow_mut().push(row);
    }

    pub(crate) fn table_print(&self) {
        let table_header_format = tabled::format::Format::new(|s| s.blue().to_string());
        let mut table = Table::new(self.data.borrow().clone());
        table
            .with(tabled::Style::psql())
            .with(Segment::all().modify().with(Alignment::left()))
            .with(Modify::new(Rows::first()).with(table_header_format))
            .with(Modify::new(Columns::single(0)).with(Width::wrap(20).keep_words()))
            .with(Modify::new(Columns::single(1)).with(Width::wrap(50).keep_words()))
            .with(Modify::new(Columns::single(2)).with(Width::wrap(10)))
            .with(Modify::new(Columns::single(3)).with(Width::wrap(30).keep_words()));

        println!("{}\n", table);
    }
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
    pub async fn receive_task_result(&'static self) {
        let mut result_reader = self.task_controller.clone().try_stream();
        while let Some(Ok(task_result_pair)) = result_reader.next().await {
            let task_id: String = task_result_pair.task_id;
            let result: TaskResultEnum = task_result_pair.result;

            match result {
                TaskResultEnum::Success(opt_rs) => {
                    let table_printer = TablePrinter::new();
                    if let Some(execution_value) = opt_rs {
                        table_printer.add_row(task_id, execution_value);
                        table_printer.table_print();
                    }
                }
                TaskResultEnum::Error(err_msg) => {
                    println!(r#"❌ task {} failed. cause by {}"#, task_id, err_msg);
                }
            }
        }
    }

    pub async fn run_tasks(
        &'static self,
        cmd_args: CommandArgs,
        config: DeploymentConfig,
        success_task: Option<Vec<TaskStatusEntity>>,
    ) -> anyhow::Result<Vec<TaskResultPair>> {
        let group_key = cmd_args.as_ref();

        let task_group = TASK_GROUP.get(group_key).unwrap();
        let tasks_execution = task_group
            .tasks(cmd_args, config.clone(), success_task)
            .unwrap();

        self.task_controller
            .run_all_tasks(tasks_execution, config)
            .await
    }
}

#[cfg(test)]
mod tests {

    use crate::cli::task::task_base::TaskId;

    #[test]
    fn test_table_flat() {
        let task_id = TaskId {
            cmd: "deploy".to_string(),
            task: "apache-cassandra-4.1-rc1-bin.tar.gz_unpack".to_string(),
            host: "172.31.24.222".to_string(),
        };

        let table = task_id.pretty_string();
        println!("{}", table);
    }
}
