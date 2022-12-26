use crate::cli::config::{load_remote_env, DeploymentConfig};
use crate::cli::task::task_group::{TaskExecutionContext, TASK_GROUP};
use crate::cli::CommandArgs;
use crate::enum_into_trait;
use crate::state::task_status_operation::TaskStatusEntity;
use async_trait::async_trait;
use dyn_clone::DynClone;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::string::ToString;
use std::sync::{Arc, LazyLock};
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{error, info, instrument};
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
        use $crate::cli::task::ssh_conn::{SSH_EXEC_CMD_OUTPUT, SSH_EXEC_CMD_STATUS};
        let task_rs = $task_result.clone();
        let task_status = task_rs.get(SSH_EXEC_CMD_STATUS).unwrap();
        let status_code = TaskArgValue::into_inner_value::<usize>(task_status.clone());
        if status_code != 0 {
            info!(
                "{} execution failure status_code={}",
                $task_name, status_code
            );
            return Err(anyhow::anyhow!($task_err_closure(status_code)));
        } else {
            if task_rs.get(SSH_EXEC_CMD_OUTPUT).is_some() {
                let rtn_vlaue = task_rs.clone();
               $(
                let mut rtn_value = $return_value;
                rtn_value.extend(task_rs.into_iter());
               )*
               return Ok(Some(rtn_vlaue));
            } else {
                return Ok(None);
            }
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

static FINISH_: LazyLock<LastResult> = LazyLock::new(|| {
    HashMap::from([(
        "_FINISH_SIGNAL".to_string(),
        TaskArgValue::Str("".to_string()),
    )])
});

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId {
    pub cmd: String,
    pub task: String,
}

impl TaskId {
    pub fn string(&self) -> String {
        format!("cmd={},task={}", self.cmd, self.task)
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
    task_id: String,
    result: TaskResultEnum,
}

#[derive(Debug, Clone)]
struct TaskController {
    rx: crossbeam_channel::Receiver<TaskResultPair>,
    tx: crossbeam_channel::Sender<TaskResultPair>,
    task_execution_result: Arc<RwLock<HashMap<TaskId, ExecutionValue>>>,
}

/// `TaskController` is responsible for the parallelization and coordination of tasks.
/// Coordination refers to if there is a business dependency between tasks
/// and if the execution of the dependent tasks is guaranteed to be completed.
/// There are currently only linear dependencies; if there is a dependency between tasks,
/// there is only one predecessor task in the dependency chain. `TaskController` will decide
/// which batch of tasks can be parallelized according to the barrier in the task context.
impl TaskController {
    pub fn new() -> Self {
        let (tx, rx) = crossbeam_channel::bounded(2000);
        Self {
            rx,
            tx,
            task_execution_result: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn get_task_execute_result(&self, task_id: TaskId) -> Option<ExecutionValue> {
        let execution_rs_read_guard = self.task_execution_result.read().await;
        execution_rs_read_guard.get(&task_id).cloned()
    }

    async fn put_task_execute_result(&self, task_id: TaskId, execution_rs: ExecutionValue) {
        let mut execution_rs_write_guard = self.task_execution_result.write().await;
        execution_rs_write_guard.insert(task_id, execution_rs);
    }

    fn split_task(
        barrier: Option<Vec<usize>>,
        tasks: Vec<TaskInstance>,
    ) -> Vec<&'static [TaskInstance]> {
        let tasks = Box::leak(Box::new(tasks));
        if barrier.is_none() {
            vec![tasks.as_slice()]
        } else {
            let barrier_array = barrier.as_ref().unwrap();
            let mut begin;
            let mut end = 0;
            let mut all_split = vec![];
            for (idx, barrier_val) in barrier_array.iter().enumerate() {
                if idx == 0 {
                    begin = 0;
                    end = *barrier_val;
                } else {
                    begin = end;
                    end = begin + *barrier_val;
                }
                info!("TaskController run_task_split {}..{}", begin, end);
                let task_slice = &tasks[begin..end];
                all_split.push(task_slice);
            }
            all_split
        }
    }

    #[try_stream(boxed, ok = TaskResultPair, error = anyhow::Error)]
    pub async fn try_stream(self) {
        while let Ok(task_pair) = self.rx.recv() {
            let task_rs = &task_pair.result;
            let is_finish = match task_rs {
                TaskResultEnum::Success(result) => {
                    if let Some(exec_rs) = result {
                        exec_rs.contains_key("_FINISH_SIGNAL")
                    } else {
                        false
                    }
                }
                _ => true,
            };
            if is_finish {
                break;
            }
            yield task_pair;
        }
    }

    #[instrument]
    async fn run_task_split(
        &'static self,
        splits: &'static [TaskInstance],
        config: DeploymentConfig,
    ) -> anyhow::Result<Vec<TaskResultPair>> {
        let mut joins = vec![];

        splits
            .iter()
            .enumerate()
            .for_each(|(_idx, execution_context)| {
                let tx_arc = Arc::new(&self.tx);
                let cluster_name = config.deployment.cluster_name.clone();
                let join = tokio::task::spawn(async move {
                    let task = &execution_context.task;
                    let task_input = execution_context.task_input.clone();
                    let task_host = &execution_context.task_host;
                    let task_id = task.identifier();
                    let task_result_opt = self.get_task_execute_result(task_id.clone()).await;
                    let final_task_input = if let Some(mut exec_rs) = task_result_opt {
                        exec_rs.extend(task_input.into_iter());
                        exec_rs
                    } else {
                        task_input
                    };
                    let execution_rs = task.execute(task_host.clone(), final_task_input).await;
                    info!("Task {:?} execution complete", task_id);
                    if let Ok(Some(ref inner_execution_rs)) = execution_rs.as_ref() {
                        self.put_task_execute_result(task_id.clone(), inner_execution_rs.clone())
                            .await;
                    }
                    let cmd = task_id.clone().cmd;
                    let conn_tuple = task_host.ssh_conn_tuple();
                    // execution_rs,cluster,task_mame,command,task_host
                    let post_execute_rs = post_task_execute!(
                        execution_rs,
                        cluster_name,
                        task_id.as_json_string(),
                        cmd.as_str(),
                        conn_tuple.2
                    );
                    info!("Save Task {:?} execution status complete", task_id);
                    assert!(post_execute_rs.is_ok());
                    let result = match execution_rs {
                        Ok(rs) => TaskResultEnum::Success(rs),
                        Err(err_msg) => TaskResultEnum::Error(err_msg.to_string()),
                    };
                    let task_pair = TaskResultPair {
                        task_id: task_id.string(),
                        result,
                    };
                    let send_rs = tx_arc.send(task_pair.clone());
                    assert!(send_rs.is_ok());
                    task_pair
                });
                joins.push(join);
            });
        let join_result = futures::future::join_all(joins).await;
        let task_result = join_result
            .into_iter()
            .filter_map(|rs| rs.ok())
            .collect_vec();
        Ok(task_result)
    }

    /// Executes all task instances in parallel based on the `TaskExecutionContext` and returns the result.
    ///
    /// + --------parallel-------- + Pause  +  ------parallel----- +
    ///
    /// +-----+------+------+------+--------+------+-------+-------+-------+
    /// |     |      |      |      |        |      |       |       |       |
    /// |task1| task2| task3| task4| Barrier|task5 | task6 | task7 | ...   |
    /// +-----+------+------+------+--------+------+-------+-------+-------+
    pub async fn run_all_tasks(
        &'static self,
        task_execution: TaskExecutionContext,
        config: DeploymentConfig,
    ) -> anyhow::Result<Vec<TaskResultPair>> {
        let barrier = task_execution.clone().barrier;
        let tasks = task_execution.clone().executable;
        let split = TaskController::split_task(barrier, tasks);
        let mut task_result_vec = vec![];
        for task_split in split.into_iter() {
            let rs = self.run_task_split(task_split, config.clone()).await?;
            task_result_vec.push(rs);
        }
        self.tx.send(TaskResultPair {
            task_id: "".to_string(),
            result: TaskResultEnum::Success(Some(FINISH_.clone())),
        })?;
        let rtn = task_result_vec.into_iter().flatten().collect_vec();
        Ok(rtn)
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
        while let Some(Ok(rs)) = result_reader.next().await {
            println!("TaskMgr receive task result = {:?}", rs);
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
