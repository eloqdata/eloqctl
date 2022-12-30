use crate::cli::config::DeploymentConfig;
use crate::cli::task::task_base::FINISH_;
use crate::cli::task::task_base::{
    ExecutionValue, TaskId, TaskInstance, TaskResultEnum, TaskResultPair,
};
use crate::cli::task::task_group::TaskExecutionContext;
use crate::post_task_execute;
use crate::state::task_status_operation::TaskStatusEntity;
use futures_async_stream::try_stream;
use itertools::Itertools;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, instrument};

#[derive(Debug, Clone)]
pub struct TaskController {
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
                        task_id: task_id.format_string(),
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
