use crate::cli::task::group::Config;
use crate::cli::task::task_base::{
    init_finish_signal, is_verbose_task_output, TaskExecutionContext,
};
use crate::cli::task::task_base::{TaskInstance, TaskResultEnum, TaskResultPair};
use crate::post_task_execute;
use crate::state::task_status_operation::TaskStatusEntity;
use anyhow::bail;
use chrono::DateTime;
use chrono::Utc;
use futures_async_stream::try_stream;
use itertools::Itertools;
use std::sync::Arc;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct TaskController {
    rx: crossbeam_channel::Receiver<TaskResultPair>,
    tx: crossbeam_channel::Sender<TaskResultPair>,
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
        Self { rx, tx }
    }

    fn split_task(task_execution_context: &TaskExecutionContext) -> Vec<&'static [TaskInstance]> {
        let barrier = &task_execution_context.barrier;
        let task_install_vec = task_execution_context
            .executable
            .values()
            .cloned()
            .collect_vec();
        let tasks = Box::leak(Box::new(task_install_vec));
        if let Some(barrier_array) = barrier.as_ref() {
            let mut begin;
            let mut end = 0;
            let mut split = vec![];
            for (idx, barrier_val) in barrier_array.iter().enumerate() {
                if idx == 0 {
                    begin = 0;
                    end = *barrier_val;
                } else {
                    begin = end;
                    end = begin + *barrier_val;
                }
                info!("TaskController run_task_split {begin}..{end}");
                let task_slice = &tasks[begin..end];
                split.push(task_slice);
            }
            split
        } else {
            vec![tasks.as_slice()]
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
        }
    }

    pub async fn recv(&self) -> Option<TaskResultPair> {
        match self.rx.recv() {
            Ok(pair) => Some(pair),
            Err(err) => {
                warn!("task controller recevied: {err}");
                None
            }
        }
    }

    async fn run_task_split(
        &'static self,
        task_group: String,
        splits: &'static [TaskInstance],
        config: Config,
    ) -> anyhow::Result<Vec<TaskResultPair>> {
        let mut joins = vec![];

        let name = match config {
            Config::Cluster(cfg) => cfg.deployment.cluster_name.clone(),
            Config::Proxy(cfg) => cfg.proxy_service.proxy_name.clone(),
        };

        splits
            .iter()
            //.enumerate()
            .for_each(|execution_context| {
                let tx_arc = Arc::new(&self.tx);
                let task_group_arc = Arc::new(task_group.clone());
                let name = name.clone();
                let join = tokio::task::spawn(async move {
                    let task = &execution_context.task;
                    let task_input = execution_context.task_input.clone();
                    let task_host = &execution_context.task_host;
                    let task_id = task.identifier();
                    if is_verbose_task_output() {
                        println!("=> START {}", task_id.format_string());
                    }
                    let execution_rs = task.execute(task_host.clone(), task_input).await;
                    info!("Task {:?} execution complete", task_id);
                    //let cmd = task_id.clone().cmd;
                    let conn_tuple = task_host.ssh_conn_tuple();
                    // execution_rs,cluster,task_mame,command,task_host
                    //let task_group_copy = Arc::clone(&task_group_arc.clone());
                    let post_execute_rs = post_task_execute!(
                        execution_rs,
                        name,
                        task_id.as_json_string(),
                        task_group_arc.as_str(),
                        conn_tuple.2
                    );
                    info!("Save Task {:?} execution status complete", task_id);
                    assert!(post_execute_rs.is_ok());
                    let result = match execution_rs {
                        Ok(rs) => TaskResultEnum::Success(rs),
                        Err(err_msg) => {
                            let err_msg_str = err_msg.to_string();
                            error!("Task {:?} execution fail {:?}", task_id, err_msg_str);
                            TaskResultEnum::Error(err_msg_str)
                        }
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
        task_execution_context: TaskExecutionContext,
        config: Config,
        err_brk: bool,
    ) -> anyhow::Result<Vec<TaskResultPair>> {
        let task_group_string = task_execution_context.clone().task_group;
        let split = TaskController::split_task(&task_execution_context);
        let mut task_result_vec = vec![];
        for task_split in split.into_iter() {
            let rs = self
                .run_task_split(task_group_string.clone(), task_split, config.clone())
                .await?;
            if err_brk {
                for pair in rs.iter() {
                    if let TaskResultEnum::Error(err) = &pair.result {
                        bail!("task failed: {err}");
                    }
                }
            }
            task_result_vec.push(rs);
        }
        self.tx.send(TaskResultPair {
            task_id: "".to_string(),
            result: TaskResultEnum::Success(Some(init_finish_signal().clone())),
        })?;
        let rtn = task_result_vec.into_iter().flatten().collect_vec();
        Ok(rtn)
    }
}
