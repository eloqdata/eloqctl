use crate::cli::task::group::Config;
use crate::cli::task::task_base::{
    init_finish_signal, is_verbose_task_output, TaskExecutionContext, TaskId,
};
use crate::cli::task::task_base::{TaskInstance, TaskResultEnum, TaskResultPair};
use crate::post_task_execute;
use crate::state::task_status_operation::TaskStatusEntity;
use anyhow::bail;
use chrono::DateTime;
use chrono::Utc;
use futures_async_stream::try_stream;
use itertools::Itertools;
use owo_colors::OwoColorize;
use std::sync::Arc;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct TaskController {
    rx: crossbeam_channel::Receiver<TaskResultPair>,
    tx: crossbeam_channel::Sender<TaskResultPair>,
}

impl Default for TaskController {
    fn default() -> Self {
        Self::new()
    }
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

    fn stage_label_from_tasks(task_group: &str, task_split: &[TaskInstance]) -> String {
        if task_split.is_empty() {
            return Self::stage_label(task_group);
        }

        let task_ids = task_split
            .iter()
            .map(|task| task.task.identifier())
            .collect_vec();

        if task_ids.iter().all(|id| id.cmd == "ssh-check") {
            return "Checking SSH connectivity".to_string();
        }
        if task_ids.iter().all(|id| id.cmd == "run_deps") {
            return "Installing runtime dependencies".to_string();
        }
        if task_ids.iter().all(|id| id.cmd == "check") {
            return "Checking port availability".to_string();
        }
        if task_ids
            .iter()
            .all(|id| id.cmd == "deploy" && id.task == "mkdir")
        {
            return "Creating remote directories".to_string();
        }
        if task_ids
            .iter()
            .all(|id| id.cmd == "deploy" && id.task.ends_with("_download"))
        {
            return "Downloading release packages".to_string();
        }
        if task_ids.iter().all(|id| id.cmd == "extract") {
            return "Extracting release packages".to_string();
        }
        if task_ids.iter().all(|id| {
            matches!(id.cmd.as_str(), "deploy" | "update")
                && (id.task.contains("upload") || id.task.starts_with("deploy_eloq_all_"))
        }) {
            return "Syncing binaries and config files".to_string();
        }
        if task_ids.iter().all(|id| id.cmd == "config-update") {
            return "Uploading configuration files".to_string();
        }
        if task_ids.iter().all(|id| id.cmd == "install") {
            return "Bootstrapping cluster metadata".to_string();
        }
        if task_ids.iter().all(|id| id.cmd == "start") {
            return "Starting services".to_string();
        }
        if task_ids.iter().all(|id| id.cmd == "stop") {
            return "Stopping services".to_string();
        }
        if task_ids.iter().all(|id| id.cmd == "restart") {
            return "Restarting services".to_string();
        }
        if task_ids.iter().all(|id| id.cmd == "topology") {
            return "Refreshing cluster topology".to_string();
        }
        if task_ids.iter().all(|id| id.cmd == "monitor") {
            if task_ids.iter().all(|id| id.task.contains("-stop-")) {
                return "Stopping monitor components".to_string();
            }
            if task_ids.iter().all(|id| id.task.contains("-status-")) {
                return "Checking monitor status".to_string();
            }
            if task_ids.iter().all(|id| id.task.contains("-start-")) {
                return "Starting monitor components".to_string();
            }
            return "Managing monitor components".to_string();
        }
        if task_ids
            .iter()
            .all(|id| id.cmd == "remove" && id.task.starts_with("clean_log@"))
        {
            return "Deleting log service data".to_string();
        }
        if task_ids
            .iter()
            .all(|id| id.cmd == "remove" && id.task.starts_with("clean@"))
        {
            return "Deleting cluster files".to_string();
        }
        if task_ids.iter().all(|id| {
            id.cmd == "backup"
                && (id.task.starts_with("delete-")
                    || id.task.starts_with("remove-local-")
                    || id.task.starts_with("clean-backup"))
        }) {
            return "Deleting backup data".to_string();
        }
        if task_ids.iter().all(|id| id.cmd == "backup") {
            return "Running backup".to_string();
        }
        if task_ids.iter().all(|id| id.cmd == "scale") {
            return "Scaling cluster nodes".to_string();
        }

        Self::stage_label(task_group)
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
        };

        splits.iter().for_each(|execution_context| {
            let tx_arc = Arc::new(&self.tx);
            let task_group_arc = Arc::new(task_group.clone());
            let name = name.clone();
            let join = tokio::task::spawn(async move {
                let task = &execution_context.task;
                let task_input = execution_context.task_input.clone();
                let task_host = &execution_context.task_host;
                let task_id = task.identifier();
                if is_verbose_task_output() {
                    println!("  {} {}", "→".cyan(), user_friendly_task_summary(&task_id));
                }
                let start_time = std::time::Instant::now();
                let execution_rs = task.execute(task_host.clone(), task_input).await;
                let elapsed = start_time.elapsed();
                info!("Task {:?} execution complete", task_id);
                let conn_tuple = task_host.ssh_conn_tuple();
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
                    Ok(rs) => {
                        if is_verbose_task_output() {
                            println!(
                                "  {} {:.1}s {}",
                                "✓".green(),
                                elapsed.as_secs_f32(),
                                user_friendly_task_summary(&task_id)
                            );
                        }
                        TaskResultEnum::Success(rs)
                    }
                    Err(err_msg) => {
                        let err_msg_str = err_msg.to_string();
                        error!("Task {:?} execution fail {:?}", task_id, err_msg_str);
                        if is_verbose_task_output() {
                            eprintln!(
                                "  {} {} -- {}",
                                "✗".red(),
                                user_friendly_task_summary(&task_id),
                                summarize_error(&err_msg_str)
                            );
                        }
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

    fn stage_label(task_group: &str) -> String {
        match task_group {
            "check" => "Checking hosts and ports".to_string(),
            "deploy" => "Deploying packages and configuration".to_string(),
            "install" => "Bootstrapping Eloq".to_string(),
            "cluster-control-start" => "Starting Eloq services".to_string(),
            "cluster-control-status" => "Checking cluster status".to_string(),
            "update-tx-conf" => "Updating Eloq configuration".to_string(),
            "download-and-extract" => "Preparing upgrade package".to_string(),
            "upload-to-standby" => "Uploading binaries to standby nodes".to_string(),
            "upload-to-master" => "Uploading binaries to old master nodes".to_string(),
            "stop-standby" => "Stopping standby nodes".to_string(),
            "rolling-restart-failover-stop-master" => {
                "Failing over to standby and stopping old master".to_string()
            }
            "rolling-restart-round1" => "Moving traffic away from the current master".to_string(),
            "start-tx" => "Starting the original master node".to_string(),
            "wait-current-master" => "Waiting for a serving master".to_string(),
            "rolling-restart-round2" => "Moving traffic back and restarting standby".to_string(),
            "start-standby" => "Starting standby nodes".to_string(),
            "stop-voters" => "Stopping voter nodes".to_string(),
            "start-voters" => "Starting voter nodes".to_string(),
            "start-log-and-wait" | "log-service-startup" => "Starting log service".to_string(),
            "scale" => "Scaling Eloq nodes".to_string(),
            "scalelog" => "Scaling log service".to_string(),
            "backup" => "Running backup operation".to_string(),
            "dummy" => "Finalizing".to_string(),
            other => humanize_task_name(other),
        }
    }

    fn stage_summary(task_count: usize) -> String {
        match task_count {
            0 => "no work".to_string(),
            1 => "1 operation".to_string(),
            n => format!("{n} operations"),
        }
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
        let total_stages = split.len();
        let mut task_result_vec = vec![];

        if !is_verbose_task_output() {
            println!();
        }

        for (stage_idx, task_split) in split.into_iter().enumerate() {
            let stage_num = stage_idx + 1;
            let task_names: Vec<String> = task_split
                .iter()
                .map(|t| t.task.identifier().format_string())
                .collect();
            if is_verbose_task_output() {
                println!(
                    "[{task_group_string}] Stage {stage_num}/{total_stages}: {}",
                    task_names.join(", ")
                );
            } else {
                let stage_label = Self::stage_label_from_tasks(&task_group_string, task_split);
                let progress = format!("[{stage_num}/{total_stages}]");
                println!(
                    "{} {} {}",
                    progress.cyan(),
                    stage_label.bold(),
                    format!("({})", Self::stage_summary(task_split.len())).dimmed()
                );
            }
            let rs = self
                .run_task_split(task_group_string.clone(), task_split, config.clone())
                .await?;
            if err_brk {
                for pair in rs.iter() {
                    if let TaskResultEnum::Error(err) = &pair.result {
                        if !is_verbose_task_output() {
                            eprintln!("  {} {}", "Error:".red().bold(), summarize_error(err));
                        }
                        bail!("operation failed: {err}");
                    }
                }
            }
            if !is_verbose_task_output() {
                println!("  {} {}", "✓".green(), "done".dimmed());
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

fn humanize_task_name(name: &str) -> String {
    let words = name
        .replace(['-', '_'], " ")
        .split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => format!("{}{}", first.to_uppercase(), chars.as_str()),
                None => String::new(),
            }
        })
        .collect_vec();
    if words.is_empty() {
        "Running task".to_string()
    } else {
        words.join(" ")
    }
}

fn user_friendly_task_label(task: &str) -> String {
    let task = task
        .strip_prefix("txservice-")
        .or_else(|| task.strip_prefix("standby-"))
        .or_else(|| task.strip_prefix("voter-"))
        .unwrap_or(task);
    humanize_task_name(task)
}

pub(crate) fn task_action_summary(cmd: &str, task: &str) -> String {
    if cmd == "remove" && task.starts_with("clean@") {
        return "remove cluster files".to_string();
    }
    if cmd == "remove" && task.starts_with("clean_log@") {
        return "remove log service files".to_string();
    }
    if cmd == "monitor" {
        if task.contains("grafana-stop") {
            return "stop Grafana".to_string();
        }
        if task.contains("grafana-start") {
            return "start Grafana".to_string();
        }
        if task.contains("grafana-status") {
            return "check Grafana status".to_string();
        }
        if task.contains("prometheus-stop") {
            return "stop Prometheus".to_string();
        }
        if task.contains("prometheus-start") {
            return "start Prometheus".to_string();
        }
        if task.contains("prometheus-status") {
            return "check Prometheus status".to_string();
        }
        if task.contains("node_exporter-stop") {
            return "stop node_exporter".to_string();
        }
        if task.contains("node_exporter-start") {
            return "start node_exporter".to_string();
        }
        if task.contains("node_exporter-status") {
            return "check node_exporter status".to_string();
        }
    }
    user_friendly_task_label(task).to_lowercase()
}

pub(crate) fn user_friendly_task_summary(task_id: &TaskId) -> String {
    let action = task_action_summary(&task_id.cmd, &task_id.task);
    if task_id.host.is_empty() || task_id.host == "local" || task_id.host == "127.0.0.1" {
        action
    } else {
        format!("{action} on {}", task_id.host)
    }
}

pub(crate) fn summarize_error(err: &str) -> String {
    if let Some(rest) = err.strip_prefix("Remote cmd [") {
        if let Some((cmd, suffix)) = rest.split_once("] execution failed. status_code=") {
            return format!("remote command exited with status {suffix}: {cmd}");
        }
    }
    err.to_string()
}
