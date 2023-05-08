use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use std::future::Future;

use crate::cli::task::task_base::{ExecutionValue, TaskArgValue};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::state::state_base::StateOperation;
use crate::state::state_mgr::{STATE_MGR, TASK_STATUS_STATE};
use crate::state::task_status_operation::{TaskStatusEntity, TaskStatusOperation};
use anyhow::anyhow;
use std::time::Duration;
use tracing::{error, info};

#[macro_export]
macro_rules! wait_command_complete {
    ($cmd:expr,$process_cmd:expr,$ssh_session:expr, $check_fn:ident) => {{
        ctl_action_wait_complete(
            $cmd,
            $process_cmd,
            $ssh_session,
            async move |cmd, ssh_conn| ssh_conn.command(cmd.as_str(), CollectOutput).await,
            |output| -> bool { parse_process_pid(output).$check_fn() },
        )
        .await
    }};
}

pub(crate) const PROCESS_PID: &str = "_process_pid_";

pub fn parse_process_pid(process_info: String) -> Option<i32> {
    if process_info.is_empty() {
        None
    } else {
        let mut pid = None;
        let output_normal = process_info.trim();
        for line in output_normal.lines() {
            let line_normal = line.trim();
            if line_normal.is_empty() {
                continue;
            }
            if !line_normal.chars().all(char::is_numeric) {
                continue;
            }
            let parse_rs = line_normal.parse::<i32>().unwrap();
            pid = Some(parse_rs);
            break;
        }
        println!("ProcessInfo PID={pid:?}");
        pid
    }
}

pub(crate) async fn check_process_pid<F>(
    check_cmd: String,
    ssh_conn: SSHSession,
    parser_output: F,
) -> anyhow::Result<ExecutionValue>
where
    F: Fn(String) -> Option<i32>,
{
    let mut cmd_exec_rs = ssh_conn.command(check_cmd.as_str(), CollectOutput).await?;
    let cmd_status = cmd_exec_rs.get(CMD_STATUS).unwrap();
    if 0 != TaskArgValue::into_inner_value::<usize>(cmd_status.clone()) {
        error!("check_process_pid fails status={:?}", cmd_status);
        return Err(anyhow!("Cmd {} execution fails", check_cmd));
    }
    let cmd_output_value = cmd_exec_rs.get(CMD_OUTPUT).unwrap();

    let output = TaskArgValue::into_inner_value::<String>(cmd_output_value.clone());
    info!("check_process_pid cmd={},output={}", check_cmd, output);

    if let Some(pid_num) = parser_output(output) {
        cmd_exec_rs.insert(
            PROCESS_PID.to_string(),
            TaskArgValue::Str(pid_num.to_string()),
        );
    } else {
        cmd_exec_rs.insert(
            PROCESS_PID.to_string(),
            TaskArgValue::Str("NONE".to_string()),
        );
    }
    Ok(cmd_exec_rs)
}

pub(crate) async fn ctl_action_wait_complete<F1, F2, Fut2>(
    ctl_cmd: String,
    check_cmd: String,
    ssh_conn: SSHSession,
    ctl_fn: F2,
    check_fn: F1,
) -> anyhow::Result<ExecutionValue>
where
    F1: Fn(String) -> bool,
    F2: Fn(String, SSHSession) -> Fut2,
    Fut2: Future<Output = anyhow::Result<ExecutionValue>> + 'static,
{
    let mut ctl_action_rs = ctl_fn(ctl_cmd.clone(), ssh_conn.clone()).await?;
    let process_ready =
        wait_process_complete(check_cmd, ssh_conn, Duration::from_secs(5 * 60), check_fn).await?;
    if let Some(output) = ctl_action_rs.get(CMD_OUTPUT) {
        let final_output = format!(
            r#"output={},check control func return={}"#,
            TaskArgValue::into_inner_value::<String>(output.clone()),
            process_ready
        );
        ctl_action_rs.insert(CMD.to_string(), TaskArgValue::Str(ctl_cmd));
        ctl_action_rs.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(final_output));
    } else {
        ctl_action_rs.insert(CMD.to_string(), TaskArgValue::Str(ctl_cmd));
        ctl_action_rs.insert(
            CMD_OUTPUT.to_string(),
            TaskArgValue::Str(format!("check control func return={process_ready}")),
        );
    }
    Ok(ctl_action_rs)
}

pub(crate) async fn wait_process_complete<F>(
    check_status_cmd: String,
    ssh_conn: SSHSession,
    wait_timeout: Duration,
    parser_output: F,
) -> anyhow::Result<bool>
where
    F: Fn(String) -> bool,
{
    let sleep_duration = Duration::from_secs(1);
    let mut timeout_remaining = wait_timeout;
    let mut process_ready = false;
    loop {
        if timeout_remaining.as_secs() == 0 {
            info!("CheckStatus timeout");
            break;
        }
        let rs = ssh_conn
            .command(check_status_cmd.as_str(), CollectOutput)
            .await;
        println!("check_status_cmd = {rs:?}");
        if rs.as_ref().is_err() {
            let err_msg = rs.err().unwrap().to_string();
            error!(
                "CheckStatus return failed. {} {}",
                err_msg, check_status_cmd
            );
            return Err(anyhow!(err_msg));
        }
        let exec_rs = rs.as_ref().unwrap();
        let output_value = exec_rs.get(CMD_OUTPUT).unwrap();
        let output_string = TaskArgValue::into_inner_value::<String>(output_value.clone());
        process_ready = parser_output(output_string.clone());
        if process_ready {
            break;
        } else {
            std::thread::sleep(sleep_duration);
            timeout_remaining -= sleep_duration;
        }
    }
    Ok(process_ready)
}

pub(crate) async fn save_task_status(
    task_status_entity: TaskStatusEntity,
    execution_result: Option<ExecutionValue>,
) -> anyhow::Result<Option<ExecutionValue>> {
    let state_operation = STATE_MGR.get_state_operation::<TaskStatusOperation>(TASK_STATUS_STATE);

    let put_rs = state_operation.put(task_status_entity).await;
    if let Err(put_err) = put_rs {
        let err_string = put_err.to_string();
        Err(anyhow!(err_string))
    } else {
        Ok(execution_result)
    }
}
