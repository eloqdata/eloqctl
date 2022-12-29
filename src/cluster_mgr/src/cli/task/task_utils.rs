use crate::cli::task::ssh_conn::{
    SSHConn, SSH_CHECK_PROCESS_PID, SSH_EXEC_CMD_OUTPUT, SSH_EXEC_CMD_STATUS,
};
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue};
use crate::state::state_base::StateOperation;
use crate::state::state_mgr::{STATE_MGR, TASK_STATUS_STATE};
use crate::state::task_status_operation::{TaskStatusEntity, TaskStatusOperation};
use anyhow::anyhow;
use std::time::Duration;
use tracing::{error, info};

pub(crate) fn stop_service(stop_cmd: String, ssh_conn: &SSHConn) -> anyhow::Result<ExecutionValue> {
    let stop_status = ssh_conn.run_cmd(stop_cmd.clone(), false)?;
    info!("Stop cmd={},status_code={:?}", stop_cmd, stop_status,);
    Ok(stop_status)
}

pub(crate) fn check_process_pid<F>(
    check_cmd: String,
    ssh_conn: &SSHConn,
    parser_output: F,
) -> anyhow::Result<ExecutionValue>
where
    F: Fn(String) -> Option<i32>,
{
    let mut cmd_exec_rs = ssh_conn.run_cmd_sync_output(check_cmd.clone())?;
    let cmd_status = cmd_exec_rs.get(SSH_EXEC_CMD_STATUS).unwrap();

    if 0 != TaskArgValue::into_inner_value::<usize>(cmd_status.clone()) {
        error!("check_process_pid fails status={:?}", cmd_status);
        return Err(anyhow!("Cmd {} execution fails", check_cmd));
    }
    let cmd_output_value = cmd_exec_rs.get(SSH_EXEC_CMD_OUTPUT).unwrap();

    let output = TaskArgValue::into_inner_value::<String>(cmd_output_value.clone());
    info!("check_process_pid cmd={},output={}", check_cmd, output);

    if let Some(pid_num) = parser_output(output) {
        cmd_exec_rs.insert(
            SSH_CHECK_PROCESS_PID.to_string(),
            TaskArgValue::Str(pid_num.to_string()),
        );
    } else {
        cmd_exec_rs.insert(
            SSH_CHECK_PROCESS_PID.to_string(),
            TaskArgValue::Str("NONE".to_string()),
        );
    }
    Ok(cmd_exec_rs)
}

pub(crate) fn start_service<F>(
    start_cmd: String,
    check_cmd: String,
    ssh_conn: &SSHConn,
    check_fn: F,
) -> anyhow::Result<ExecutionValue>
where
    F: Fn(String) -> bool,
{
    let mut start_rs = ssh_conn.run_cmd(start_cmd.clone(), true)?;
    let status_code =
        TaskArgValue::into_inner_value::<usize>(start_rs.get(SSH_EXEC_CMD_STATUS).unwrap().clone());
    info!(
        "Start command execution completed.cmd={},status_code={}",
        start_cmd, status_code
    );
    if status_code != 0 {
        error!(
            "Start command execution failed. status_code={}, cmd={}",
            status_code, start_cmd
        );
        Err(anyhow!(format!(
            "Start failed cmd={}, cmd_code={}",
            start_cmd, status_code
        )))
    } else {
        let process_ready =
            wait_process_complete(check_cmd, ssh_conn, Duration::from_secs(5 * 60), check_fn)?;
        if let Some(output) = start_rs.get(SSH_EXEC_CMD_OUTPUT) {
            let final_output = format!(
                r#"output={}, process_running={}"#,
                TaskArgValue::into_inner_value::<String>(output.clone()),
                process_ready
            );
            start_rs.insert(
                SSH_EXEC_CMD_OUTPUT.to_string(),
                TaskArgValue::Str(final_output),
            );
        } else {
            start_rs.insert(
                SSH_EXEC_CMD_OUTPUT.to_string(),
                TaskArgValue::Str(format!("process_running={}", process_ready)),
            );
        }
        Ok(start_rs)
    }
}

pub(crate) fn wait_process_complete<F>(
    check_status_cmd: String,
    ssh_conn: &SSHConn,
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
        let rs = ssh_conn.run_cmd(check_status_cmd.clone(), true);
        if rs.as_ref().is_err() {
            let err_msg = rs.err().unwrap().to_string();
            error!("CheckStatus return failed. {}", err_msg);
            return Err(anyhow!(err_msg));
        }
        let exec_rs = rs.as_ref().unwrap();
        let output_value = exec_rs.get(SSH_EXEC_CMD_OUTPUT).unwrap();
        let output_string = TaskArgValue::into_inner_value::<String>(output_value.clone());
        info!("CheckStatus output={}", output_string.as_str());
        process_ready = parser_output(output_string);
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
