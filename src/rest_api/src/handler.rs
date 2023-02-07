use crate::global_handler::GlobalCommandHandler;
use crate::{MonographConnInfo, RequestPayload, ResponseData, WebHandleError};
use actix_web::{get, post, web, HttpResponse, Responder};
use anyhow::anyhow;
use cluster_mgr::cli::task::task_base::TaskId;
use cluster_mgr::cli::CommandArgs;
use cluster_mgr::config::config_base::DeploymentConfig;
use serde_json::{json, Value};
// use tracing::info;

#[get("/check_health")]
pub async fn check_health() -> impl Responder {
    HttpResponse::Ok().content_type("text/plain").body("ok")
}

fn validate_command(command: &str, support_cmd: &[&str]) -> Result<(), WebHandleError> {
    let cmd_str = command.to_lowercase();
    if cmd_str.is_empty() || !support_cmd.contains(&cmd_str.as_str()) {
        let support_cmd_list = support_cmd.join(",");
        let err_msg = format!(
            "un support command = {cmd_str}, for now support command list {support_cmd_list}"
        );
        Err(WebHandleError {
            err: anyhow!(err_msg),
        })
    } else {
        Ok(())
    }
}

fn build_command_from_str(cmd_str: &str, cluster: Option<String>) -> CommandArgs {
    match cmd_str {
        "install" => CommandArgs::Install {
            cluster: cluster.unwrap(),
        },
        "start" => CommandArgs::Start {
            cluster: cluster.unwrap(),
        },
        "stop" => CommandArgs::Stop {
            cluster: cluster.unwrap(),
            force: Some("false".to_string()),
        },
        "deploy" => CommandArgs::Deploy {
            topology_file: "_NONE".to_string(),
        },
        "status" => CommandArgs::Status {
            cluster: cluster.unwrap(),
            user: None,
            password: None,
        },
        _ => unreachable!(),
    }
}

#[post("/deploy")]
pub async fn deploy_cluster(
    global_handler: web::Data<GlobalCommandHandler>,
    post_deployment: web::Json<DeploymentConfig>,
) -> impl Responder {
    let deploy_without_topology_file = build_command_from_str("deploy", None);
    global_handler.submit(RequestPayload {
        command: Some(deploy_without_topology_file),
        config: Some(post_deployment.0),
    });
    HttpResponse::Ok().finish()
}

#[post("/ctl_cmd/{cluster}/{command}")]
pub async fn ctl_cluster(
    global_handler: web::Data<GlobalCommandHandler>,
    param: web::Path<(String, String)>,
) -> Result<impl Responder, WebHandleError> {
    let (cluster, command) = param.into_inner();
    validate_command(command.as_str(), &["start", "stop", "install"])?;
    let ctl_command = build_command_from_str(command.as_str(), Some(cluster));
    global_handler.submit(RequestPayload {
        command: Some(ctl_command),
        config: None,
    });
    Ok(HttpResponse::Ok().finish())
}

#[get("/ctl_cmd_status/{cluster}/{command}")]
pub async fn check_cmd_status(
    global_handler: web::Data<GlobalCommandHandler>,
    param: web::Path<(String, String)>,
) -> Result<impl Responder, WebHandleError> {
    let (cluster, command) = param.into_inner();
    validate_command(
        command.as_str(),
        &["start", "stop", "install", "status", "deploy"],
    )?;

    let cmd_executor = global_handler.get_command_executor();
    let deployment_config_opt = cmd_executor
        .state_mgr()
        .load_deployment_from_state(cluster.as_str())
        .await?;
    // cmd_executor.load_deployment_from_state(cluster_str).await?;
    if let Some(deployment_config) = deployment_config_opt {
        let cmd_args = build_command_from_str(command.as_str(), Some(cluster.clone()));
        let task_context = cmd_executor.task_context(cmd_args, deployment_config);
        let task_ids = task_context.list_task_ids();
        let cmd_vec = vec![task_context.task_group];
        let task_status_vec = cmd_executor
            .state_mgr()
            .load_task_status_from_state(cluster, None, Some(cmd_vec))
            .await?;
        let mut success = Vec::new();
        let mut failure = Vec::new();
        task_status_vec.iter().for_each(|task_status| {
            let task_id = TaskId::from_json_string(task_status.clone().task);
            let update_timestamp = task_status
                .update_timestamp
                .format("%Y-%m-%d %H:%M:%S")
                .to_string();
            let task_id_with_time = json!({
                "cmd": task_id.cmd,
                "task": task_id.task,
                "host": task_id.host,
                "cmd_datetime":update_timestamp,
            });
            if task_status.task_status == 0 {
                success.push(task_id_with_time);
            } else {
                failure.push(task_id_with_time);
            }
        });
        // info!("/status/cluster/command all_task_id = {:#?}", task_ids);
        let status = if !failure.is_empty() {
            "failure"
        } else if task_status_vec.len() == task_ids.len() {
            "success"
        } else if task_status_vec.is_empty() {
            "none"
        } else {
            "progress"
        };
        let rsp_data = json!({ "status": status, "success": success, "failure": failure});
        Ok(HttpResponse::Ok()
            .content_type("application/json")
            .json(ResponseData {
                code: 200,
                msg: None,
                data: Some(rsp_data),
            }))
    } else {
        Ok(HttpResponse::Ok()
            .content_type("application/json")
            .json(ResponseData {
                code: 200,
                msg: None,
                data: Some(
                    json!({ "status": "none", "success": Value::Null, "failure": Value::Null}),
                ),
            }))
    }
}

#[post("/cluster_status/{cluster}")]
pub async fn mono_service_status(
    global_handler: web::Data<GlobalCommandHandler>,
    param_cluster: web::Path<String>,
    conn_info: web::Json<MonographConnInfo>,
) -> Result<impl Responder, WebHandleError> {
    let mono_conn_info = conn_info.0;
    let status_cmd = CommandArgs::Status {
        cluster: param_cluster.to_string(),
        user: Some(mono_conn_info.user),
        password: Some(mono_conn_info.password),
    };
    global_handler.submit(RequestPayload {
        command: Some(status_cmd),
        config: None,
    });
    Ok(HttpResponse::Ok().finish())
}
