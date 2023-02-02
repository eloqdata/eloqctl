use crate::server::{LongTaskRequestHandler, RequestPayload};
use crate::{Response, WebHandleError, SUPPORT_CMD};
use actix_web::web::Json;
use actix_web::{get, post, web, HttpResponse, Responder};
use anyhow::anyhow;
use cluster_mgr::cli::config::DeploymentConfig;
use cluster_mgr::cli::task::task_base::TaskId;
use cluster_mgr::cli::CommandArgs;
use serde_json::{json, Value};
// use tracing::info;

#[get("/check_health")]
pub async fn check_health() -> impl Responder {
    HttpResponse::Ok().content_type("text/plain").body("ok")
}

fn validate_command(command: &str) -> Result<(), WebHandleError> {
    let cmd_str = command.to_lowercase();
    if cmd_str.is_empty() || !SUPPORT_CMD.contains(&cmd_str.as_str()) {
        let support_cmd_list = SUPPORT_CMD.join(",");
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
        "status" => CommandArgs::Status {
            cluster: cluster.unwrap(),
        },
        "deploy" => CommandArgs::Deploy {
            topology_file: "_NONE".to_string(),
        },
        _ => unreachable!(),
    }
}

#[post("/deploy")]
pub async fn deploy_cluster(
    global_handler: web::Data<LongTaskRequestHandler>,
    post_deployment: Json<DeploymentConfig>,
) -> impl Responder {
    let deploy_without_topology_file = build_command_from_str("deploy", None);
    global_handler.submit(RequestPayload {
        command: Some(deploy_without_topology_file),
        config: Some(post_deployment.0),
    });
    HttpResponse::Ok().finish()
}

#[post("/control/{cluster}/{command}")]
pub async fn ctl_cluster(
    global_handler: web::Data<LongTaskRequestHandler>,
    param: web::Path<(String, String)>,
) -> Result<impl Responder, WebHandleError> {
    let (cluster, command) = param.into_inner();
    if command == "deploy" {
        return Err(WebHandleError {
            err: anyhow!("The /control/cluster/command  does not support the deploy command."),
        });
    }
    validate_command(command.as_str())?;
    let ctl_command = build_command_from_str(command.as_str(), Some(cluster));
    global_handler.submit(RequestPayload {
        command: Some(ctl_command),
        config: None,
    });
    Ok(HttpResponse::Ok().finish())
}

#[get("/status/{cluster}/{command}")]
pub async fn check_cmd_status(
    global_handler: web::Data<LongTaskRequestHandler>,
    param: web::Path<(String, String)>,
) -> Result<impl Responder, WebHandleError> {
    let (cluster, command) = param.into_inner();
    validate_command(command.as_str())?;
    let cmd_executor = global_handler.get_command_executor();
    let cluster_str = cluster.as_str();
    let deployment_config_opt = cmd_executor.get_deployment_by_cluster(cluster_str).await?;
    if let Some(deployment_config) = deployment_config_opt {
        let cmd_args = build_command_from_str(command.as_str(), Some(cluster_str.to_string()));
        let task_context = cmd_executor.task_context(cmd_args, deployment_config);
        let task_ids = task_context.list_task_ids();
        let cmd_vec = vec![task_context.task_group];
        let task_status_vec = cmd_executor
            .get_task_status_by_condition(cluster, None, Some(cmd_vec))
            .await?;
        let mut success = Vec::new();
        let mut failure = Vec::new();
        task_status_vec.iter().for_each(|task_status| {
            if task_status.task_status == 0 {
                success.push(TaskId::from_json_string(task_status.clone().task));
            } else {
                failure.push(TaskId::from_json_string(task_status.clone().task));
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
            .json(Response {
                code: 200,
                msg: None,
                data: Some(rsp_data),
            }))
    } else {
        Ok(HttpResponse::Ok()
            .content_type("application/json")
            .json(Response {
                code: 200,
                msg: None,
                data: Some(
                    json!({ "status": "none", "success": Value::Null, "failure": Value::Null}),
                ),
            }))
    }
}
