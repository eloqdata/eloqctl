use crate::cli::cmd_base::CommandExecutor;
use crate::cli::config::DeploymentConfig;
use crate::cli::task::task_base::TaskId;
use crate::cli::CommandArgs;
use crate::web::{Response, WebHandleError, SUPPORT_CMD};
use actix_web::web::Json;
use actix_web::{get, post, web, HttpResponse, Responder, Result};
use anyhow::anyhow;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::info;

#[get("/check_health")]
pub async fn check_health() -> impl Responder {
    HttpResponse::Ok()
        .content_type("text/plain")
        .body("I'm OK.")
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

#[post("/deploy")]
pub async fn deploy_cluster(
    cmd_executor: web::Data<Arc<CommandExecutor>>,
    post_deployment: Json<DeploymentConfig>,
) -> impl Responder {
    actix_web::rt::spawn(async move {
        let cmd_executor = Box::leak(Box::new(cmd_executor));
        let deploy_without_topology_file = CommandArgs::Deploy {
            topology_file: "_NONE".to_string(),
        };
        let deployment_config = post_deployment.0;
        let submit_rs = cmd_executor
            .run(deploy_without_topology_file, Some(deployment_config))
            .await;
        info!(
            "MonoClusterWebService submit deploy task complete {:?}",
            submit_rs
        );
    });
    HttpResponse::Ok().finish()
}

#[post("/control/{cluster}/{command}")]
pub async fn ctl_cluster(
    cmd_executor: web::Data<Arc<CommandExecutor>>,
    cluster: web::Path<String>,
    command: web::Path<String>,
) -> Result<impl Responder, WebHandleError> {
    validate_command(command.as_str())?;
    let command_arg = match command.as_str() {
        "install" => Some(CommandArgs::Install {
            cluster: cluster.to_string(),
        }),
        "start" => Some(CommandArgs::Start {
            cluster: cluster.to_string(),
        }),
        "stop" => Some(CommandArgs::Stop {
            cluster: cluster.to_string(),
            force: Some("false".to_string()),
        }),
        "status" => Some(CommandArgs::Status {
            cluster: cluster.to_string(),
        }),
        _ => None,
    };
    if let Some(ctl_command) = command_arg {
        actix_web::rt::spawn(async move {
            let cmd_executor = Box::leak(Box::new(cmd_executor));
            let ctl_cmd_submit = cmd_executor.run(ctl_command, None).await;
            info!(
                "MonoClusterWebService submit cluster ctl task complete {:?}",
                ctl_cmd_submit
            );
        });
        Ok(HttpResponse::Ok().finish())
    } else {
        Ok(HttpResponse::BadRequest().finish())
    }
}

#[get("/status/{cluster}/{command}")]
pub async fn check_cmd_status(
    cmd_executor: web::Data<Arc<CommandExecutor>>,
    cluster: web::Path<String>,
    command: web::Path<String>,
) -> Result<impl Responder, WebHandleError> {
    validate_command(command.as_str())?;
    let cmd_executor = cmd_executor;
    let cluster_str = cluster.as_str();
    let host_vec = cmd_executor.get_cluster_host(cluster_str).await?;
    if let Some(hosts) = host_vec {
        let command_str = command.as_str();
        let task_status_vec = cmd_executor
            .get_task_status_by_hosts(cluster_str, command_str, &hosts)
            .await?;
        let mut success = Vec::new();
        let mut failure = Vec::new();
        for task_status in task_status_vec.iter() {
            if task_status.task_status == 0 {
                success.push(TaskId::from_json_string(task_status.clone().task));
            } else {
                failure.push(TaskId::from_json_string(task_status.clone().task));
            }
        }

        let status = if !failure.is_empty() {
            "failure"
        } else if hosts.len() == task_status_vec.len() {
            "success"
        } else {
            "progress"
        };
        let success = failure.is_empty() && hosts.len() == task_status_vec.len();
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
