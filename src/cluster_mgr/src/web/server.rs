use crate::cli::cmd_base::CommandExecutor;
use crate::cli::config::CONFIG_PATH_DIR;
use crate::web::web_handler::{check_cmd_status, check_health, ctl_cluster, deploy_cluster};
use actix_web::{middleware, web, App, HttpServer};
use std::env;
use std::sync::Arc;
use tracing::info;

macro_rules! server_listen_addr {
    ($addr_or_port:expr, $default:expr) => {{
        if let Some(value) = $addr_or_port {
            value
        } else {
            $default
        }
    }};
}

#[derive(Clone)]
pub struct CliMgrHttpServer;

impl CliMgrHttpServer {
    pub async fn start_by_default(config_path: String) -> anyhow::Result<()> {
        CliMgrHttpServer::start(None, None, config_path).await
    }

    pub async fn start(
        addr: Option<&str>,
        port: Option<u16>,
        config_path: String,
    ) -> anyhow::Result<()> {
        let listen_addr = server_listen_addr!(addr, "127.0.0.1");
        let listen_port = server_listen_addr!(port, 8090);
        info!("CliMgrHttpServer start at {listen_addr}:{listen_port}");
        env::set_var(CONFIG_PATH_DIR, config_path);
        let cmd_executor = web::Data::new(Arc::new(CommandExecutor::new()));
        let server = HttpServer::new(move || {
            App::new()
                .wrap(middleware::Logger::default())
                .app_data(cmd_executor.clone())
                .service(check_health)
                .service(check_cmd_status)
                .service(deploy_cluster)
                .service(ctl_cluster)
                .service(
                    web::resource("/")
                        .route(web::get().to(|| async { "Hi man. I'm CliMgrHttpServer" })),
                )
        })
        .bind((listen_addr, listen_port))?
        .run();
        Ok(server.await?)
    }
}

#[cfg(test)]
mod tests {
    use crate::web::server::CliMgrHttpServer;
    use std::path::PathBuf;

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_server_start() {
        tokio::task::spawn(async {
            let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let config_path = manifest_dir.join("config");
            let srv =
                CliMgrHttpServer::start_by_default(config_path.to_str().unwrap().to_string()).await;
            assert!(srv.is_ok());
        });
        let response = reqwest::get("http://127.0.0.1:8090/check_health")
            .await
            .unwrap();
        println!("response = {response:#?}");
        let is_success = response.status().is_success();
        assert!(is_success);
        //assert_eq!(response.status().as_u16(), 200);
        let rsp_content = response.bytes().await.unwrap();
        let rsp_string = String::from_utf8_lossy(rsp_content.as_ref());
        println!("check health response: {rsp_string}");
    }
}
