use crate::global_handler::GlobalCommandHandler;
use crate::handler::{
    check_cmd_status, check_health, ctl_cluster, deploy_cluster, install_run_deps, launch_cluster,
    mono_service_status,
};
use crate::listen_exit_signal;
use actix_server::Server;
use actix_web::{middleware, web, App, HttpServer};
use cluster_mgr::cli::cmd_base::CommandExecutor;
use std::path::PathBuf;
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

pub struct CliMgrHttpServer {}

unsafe impl Send for CliMgrHttpServer {}

impl CliMgrHttpServer {
    pub async fn start(
        &'static self,
        addr: Option<String>,
        port: Option<u16>,
        home: Option<PathBuf>,
    ) -> anyhow::Result<()> {
        let server = CliMgrHttpServer::new_http_server(addr, port, home).await?;
        let web_handler = server.handle();
        info!("Starting CliMgrHttpServer.");
        let shutdown_join = tokio::spawn(async move {
            listen_exit_signal(web_handler, |web_handler| async move {
                info!("Stopping CliMgrHttpServer web_handler offload.");
                web_handler.stop(true).await;
            })
            .await
        });
        tokio::spawn(async move { server.await.unwrap() }).await?;
        shutdown_join.await?;
        Ok(())
    }

    async fn new_http_server(
        addr: Option<String>,
        port: Option<u16>,
        home: Option<PathBuf>,
    ) -> anyhow::Result<Server> {
        let listen_addr = server_listen_addr!(addr, "127.0.0.1".to_string());
        let listen_port = server_listen_addr!(port, 8090);
        let handler = GlobalCommandHandler::new(CommandExecutor::new(home)).await;
        let global_handler = web::Data::new(handler);
        let server = HttpServer::new(move || {
            App::new()
                .wrap(middleware::Logger::default())
                .app_data(global_handler.clone())
                .service(check_health)
                .service(check_cmd_status)
                .service(deploy_cluster)
                .service(ctl_cluster)
                .service(mono_service_status)
                .service(install_run_deps)
                .service(launch_cluster)
                .service(web::resource("/").route(
                    web::get().to(|| async { "Hey man. I'm MonographDB cluster RESTful Service." }),
                ))
        })
        .shutdown_timeout(20)
        .disable_signals()
        .bind((listen_addr.as_str(), listen_port))?
        .run();
        Ok(server)
    }
}
