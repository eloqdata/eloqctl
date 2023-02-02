use crate::handler::{check_cmd_status, check_health, ctl_cluster, deploy_cluster};
use actix_server::Server;
use actix_web::{middleware, web, App, HttpServer};
use cluster_mgr::cli::cmd_base::CommandExecutor;
use cluster_mgr::cli::config::{DeploymentConfig, CONFIG_PATH_DIR};
use cluster_mgr::cli::CommandArgs;
use std::env;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
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

async fn listen_exit_signal<F, Fut, T>(t: T, call_back: F)
where
    T: Send + 'static,
    F: Fn(T) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut interrupt = signal(SignalKind::interrupt()).unwrap();
    let mut terminate = signal(SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = interrupt.recv() => {
           info!("Recv interrupt.");
           call_back(t).await;
        }
        _ = terminate.recv() => {
           info!("Recv terminate.");
           call_back(t).await;
        }
    }
}

#[derive(Clone, Debug)]
pub struct RequestPayload {
    pub command: Option<CommandArgs>,
    pub config: Option<DeploymentConfig>,
}

#[derive(Clone)]
pub struct LongTaskRequestHandler {
    cmd_executor: Arc<CommandExecutor>,
    tx: crossbeam_channel::Sender<RequestPayload>,
    rx: crossbeam_channel::Receiver<RequestPayload>,
}

impl LongTaskRequestHandler {
    pub async fn new(cmd_executor: CommandExecutor) -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        let handler = LongTaskRequestHandler {
            cmd_executor: Arc::new(cmd_executor),
            tx,
            rx,
        };
        let handler_arc = Arc::new(handler.clone());
        let handler_clone = Arc::clone(&handler_arc);
        tokio::spawn(async move {
            listen_exit_signal(handler_clone, |handler_clone| async move {
                handler_clone.close()
            })
            .await;
        });
        tokio::spawn(async move {
            let _ = handler_arc.handle().await;
        });
        handler
    }

    pub fn get_command_executor(&self) -> &CommandExecutor {
        self.cmd_executor.as_ref()
    }

    fn close(&self) {
        info!("LongTaskRequestHandler will exit.");
        self.tx
            .send(RequestPayload {
                command: None,
                config: None,
            })
            .unwrap();
    }

    pub fn submit(&self, payload: RequestPayload) {
        self.tx.send(payload).unwrap();
    }

    pub async fn handle(&self) -> anyhow::Result<()> {
        let cmd_executor = Box::leak(Box::new(self.cmd_executor.clone()));
        while let Ok(payload) = self.rx.recv() {
            let cmd_opt = payload.command;
            if cmd_opt.is_none() {
                break;
            }
            let cmd = cmd_opt.unwrap();
            info!("Global handler process command={}", cmd.as_ref());
            match cmd.as_ref() {
                "deploy" => {
                    let config = payload.config.unwrap();
                    cmd_executor.run(cmd, Some(config)).await?
                }
                _ => cmd_executor.run(cmd, None).await?,
            }
        }
        Ok(())
    }
}

pub struct CliMgrHttpServer {}

unsafe impl Send for CliMgrHttpServer {}

impl CliMgrHttpServer {
    pub async fn start(
        &'static self,
        addr: Option<String>,
        port: Option<u16>,
        config_path: String,
    ) -> anyhow::Result<()> {
        let server = CliMgrHttpServer::new_http_server(addr, port, config_path).await?;
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
        config_path: String,
    ) -> anyhow::Result<Server> {
        let listen_addr = server_listen_addr!(addr, "127.0.0.1".to_string());
        let listen_port = server_listen_addr!(port, 8090);
        env::set_var(CONFIG_PATH_DIR, config_path);
        let handler = LongTaskRequestHandler::new(CommandExecutor::new()).await;
        let global_handler = web::Data::new(handler);
        let server = HttpServer::new(move || {
            App::new()
                .wrap(middleware::Logger::default())
                .app_data(global_handler.clone())
                .service(check_health)
                .service(check_cmd_status)
                .service(deploy_cluster)
                .service(ctl_cluster)
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
