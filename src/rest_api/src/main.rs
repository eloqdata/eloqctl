use clap::Parser;
use cluster_mgr::cli::config::CONFIG_PATH_DIR;
use cluster_mgr::cli::task::task_base::NOT_PRINT_TASK_RESULT;
use rest_api::server::CliMgrHttpServer;
use rest_api::ServerCommandArgs;
use tracing::{info, Level};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let server_cmd_args = ServerCommandArgs::parse();
    let config_path = server_cmd_args.config.to_str().unwrap().to_string();
    info!("MonoClusterREST command args = {:?}", server_cmd_args);
    std::env::set_var(NOT_PRINT_TASK_RESULT, "true");
    std::env::set_var(CONFIG_PATH_DIR, config_path.clone());
    let srv_static = Box::leak(Box::new(CliMgrHttpServer {}));
    srv_static
        .start(server_cmd_args.addr, server_cmd_args.port, config_path)
        .await?;
    Ok(())
}
