use clap::Parser;
use cluster_mgr::{
    cli::{cmd_base::NOT_PRINT_TASK_RESULT, set_home_dir},
    config::CONFIG_PATH_DIR,
};
use rest_api::server::CliMgrHttpServer;
use rest_api::ServerCommandArgs;
use std::env;
use tracing::{info, Level};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let server_cmd_args = ServerCommandArgs::parse();
    info!("MonoClusterREST command args = {:?}", server_cmd_args);
    std::env::set_var(NOT_PRINT_TASK_RESULT, "true");
    set_home_dir(&server_cmd_args.home)?;
    let srv_static = Box::leak(Box::new(CliMgrHttpServer {}));
    srv_static
        .start(
            server_cmd_args.addr,
            server_cmd_args.port,
            env::var(CONFIG_PATH_DIR).unwrap(),
        )
        .await?;
    Ok(())
}
