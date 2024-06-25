use clap::Parser;
use cluster_mgr::cli::cmd_base::CommandExecutor;
use cluster_mgr::cli::ClusterMgrCommandArgs;
use std::process::exit;
use tracing::{error, info, Level};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args = ClusterMgrCommandArgs::parse();
    let level = if args.quiet { Level::WARN } else { Level::INFO };
    tracing_subscriber::fmt().with_max_level(level).init();
    info!("ClusterMgr Tracing Level = {level:?}");
    let executor = Box::leak(Box::new(CommandExecutor::new(args.home)));
    if let Some(command) = args.command {
        info!("ClusterMgr receive {:?} command", command.clone());
        if let Err(e) = executor.run(command, None).await {
            error!("{}", e);
            exit(1);
        }
    }
}
