use clap::Parser;
use cluster_mgr::cli::cmd_base::CommandExecutor;
use cluster_mgr::cli::config::CONFIG_PATH_DIR;
use cluster_mgr::cli::ClusterMgrCommandArgs;
use std::env;
use tracing::{error, Level};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let level = if let Ok(tracing_env) = env::var("MONO_CLUSTER_MGR_TRACING") {
        if !tracing_env.is_empty() && tracing_env.to_lowercase() == "true" {
            Level::INFO
        } else {
            Level::WARN
        }
    } else {
        Level::WARN
    };
    println!("ClusterMgr Tracing Level = {:?}", level);
    let filter = EnvFilter::from_default_env()
        .add_directive("russh::client::encrypted=warn".parse().unwrap());
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_env_filter(filter)
        .init();
    let cluster_mgr_cmd = ClusterMgrCommandArgs::parse();
    let config_path = match cluster_mgr_cmd.config {
        Some(ref config) => config.to_str().unwrap().to_string(),
        None => {
            let current_dir = env::current_dir().unwrap();
            let config_path_buf = current_dir.join("config");
            let config_path = config_path_buf.as_path();
            if !config_path.exists() {
                error!(
                    "The config [{:?}] folder was not found in the current process's directory",
                    config_path_buf
                );
                return;
            } else {
                config_path.to_str().unwrap().to_string()
            }
        }
    };
    env::set_var(CONFIG_PATH_DIR, config_path.clone());
    let cmd_executor = Box::leak(Box::new(CommandExecutor::new()));
    if let Some(command) = cluster_mgr_cmd.command {
        println!("ClusterMgr receive {:?} command", command.clone());
        let _rs = cmd_executor.run(command).await;
    }
}
