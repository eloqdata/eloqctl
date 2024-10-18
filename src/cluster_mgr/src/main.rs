use clap::Parser;
use cluster_mgr::cli::cmd_base::CmdExecutor;
use cluster_mgr::cli::{Command, HOME_DIR};
use owo_colors::OwoColorize;
use std::process::exit;
use tracing::{error, info};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let cmd = Command::parse();
    let home = CmdExecutor::home_init(cmd.home).expect("home dir init failed");
    if let Some(sub) = cmd.subcmd {
        let log_path = home.join("logs").join(format!("last-{}.log", sub.as_ref()));
        let log_file = std::fs::File::create(&log_path).expect("can't create log");
        tracing_subscriber::fmt()
            .with_writer(log_file)
            .with_ansi(false)
            .init();

        let executor = Box::leak(Box::new(CmdExecutor::new(home)));
        info!("command: {:#?}", sub);
        if let Err(e) = executor.run(sub, None, cmd.quiet).await {
            error!("{}", e);
            eprintln!("{}: {e}\nlogfile: {}", "FAIL".red(), log_path.display());
            exit(1);
        }
    } else {
        println!("eloqctl is the cluster management tool of eloqdata.");
        println!("{HOME_DIR}={home:?}");
        println!("Use `eloqctl --help` to see how to use it.");
    }
}
