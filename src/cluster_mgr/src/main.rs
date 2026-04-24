use clap::{CommandFactory, Parser};
use clap_complete::generate;
use cluster_mgr::cli::cmd_base::CmdExecutor;
use cluster_mgr::cli::{Command, SubCommand, HOME_DIR};
use owo_colors::OwoColorize;
use std::io;
use std::process::exit;
use tracing::{error, info};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 2 && matches!(args[1].as_str(), "-v" | "-V" | "--version") {
        println!("eloqctl version output {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    let cmd = Command::parse_from(&args);
    if let Some(SubCommand::Completion { shell, output }) = &cmd.subcmd {
        let mut app = Command::command();
        let mut writer: Box<dyn io::Write> =
            match output {
                Some(path) => Box::new(std::fs::File::create(path).unwrap_or_else(|e| {
                    panic!("failed to create completion file {:?}: {}", path, e)
                })),
                None => Box::new(io::stdout()),
            };
        generate(shell.as_clap_shell(), &mut app, "eloqctl", &mut writer);
        return;
    }

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
        if let Err(e) = executor.run(sub, None, cmd.quiet, cmd.verbose).await {
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
