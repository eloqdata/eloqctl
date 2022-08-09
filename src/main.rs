use clap::Parser;
use monograph_waiter::config::{MONOGRAPH_WATER_CONFIG_DIR, MONOGRAPH_WORKSPACE_DIR};
use monograph_waiter::extract_config_value;
use monograph_waiter::term::CmdCli;

const BANNER: &str = r#"
  __  __                                         _      __        __    _ _
 |  \/  | ___  _ __   ___   __ _ _ __ __ _ _ __ | |__   \ \      / /_ _(_) |_ ___ _ __
 | |\/| |/ _ \| '_ \ / _ \ / _` | '__/ _` | '_ \| '_ \   \ \ /\ / / _` | | __/ _ \ '__|
 | |  | | (_) | | | | (_) | (_| | | | (_| | |_) | | | |   \ V  V / (_| | | ||  __/ |
 |_|  |_|\___/|_| |_|\___/ \__, |_|  \__,_| .__/|_| |_|    \_/\_/ \__,_|_|\__\___|_|
                           |___/          |_|
"#;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct CmdCliOptions {
    /// config file location
    #[clap(short = 'c', long, value_parser)]
    config: String,
    #[clap(short = 'l', long, value_parser)]
    logfile: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("{}", BANNER);
    println!();
    println!("!!! Welcome Monograph Waiter!!!");
    println!("!!!「Monograph Waiter」 is the productivity tool for developers.");
    println!("!!! Type help to list all commands. Use 'exit' to quit the command line.");
    println!();
    let cmd_cli_options: CmdCliOptions = CmdCliOptions::parse();
    std::env::set_var(MONOGRAPH_WATER_CONFIG_DIR, cmd_cli_options.config.clone());
    std::env::set_var(
        MONOGRAPH_WORKSPACE_DIR,
        extract_config_value!("common", Common, Some(cmd_cli_options.config))
            .clone()
            .workspace,
    );
    println!(
        "ENV MONOGRAPH_WATER_CONFIG_DIR {:?}\n MONOGRAPH_WORKSPACE_DIR {:?}",
        std::env::var(MONOGRAPH_WATER_CONFIG_DIR),
        std::env::var(MONOGRAPH_WORKSPACE_DIR)
    );
    CmdCli.start().await;
    Ok(())
}
