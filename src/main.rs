use clap::Parser;
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

fn main() {
    println!("{}", BANNER);
    println!();
    println!("!!! Welcome Monograph Waiter!!!");
    println!("!!!「Monograph Waiter」 is the productivity tool for developers.");
    println!("!!! Type help to list all commands. Use 'exit' to quit the command line.");
    println!();
    let cmd_cli_options = CmdCliOptions::parse();
    println!("load config from={:?}", cmd_cli_options);
    CmdCli.start();
}
