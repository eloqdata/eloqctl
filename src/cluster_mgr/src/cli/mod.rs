use crate::config::{deployment::Product, StorageProvider, TopoFormat};
use clap::{Parser, Subcommand};
use itertools::Itertools;
use std::{env, fs::create_dir_all, path::PathBuf};
use strum_macros::AsRefStr;

pub mod cmd_base;
mod cmd_printer;
pub mod ssh;
pub mod task;
pub mod util;

pub const CMD_STATUS: &str = "_cmd_status_";
pub const CMD_OUTPUT: &str = "_cmd_output_";
pub const CMD: &str = "_cmd_";

#[derive(Parser, Default, Debug)]
#[command(author, version = "0.6.0", about = "EloqData cluster management tool")]
#[command(next_line_help = true)]
pub struct Command {
    #[arg(long, value_name = "home-dir")]
    pub home: Option<PathBuf>,
    #[arg(short, long, default_value_t = false)]
    pub quiet: bool,
    #[command(subcommand)]
    pub subcmd: Option<SubCommand>,
}

#[derive(Subcommand, Clone, Debug, Hash, PartialEq, Eq, AsRefStr)]
#[command(next_line_help = true)]
pub enum SubCommand {
    #[command(long_about = "Launch a demo quickly")]
    #[strum(serialize = "demo")]
    Demo {
        product: Product,
        #[arg(short, long, default_value = "cassandra")]
        store: StorageProvider,
        #[arg(short, long, default_value = "latest")]
        version: String,
        #[arg(long, default_value_t = false)]
        skip_deps: bool,
        #[arg(long, default_value_t = false)]
        unlimited: bool,
        #[arg(long, default_value_t = false)]
        no_monitor: bool,
        #[arg(long, default_value_t = false)]
        joint_wal: bool,
        #[arg(long, value_delimiter = ';', value_name = "contact-points")]
        ext_cass: Vec<String>,
        #[arg(long)]
        cass_port: Option<u16>,
        #[arg(long, value_name = "user:password")]
        cass_auth: Option<String>,
    },
    #[command(long_about = "Check whether cluster can be deployed")]
    #[strum(serialize = "check")]
    Check { topology_file: String },
    #[command(long_about = "Launch a cluster quickly")]
    #[strum(serialize = "launch")]
    Launch {
        topology_file: String,
        #[arg(short, long, default_value_t = false)]
        skip_deps: bool,
    },
    #[strum(serialize = "start")]
    #[command(long_about = "Start the specified cluster(TxService/LogService/Storage)")]
    Start { cluster: String },
    #[command(long_about = "Stop the specified cluster(TxService/LogService/Storage)")]
    #[strum(serialize = "stop")]
    Stop {
        cluster: String,
        #[arg(short, long, default_value_t = false)]
        force: bool,
        #[arg(short, long, default_value_t = false)]
        all: bool,
    },
    #[command(long_about = "Restart the specified cluster")]
    #[strum(serialize = "restart")]
    Restart { cluster: String },
    #[command(long_about = "Check cluster status")]
    #[strum(serialize = "status")]
    Status {
        cluster: String,
        #[arg(short, long, value_name = "EloqSQL user")]
        user: Option<String>,
        #[arg(short, long, value_name = "EloqSQL password")]
        password: Option<String>,
        #[arg(short, long, value_name = "Wait cluster ready")]
        wait: Option<u16>,
    },
    #[command(long_about = "Update cluster version. This will stop/update/start the cluster")]
    #[strum(serialize = "update")]
    Update {
        cluster: Option<String>,
        version: Option<String>,
        #[arg(long, value_name = "version")]
        cassandra: Option<String>,
        #[arg(long, value_name = "url")]
        cass_mirror: Option<String>,
    },
    #[command(
        long_about = "Update the configuration file and restart the tx service. Note: Please edit config/Eloq**.cnf first"
    )]
    #[strum(serialize = "update-conf")]
    UpdateConf {
        cluster: String,
        #[arg(long, default_value_t = false)]
        restart: bool,
    },
    #[command(long_about = "Remove cluster")]
    #[strum(serialize = "remove")]
    Remove { cluster: String },
    #[command(long_about = "Inspect cluster configuration")]
    #[strum(serialize = "inspect")]
    Inspect {
        cluster: String,
        #[arg(short, long)]
        format: Option<TopoFormat>,
    },
    #[command(long_about = "Connect to cluster")]
    #[strum(serialize = "connect")]
    Connect { cluster: String },

    #[command(long_about = "List already created clusters")]
    #[strum(serialize = "list")]
    List,
    #[command(long_about = "List available versions")]
    #[strum(serialize = "versions")]
    Versions {
        #[arg(long)]
        product: Option<Product>,
        #[arg(long)]
        store: Option<StorageProvider>,
    },

    #[command(long_about = "monitor control component")]
    #[strum(serialize = "monitor")]
    Monitor { cluster: String, command: String },
    #[command(long_about = "LogService control component")]
    #[strum(serialize = "log-srv")]
    LogService { cluster: String, command: String },

    #[command(long_about = "Execute custom shell commands")]
    #[strum(serialize = "exec_cmd")]
    Exec {
        command: String,
        topology_file: String,
    },
    #[command(long_about = "Install package dependencies")]
    #[strum(serialize = "run-deps")]
    RunDeps { topology_file: String },
    #[strum(serialize = "deploy")]
    #[command(long_about = "Deploy a cluster by specifying the topology file")]
    Deploy { topology_file: String },
    #[strum(serialize = "install")]
    #[command(long_about = "Bootstrap the specified cluster to generate catalog")]
    Install { cluster: String },
    #[strum(serialize = "scale")]
    Scale {
        #[arg(short, long)]
        cluster: String,
        #[arg(long)]
        add_tx_node: Vec<String>,
        #[arg(long)]
        del_tx_node: Vec<String>,
    },
}

pub const HOME_DIR: &str = "ELOQCTL_HOME";

pub fn home_path() -> PathBuf {
    PathBuf::from(env::var(HOME_DIR).unwrap())
}

pub fn download_dir() -> PathBuf {
    home_path().join("download")
}

pub fn download_file_path(download_files: Vec<String>) -> Vec<PathBuf> {
    let download_dir = download_dir();
    download_files
        .iter()
        .map(|file| download_dir.join(file.as_str()))
        .collect_vec()
}

pub fn upload_dir() -> PathBuf {
    home_path().join("upload")
}

pub fn upload_host_dir(host: &str) -> PathBuf {
    let dir = upload_dir().join(host);
    create_dir_all(dir.as_path()).expect("create upload directory for host");
    dir
}
