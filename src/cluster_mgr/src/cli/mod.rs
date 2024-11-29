use crate::config::{deployment::Product, StorageProvider, TopoFormat};
use chrono::{DateTime, Duration, Local, NaiveDateTime, TimeZone, Utc};
use clap::{Parser, Subcommand};
use humantime::Duration as HumanDuration;
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
    #[command(long_about = "Operations on the proxy service")]
    #[strum(serialize = "proxy")]
    Proxy {
        #[command(subcommand)]
        command: ProxyCommand,
    },
    #[command(long_about = "Launch a cluster quickly")]
    #[strum(serialize = "launch")]
    Launch {
        topology_file: String,
        #[arg(short, long, default_value_t = false)]
        skip_deps: bool,
    },

    #[strum(serialize = "start")]
    #[command(long_about = "Start cluster(TxService/LogService/Storage)")]
    Start {
        cluster: String,
        #[arg(long)]
        nodes: Vec<String>,
    },
    #[command(long_about = "Stop cluster components")]
    #[strum(serialize = "stop")]
    Stop {
        cluster: String,
        #[arg(long, default_value = "true")]
        tx: Option<bool>,
        #[arg(long, default_value_t = false)]
        log: bool,
        #[arg(long, default_value_t = false)]
        store: bool,
        #[arg(long, default_value_t = false)]
        monitor: bool,
        #[arg(short, long, default_value_t = false)]
        force: bool,
        #[arg(short, long, default_value_t = false)]
        all: bool,
        #[arg(long, value_name = "cluster password")]
        password: Option<String>,
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
        long_about = "Update config file by ~/.eloqctl/upload/{cluster_name}/Eloqkv.ini and restart"
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
    Monitor { command: String, cluster: String },

    #[command(long_about = "LogService control component")]
    #[strum(serialize = "log-srv")]
    LogService { command: String, cluster: String },

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

    #[strum(serialize = "backup")]
    Backup {
        cluster: String,
        #[command(subcommand)]
        command: BackupCommand,
    },
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    s.parse::<HumanDuration>()
        .map(|d| Duration::from_std(*d).unwrap())
        .map_err(|e| e.to_string())
}

fn parse_datetime(s: &str) -> Result<DateTime<Utc>, String> {
    // Try parsing with timezone information (RFC 3339)
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }
    // Try parsing without timezone, format "%Y-%m-%d %H:%M:%S", assuming local time
    if let Ok(naive_dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        let local_dt = Local
            .from_local_datetime(&naive_dt)
            .single()
            .ok_or_else(|| format!("Invalid or ambiguous local datetime: '{}'", s))?;
        return Ok(local_dt.with_timezone(&Utc));
    }
    // Try parsing without timezone, format "%Y-%m-%dT%H:%M:%S", assuming local time
    if let Ok(naive_dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        let local_dt = Local
            .from_local_datetime(&naive_dt)
            .single()
            .ok_or_else(|| format!("Invalid or ambiguous local datetime: '{}'", s))?;
        return Ok(local_dt.with_timezone(&Utc));
    }
    // If all parsing attempts fail, return an error
    Err(format!(
        "Invalid timestamp '{}'. Accepted formats are:\n\
        - RFC 3339: '2024-11-14T15:01:00Z'\n\
        - 'YYYY-MM-DD HH:MM:SS': '2024-11-14 15:01:00' (assumed local time)\n\
        - 'YYYY-MM-DDTHH:MM:SS': '2024-11-14T15:01:00' (assumed local time)",
        s
    ))
}

#[derive(Subcommand, Clone, Debug, Hash, PartialEq, Eq, AsRefStr)]
#[command(next_line_help = true)]
pub enum BackupCommand {
    #[strum(serialize = "start")]
    Start {
        #[arg(long)]
        path: String,
        #[arg(long, value_name = "cluster password")]
        password: Option<String>,
        #[arg(long, value_name = "destination host")]
        dest_host: Option<String>,
        #[arg(long, value_name = "destination username")]
        dest_user: Option<String>,
    },
    #[strum(serialize = "list")]
    List {
        // #[arg(long)]
        // before_datetime: String,
    },
    #[strum(serialize = "remove")]
    Remove {
        #[arg(
            long,
            value_name = "PERIOD",
            help = "Deletes all snapshots older than the specified period.\n\
            Accepted formats:\n\
            - '2 days'\n\
            - '15h'\n\
            - '1 week'\n\
            - '3 months'\n\
            - '1y 6mo 2w 4d 3h 5m 7s'\n\
            See https://docs.rs/humantime/latest/humantime/fn.parse_duration.html for more details.",
            value_parser = parse_duration
        )]
        until: Option<Duration>,

        #[arg(
            long,
            value_name = "TIMESTAMP",
            help = "Deletes all snapshots created before this timestamp.\n\
            Accepted formats:\n\
            - RFC 3339: '2024-11-14T15:01:00Z'\n\
            - 'YYYY-MM-DD HH:MM:SS' (assumed local time zone)\n\
            - 'YYYY-MM-DDTHH:MM:SS' (assumed local time zone)",
            value_parser = parse_datetime
        )]
        before: Option<chrono::DateTime<chrono::Utc>>,
    },
    #[strum(serialize = "dump-aof")]
    DumpAOF {
        #[arg(long)]
        rocksdb_path: String,
        #[arg(long)]
        output_file_dir: String,
        #[arg(long)]
        thread_count: Option<String>,
    },
    #[strum(serialize = "dump-rdb")]
    DumpRDB {
        #[arg(long)]
        rocksdb_path: String,
        #[arg(long)]
        output_file_dir: String,
        #[arg(long)]
        thread_count: Option<String>,
    },
}

#[derive(Subcommand, Clone, Debug, Hash, PartialEq, Eq, AsRefStr)]
#[command(next_line_help = true)]
pub enum ProxyCommand {
    #[strum(serialize = "start")]
    Start {
        #[arg(long)]
        config: String,
    },
    #[strum(serialize = "stop")]
    Stop {
        #[arg(long)]
        proxy_name: String,
    },
    #[strum(serialize = "add")]
    Add {
        #[arg(long)]
        proxy_name: String,
        #[arg(long)]
        cluster_name: String,
    },
    #[strum(serialize = "remove")]
    Remove {
        #[arg(long)]
        proxy_name: String,
        #[arg(long)]
        cluster_name: String,
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

pub fn create_upload_cluster_dir(dir: &str) -> PathBuf {
    let dir_buf = upload_dir().join(dir);
    create_dir_all(dir_buf.as_path()).expect("create upload directory for host");
    dir_buf
}
