use crate::config::{deployment::Product, StorageProvider, CONFIG_PATH_DIR};
use anyhow::anyhow;
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use std::{env, fs::create_dir_all, path::PathBuf};
use strum_macros::AsRefStr;

pub mod cmd_base;
mod cmd_printer;
pub mod ssh;
pub mod task;

pub const CMD_STATUS: &str = "_cmd_status_";
pub const CMD_OUTPUT: &str = "_cmd_output_";
pub const CMD: &str = "_cmd_";

#[derive(Parser, Default, Debug)]
#[command(author, version = "1.0.0", about = "MonographDB Cluster Manager Cli")]
#[command(next_line_help = true)]
pub struct ClusterMgrCommandArgs {
    #[arg(long, value_name = "HOME_DIR")]
    pub home: Option<PathBuf>,
    #[command(subcommand)]
    pub command: Option<CommandArgs>,
}

#[derive(Subcommand, Clone, Debug, Hash, PartialEq, Eq, AsRefStr)]
#[command(next_line_help = true)]
pub enum CommandArgs {
    #[strum(serialize = "deploy")]
    #[command(
        long_about = "Deploy the MonographDB cluster by specifying the cluster_topology.yaml file\n./cluster_mgr deploy --topology-file  ${PWD}/config/deployment.yaml
    "
    )]
    Deploy {
        #[arg(short, long, value_name = "CLUSTER TOPOLOGY FILE")]
        topology_file: String,
    },
    #[strum(serialize = "install")]
    #[command(
        long_about = "bootstrap MonographDB to generate catalog. You need to specify the cluster name.\n./cluster_mgr install --cluster $CLUSTER_NAME
    "
    )]
    Install {
        #[arg(short = 'c', long, value_name = "CLUSTER NAME")]
        cluster: String,
    },
    #[strum(serialize = "start")]
    #[command(
        long_about = "Start the MonographDB cluster(TxService LogService Storage). with the specified cluster name\n./cluster_mgr start  --cluster $CLUSTER_NAME
    "
    )]
    Start {
        #[arg(short = 'l', long, value_name = "CLUSTER NAME")]
        cluster: String,
    },
    #[command(
        long_about = "Stop the MonographDB cluster(TxService LogService Storage). with the specified cluster name"
    )]
    #[strum(serialize = "stop")]
    Stop {
        #[arg(long, value_name = "CLUSTER NAME")]
        cluster: String,
        #[arg(short, long, default_value_t = false)]
        force: bool,
        #[arg(short, long, default_value_t = false)]
        all: bool,
    },
    #[command(
        long_about = "Restart the MonographDB cluster with the specified cluster name.\n./cluster_mgr restart --cluster $CLUSTER_NAME
    "
    )]
    #[strum(serialize = "restart")]
    Restart {
        #[arg(short, long, value_name = "CLUSTER NAME")]
        cluster: String,
    },

    #[command(long_about = "Execute custom shell commands.\n\
./cluster_mgr exec --command 'ls -la /data1/' --topology-file  ${PWD}/config/deployment.yaml")]
    #[strum(serialize = "exec_cmd")]
    Exec {
        #[arg(long, value_name = "SHELL COMMAND/SCRIPT")]
        command: String,
        #[arg(long, value_name = "CLUSTER TOPOLOGY FILE")]
        topology_file: String,
    },

    #[command(
        long_about = "Check MonographDB cluster status. If the username password is given,\n the connection to the target database is established, otherwise, the ps command is executed.
./cluster_mgr status --cluster $CLUSTER_NAME --user $DB_USER --password $DB_PASSWORD
    "
    )]
    #[strum(serialize = "status")]
    Status {
        #[arg(short, long, value_name = "CLUSTER NAME")]
        cluster: String,
        #[arg(short, long, value_name = "MonographDB user")]
        user: Option<String>,
        #[arg(short, long, value_name = "MonographDB password")]
        password: Option<String>,
        #[arg(short, long, value_name = "Wait cluster ready")]
        wait: Option<u16>,
    },
    #[command(
        long_about = "Install MonographDB runtime dependencies.\n./cluster_mgr run-deps --topology-file ${PWD}/config/deployment.yaml
    "
    )]
    #[strum(serialize = "run-deps")]
    RunDeps {
        #[arg(short, long, value_name = "CLUSTER TOPOLOGY FILE")]
        topology_file: String,
    },
    #[command(
        long_about = "Check whether cluster can be deployed\n./cluster_mgr check --topology-file ${PWD}/config/deployment.yaml"
    )]
    #[strum(serialize = "check")]
    Check {
        #[arg(long)]
        topology_file: String,
    },
    #[command(
        long_about = "Start or stop monitoring components,including prometheus, grafana,node_exporter,mysql_exporter.\n./cluster_mgr monitor --cluster $CLUSTER_NAME --command start | stop
    "
    )]
    #[strum(serialize = "monitor")]
    Monitor {
        #[arg(short, long, value_name = "CLUSTER NAME")]
        cluster: String,
        #[arg(long, value_name = "MONITOR START/STOP COMMAND")]
        command: String,
    },
    #[command(
        long_about = "Start or stop LogService This command is only available if LogService is deployed standalone\n ./cluster_mgr log-service --cluster $CLUSTER_NAME --command start | stop
    "
    )]
    #[strum(serialize = "log-srv")]
    LogService {
        #[arg(short, long, value_name = "CLUSTER NAME")]
        cluster: String,
        #[arg(long, value_name = "LogService START/STOP/STATUS COMMAND")]
        command: String,
    },
    #[command(
        long_about = "According to the deployment.yaml, update the related monograph_db cluster by stopping the cluster, replacing the package, and starting the cluster. \n./cluster_mgr upgrade --topology_file ${PWD}/config/deployment.yaml
    "
    )]
    #[strum(serialize = "upgrade")]
    Upgrade {
        #[arg(short, long, value_name = "CLUSTER TOPOLOGY FILE")]
        topology_file: String,
    },
    #[command(
        long_about = "Update the configuration file and restart the tx service (the default value of restart is true). \
        Note: Please edit config/my_template.cnf first\n ./cluster_mgr update-conf --cluster $CLUSTER_NAME"
    )]
    #[strum(serialize = "update-conf")]
    UpdateConf {
        #[arg(short, long, value_name = "CLUSTER NAME")]
        cluster: String,
        #[arg(long, default_value_t = false)]
        restart: bool,
    },
    #[command(
        long_about = "Launch a cluster quickly.\ncluster_mgr launch --topology-file  ${PWD}/config/deployment.yaml"
    )]
    #[strum(serialize = "launch")]
    Launch {
        #[arg(short, long, value_name = "CLUSTER TOPOLOGY FILE")]
        topology_file: String,
    },
    #[command(long_about = "Remove cluster.\ncluster_mgr remove --cluster $CLUSTER_NAME")]
    #[strum(serialize = "remove")]
    Remove {
        #[arg(short, long, value_name = "CLUSTER NAME")]
        cluster: String,
    },
    #[command(long_about = "Launch a demo quickly.\ncluster_mgr demo --product eloq-sql")]
    #[strum(serialize = "demo")]
    Demo {
        #[arg(short, long, value_name = "Product")]
        product: Product,
        #[arg(short, long, default_value = "cassandra")]
        store: StorageProvider,
        #[arg(short, long, default_value = "latest")]
        version: String,
    },
    #[command(long_about = "List created clusters")]
    #[strum(serialize = "list")]
    List,
    #[command(long_about = "Inspect cluster configuration")]
    #[strum(serialize = "inspect")]
    Inspect {
        #[arg(short, long, value_name = "CLUSTER NAME")]
        cluster: String,
    },
}

pub const HOME_DIR: &str = "CLUSTER_MGR_HOME";

pub fn set_home_dir(home: &Option<PathBuf>) -> anyhow::Result<()> {
    match home {
        Some(ref home) => env::set_var(HOME_DIR, home),
        None => {
            if env::var(HOME_DIR).is_err() {
                env::set_var(HOME_DIR, env::current_dir().unwrap())
            }
        }
    };
    // check config directory
    let cnf_dir = home_path().join("config");
    if !cnf_dir.exists() {
        return Err(anyhow!("Config path not exist: {} ", cnf_dir.display()));
    }
    env::set_var(CONFIG_PATH_DIR, cnf_dir);
    if !download_dir().exists() {
        std::fs::create_dir(download_dir())?;
    }
    if !upload_dir().exists() {
        std::fs::create_dir(upload_dir())?;
    }
    Ok(())
}

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

pub fn file_process_progress(file_name: String, process_chars: &str) -> ProgressBar {
    let cmd_pb = ProgressBar::hidden();
    let sty = format!(
        "{{spinner:.green}} {file_name:14}: [{{elapsed_precise}}] \
        [{{wide_bar:.green/white}}] \
        {{bytes}}/{{total_bytes}} ({{eta}})",
    );
    cmd_pb.set_style(
        ProgressStyle::default_spinner()
            .template(sty.as_str())
            .unwrap()
            .progress_chars(process_chars),
    );
    cmd_pb
}
