use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use std::path::PathBuf;
use strum_macros::AsRefStr;
use tracing::error;

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
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,
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
        long_about = "Stop the MonographDB cluster(TxService LogService Storage). with the specified cluster name.\n
./cluster_mgr stop --cluster $CLUSTER_NAME --force true|false  --all true|false
    "
    )]
    #[strum(serialize = "stop")]
    Stop {
        #[arg(long, value_name = "CLUSTER NAME")]
        cluster: String,
        #[arg(short, long, value_name = "FORCE STOP")]
        force: Option<String>,
        #[arg(short, long, value_name = "STOP ALL")]
        all: Option<String>,
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
        Note: Please edit conf/my_template.cnf first\n ./cluster_mgr update-conf --cluster $CLUSTER_NAME --restart true | false"
    )]
    #[strum(serialize = "update-conf")]
    UpdateConf {
        #[arg(short, long, value_name = "CLUSTER NAME")]
        cluster: String,
        #[arg(long, value_name = "Whether to restart the TX service.TURE|FALSE ")]
        restart: Option<String>,
    },
    #[command(
        long_about = "Build a playground quickly.\n./cluster_mgr play --topology-file  ${PWD}/config/playground.yaml"
    )]
    #[strum(serialize = "play")]
    Play {
        #[arg(short, long, value_name = "CLUSTER TOPOLOGY FILE")]
        topology_file: String,
    },
}

pub fn download_dir() -> PathBuf {
    let download_dir = dirs::download_dir();
    if download_dir.is_none() {
        let download_path_buf = dirs::home_dir()
            .unwrap()
            .join("Downloads")
            .join("mono-cluster-cli");
        let download_path_create_rs = std::fs::create_dir_all(download_path_buf.as_path());
        if let Err(create_err) = download_path_create_rs {
            let err_msg = create_err.to_string();
            error!("Create download path  {download_path_buf:#?} error {err_msg}");
            panic!("Create download path Error cause by {err_msg:?} ");
        }
        download_path_buf
    } else {
        dirs::download_dir().unwrap()
    }
}

pub fn download_file_path(download_files: Vec<String>) -> Vec<PathBuf> {
    let download_dir = download_dir();
    download_files
        .iter()
        .map(|file| download_dir.join(file.as_str()))
        .collect_vec()
}

pub fn file_process_progress(
    total_size: u64,
    file_name: String,
    process_chars: &str,
) -> ProgressBar {
    let cmd_pb = ProgressBar::new(total_size);
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
