use crate::cli::create_upload_cluster_dir;
use crate::cli::task::group::Config;
use crate::cli::task::task_base::{TaskArgValue, TaskHost, TaskId, TaskInstance};
use crate::cli::task::task_utils::{ClusterNodesWithConfig, ScaleOperationType};
use crate::cli::task::upload::data_dir_upload_builder::DataDirUploadBuilder;
use crate::cli::task::upload::eloq_upload_builder::EloqUpload;
use crate::cli::task::upload::eloq_upload_builder::EloqUploadBuilder;
use crate::cli::task::upload::monitor_upload_builder::MonitorInfraConfUploadBuilder;
use crate::cli::task::upload::tx_conf_upload_builder::TxConfUpload;
use crate::cli::task::upload::upload_task::UploadTask;
use crate::config::config_base::UploadFile;
use crate::config::connection::Connection;
use crate::config::deployment::Deployment;
use indexmap::IndexMap;
use itertools::Itertools;
use local_ip_address::local_ip;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::watch;
use walkdir::WalkDir;

pub trait UploadTaskBuilder {
    /// During the deployment phase, it is necessary to generate the corresponding upload execution tasks based on the deployment.yaml.
    /// These tasks include but are not limited to:
    /// 1. Uploading EloqDB TxService, including configuration files and bootstrap database commands for each instance.
    /// 2. Uploading EloqDB LogService, including start-up commands for each instance (if configured).
    /// 3. Uploading the Monitor component, including NodeExporter, Prometheus,
    ///    Grafana, and configuration files for all components.
    fn build(&self, config: &Config) -> IndexMap<TaskId, TaskInstance>;
}

pub(crate) const TRANSFER_COMMAND: &str = "_transfer_cmd_";
pub(crate) const TRANSFER_ARGS: &str = "_transfer_args_";
pub(crate) const SOURCE_IP: &str = "_source_ip_";

pub(crate) fn scp_args(upload_file: &UploadFile, conn: Connection) -> Vec<String> {
    let auth_key = conn.ssh_auth_key().unwrap();
    let (remote_host, port) = conn.ssh_endpoint(&upload_file.host);
    let mut args = vec![
        "-o".to_string(),
        "UserKnownHostsFile=/dev/null".to_string(),
        "-o".to_string(),
        "StrictHostKeyChecking=no".to_string(),
        "-o".to_string(),
        "PasswordAuthentication=no".to_string(),
        "-o".to_string(),
        "BatchMode=yes".to_string(),
        "-o".to_string(),
        "ConnectTimeout=10".to_string(),
        "-o".to_string(),
        "PreferredAuthentications=publickey".to_string(),
        "-i".to_string(),
        auth_key,
        "-P".to_string(),
        port.to_string(),
    ];
    if upload_file.copy_dir {
        args.push("-r".to_string());
    }
    args.push(upload_file.source.trim().to_string());
    args.push(format!(
        "{}@{}:{}",
        conn.username,
        remote_host,
        upload_file.dest.trim()
    ));
    args
}

pub(crate) fn sync_args(upload_file: &UploadFile, conn: Connection) -> Vec<String> {
    let auth_key = conn.ssh_auth_key().unwrap();
    let (remote_host, port) = conn.ssh_endpoint(&upload_file.host);
    let ssh_cmd = format!(
        "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o PasswordAuthentication=no -o BatchMode=yes -o ConnectTimeout=10 -o PreferredAuthentications=publickey -i {} -p {}",
        auth_key, port
    );
    let mut args = vec![
        "-az".to_string(),
        "-e".to_string(),
        ssh_cmd,
        "--rsync-path".to_string(),
        format!("mkdir -p {} && rsync", upload_file.dest.trim()),
        "--partial".to_string(),
        "--progress".to_string(),
    ];
    if upload_file.delete_remote {
        args.push("--delete".to_string());
    }
    let mut source = upload_file.source.trim().to_string();
    if upload_file.copy_dir && !source.ends_with('/') {
        source.push('/');
    }
    args.push(source);
    args.push(format!(
        "{}@{}:{}",
        conn.username,
        remote_host,
        upload_file.dest.trim()
    ));
    args
}

#[derive(Clone, Debug)]
pub enum UploadTaskBuilderType {
    DataDir,
    EloqAll,
    MonitorConf,
    TxConf,
    EloqImage,
    ScaleTxConf,
}

#[macro_export]
macro_rules! build_upload_tasks {
    ($builder_impl:ident, $conf: expr) => {{
        $builder_impl {}.build($conf)
    }};
}

pub fn upload_tasks(
    builder_type: UploadTaskBuilderType,
    conf: &Config,
) -> IndexMap<TaskId, TaskInstance> {
    match builder_type {
        UploadTaskBuilderType::DataDir => DataDirUploadBuilder {}.build(conf),
        UploadTaskBuilderType::EloqAll => EloqUploadBuilder {}.build(conf),
        UploadTaskBuilderType::MonitorConf => MonitorInfraConfUploadBuilder {}.build(conf),
        UploadTaskBuilderType::TxConf => TxConfUpload {}.build(conf),
        UploadTaskBuilderType::ScaleTxConf => unreachable!(),
        UploadTaskBuilderType::EloqImage => {
            let cluster_config = match conf {
                Config::Cluster(cfg) => cfg,
                _ => panic!("Expected ClusterConfig for TxConfUpload"),
            };
            EloqUpload::build_tasks(
                conf,
                "update",
                "upload_eloq_image",
                EloqUpload::eloq_image_upload(&cluster_config.deployment),
            )
        }
    }
}

pub fn upload_tasks_with_nodes(
    builder_type: UploadTaskBuilderType,
    conf: &Config,
    operation_type: &ScaleOperationType,
    nodes: &Vec<String>,
    is_candidate: &Option<Vec<bool>>,
    scale_op_rx: watch::Receiver<ClusterNodesWithConfig>,
) -> IndexMap<TaskId, TaskInstance> {
    match builder_type {
        UploadTaskBuilderType::ScaleTxConf => {
            let mut result = IndexMap::new();

            let upload_tasks =
                TxConfUpload {}.build_with_nodes(conf, operation_type, nodes, is_candidate);
            result.extend(upload_tasks);

            // Add tasks to upload the cluster configuration to new nodes
            let cluster_config_upload_tasks = TxConfUpload::build_cluster_config_upload_tasks(
                conf,
                operation_type,
                nodes,
                &scale_op_rx,
            );

            // Merge the cluster config upload tasks with the configuration file upload tasks
            result.extend(cluster_config_upload_tasks);

            result
        }
        _ => unreachable!(),
    }
}

#[allow(dead_code)]
pub(crate) fn create_temp_dir(prefix: &str, parent_dir: &str) -> anyhow::Result<PathBuf> {
    let suffix: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect();
    let tmp_dir = PathBuf::from(parent_dir).join(format!("_{prefix}.{suffix}"));
    std::fs::create_dir_all(tmp_dir.clone())?;
    Ok(tmp_dir)
}

pub(crate) fn get_source_host(host: Option<String>) -> String {
    if let Some(source_host) = host {
        source_host
    } else {
        let local_ip = local_ip();
        match local_ip {
            Err(e) => panic!("ClusterMgr get LocalIp Err {e}"),
            Ok(my_ip_addr) => my_ip_addr.to_string(),
        }
    }
}

pub(crate) fn list_files_by_host(host: &str, config: &Deployment) -> Vec<String> {
    let dir = format!("{}/{}", config.cluster_name, host);
    let paths = WalkDir::new(create_upload_cluster_dir(&dir))
        .min_depth(1)
        .into_iter()
        .filter_map(|entry_rs| entry_rs.ok())
        .map(|entry| entry.into_path())
        .collect_vec();
    paths
        .into_iter()
        .map(|pb| pb.to_str().unwrap().to_string())
        .collect()
}

pub(crate) fn build_task_instance(
    source_host: String,
    upload_file: UploadFile,
    config: &Config,
    cmd: &str,
    task_name: &str,
) -> (TaskId, TaskInstance) {
    let host = &upload_file.host;
    let task_id = TaskId {
        cmd: cmd.to_string(),
        task: task_name.to_string(),
        host: host.to_string(),
    };

    let conn = config.conn_ref();
    let (transfer_bin, transfer_args) = if upload_file.copy_dir {
        ("rsync".to_string(), sync_args(&upload_file, conn.clone()))
    } else {
        ("scp".to_string(), scp_args(&upload_file, conn.clone()))
    };
    let upload_task = UploadTask::new(task_id.clone());
    (
        task_id,
        TaskInstance {
            task_input: HashMap::from([
                (
                    TRANSFER_COMMAND.to_string(),
                    TaskArgValue::Str(transfer_bin),
                ),
                (TRANSFER_ARGS.to_string(), TaskArgValue::List(transfer_args)),
                (SOURCE_IP.to_string(), TaskArgValue::Str(source_host)),
            ]),
            task: Box::new(upload_task),
            task_host: TaskHost::Local {},
        },
    )
}
