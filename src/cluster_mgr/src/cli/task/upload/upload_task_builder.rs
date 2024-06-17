use crate::cli::task::task_base::{TaskArgValue, TaskHost, TaskId, TaskInstance};
use crate::cli::task::upload::cass_conf_upload_builder::CassConfUploadBuilder;
use crate::cli::task::upload::codis_upload::CodisUpload;
use crate::cli::task::upload::data_dir_upload_builder::DataDirUploadBuilder;
use crate::cli::task::upload::monitor_upload_builder::*;
use crate::cli::task::upload::monograph_upload_builder::{EloqUpload, MonographUploadBuilder};
use crate::cli::task::upload::tx_conf_upload_builder::TxConfUpload;
use crate::cli::task::upload::upload_task::UploadTask;
use crate::cli::{upload_dir, upload_host_dir};
use crate::config::config_base::{DeploymentConfig, UploadFile};
use crate::config::connection::Connection;
use crate::config::deployment::Product;
use crate::config::{CREATE_MONITOR_USER_SQL_FILE, MONOGRAPH_INSTALL_SCRIPT};
use indexmap::IndexMap;
use itertools::Itertools;
use local_ip_address::local_ip;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::collections::HashMap;
use std::path::PathBuf;
use walkdir::WalkDir;

pub trait UploadTaskBuilder {
    /// During the deployment phase, it is necessary to generate the corresponding upload execution tasks based on the deployment.yaml.
    /// These tasks include but are not limited to:
    /// 1. Uploading MonographDB TxService, including configuration files and bootstrap database commands for each instance.
    /// 2. Uploading MonographDB LogService, including start-up commands for each instance (if configured).
    /// 3. Uploading the Monitor component, including (NodeExporter, MySQLExporter, Prometheus, Grafana, Cassandra Monitor)
    ///    and configuration files for all components.
    /// 4. Modifying and uploading the configuration files for Cassandra config (cassandra.yml, jvm11-server.options).
    fn build(&self, config: &DeploymentConfig) -> IndexMap<TaskId, TaskInstance>;
}

pub(crate) const SCP_COMMAND: &str = "_scp_cmd_";
pub(crate) const SOURCE_IP: &str = "_source_ip_";

// r#"scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no {copy_dir}
// {scp_auth_key} -P {port} {source_path_str}
// {remote_user}@{remote_host}:{remote_install_dir}/{dest_file_name}"#,
pub(crate) const SCP_COMMAND_TEMPLATE: &str = "scp -o UserKnownHostsFile=/dev/null \
-o StrictHostKeyChecking=no _COPY_DIR -i _SCP_AUTH_KEY -P_SCP_PORT \
_SOURCE  _REMOTE_USER@_REMOTE_HOST:_DEST";

pub(crate) fn scp(upload_file: &UploadFile, conn: Connection) -> String {
    let auth_key = conn.ssh_auth_key().unwrap();
    let port = conn.ssh_port();
    let copy_dir = if upload_file.copy_dir { "-r" } else { "" };
    SCP_COMMAND_TEMPLATE
        .replace("_COPY_DIR", copy_dir)
        .replace("_SCP_AUTH_KEY", auth_key.as_str())
        .replace("_SCP_PORT", port.to_string().as_str())
        .replace("_SOURCE", upload_file.source.as_str())
        .replace("_REMOTE_USER", conn.username.as_str())
        .replace("_REMOTE_HOST", upload_file.host.as_str())
        .replace("_DEST", upload_file.dest.as_str())
}

#[derive(Clone, Debug)]
pub enum UploadTaskBuilderType {
    CassConf,
    DataDir,
    MonographAll,
    MonitorConf,
    MonographConf,
    Codis,
    EloqImage,
}

#[macro_export]
macro_rules! build_upload_tasks {
    ($builder_impl:ident, $conf: expr) => {{
        $builder_impl {}.build($conf)
    }};
}

pub fn upload_tasks(
    builder_type: UploadTaskBuilderType,
    conf: &DeploymentConfig,
) -> IndexMap<TaskId, TaskInstance> {
    match builder_type {
        UploadTaskBuilderType::CassConf => CassConfUploadBuilder {}.build(conf),
        UploadTaskBuilderType::DataDir => DataDirUploadBuilder {}.build(conf),
        UploadTaskBuilderType::MonographAll => MonographUploadBuilder {}.build(conf),
        UploadTaskBuilderType::MonitorConf => MonitorInfraConfUploadBuilder {}.build(conf),
        UploadTaskBuilderType::MonographConf => TxConfUpload {}.build(conf),
        UploadTaskBuilderType::Codis => CodisUpload {}.build(conf),
        UploadTaskBuilderType::EloqImage => EloqUpload::build_tasks(
            conf,
            "update",
            "upload_image",
            EloqUpload::eloq_image_upload(&conf.deployment),
        ),
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

pub(crate) fn list_files_by_host(host: &str, product: Product) -> Vec<String> {
    let mut paths = WalkDir::new(upload_host_dir(host))
        .min_depth(1)
        .into_iter()
        .filter_map(|entry_rs| entry_rs.ok())
        .map(|entry| entry.into_path())
        .collect_vec();
    if product == Product::EloqSQL {
        paths.push(upload_dir().join("my_local.cnf"));
        paths.push(upload_dir().join(MONOGRAPH_INSTALL_SCRIPT));
        paths.push(upload_dir().join(CREATE_MONITOR_USER_SQL_FILE));
    }
    paths
        .into_iter()
        .map(|pb| pb.to_str().unwrap().to_string())
        .collect()
}

pub(crate) fn build_task_instance(
    source_host: String,
    upload_file: UploadFile,
    config: &DeploymentConfig,
    cmd: &str,
    task_name: &str,
) -> (TaskId, TaskInstance) {
    let host = &upload_file.host;
    let task_id = TaskId {
        cmd: cmd.to_string(),
        task: task_name.to_string(),
        host: host.to_string(),
    };

    let conn = &config.connection;
    let scp_cmd = scp(&upload_file, conn.clone());
    let upload_task = UploadTask::new(config.clone(), task_id.clone());
    (
        task_id,
        TaskInstance {
            task_input: HashMap::from([
                (SCP_COMMAND.to_string(), TaskArgValue::Str(scp_cmd)),
                (SOURCE_IP.to_string(), TaskArgValue::Str(source_host)),
            ]),
            task: Box::new(upload_task),
            task_host: TaskHost::Local {},
        },
    )
}
