use crate::cli::task::task_base::{TaskArgValue, TaskHost, TaskId, TaskInstance};
use crate::cli::task::upload::cass_conf_upload_builder::CassConfUploadBuilder;
use crate::cli::task::upload::data_dir_upload_builder::DataDirUploadBuilder;
use crate::cli::task::upload::monitor_upload_builder::*;
use crate::cli::task::upload::monograph_upload_builder::MonographUploadBuilder;
use crate::cli::task::upload::upload_task::UploadTask;
use crate::config::config_base::{DeploymentConfig, UploadFile};
use crate::config::connection::Connection;
use indexmap::IndexMap;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::collections::HashMap;
use std::path::PathBuf;

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

// r#"scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no {copy_dir}
// {scp_auth_key} -P {port} {source_path_str}
// {remote_user}@{remote_host}:{remote_install_dir}/{dest_file_name}"#,
pub(crate) const SCP_COMMAND_TEMPLATE: &str = r#"scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no _COPY_DIR \
-i _SCP_AUTH_KEY -P _SCP_PORT _SOURCE \
_REMOTE_USER@_REMOTE_HOST:_DEST"#;

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
    InstallTar,
    MonitorConf,
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
        UploadTaskBuilderType::InstallTar => MonographUploadBuilder {}.build(conf),
        UploadTaskBuilderType::MonitorConf => MonitorInfraConfUploadBuilder {}.build(conf),
    }
}

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

pub(crate) fn build_task_instance(
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
    let conn_user = conn.clone().username;
    let ssh_port = conn.ssh_port() as usize;
    let remote_host = TaskHost::Remote {
        user: conn_user,
        port: ssh_port,
        hosts: host.to_string(),
    };
    let scp_cmd = scp(&upload_file, conn.clone());
    let upload_task = UploadTask::new(config.clone(), task_id.clone());
    (
        task_id,
        TaskInstance {
            task_input: HashMap::from([(SCP_COMMAND.to_string(), TaskArgValue::Str(scp_cmd))]),
            task: Box::new(upload_task),
            task_host: remote_host,
        },
    )
}
