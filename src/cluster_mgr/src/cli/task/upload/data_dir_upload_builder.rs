use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, UploadTaskBuilder,
};
use crate::cli::upload_dir;
use crate::config::config_base::{DeployConfig, UploadFile};
use indexmap::IndexMap;

pub struct DataDirUploadBuilder;

impl UploadTaskBuilder for DataDirUploadBuilder {
    /// Upload the MonographDB data_dir to the remote host.
    fn build(&self, config: &DeployConfig) -> IndexMap<TaskId, TaskInstance> {
        let deployment_ref = &config.deployment;
        if deployment_ref.tx_service.is_none() {
            return IndexMap::new();
        }
        let txsrv = deployment_ref.tx_service.as_ref().unwrap();
        let bootstrap_host = txsrv.bootstrap_host();
        let local = get_source_host(None);
        let txsrv_home = config.deployment.tx_srv_home();
        let datafarm = upload_dir().join("datafarm").to_string_lossy().to_string();
        txsrv
            .host
            .iter()
            .filter(|host| !host.as_str().eq(bootstrap_host.as_str()))
            .map(|host| UploadFile {
                source: datafarm.clone(),
                dest: txsrv_home.clone(),
                extension: "datafarm".to_string(),
                host: host.to_string(),
                copy_dir: true,
            })
            .map(|upload_file| {
                build_task_instance(
                    local.clone(),
                    upload_file,
                    config,
                    "install",
                    "upload_datafarm",
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>()
    }
}
