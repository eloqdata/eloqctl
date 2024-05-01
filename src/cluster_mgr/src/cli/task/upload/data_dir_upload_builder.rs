use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, UploadTaskBuilder,
};
use crate::cli::upload_dir;
use crate::config::config_base::{DeploymentConfig, UploadFile};
use crate::config::DeploymentPackage;
use indexmap::IndexMap;

pub struct DataDirUploadBuilder;

impl UploadTaskBuilder for DataDirUploadBuilder {
    /// Upload the MonographDB data_dir to the remote host.
    fn build(&self, config: &DeploymentConfig) -> IndexMap<TaskId, TaskInstance> {
        if config.get_host_list(DeploymentPackage::MonographTx).len() == 1 {
            return IndexMap::new();
        }
        let deployment_ref = &config.deployment;
        let bootstrap_host = deployment_ref.bootstrap_host();
        let local = get_source_host(None);
        let install_dir = config.install_dir();
        let datafarm = upload_dir().join("datafarm").to_string_lossy().to_string();
        deployment_ref
            .tx_service
            .host
            .iter()
            .filter(|host| !host.as_str().eq(bootstrap_host.as_str()))
            .map(|host| UploadFile {
                source: datafarm.clone(),
                dest: install_dir.clone(),
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
