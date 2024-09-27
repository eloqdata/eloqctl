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
        let local = get_source_host(None);
        let txsrv_home = config.deployment.tx_srv_home();
        let datafarm = upload_dir().join("datafarm").to_string_lossy().to_string();

        // Proceed to iterate over the merged hosts list
        let hosts = deployment_ref.tx_service.merge_hosts();
        hosts
            .iter()
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
