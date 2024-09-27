use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, UploadTaskBuilder,
};
use crate::config::config_base::{DeployConfig, UploadFile};
use crate::config::DeploymentPackage;
use indexmap::IndexMap;
use itertools::Itertools;
use std::path::Path;

pub struct TxConfUpload;

impl UploadTaskBuilder for TxConfUpload {
    fn build(&self, config: &DeployConfig) -> IndexMap<TaskId, TaskInstance> {
        let all_conf_path = config
            .gen_all_monograph_configs()
            .expect("Failed generate my_HOST.ini")
            .iter()
            .map(|path_buf| path_buf.to_str().unwrap().to_string())
            .collect_vec();
        let remote_dest = config.deployment.tx_srv_home();
        let upload_cnf_files = config
            .get_host_list(DeploymentPackage::MonographTx)
            .iter()
            .flat_map(|host| {
                all_conf_path
                    .iter()
                    .filter(|path| path.contains(host.as_str()))
                    .map(|path| UploadFile {
                        source: path.to_string(),
                        dest: remote_dest.clone(),
                        extension: "ini".to_string(),
                        host: host.to_string(),
                        copy_dir: false,
                    })
            })
            .collect_vec();

        println!("{:?}", upload_cnf_files);

        let source_host = get_source_host(None);
        upload_cnf_files
            .iter()
            .map(|upload_file| {
                let host = upload_file.host.clone();
                let source_path = upload_file.source.clone();

                let file_stem_str = Path::new(&source_path)
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .unwrap_or("unknown");

                build_task_instance(
                    source_host.clone(),
                    upload_file.clone(),
                    config,
                    "config-update",
                    &format!("upload-ini-{host}-{file_stem_str}"),
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>()
    }
}
