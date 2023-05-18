use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, create_temp_dir, UploadTaskBuilder,
};
use crate::config::config_base::{DeploymentConfig, UploadFile};
use indexmap::IndexMap;
use itertools::Itertools;

pub struct CassConfUploadBuilder;

impl UploadTaskBuilder for CassConfUploadBuilder {
    /// Upload the cassandra.yaml and jvm11-server.options or cassandra-env.sh file to the remote host (remote host list from deployment.yaml).
    fn build(&self, config: &DeploymentConfig) -> IndexMap<TaskId, TaskInstance> {
        let deployment = config.deployment.clone();
        let monitor = deployment.monitor;
        let install_dir = config.install_dir();
        let cass_config_rs = config.deployment.storage_service.gen_cassandra_config(
            install_dir.clone(),
            deployment.cluster_name,
            monitor,
        );
        assert!(cass_config_rs.is_ok());
        let cass_config = cass_config_rs.unwrap();
        let dest_file = format!("{install_dir}/apache-cassandra/conf");
        cass_config
            .into_iter()
            .map(|(host, cass_configs)| {
                let source_path = cass_configs
                    .iter()
                    .map(|path_buf| {
                        let path = path_buf.as_path();
                        let extension = path.extension().unwrap().to_str().unwrap();
                        if extension.eq("yaml") {
                            let tmp_prefix = format!("cass_conf_{host}");
                            let cass_conf_tmp =
                                create_temp_dir(tmp_prefix.as_str(), "/tmp").unwrap();
                            let cass_conf_path_ref = cass_conf_tmp.as_path();
                            let dest_file = cass_conf_path_ref.join("cassandra.yaml");
                            std::fs::copy(path, dest_file.as_path()).unwrap();
                            dest_file.to_str().unwrap().to_string()
                        } else {
                            let path = path.to_str();
                            path.unwrap().to_string()
                        }
                    })
                    .join(" ");
                UploadFile {
                    source: source_path,
                    dest: dest_file.clone(),
                    extension: "".to_string(),
                    host,
                    copy_dir: false,
                }
            })
            .map(|upload_file| {
                build_task_instance(upload_file, config, "install", "cass_conf_upload")
            })
            .collect::<IndexMap<TaskId, TaskInstance>>()
    }
}
