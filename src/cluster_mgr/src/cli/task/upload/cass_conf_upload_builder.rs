use crate::cli::task::group::Config;
use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, UploadTaskBuilder,
};
use crate::config::config_base::UploadFile;
use indexmap::IndexMap;
use itertools::Itertools;

pub struct CassConfUploadBuilder;

impl UploadTaskBuilder for CassConfUploadBuilder {
    /// Upload the cassandra.yaml and jvm11-server.options or cassandra-env.sh file to the remote host (remote host list from deployment.yaml).
    fn build(&self, config: &Config) -> IndexMap<TaskId, TaskInstance> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => panic!("Expected ClusterConfig for CassConfUploadBuilder"),
        };

        let deployment = cluster_config.deployment.clone();
        let install_dir = cluster_config.install_dir();
        let cass_config_rs = cluster_config
            .deployment
            .gen_cassandra_config(install_dir.clone(), deployment.cluster_name.clone());
        assert!(cass_config_rs.is_ok());
        let source_host = get_source_host(None);
        let cass_config = cass_config_rs.unwrap();
        let dest_file = format!("{}/conf", deployment.cassandra_home());
        cass_config
            .into_iter()
            .map(|(host, cass_configs)| {
                let source_path = cass_configs
                    .into_iter()
                    .map(|path_buf| path_buf.as_path().to_str().unwrap().to_string())
                    .join(" ");
                let upload_file = UploadFile {
                    source: source_path,
                    dest: dest_file.clone(),
                    extension: "".to_string(),
                    host,
                    copy_dir: false,
                };
                build_task_instance(
                    source_host.clone(),
                    upload_file,
                    config,
                    "install",
                    "cass_conf_upload",
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>()
    }
}
