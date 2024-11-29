use crate::cli::task::group::Config;
use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, UploadTaskBuilder,
};
use crate::config::config_base::UploadFile;
use crate::config::deployment::Codis;

use indexmap::IndexMap;

pub struct CodisUpload;

impl UploadTaskBuilder for CodisUpload {
    fn build(&self, config: &Config) -> IndexMap<TaskId, TaskInstance> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => panic!("Expected ClusterConfig for CodisUpload"),
        };

        let codis = cluster_config
            .deployment
            .codis
            .as_ref()
            .expect("codis not configured");
        let source_host = get_source_host(None);
        let source = cluster_config
            .deployment
            .codis_proxy_config()
            .expect("codis proxy config")
            .to_str()
            .unwrap()
            .to_string();
        let mut tasks = codis
            .proxy
            .iter()
            .map(|host| {
                let upload = UploadFile {
                    source: source.clone(),
                    dest: Codis::dir(&cluster_config.install_dir()),
                    extension: "toml".to_owned(),
                    host: host.clone(),
                    copy_dir: false,
                };
                build_task_instance(
                    source_host.clone(),
                    upload,
                    config,
                    "install",
                    "codis_proxy",
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>();

        let source = cluster_config
            .deployment
            .codis_dashboard_config()
            .expect("codis dashboard config")
            .to_str()
            .unwrap()
            .to_string();
        let upload = UploadFile {
            source: source.clone(),
            dest: Codis::dir(&cluster_config.install_dir()),
            extension: "toml".to_owned(),
            host: codis.dashboard.clone(),
            copy_dir: false,
        };
        let (id, task) = build_task_instance(
            source_host.clone(),
            upload,
            config,
            "install",
            "codis_dashboard",
        );
        tasks.insert(id, task);
        tasks
    }
}
