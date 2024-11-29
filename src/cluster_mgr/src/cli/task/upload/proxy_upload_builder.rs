use crate::cli::task::group::Config;
use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, UploadTaskBuilder,
};
use crate::cli::{download_dir, upload_dir};
use crate::config::config_base::UploadFile;
use crate::config::PROXY_CONF_TEMPLATE;
use crate::config::{config_template, PROXY_BIN};
use indexmap::IndexMap;
use std::fs;

pub struct ProxyUploadBuilder;

impl UploadTaskBuilder for ProxyUploadBuilder {
    fn build(&self, config: &Config) -> IndexMap<TaskId, TaskInstance> {
        let proxy_config = match config {
            Config::Proxy(cfg) => cfg,
            _ => panic!("Expected ProxyConfig for ProxyUploadBuilder"),
        };

        // copy eloqproxy.ini from ~/.eloqctl/config to ~/.eloqctl/upload
        let config_template_file_path =
            config_template(PROXY_CONF_TEMPLATE).expect("get proxy config template error");
        let env_sh = upload_dir().join(PROXY_CONF_TEMPLATE);
        fs::copy(&config_template_file_path, &env_sh).expect("copy proxy config template error");

        let conf_path = proxy_config
            .gen_proxy_configs()
            .expect("Failed to generate proxy configurations")
            .to_string_lossy()
            .into_owned();

        let source_host = get_source_host(None);
        let mut tasks: IndexMap<TaskId, TaskInstance> = Default::default();

        proxy_config
            .proxy_service
            .proxy_hosts
            .iter()
            .for_each(|host| {
                let upload_cnf_file = UploadFile {
                    source: conf_path.clone(),
                    dest: proxy_config.proxy_service.install_dir(),
                    extension: "ini".to_string(),
                    host: host.clone(),
                    copy_dir: false,
                };
                let (id, task) = build_task_instance(
                    source_host.clone(),
                    upload_cnf_file.clone(),
                    config,
                    "proxy-ini",
                    "upload-proxy-ini",
                );
                tasks.insert(id, task);

                let bin_source = download_dir().join(PROXY_BIN);
                let upload_bin = UploadFile {
                    source: bin_source.to_str().unwrap().to_string(),
                    dest: proxy_config.proxy_service.install_dir(),
                    extension: "".to_string(),
                    host: host.clone(),
                    copy_dir: false,
                };
                let (id, task) = build_task_instance(
                    source_host.clone(),
                    upload_bin,
                    config,
                    "proxy-bin",
                    "upload-proxy-bin",
                );
                tasks.insert(id, task);
            });

        tasks
    }
}
