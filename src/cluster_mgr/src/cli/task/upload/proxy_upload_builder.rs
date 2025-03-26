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
use std::io::{Read, Write};
use std::path::PathBuf;

pub struct ProxyUploadBuilder;

impl UploadTaskBuilder for ProxyUploadBuilder {
    fn build(&self, config: &Config) -> IndexMap<TaskId, TaskInstance> {
        let proxy_config = match config {
            Config::Proxy(cfg) => cfg,
            _ => panic!("Expected ProxyConfig for ProxyUploadBuilder"),
        };

        // Read the template config file
        let config_template_file_path =
            config_template(PROXY_CONF_TEMPLATE).expect("get proxy config template error");

        // Copy the template to upload dir first (this creates eloqproxy.ini)
        let template_dest = upload_dir().join(PROXY_CONF_TEMPLATE);
        fs::copy(&config_template_file_path, &template_dest)
            .expect("Failed to copy proxy config template to upload dir");

        // Generate full proxy config (the original with all proxy_addrs)
        let full_config = proxy_config
            .gen_proxy_configs()
            .expect("Failed to generate proxy configurations");

        // Load the fully populated original ini
        let full_ini = ini::Ini::load_from_file(&full_config)
            .expect("Failed to load complete proxy config as INI");

        // Generate individual config files for each proxy address
        let template_base = PROXY_CONF_TEMPLATE.trim_end_matches(".ini");
        let upload_base_dir = upload_dir();

        // For each proxy address in proxy_addrs, create a separate config file
        for (idx, proxy_addr) in proxy_config.proxy_service.proxy_addrs.iter().enumerate() {
            // Create a new INI file starting with all settings from the full config
            let mut proxy_ini = full_ini.clone();

            // Override only the proxy_addr field for this specific address
            proxy_ini.set_to::<String>(None, "proxy_addr".to_string(), proxy_addr.clone());

            // Set individual web_service_port if available
            if idx < proxy_config.proxy_service.web_service_ports.len() {
                let web_service_port = &proxy_config.proxy_service.web_service_ports[idx];
                proxy_ini.set_to::<String>(
                    None,
                    "web_service_port".to_string(),
                    web_service_port.clone(),
                );
            } else if !proxy_config.proxy_service.web_service_ports.is_empty() {
                // If we have more proxy addresses than web service ports, cycle through them
                let port_idx = idx % proxy_config.proxy_service.web_service_ports.len();
                let web_service_port = &proxy_config.proxy_service.web_service_ports[port_idx];
                proxy_ini.set_to::<String>(
                    None,
                    "web_service_port".to_string(),
                    web_service_port.clone(),
                );
            }

            // Create the config file path with index
            let config_filename = format!("{}_{}.ini", template_base, idx);
            let config_path = upload_base_dir.join(&config_filename);

            // Write the customized INI file
            proxy_ini.write_to_file(&config_path).expect(&format!(
                "Failed to write config content to {}",
                config_filename
            ));
        }

        let source_host = get_source_host(None);
        let mut tasks: IndexMap<TaskId, TaskInstance> = Default::default();

        // For each proxy address, create a task to upload the corresponding config file
        for (idx, proxy_addr) in proxy_config.proxy_service.proxy_addrs.iter().enumerate() {
            let host = proxy_addr
                .split(':')
                .next()
                .map(|s| s.to_string())
                .expect("Invalid proxy address format");

            // Configure the upload task for the specific config file
            let config_filename = format!("{}_{}.ini", template_base, idx);
            let config_path = upload_base_dir.join(&config_filename);

            let upload_cnf_file = UploadFile {
                source: config_path.to_string_lossy().into_owned(),
                dest: proxy_config.proxy_service.install_dir(),
                extension: "".to_string(), // Empty because filename already includes extension
                host: host.clone(),
                copy_dir: false,
            };

            let (id, task) = build_task_instance(
                source_host.clone(),
                upload_cnf_file,
                config,
                &format!("proxy-ini-{}", idx),
                &format!("upload-proxy-ini-{}", idx),
            );
            tasks.insert(id, task);

            // Upload binary to the same host
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
                &format!("proxy-bin-{}", idx),
                &format!("upload-proxy-bin-{}", idx),
            );
            tasks.insert(id, task);
        }

        tasks
    }
}
