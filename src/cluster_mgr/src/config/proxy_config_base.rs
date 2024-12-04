use crate::all_hosts_merge;
use crate::cli::upload_dir;
use crate::cli::HOME_DIR;
use crate::config::config_path_string;
use crate::config::connection::Connection;
use crate::config::proxy_service::ProxyService;
use crate::config::{DeploymentPackage, PathBuf, PROXY_CONF_TEMPLATE};
use anyhow::anyhow;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::fs;
use tracing::info;

#[serde_with::skip_serializing_none]
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ProxyConfig {
    pub connection: Connection,
    pub proxy_service: ProxyService,
}

impl ProxyConfig {
    pub fn get_host_list(&self, _service: DeploymentPackage) -> Vec<String> {
        self.proxy_service
            .proxy_addrs
            .iter()
            .filter_map(|addr| addr.split(':').next().map(|s| s.to_string()))
            .collect::<Vec<String>>()
    }

    pub fn get_unique_host_list(&self) -> Vec<String> {
        let all_hosts = all_hosts_merge!(self, Proxy,);
        all_hosts.iter().unique().cloned().collect_vec()
    }

    pub fn gen_proxy_configs(&self) -> anyhow::Result<PathBuf> {
        let cnf_path = upload_dir().join(PROXY_CONF_TEMPLATE);
        let mut proxy_ini =
            ini::Ini::load_from_file(&cnf_path).expect("can not load proxy config template");

        let proxy_addr_list = self.proxy_service.proxy_addrs.join(",");
        proxy_ini.set_to::<String>(None, "proxy_addr".to_string(), proxy_addr_list);

        let web_service_port_list = self
            .proxy_service
            .web_service_ports
            .iter()
            .flat_map(|port| {
                port.split(',') // Split on ','
                    .map(str::trim) // Trim any whitespace
            })
            .collect::<Vec<&str>>()
            .join(",");
        proxy_ini.set_to::<String>(None, "web_service_port".to_string(), web_service_port_list);

        let eloqkv_cluster_addr_list = self
            .proxy_service
            .eloqkv_cluster_addr
            .iter()
            .flat_map(|host_str| {
                host_str
                    .split(',') // Split on ','
                    .map(str::trim) // Trim any whitespace
            })
            .collect::<Vec<&str>>()
            .join(",");
        proxy_ini.set_to::<String>(
            None,
            "eloqkv_cluster_addr".to_string(),
            eloqkv_cluster_addr_list,
        );

        let eloqkv_cluster_token_list = self
            .proxy_service
            .eloqkv_cluster_token
            .iter()
            .flat_map(|host_str| {
                host_str
                    .split(',') // Split on ','
                    .map(str::trim) // Trim any whitespace
            })
            .collect::<Vec<&str>>()
            .join(",");
        proxy_ini.set_to::<String>(
            None,
            "eloqkv_cluster_token".to_string(),
            eloqkv_cluster_token_list,
        );

        let eloqkv_cluster_password_list = self
            .proxy_service
            .eloqkv_cluster_password
            .iter()
            .flat_map(|host_str| {
                host_str
                    .split(',') // Split on ','
                    .map(str::trim) // Trim any whitespace
            })
            .collect::<Vec<&str>>()
            .join(",");
        proxy_ini.set_to::<String>(
            None,
            "eloqkv_cluster_password".to_string(),
            eloqkv_cluster_password_list,
        );

        proxy_ini.write_to_file(&cnf_path)?;
        Ok(cnf_path)
    }

    fn read_config_from_file(path: String) -> anyhow::Result<Self> {
        let content = fs::read_to_string(path)?
            .replace("${USER}", &whoami::username())
            .replace(&format!("${{{HOME_DIR}}}"), &std::env::var(HOME_DIR)?);
        let proxy_config = serde_yaml::from_str::<ProxyConfig>(&content)?;
        Ok(proxy_config)
    }

    pub fn load(path: Option<String>) -> anyhow::Result<Self> {
        let path_string = config_path_string(path)?;
        info!("ProxyConfig load file from {}", path_string);
        let config = ProxyConfig::read_config_from_file(path_string.clone())
            .map_err(|err| anyhow!("{path_string}: {err}"))?;
        config.connection.auth.check_keypair()?;
        Ok(config)
    }

    pub fn load_from_string(config_content: String) -> anyhow::Result<Self> {
        let proxy_config_rs = serde_yaml::from_str::<ProxyConfig>(config_content.as_str());
        if let Ok(proxy_config) = proxy_config_rs {
            Ok(proxy_config)
        } else {
            Err(anyhow!(proxy_config_rs.err().unwrap().to_string()))
        }
    }

    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(self).unwrap()
    }
}
