use crate::config::ConfigErr::DownloadUrlFormatErr;
use anyhow::anyhow;
use itertools::Itertools;
use serde_yaml::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use strum_macros::{AsRefStr, Display};
use thiserror::Error;
use tracing::error;
use url::Url;

pub mod config_base;
pub mod connection;
pub mod deployment;
#[allow(dead_code)]
pub mod log_service;
pub mod monitor;
pub mod proxy_config_base;
pub mod proxy_service;
pub mod storage_service_config;

pub const ELOQKV_TEMPLATE_INI: &str = "EloqKv.ini";
pub const ELOQDSS_TEMPLATE_INI: &str = "EloqDss.ini";
pub const PROXY_CONF_TEMPLATE: &str = "eloqproxy.ini";
pub const PROXY_BIN: &str = "eloqkv-proxy";
pub const ELOQKV_NODE_INI: &str = "EloqKv-node";
pub const START_LOG_TEMPLATE: &str = "start_tx_log.bash";
pub const JVM_SETTING_HOLDER: &str = "_GC_SETTINGS_PLACEHOLDER_";
pub const PROMETHEUS_CONFIG_TEMPLATE: &str = "mono_prometheus.yaml";
pub const ALERTMANAGER_CONFIG_TEMPLATE: &str = "mono_alertmanager.yaml";
pub const MONITOR_DIR: &str = "monitor";
pub const ALERT_RULES_TEMPLATE: &str = "alert.rules";

pub const GRAFANA_DASHBOARDS_CONFIG_TEMPLATE: &str = "grafana_dashboards.yaml";

pub const PROMETHEUS_CONFIG_FILE: &str = "prometheus.yml";
pub const ALERTMANAGER_CONFIG_FILE: &str = "alertmanager.yml";
pub const GRAFANA_PROMETHEUS_DS_FILE: &str = "prometheus-datasource.yml";

pub const GRAFANA_CONFIG_TEMPLATE_V9: &str = "grafana_config.ini";
pub const GRAFANA_CONFIG_TEMPLATE_V13: &str = "grafana_config_v13.ini";
pub const GRAFANA_CONFIG_FILE: &str = "custom.ini";
pub const ALERTMANAGER_WEBHOOK_ADAPTER_FEISHU_TEMPLATE: &str = "feishu.zh.tmpl";
pub const PROMETHEUSALERT_CONFIG_TEMPLATE: &str = "prometheusalert_app.conf";
pub const PROMETHEUSALERT_CONFIG_FILE: &str = "app.conf";

pub const CDN: &str = "https://download.eloqdata.com";

#[macro_export]
macro_rules! all_hosts_merge {
    ($config_ref:expr, $($pkg_name:ident $(,)?)*) => {{
        let mut all_hosts = vec![];
        $(
           let host_vec = $config_ref.get_host_list(DeploymentPackage::$pkg_name);
           if !host_vec.is_empty(){
               all_hosts.extend(host_vec.into_iter());
           }
        )*
        all_hosts
    }};
}

#[derive(PartialEq, Eq, Clone, Error, Debug)]
pub enum ConfigErr {
    #[error("EloqDB storage provider config error [{0}].")]
    StorageConfigErr(String),
    #[error("The download url format is incorrect. Storage Provider is {0}")]
    DownloadUrlFormatErr(String),
}

pub const CONFIG_PATH_DIR: &str = "DEFAULT_CLUSTER_MGR_CLI_CONFIG";
pub const UPLOAD_PATH_DIR: &str = "DEFAULT_CLUSTER_MGR_CLI_UPLOAD";

pub const SECTION_LOCAL: &str = "local";
pub const SECTION_CLUSTER: &str = "cluster";
pub const SECTION_STORE: &str = "store";
pub const SECTION_METRIC: &str = "metrics";
pub const SECTION_PROXY: &str = "proxy";

#[derive(Hash, Debug, Clone, PartialEq, Eq, AsRefStr, Display, clap::ValueEnum)]
pub enum StorageProvider {
    #[strum(serialize = "dynamodb")]
    Dynamodb,
    #[strum(serialize = "rocksdb")]
    Rocksdb,
    #[strum(serialize = "eloqdss")]
    #[clap(name = "eloqdss")]
    EloqDSS,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum DownloadUrl {
    Local(Url),
    Remote(Url),
}

impl DownloadUrl {
    pub fn is_local(&self) -> bool {
        match self {
            DownloadUrl::Local(_url) => true,
            DownloadUrl::Remote(_url) => false,
        }
    }

    pub fn file_name(&self) -> String {
        let url = match self {
            DownloadUrl::Local(local_url) => local_url,
            DownloadUrl::Remote(remote_url) => remote_url,
        };
        url.path_segments()
            .unwrap()
            .next_back()
            .unwrap()
            .to_string()
    }

    pub fn cache_dir(&self) -> anyhow::Result<String> {
        let mut dir = crate::cli::download_dir();
        match self {
            DownloadUrl::Local(_) => {}
            DownloadUrl::Remote(url) => {
                if url.domain() == Some("download.eloqdata.com") {
                    let mut seg = url.path_segments().unwrap();
                    let _filename = seg.next_back();
                    for d in seg {
                        dir.push(d);
                    }
                } else if url.domain() == Some("github.com") {
                    let file_name = self.file_name();
                    if let Some(parsed) = crate::github_release::parse_asset_name(&file_name) {
                        let product_dir = match parsed.product.as_str() {
                            "log-service" => "logservice",
                            other => other,
                        };
                        dir.push(product_dir);
                        dir.push(parsed.store);
                    }
                }
            }
        }
        Ok(dir.to_str().unwrap().to_string())
    }

    pub fn get_url(&self) -> String {
        match self {
            DownloadUrl::Local(url_string) => {
                let url = Url::parse(url_string.as_str()).unwrap();
                url.path().to_string()
            }
            DownloadUrl::Remote(url) => url.to_string(),
        }
    }

    pub fn from_url_str(url_str: &str) -> anyhow::Result<Self> {
        let url_rs = url::Url::parse(url_str);
        if let Err(err) = url_rs {
            error!("The Url format is incorrect {:?}", err.to_string());
            Err(anyhow!(DownloadUrlFormatErr(err.to_string())))
        } else {
            let url = url_rs.unwrap();
            let schema = url.scheme().to_lowercase();
            match schema.as_str() {
                "file" => Ok(DownloadUrl::Local(url)),
                "http" | "https" => Ok(DownloadUrl::Remote(url)),
                _ => {
                    panic!(
                        "The url schema is incorrect. For now only support file or http. {url_str}",
                    );
                }
            }
        }
    }
}

#[derive(Hash, Debug, Clone, PartialEq, Eq, AsRefStr)]
pub enum DeploymentPackage {
    #[strum(serialize = "eloq")]
    EloqTx,
    #[strum(serialize = "storage")]
    Storage,
    #[strum(serialize = "prometheus")]
    Prometheus,
    #[strum(serialize = "alertmanager")]
    Alertmanager,
    #[strum(serialize = "grafana")]
    Grafana,
    #[strum(serialize = "prometheusalert")]
    PrometheusAlert,
    #[strum(serialize = "eloq_log")]
    EloqLog,
    #[strum(serialize = "eloq_standby")]
    EloqStandby,
    #[strum(serialize = "eloq_voter")]
    EloqVoter,
    #[strum(serialize = "proxy")]
    Proxy,
}

pub fn config_path_string(path: Option<String>) -> anyhow::Result<String> {
    if let Some(path_string) = path {
        Ok(path_string)
    } else {
        Ok(std::env::var(CONFIG_PATH_DIR)?)
    }
}

pub fn load_remote_env(path: Option<String>) -> anyhow::Result<HashMap<String, String>> {
    let file = File::open(PathBuf::from(config_path_string(path)?).join("remote_env"))?;
    let mut reader = BufReader::new(file);
    let mut file_content_buf = String::new();
    reader.read_to_string(&mut file_content_buf)?;

    let env_props = file_content_buf
        .lines()
        .filter(|line| line.contains('='))
        .map(|line| {
            let splits = line.split('=').collect_vec();
            assert_eq!(splits.len(), 2);
            (splits[0].to_string(), splits[1].to_string())
        })
        .collect::<HashMap<String, String>>();

    Ok(env_props)
}

// ~/.eloqctl/config
pub fn config_template(file_name: &str) -> anyhow::Result<PathBuf> {
    let config_path = std::env::var(CONFIG_PATH_DIR)?;
    let path_buf = PathBuf::from(config_path.as_str()).join(file_name);
    if path_buf.exists() {
        Ok(path_buf)
    } else {
        Err(anyhow!("EloqDB config not found in the {:?}", path_buf))
    }
}

// ~/.eloqctl/upload/{cluster_name}
pub fn cluster_config_template(cluster_name: &str, file_name: &str) -> anyhow::Result<PathBuf> {
    let upload_path = std::env::var(UPLOAD_PATH_DIR)?;
    let path_buf = PathBuf::from(upload_path.as_str())
        .join(cluster_name)
        .join(file_name);
    if path_buf.exists() {
        Ok(path_buf)
    } else {
        Err(anyhow!(
            "Cluster ({}) config template not found in the {:?}",
            cluster_name,
            path_buf
        ))
    }
}

pub fn load_yaml_config_template(template_name: &str) -> anyhow::Result<HashMap<String, Value>> {
    let cass_template_path_buf = config_template(template_name)?;
    let cass_opened_file = File::open(cass_template_path_buf.as_path())?;
    let yaml_map = serde_yaml::from_reader::<File, HashMap<String, Value>>(cass_opened_file)?;
    Ok(yaml_map)
}
