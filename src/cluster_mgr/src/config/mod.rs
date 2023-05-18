use crate::config::ConfigErr::DownloadUrlFormatErr;
use anyhow::anyhow;
use itertools::Itertools;
use serde_yaml::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use strum_macros::AsRefStr;
use thiserror::Error;
use tracing::error;
use url::Url;

pub mod config_base;
pub mod connection;
pub mod deployment;
pub mod log_service;
pub mod monitor;
pub mod storage_service_config;

pub const MONOGRAPH_CONF_TEMPLATE: &str = "my_template.cnf";
pub const MONOGRAPH_CONF_DYNAMO_TEMPLATE: &str = "my_template_dynamo.cnf";

pub const START_MONOGRAPH_SCRIPT: &str = "start_monographdb.bash";
pub const START_MONOGRAPH_TEMPLATE: &str = "start_monographdb.template";

pub const START_LOG_TEMPLATE: &str = "start_tx_log.template";

pub const MONOGRAPH_INSTALL_TEMPLATE: &str = "monograph_install_db.template";
pub const MONOGRAPH_INSTALL_SCRIPT: &str = "monograph_install_db.bash";
pub const CASSANDRA_CONF_TEMPLATE: &str = "cassandra_template.yaml";
pub const CASSANDRA_ENV_TEMPLATE: &str = "cassandra-env-template";
pub const CASSANDRA_JVM_SERVER_CONF: &str = "jvm11-server.options";
pub const PROMETHEUS_CONFIG_TEMPLATE: &str = "mono_prometheus.yaml";

pub const PROMETHEUS_CONFIG_FILE: &str = "prometheus.yml";
pub const CASS_MCAC_CONF_FILE: &str = "tg_mcac.json";
pub const GRAFANA_PROMETHEUS_DS_FILE: &str = "prometheus-datasource.yml";

pub const MCAC_PROMETHEUS_CONFIG_TEMPLATE: &str = "mcac_prometheus.yaml";

pub const GRAFANA_CONFIG_TEMPLATE: &str = "grafana_config.ini";
pub const GRAFANA_CONFIG_FILE: &str = "defaults.ini";

pub const CREATE_MONITOR_USER_SQL_FILE: &str = "create_monitor_user.sql";
pub const MYSQL_EXPORTER_CLIENT_CONFIG: &str = "mysql_exporter.cnf";

#[macro_export]
macro_rules! gen_db_script {
    ($script_name:expr, $build_script_func:expr) => {{
        let script_rs = $build_script_func;
        if let Ok(script) = script_rs {
            let script_location = $crate::cli::download_dir().join($script_name);
            std::fs::write(script_location.clone(), script).unwrap();
            Ok(script_location)
        } else {
            Err(script_rs.err().unwrap())
        }
    }};
}

#[macro_export]
macro_rules! gen_db_misc_files {
    ($self:ident,$build_func:ident, $script_template:expr) => {{
        let script = $self.$build_func()?;
        let script_location = download_dir().join($script_template);
        std::fs::write(script_location.clone(), script).unwrap();
        Ok(script_location)
    }};
}

#[derive(PartialEq, Eq, Clone, Error, Debug)]
pub enum ConfigErr {
    #[error("MonographDB storage provider config error [{0}].For now only support Cassandra or DynamoDB, \
    You can choose either one.")]
    StorageConfigErr(String),
    #[error("The current configuration of the storage provider is not Cassandra. Storage Provider is {0}")]
    GenCassandraConfigErr(String),
    #[error("The download url format is incorrect. Storage Provider is {0}")]
    DownloadUrlFormatErr(String),
}

pub const CONFIG_PATH_DIR: &str = "CLUSTER_MGR_CLI_CONFIG";
pub const CONFIG_MARIADB_SECTION: &str = "mariadb";

#[derive(Hash, Debug, Clone, PartialEq, Eq, AsRefStr)]
pub enum StorageProvider {
    #[strum(serialize = "cassandra")]
    Cassandra,
    #[strum(serialize = "dynamodb")]
    DynamoDB,
}

#[derive(Debug, Clone)]
pub enum DownloadUrl {
    Local(String),
    Remote(String),
}

impl DownloadUrl {
    pub fn is_local(&self) -> bool {
        match self {
            DownloadUrl::Local(_url) => true,
            DownloadUrl::Remote(_url) => false,
        }
    }

    pub fn file_name(&self) -> String {
        let url_string = match self {
            DownloadUrl::Local(local_url) => local_url.to_string(),
            DownloadUrl::Remote(remote_url) => remote_url.to_string(),
        };
        let url = Url::parse(url_string.as_str()).unwrap();
        let path_segments = url.path_segments().unwrap();
        path_segments.last().unwrap().to_string()
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
                "file" => Ok(DownloadUrl::Local(url_str.to_string())),
                "http" | "https" => Ok(DownloadUrl::Remote(url_str.to_string())),
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
    #[strum(serialize = "monograph")]
    MonographTx,
    #[strum(serialize = "storage")]
    Storage,
    #[strum(serialize = "prometheus")]
    Prometheus,
    #[strum(serialize = "grafana")]
    Grafana,
    #[strum(serialize = "monograph_log")]
    MonographLog,
}

pub fn config_path_string(path: Option<String>) -> anyhow::Result<String> {
    if let Some(path_string) = path {
        Ok(path_string)
    } else {
        Ok(std::env::var(CONFIG_PATH_DIR)?)
    }
}

pub fn load_remote_env(path: Option<String>) -> anyhow::Result<HashMap<String, String>> {
    let path_string = config_path_string(path)?;
    let file = File::open(PathBuf::from(path_string).join("remote_env"))?;
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

pub fn config_template(file_name: &str) -> anyhow::Result<PathBuf> {
    let config_path_var_rs = std::env::var(CONFIG_PATH_DIR);
    assert!(config_path_var_rs.is_ok());
    let config_path = config_path_var_rs.unwrap();
    let path_buf = PathBuf::from(config_path.as_str()).join(file_name);
    if path_buf.exists() {
        Ok(path_buf)
    } else {
        Err(anyhow!(
            "MonographDB config not found in the {:?}",
            path_buf
        ))
    }
}

pub fn load_yaml_config_template(template_name: &str) -> anyhow::Result<HashMap<String, Value>> {
    let cass_template_path_buf = config_template(template_name)?;
    let cass_opened_file = File::open(cass_template_path_buf.as_path())?;
    // cassandra.yaml config object
    let cass_conf_map = serde_yaml::from_reader::<File, HashMap<String, Value>>(cass_opened_file)?;
    Ok(cass_conf_map)
}
