use crate::config::ConfigErr::DownloadUrlFormatErr;
use anyhow::anyhow;
use itertools::Itertools;
use regex::Regex;
use serde_yaml::Value;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
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
pub mod storage_service_config;

pub const ELOQSQL_INI: &str = "EloqSql.ini";
pub const ELOQSQL_DYNAMO_INI: &str = "EloqSqlDynamo.ini";
pub const ELOQSQL_CLIENT_PORT: u16 = 3306;
pub const ELOQKV_TEMPLATE_INI: &str = "EloqKv";
pub const ELOQKV_INI: &str = "EloqKv-tx";
pub const ELOQKV_STANDBY_INI: &str = "EloqKv-standby";
pub const ELOQKV_VOTER_INI: &str = "EloqKv-voter";
pub const CODIS_PROXY_CNF: &str = "codis_proxy.toml";
pub const CODIS_DASHBOARD_CNF: &str = "codis_dashboard.toml";

pub const START_LOG_TEMPLATE: &str = "start_tx_log.bash";
pub const MONOGRAPH_INSTALL_SCRIPT: &str = "monograph_install_db.bash";
pub const CASSANDRA_CONF_TEMPLATE: &str = "cassandra_template.yaml";
pub const CASSANDRA_ENV_TEMPLATE: &str = "cassandra-env-template";
pub const CASSANDRA_JVM_OPTION: &str = "jvm11-server.options";
pub const CASSANDRA_JVM_TEMPLATE: &str = "jvm11-server.template";
pub const JVM_SETTING_HOLDER: &str = "_GC_SETTINGS_PLACEHOLDER_";
pub const PROMETHEUS_CONFIG_TEMPLATE: &str = "mono_prometheus.yaml";

pub const GRAFANA_DASHBOARDS_CONFIG_TEMPLATE: &str = "grafana_dashboards.yaml";

pub const PROMETHEUS_CONFIG_FILE: &str = "prometheus.yml";
pub const CASS_MCAC_CONF_FILE: &str = "tg_mcac.json";
pub const GRAFANA_PROMETHEUS_DS_FILE: &str = "prometheus-datasource.yml";

pub const MCAC_PROMETHEUS_CONFIG_TEMPLATE: &str = "mcac_prometheus.yaml";

pub const GRAFANA_CONFIG_TEMPLATE: &str = "grafana_config.ini";
pub const GRAFANA_CONFIG_FILE: &str = "defaults.ini";

pub const CREATE_MONITOR_USER_SQL_FILE: &str = "create_monitor_user.sql";
pub const MYSQL_EXPORTER_CLIENT_CONFIG: &str = "mysql_exporter.cnf";

pub const CLOUDFRONT: &str = "download.eloqdata.com";
pub const CDN: &str = "https://download.eloqdata.com";

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
        let script_location = upload_dir().join($script_template);
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
pub const SECTION_MARIADB: &str = "mariadb";
pub const SECTION_LOCAL: &str = "local";
pub const SECTION_CLUSTER: &str = "cluster";
pub const SECTION_STORE: &str = "store";
pub const SECTION_METRIC: &str = "metrics";

#[derive(Hash, Debug, Clone, PartialEq, Eq, AsRefStr, Display, clap::ValueEnum)]
pub enum TopoFormat {
    Yaml,
    Json,
}

#[derive(Hash, Debug, Clone, PartialEq, Eq, AsRefStr, Display, clap::ValueEnum)]
pub enum StorageProvider {
    #[strum(serialize = "cassandra")]
    Cassandra,
    #[strum(serialize = "dynamodb")]
    Dynamodb,
    #[strum(serialize = "rocksdb")]
    Rocksdb,
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
        url.path_segments().unwrap().last().unwrap().to_string()
    }

    pub fn cache_dir(&self) -> anyhow::Result<String> {
        let mut dir = crate::cli::download_dir();
        match self {
            DownloadUrl::Local(_) => {}
            DownloadUrl::Remote(url) => {
                if url.domain() == Some(CLOUDFRONT) {
                    let mut seg = url.path_segments().unwrap();
                    let _filename = seg.next_back();
                    for d in seg {
                        dir.push(d);
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
    #[strum(serialize = "codis")]
    Codis,
    #[strum(serialize = "monograph_standby")]
    MonographStandby,
    #[strum(serialize = "monograph_voter")]
    MonographVoter,
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

pub fn config_template(file_name: &str) -> anyhow::Result<PathBuf> {
    let config_path = std::env::var(CONFIG_PATH_DIR)?;
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
    let yaml_map = serde_yaml::from_reader::<File, HashMap<String, Value>>(cass_opened_file)?;
    Ok(yaml_map)
}

pub fn cassandra_used_ports() -> Vec<(String, u16)> {
    let cass_cnf =
        load_yaml_config_template(CASSANDRA_CONF_TEMPLATE).expect("cassandra config invalid");
    let mut used = vec![];
    for name in ["native_transport_port", "storage_port", "ssl_storage_port"] {
        let port = cass_cnf
            .get(name)
            .expect("port is not configured")
            .as_u64()
            .expect("port is invalid") as u16;
        used.push((name.to_owned(), port));
    }

    let re = Regex::new(r#"^(?<name>\w+)_PORT="(?<port>\d{1,5})""#).expect("invalid regex pattern");
    let cass_env_p = config_template(CASSANDRA_ENV_TEMPLATE).expect("cassandra env config missing");
    let cass_env = fs::File::open(cass_env_p).unwrap();
    for line in BufReader::new(cass_env).lines().map(|l| l.unwrap()) {
        if let Some(caps) = re.captures(&line) {
            if let Ok(p) = caps["port"].parse::<u16>() {
                used.push((caps["name"].to_owned(), p));
            } else {
                error!("cassandra port of {} is {}", &caps["name"], &caps["port"]);
            }
        }
    }
    used
}
