use crate::config::ConfigErr::DownloadUrlFormatErr;
use anyhow::anyhow;
use itertools::Itertools;
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
pub enum DeploymentService {
    #[strum(serialize = "monograph")]
    Monograph,
    #[strum(serialize = "storage")]
    Storage,
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
