use once_cell::sync::Lazy;
use serde_derive::Deserialize;

pub static MONOGRAPH_GIT_REPOS: Lazy<Vec<String>> = Lazy::new(|| {
    vec![
        "rocksdb".to_string(),
        "log_service".to_string(),
        "tx_service".to_string(),
        "monograph".to_string(),
        "cass".to_string(),
        "mariadb".to_string(),
    ]
});

#[derive(Clone, Debug, Deserialize)]
pub struct Common {
    pub workspace: String,
    pub initialize_script: String,
    pub compile: Compile,
    pub monograph: Monograph,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Compile {
    pub download: Download,
    pub git: Git,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Monograph {
    pub storage: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Git {
    pub brpc: GitArgs,
    pub braft: GitArgs,
    pub catch2: GitArgs,
    pub aws: GitArgs,
    pub log_service: GitArgs,
    pub tx_service: GitArgs,
    pub monograph: GitArgs,
    pub cass: GitArgs,
    pub mariadb: GitArgs,
    pub rocksdb: GitArgs,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GitArgs {
    pub git: String,
    pub branch: Option<String>,
    pub build: Option<String>,
    pub options: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Download {
    pub protobuf: DownloadArgs,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DownloadArgs {
    pub url: String,
    pub build: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Cassandra {
    pub download: CassandraDownload,
    pub command: CassandraCommand,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CassandraDownload {
    pub url: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CassandraCommand {
    pub start_script: String,
}
