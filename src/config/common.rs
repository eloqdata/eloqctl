use serde_derive::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Common {
    pub workspace: String,
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
}

#[derive(Clone, Debug, Deserialize)]
pub struct GitArgs {
    pub git: String,
    pub branch: Option<String>,
    pub build: Option<String>,
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
