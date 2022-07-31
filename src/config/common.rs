use serde_derive::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Common {
    workspace: String,
    compile: Compile,
    monograph: Monograph,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Compile {
    download: Download,
    git: Git,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Monograph {
    storage: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Git {
    brpc: GitArgs,
    braft: GitArgs,
    catch2: GitArgs,
    aws: GitArgs,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GitArgs {
    git: String,
    branch: Option<String>,
    build: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Download {
    protobuf: DownloadArgs,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DownloadArgs {
    url: String,
    build: Option<String>,
}
