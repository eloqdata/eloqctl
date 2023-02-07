use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Deployment {
    pub install_image: String,
    pub cluster_name: String,
    pub install_dir: String,
    pub port: Port,
    pub mono_service: MonographService,
    pub storage_service: StorageService,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Port {
    pub mysql_port: u16,
    pub monograph_port: MonographPort,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct MonographPort {
    pub start: u16,
    pub end: u16,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct MonographService {
    pub host: Vec<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct StorageService {
    pub cassandra: Option<Cassandra>,
    pub dynamodb: Option<Dynamodb>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Cassandra {
    pub host: Vec<String>,
    pub download_url: String,
    pub storage_cluster: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Dynamodb {
    pub access_key_id: String,
    pub secret_key: String,
    pub region: String,
    pub endpoint: String,
}
