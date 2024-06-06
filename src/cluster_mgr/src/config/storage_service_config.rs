use crate::cli::upload_dir;
use crate::config::config_base::CASSANDRA_COLLECTOR_AGENT_FILE_KEY;
use crate::config::monitor::Monitor;
use crate::config::{
    config_template, load_yaml_config_template, StorageProvider, CASSANDRA_CONF_TEMPLATE,
    CASSANDRA_ENV_TEMPLATE,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;
use std::io::Write;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct StorageService {
    pub cassandra: Option<Cassandra>,
    pub dynamodb: Option<Dynamodb>,
    pub rocksdb: Option<RocksDB>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct CassConnect {
    pub port: Option<u16>,
    pub user: Option<String>,
    pub password: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct CassDeploy {
    pub download_url: String,
    pub cluster_name: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum CassKind {
    Internal(CassDeploy),
    External(CassConnect),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Cassandra {
    pub host: Vec<String>,
    pub kind: CassKind,
}

impl Cassandra {
    pub const MAX_SEED: usize = 3;
    pub fn internal(&self) -> Option<&CassDeploy> {
        if let CassKind::Internal(deploy) = &self.kind {
            Some(deploy)
        } else {
            None
        }
    }
    pub fn external(&self) -> Option<&CassConnect> {
        if let CassKind::External(conn) = &self.kind {
            Some(conn)
        } else {
            None
        }
    }

    pub fn client_port(&self) -> Result<u16> {
        match &self.kind {
            CassKind::Internal(_) => {
                let port = load_yaml_config_template(CASSANDRA_CONF_TEMPLATE)?
                    .get("native_transport_port")
                    .expect("native_transport_port is not configured")
                    .as_u64()
                    .expect("native_transport_port is invalid");
                Ok(port as u16)
            }
            CassKind::External(conn) => Ok(conn.port.unwrap_or(9042)),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Dynamodb {
    pub access_key_id: String,
    pub secret_key: String,
    pub region: String,
    pub endpoint: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct RocksS3 {
    pub aws_id: String,
    pub aws_secret: String,
    pub region: String,
    pub bucket_name: String,
    pub bucket_prefix: String,
    pub target_file_size_base: String,
    pub sst_file_cache_size: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct RocksGCP {
    pub region: String,
    pub bucket_name: String,
    pub bucket_prefix: String,
    pub target_file_size_base: String,
    pub sst_file_cache_size: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum RocksDB {
    Local,
    S3(RocksS3),
    GCS(RocksGCP),
}

impl StorageService {
    pub fn provider(&self) -> Option<StorageProvider> {
        if self.cassandra.is_some() {
            Some(StorageProvider::Cassandra)
        } else if self.dynamodb.is_some() {
            Some(StorageProvider::Dynamodb)
        } else if self.rocksdb.is_some() {
            Some(StorageProvider::Rocksdb)
        } else {
            None
        }
    }

    pub fn pretty_name(&self) -> String {
        let mut name = self.provider().unwrap().to_string();
        if let Some(rocks) = &self.rocksdb {
            name = match rocks {
                RocksDB::Local => name,
                RocksDB::S3(_) => format!("{name}_s3"),
                RocksDB::GCS(_) => format!("{name}_gcs"),
            }
        }
        name
    }

    pub fn gen_cassandra_env(
        &self,
        install_dir: String,
        monitor_opt: Option<&Monitor>,
    ) -> anyhow::Result<bool> {
        if let Some(monitor) = monitor_opt {
            if monitor.cassandra_collector.is_some() {
                // let install_dir = self.install_dir();
                let mcac_root =
                    format!("MCAC_ROOT={install_dir}/{CASSANDRA_COLLECTOR_AGENT_FILE_KEY}\n",);
                let append_jvm_opts =
                    r#"JVM_OPTS="$JVM_OPTS -javaagent:${MCAC_ROOT}/lib/datastax-mcac-agent.jar""#;

                let cass_env_file_path = config_template(CASSANDRA_ENV_TEMPLATE)?;
                let final_cass_env = upload_dir().join("cassandra-env.sh");
                fs::copy(cass_env_file_path, final_cass_env.clone())?;
                let mut cass_env_file = File::options()
                    .write(true)
                    .append(true)
                    .open(final_cass_env)?;
                cass_env_file.write_all(mcac_root.as_bytes())?;
                cass_env_file.write_all(append_jvm_opts.as_bytes())?;
                cass_env_file.flush()?;
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }
}
