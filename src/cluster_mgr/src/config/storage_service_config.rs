use crate::cli::upload_dir;
use crate::config::config_base::CASSANDRA_COLLECTOR_AGENT_FILE_KEY;
use crate::config::monitor::Monitor;
use crate::config::{config_template, StorageProvider, CASSANDRA_ENV_TEMPLATE};
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
pub struct Cassandra {
    pub host: Vec<String>,
    pub download_url: String,
    pub storage_cluster: Option<String>,
}

impl Cassandra {
    pub const MAX_SEED: usize = 3;
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
            Some(StorageProvider::DynamoDB)
        } else if self.rocksdb.is_some() {
            Some(StorageProvider::RocksDB)
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
