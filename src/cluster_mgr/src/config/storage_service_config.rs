use crate::cli::upload_dir;
use crate::config::config_base::CASSANDRA_COLLECTOR_AGENT_FILE_KEY;
use crate::config::{
    config_template, load_yaml_config_template, StorageProvider, CASSANDRA_CONF_TEMPLATE,
    CASSANDRA_ENV_TEMPLATE,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

#[serde_with::skip_serializing_none]
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
    pub mirror: Option<String>,
    pub version: String,
    pub cluster_name: Option<String>,
}

impl CassDeploy {
    pub fn image_url(&self) -> String {
        let mirror = self.mirror();
        let version = &self.version;
        format!("{mirror}/cassandra/{version}/apache-cassandra-{version}-bin.tar.gz")
    }

    pub fn image_file(&self) -> String {
        format!("apache-cassandra-{}-bin.tar.gz", self.version)
    }

    pub fn mirror(&self) -> &str {
        self.mirror.as_deref().unwrap_or("https://dlcdn.apache.org")
    }
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
pub struct RocksLocal {
    pub path: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct RocksS3 {
    #[serde(alias = "aws_id")]
    pub aws_access_key_id: String,
    #[serde(alias = "aws_secret")]
    pub aws_secret_key: String,
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
pub struct RocksMinio {
    #[serde(alias = "aws_id")]
    pub aws_access_key_id: String,
    #[serde(alias = "aws_secret")]
    pub aws_secret_key: String,
    pub bucket_name: String,
    pub bucket_prefix: String,
    pub endpoint: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum RocksDB {
    LOCAL(RocksLocal),
    S3(RocksS3),
    GCS(RocksGCP),
    MINIO(RocksMinio),
    #[serde(rename = "ELOQDSS_ROCKSDB")]
    EloqDssRocksdb(EloqDss),
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
                RocksDB::LOCAL(_) => name,
                RocksDB::S3(_) => "rocks_s3".to_owned(),
                RocksDB::GCS(_) => "rocks_gcs".to_owned(),
                // Treat MINIO as S3 for naming/downloading purposes
                RocksDB::MINIO(_) => "rocks_s3".to_owned(),
                // DSS RocksDB tarball path key
                RocksDB::EloqDssRocksdb(_) => "eloqdss_rocksdb".to_owned(),
            }
        }
        name
    }

    pub fn gen_cassandra_env(&self, cluster_name: &str, install_dir: String) -> Result<PathBuf> {
        let mcac_root = format!("MCAC_ROOT={install_dir}/{CASSANDRA_COLLECTOR_AGENT_FILE_KEY}\n",);
        let append_jvm_opts =
            r#"JVM_OPTS="$JVM_OPTS -javaagent:${MCAC_ROOT}/lib/datastax-mcac-agent.jar""#;
        let cass_env_file_path = config_template(CASSANDRA_ENV_TEMPLATE)?;

        let env_sh = upload_dir().join(cluster_name).join("cassandra-env.sh");

        // // Ensure directory exists
        // if let Some(parent) = env_sh.parent() {
        //     fs::create_dir_all(parent)?;
        // }

        fs::copy(cass_env_file_path, env_sh.clone())?;
        let mut cass_env_file = File::options().append(true).open(&env_sh)?;
        cass_env_file.write_all(mcac_root.as_bytes())?;
        cass_env_file.write_all(append_jvm_opts.as_bytes())?;
        cass_env_file.flush()?;
        Ok(env_sh)
    }

    pub fn inner_cass(&self) -> Option<&CassDeploy> {
        self.cassandra
            .as_ref()
            .map(|cass| cass.internal())
            .unwrap_or(None)
    }

    /// Returns true if RocksDB storage is cloud-based (S3, GCS, MINIO)
    /// Note: EloqDssRocksdb is NOT cloud-based
    pub fn is_rocksdb_cloud(&self) -> bool {
        if let Some(rocksdb) = &self.rocksdb {
            match rocksdb {
                RocksDB::LOCAL(_) | RocksDB::EloqDssRocksdb(_) => false,
                RocksDB::S3(_) | RocksDB::GCS(_) | RocksDB::MINIO(_) => true,
            }
        } else {
            false
        }
    }

    /// Returns true if RocksDB storage is S3 (including MINIO)
    pub fn is_rocksdb_s3(&self) -> bool {
        if let Some(rocksdb) = &self.rocksdb {
            matches!(rocksdb, RocksDB::S3(_) | RocksDB::MINIO(_))
        } else {
            false
        }
    }

    /// Get S3 bucket name and credentials for S3 storage
    /// Returns (bucket_name, aws_id, aws_secret, region, endpoint)
    pub fn get_s3_config(&self) -> Option<(String, String, String, String, Option<String>)> {
        if let Some(rocksdb) = &self.rocksdb {
            match rocksdb {
                RocksDB::S3(s3) => {
                    let bucket = format!("eloqkv-{}-{}", s3.bucket_prefix, s3.bucket_name);
                    Some((
                        bucket,
                        s3.aws_access_key_id.clone(),
                        s3.aws_secret_key.clone(),
                        s3.region.clone(),
                        None,
                    ))
                }
                RocksDB::MINIO(minio) => {
                    let bucket = format!("eloqkv-{}-{}", minio.bucket_prefix, minio.bucket_name);
                    Some((
                        bucket,
                        minio.aws_access_key_id.clone(),
                        minio.aws_secret_key.clone(),
                        "us-east-1".to_string(),
                        Some(minio.endpoint.clone()),
                    ))
                }
                _ => None,
            }
        } else {
            None
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct EloqDss {
    /// List of peer host:port entries where `dss_server` should be started.
    /// Example: ["127.0.0.1:9100", "10.0.0.2:9100"]
    pub peer_host_ports: Vec<String>,
    /// Optional S3-like configuration for DSS RocksDBCloud
    #[serde(alias = "aws_id")]
    pub aws_access_key_id: Option<String>,
    #[serde(alias = "aws_secret")]
    pub aws_secret_key: Option<String>,
    pub region: Option<String>,
    pub bucket_name: Option<String>,
    pub bucket_prefix: Option<String>,
    pub target_file_size_base: Option<String>,
    pub sst_file_cache_size: Option<String>,
}
