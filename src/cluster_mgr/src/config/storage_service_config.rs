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
    #[serde(rename = "eloqdss")]
    pub eloqdss: Option<DataStoreService>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rocksdb_max_background_jobs: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rocksdb_max_background_flush: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rocksdb_max_background_compaction: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rocksdb_level0_stop_writes_trigger: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rocksdb_level0_slowdown_writes_trigger: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rocksdb_level0_file_num_compaction_trigger: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rocksdb_max_write_buffer_number: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rocksdb_write_buffer_size: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rocksdb_enable_stats: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rocksdb_stats_dump_period_sec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rocksdb_storage_path: Option<String>,
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
        } else if self.eloqdss.is_some() {
            Some(StorageProvider::EloqDSS)
        } else {
            None
        }
    }

    pub fn pretty_name(&self) -> String {
        let provider = self.provider().unwrap();
        if provider == StorageProvider::EloqDSS {
            // Return the name depending on the backend type
            if let Some(dss) = &self.eloqdss {
                match dss.backend_config() {
                    DataStoreServiceBackend::EloqStore(_) => "eloqdss_eloqstore".to_owned(),
                    // future backends can be added here
                }
            } else {
                "eloqdss".to_owned()
            }
        } else {
            let mut name = provider.to_string();
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
                    let bucket = format!("{}{}", s3.bucket_prefix, s3.bucket_name);
                    Some((
                        bucket,
                        s3.aws_access_key_id.clone(),
                        s3.aws_secret_key.clone(),
                        s3.region.clone(),
                        None,
                    ))
                }
                RocksDB::MINIO(minio) => {
                    let bucket = format!("{}{}", minio.bucket_prefix, minio.bucket_name);
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

// DataStoreService related types

/// Mode for DataStoreService: whether DSS server is managed by eloqctl or external
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum DataStoreServiceMode {
    #[serde(rename = "internal")]
    Internal, // eloqctl manages dss_server process
    #[serde(rename = "external")]
    External, // external dss_server exists, not managed
}

/// Backend for DataStoreService: currently supports EloqStore, future can extend to BigTable, etc.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum DataStoreServiceBackend {
    #[serde(rename = "eloqstore")]
    EloqStore(EloqStoreConfig),
    // Future backends can be added here, e.g.:
    // #[serde(rename = "bigtable")]
    // BigTable(BigTableBackendConfig),
}

/// Cloud storage configuration for EloqStore
#[serde_with::skip_serializing_none]
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct EloqStoreCloudConfig {
    /// Cloud storage type (e.g., "s3", "azure", "gcs")
    #[serde(default = "default_cloud_type")]
    pub cloud_type: String,
    /// Cloud provider (e.g., "Minio", "AWS", "Other")
    #[serde(default = "default_cloud_provider")]
    pub cloud_provider: String,
    /// Access key ID for object storage (S3/MinIO)
    pub access_key_id: String,
    /// Secret access key for object storage (S3/MinIO)
    pub secret_access_key: String,
    /// Endpoint URL for object storage (e.g., http://127.0.0.1:9900 for MinIO)
    pub endpoint: String,
}

fn default_cloud_type() -> String {
    "s3".to_string()
}

fn default_cloud_provider() -> String {
    "Minio".to_string()
}

/// Configuration for EloqStore backend
#[serde_with::skip_serializing_none]
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct EloqStoreConfig {
    pub eloq_store_worker_num: Option<u32>,
    pub eloq_store_data_path_list: Option<String>,
    /// Cloud store path for cloud mode (empty or None means local mode)
    /// Format: "remote:path" where remote is rclone config name
    pub eloq_store_cloud_store_path: Option<String>,
    /// Cloud worker count for cloud mode
    pub eloq_store_cloud_worker_count: Option<u32>,
    /// Data append mode for EloqStore (default: false)
    #[serde(default = "default_eloq_store_data_append_mode")]
    pub eloq_store_data_append_mode: Option<bool>,
    /// Rclone daemon ports (comma-separated), one rclone process per port
    pub eloq_store_cloud_store_daemon_ports: Option<String>,
    /// Cloud storage configuration (required when cloud_store_path is set)
    /// Using #[serde(flatten)] to flatten the nested structure in YAML
    #[serde(flatten)]
    pub eloq_store_cloud_config: Option<EloqStoreCloudConfig>,
}

fn default_eloq_store_data_append_mode() -> Option<bool> {
    Some(false)
}

/// DataStoreService configuration
#[serde_with::skip_serializing_none]
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct DataStoreService {
    pub backend: DataStoreServiceBackend,
    /// Peer host:port entries for Remote mode (required for Remote, ignored for Local)
    pub peer_host_ports: Option<Vec<String>>,
    /// Whether DSS server is managed by eloqctl (Internal) or external (External)
    /// Only relevant for Remote mode. Default is Internal.
    #[serde(default = "default_dss_mode")]
    pub mode: DataStoreServiceMode,
}

fn default_dss_mode() -> DataStoreServiceMode {
    DataStoreServiceMode::Internal // Default: eloqctl manages dss_server
}

impl DataStoreService {
    /// Returns true if this DataStoreService is in Local mode
    pub fn is_local_mode(&self) -> bool {
        !self.is_remote_mode()
    }

    /// Returns true if this DataStoreService is in Remote mode
    pub fn is_remote_mode(&self) -> bool {
        self.peer_host_ports
            .as_ref()
            .map(|ports| !ports.is_empty())
            .unwrap_or(false)
    }

    /// Returns true if this DataStoreService requires separate DSS process
    pub fn requires_dss(&self) -> bool {
        self.is_remote_mode()
    }

    /// Get backend configuration (generic method for all backends)
    /// Callers should match on the returned enum to handle different backend types
    pub fn backend_config(&self) -> &DataStoreServiceBackend {
        &self.backend
    }

    /// Returns true if DSS server is external (not managed by eloqctl)
    /// Only relevant for Remote mode
    pub fn is_external(&self) -> bool {
        if self.is_remote_mode() {
            matches!(self.mode, DataStoreServiceMode::External)
        } else {
            false // Local mode doesn't need DSS process
        }
    }
}

impl EloqStoreConfig {
    /// Returns true if cloud mode is enabled (cloud_store_path is not empty)
    pub fn is_cloud_mode(&self) -> bool {
        self.eloq_store_cloud_store_path
            .as_ref()
            .map(|s| !s.is_empty())
            .unwrap_or(false)
    }

    /// Parse cloud_store_path into (remote_name, path)
    /// Format: "remote:path"
    pub fn parse_cloud_store_path(&self) -> Option<(String, String)> {
        self.eloq_store_cloud_store_path.as_ref().and_then(|s| {
            s.split_once(':')
                .map(|(remote, path)| (remote.to_string(), path.to_string()))
        })
    }

    /// Get rclone daemon ports as Vec<String>
    pub fn get_daemon_ports(&self) -> Vec<String> {
        self.eloq_store_cloud_store_daemon_ports
            .as_ref()
            .map(|s| s.split(',').map(|p| p.trim().to_string()).collect())
            .unwrap_or_default()
    }

    /// Get cloud config (returns None if not in cloud mode)
    pub fn get_cloud_config(&self) -> Option<&EloqStoreCloudConfig> {
        self.eloq_store_cloud_config.as_ref()
    }

    /// Compute default eloq_store_data_path for a given eloq_data_path
    ///
    /// # Arguments
    /// * `eloq_data_path` - EloqKV data path, format: `{tx_srv_home}/data/port-{port}`
    ///
    /// # Returns
    /// Default data path: `{eloq_data_path}/eloq_dss/eloqstore_data`
    ///
    /// # Example
    /// ```
    /// let eloq_data_path = "/tmp/eloqkv_data";
    /// let default_path = EloqStoreConfig::compute_default_eloq_store_data_path(eloq_data_path);
    /// // Returns: "/tmp/eloqkv_data/eloq_dss/eloqstore_data"
    /// ```
    pub fn compute_default_eloq_store_data_path(eloq_data_path: &str) -> String {
        format!("{}/eloq_dss/eloqstore_data", eloq_data_path)
    }
}
