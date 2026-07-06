use crate::cli::task::eloq_tx_ctl_task::ServerType;
use crate::cli::{create_upload_cluster_dir, upload_dir};
use crate::config::config_base::{export_asan, ELOQ_FILE_KEY, ELOQ_LOG_FILE_KEY, LOG_SERVICE_HOME};
use crate::config::log_service::LogService;
use crate::config::monitor::Monitor;
use crate::config::storage_service_config::{DataStoreServiceBackend, RocksDB, StorageService};
use crate::config::{
    cluster_config_template, DeploymentPackage, DownloadUrl, StorageProvider, ELOQDSS_TEMPLATE_INI,
    ELOQKV_NODE_INI, ELOQKV_TEMPLATE_INI, SECTION_CLUSTER, SECTION_LOCAL, SECTION_METRIC,
    SECTION_STORE,
};
use anyhow::{anyhow, bail, Result};
use chrono::Local;
use configparser::ini::Ini;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::IpAddr;
use std::path::PathBuf;
use strum_macros::Display;

// pub(crate) static VERSION_PATT: LazyLock<Regex> =
//     LazyLock::new(|| Regex::new(r"(0|[1-9][0-9]?)\.(0|[1-9][0-9]?)\.(0|[1-9][0-9]?)").unwrap());

#[macro_export]
macro_rules! download_urls {
    ($download_link:expr, $({$url_key:expr, $url_value:expr} $(,)?)*) => {
        $(
          $download_link.insert($url_key.to_string(), DownloadUrl::from_url_str($url_value).unwrap());
        )*
    };
}

macro_rules! set_by_user {
    ($opt_val:expr, $T:ty) => {
        if let Some(v) = $opt_val {
            if v == "${OVERRIDE}" {
                None
            } else {
                Some(v.parse::<$T>()?)
            }
        } else {
            None
        }
    };
}

macro_rules! extract_monitor_host {
    ($deployment_ref:expr, $monitor_component:ident) => {{
        $deployment_ref
            .monitor
            .as_ref()
            .and_then(|monitor| monitor.$monitor_component.as_ref())
            .map(|component| vec![component.host.clone()])
            .unwrap_or_else(Vec::new)
    }};
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Port {
    pub cs_conn: Option<u16>,
    pub eloq_port: Option<EloqPort>,
}

impl Port {
    pub fn contains(&self, p: u16) -> bool {
        if let Some(eloq_port) = &self.eloq_port {
            if p >= eloq_port.start && p <= eloq_port.end {
                return true;
            }
        }
        if let Some(mysql_port) = self.cs_conn {
            if mysql_port == p {
                return true;
            }
        }
        false
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct EloqPort {
    pub start: u16,
    pub end: u16,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct EloqService {
    pub image: Option<String>,
    pub tx_host_ports: Vec<String>,
    pub standby_host_ports: Option<Vec<String>>,
    pub voter_host_ports: Option<Vec<String>>,
    pub requirepass: Option<String>,
    pub enable_cache_replacement: Option<String>,
    pub client_port: Option<u16>, // only used in mysql
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_standby_lag: Option<u32>,
}

impl EloqService {
    fn parse_standby_voter_hosts(&self, hosts_vec: Vec<String>) -> Vec<String> {
        hosts_vec
            .iter()
            .flat_map(|hosts_str| {
                hosts_str
                    .split(['|', ','])
                    .filter_map(|s| s.split(':').next())
                    .map(|s| s.to_string())
            })
            .collect()
    }

    fn parse_tx_hosts(&self, hosts_vec: Vec<String>) -> Vec<String> {
        hosts_vec
            .iter()
            .flat_map(|hosts_str| {
                hosts_str
                    .split([','])
                    .filter_map(|s| s.split(':').next())
                    .map(|s| s.to_string())
            })
            .collect()
    }

    pub fn merge_hosts(&self) -> Vec<String> {
        // Collect hosts from tx_service.tx_host_ports
        let mut hosts: Vec<String> = self.parse_tx_hosts(self.tx_host_ports.clone());

        // Parse standby_host_ports and add to the hosts list
        if let Some(standby_hosts_str) = &self.standby_host_ports {
            let standby_hosts_list = self.parse_standby_voter_hosts(standby_hosts_str.clone());
            hosts.extend(standby_hosts_list);
        }

        // Parse voter_host_ports and add to the hosts list
        if let Some(voter_hosts_str) = &self.voter_host_ports {
            let voter_hosts_list = self.parse_standby_voter_hosts(voter_hosts_str.clone());
            hosts.extend(voter_hosts_list);
        }

        // Remove duplicates by converting to a HashSet and back to a Vec
        let hosts_set: HashSet<String> = hosts.into_iter().collect();
        let mut hosts: Vec<String> = hosts_set.into_iter().collect();

        // Sort the hosts for consistent ordering
        hosts.sort();

        hosts
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, clap::ValueEnum, Display)]
pub enum Product {
    #[serde(alias = "eloqkv", alias = "eloq-kv", alias = "KV")]
    EloqKV,
}

impl Product {
    pub fn name(&self) -> &str {
        match self {
            Product::EloqKV => "eloqkv",
        }
    }

    pub fn home(&self) -> &str {
        match self {
            Product::EloqKV => "EloqKV",
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Hardware {
    pub cpu: u16,
    pub memory: u32, // MiB
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_dispatcher_num: Option<u16>,
    /// Optional per-node override for the node's base data directory
    /// (the `[local] eloq_data_path` INI key). Defaults to
    /// `{install_dir}/EloqKV/data/port-{port}`. Overriding it relocates the
    /// whole node data tree; the EloqStore default path follows this base
    /// unless `eloq_store_data_path_list` is also set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eloq_data_path: Option<String>,
    /// Optional per-node override for the EloqStore data directory list
    /// (the `[store] eloq_store_data_path_list` INI key). When multiple nodes
    /// run on the same host, an `ip:port`-keyed entry lets each node use a
    /// distinct path (e.g. spread across disks). Takes precedence over the
    /// shared `storage_service` value and the auto-derived default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eloq_store_data_path_list: Option<String>,
}

pub enum Version {
    Nightly,
    Debug,
    Tag([u32; 3]),
    Devel(String),
}

pub enum NodeType {
    Voter,
    Candidate,
}

#[serde_with::skip_serializing_none]
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Deployment {
    pub product: Product,
    pub version: Option<String>,
    pub cluster_name: String,
    pub install_dir: String,
    pub tx_service: EloqService,
    pub log_service: Option<LogService>,
    pub storage_service: Option<StorageService>,
    pub monitor: Option<Monitor>,
    pub hardware: Option<HashMap<String, Hardware>>,
    pub enable_wal: Option<bool>,
    pub enable_io_uring: Option<bool>,
    #[serde(rename = "checkpointer_interval", alias = "checkpoint_interval")]
    pub checkpoint_interval: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_tls: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maxclients: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_mode: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment_variables: Option<HashMap<String, String>>,
}

impl Deployment {
    pub fn tx_image(&self) -> &str {
        self.tx_service.image.as_deref().unwrap_or("latest")
    }

    pub fn log_image(&self) -> Option<&str> {
        if let Some(srv) = &self.log_service {
            srv.image.as_deref()
        } else {
            None
        }
    }

    /// Look up the hardware spec for a node.
    ///
    /// Keys in the `hardware` map may be written in two formats:
    /// - `ip:port` (preferred) — addresses a single node, required to give
    ///   distinct specs to multiple nodes co-located on the same host.
    /// - `ip` (legacy) — shared by every node running on that host.
    ///
    /// The `ip:port` key is tried first; when absent we fall back to the
    /// bare `ip` key so existing single-node-per-host topologies keep working.
    pub fn get_hardware(&self, host: &str, port: &str) -> Option<&Hardware> {
        let all_hw = self.hardware.as_ref()?;
        all_hw
            .get(&format!("{host}:{port}"))
            .or_else(|| all_hw.get(host))
    }

    /// Returns the cluster-wide (shared) `eloq_store_data_path_list` from the
    /// embedded EloqStore backend config, if one was explicitly set. Used to
    /// decide whether a per-node `eloq_data_path` override should also redirect
    /// the EloqStore data directory (an explicit shared value still wins).
    fn global_eloq_store_data_path_list(&self) -> Option<&str> {
        match self
            .storage_service
            .as_ref()?
            .eloqdss
            .as_ref()?
            .backend_config()
        {
            DataStoreServiceBackend::EloqStore(config) => {
                config.eloq_store_data_path_list.as_deref()
            }
        }
    }

    pub fn product(&self) -> Product {
        self.product.clone()
    }

    pub fn version_str(&self) -> &str {
        self.version.as_deref().unwrap_or("latest")
    }

    pub fn version(&self) -> Option<Version> {
        self.version.as_ref().map(|ver| parse_version(ver))
    }

    pub fn tls_enabled(&self) -> bool {
        self.enable_tls.unwrap_or(false)
    }

    pub fn tls_cert_install_dir(&self) -> String {
        format!("{}/ssl", self.install_dir())
    }

    fn sanitize_file_component(input: &str) -> String {
        input
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    }

    fn tls_file_names(&self, host: &str, port: &str) -> (String, String) {
        let host_part = Self::sanitize_file_component(host);
        (
            format!("eloqkv-tls-{host_part}-{port}.crt"),
            format!("eloqkv-tls-{host_part}-{port}.key"),
        )
    }

    fn ensure_tls_cert_for_node(&self, host: &str, port: &str) -> anyhow::Result<(String, String)> {
        use rcgen::{CertificateParams, DistinguishedName, DnType, IsCa, KeyPair, SanType};
        let (cert_name, key_name) = self.tls_file_names(host, port);
        let host_dir = create_upload_cluster_dir(&format!("{}/{}", self.cluster_name, host));
        let cert_path = host_dir.join(&cert_name);
        let key_path = host_dir.join(&key_name);

        if cert_path.exists() && key_path.exists() {
            return Ok((cert_name, key_name));
        }

        let mut params = CertificateParams::default();
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, host);
        params.distinguished_name = dn;
        params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);

        let mut san_list: Vec<SanType> = vec![SanType::DnsName(host.try_into()?)];
        if host == "localhost" {
            san_list.push(SanType::DnsName("localhost".try_into()?));
            san_list.push(SanType::IpAddress("127.0.0.1".parse()?));
        } else if let Ok(ip) = host.parse::<IpAddr>() {
            san_list.push(SanType::IpAddress(ip));
        }
        params.subject_alt_names = san_list;

        let key_pair = KeyPair::generate()?;
        let cert = params.self_signed(&key_pair)?;

        std::fs::write(&cert_path, cert.pem())?;
        std::fs::write(&key_path, key_pair.serialize_pem())?;

        Ok((cert_name, key_name))
    }

    pub fn ensure_tls_certs_for_all_kv_nodes(&self) -> anyhow::Result<()> {
        if !self.tls_enabled() || self.product() != Product::EloqKV {
            return Ok(());
        }

        let mut seen = HashSet::new();
        let mut add_host_port = |token: &str| -> anyhow::Result<()> {
            let token = token.trim();
            if token.is_empty() {
                return Ok(());
            }
            let mut parts = token.split(':');
            let host = parts.next().unwrap_or_default().trim();
            let port = parts.next().unwrap_or_default().trim();
            if host.is_empty() || port.is_empty() {
                return Ok(());
            }
            let pair = (host.to_string(), port.to_string());
            if seen.insert(pair.clone()) {
                self.ensure_tls_cert_for_node(&pair.0, &pair.1)?;
            }
            Ok(())
        };

        for hp_list in &self.tx_service.tx_host_ports {
            for token in hp_list.split(',') {
                add_host_port(token)?;
            }
        }
        if let Some(standby_list) = &self.tx_service.standby_host_ports {
            for hp_list in standby_list {
                for token in hp_list.split(['|', ',']) {
                    add_host_port(token)?;
                }
            }
        }
        if let Some(voter_list) = &self.tx_service.voter_host_ports {
            for hp_list in voter_list {
                for token in hp_list.split(['|', ',']) {
                    add_host_port(token)?;
                }
            }
        }
        Ok(())
    }

    pub fn install_dir(&self) -> String {
        let base = self.install_dir.trim_end_matches('/');
        if base.is_empty() {
            return self.cluster_name.clone();
        }

        // Backward compatible behavior:
        // - install_dir as base dir: /home/eloq/eloqkv-cluster-01 => /home/eloq/eloqkv-cluster-01/<cluster>
        // - install_dir already ends with cluster name: keep as-is to avoid duplicated /<cluster>/<cluster>
        if base
            .rsplit('/')
            .next()
            .map(|last| last == self.cluster_name)
            .unwrap_or(false)
        {
            base.to_string()
        } else {
            format!("{base}/{}", self.cluster_name)
        }
    }

    pub fn tx_srv_home(&self) -> String {
        format!("{}/{}", &self.install_dir(), self.product().home())
    }

    pub fn log_srv_home(&self) -> String {
        format!("{}/{LOG_SERVICE_HOME}", &self.install_dir())
    }

    pub fn uses_eloqstore_storage(&self) -> bool {
        self.storage_service
            .as_ref()
            .and_then(|storage| storage.eloqdss.as_ref())
            .map(|dss| matches!(dss.backend_config(), DataStoreServiceBackend::EloqStore(_)))
            .unwrap_or(false)
    }

    pub fn tx_srv_ini(&self, port: &str) -> String {
        format!("{}/{}-{}.ini", &self.tx_srv_home(), ELOQKV_NODE_INI, port)
    }

    pub fn dss_srv_ini(&self, port: &str) -> String {
        let home = self.tx_srv_home();
        format!("{home}/EloqDss-{}.ini", port)
    }

    pub fn asan_logs(&self) -> String {
        format!("{}/logs", &self.tx_srv_home())
    }

    pub fn node_srv_logs(&self, port: &str) -> String {
        format!("{}/logs/node-{}", &self.tx_srv_home(), port)
    }

    pub fn tx_srv_bin(&self) -> String {
        format!("{}/bin/eloqkv", &self.tx_srv_home())
    }

    pub fn client_bin(&self) -> String {
        let tx_home = self.tx_srv_home();
        format!("{tx_home}/bin/eloqkv-cli")
    }

    pub fn get_redis_keyspace(&self) -> anyhow::Result<String> {
        let my_local = upload_dir()
            .join(&self.cluster_name)
            .join("redis_local.ini");
        if !my_local.exists() {
            self.gen_eloqkv_node_config(None, None)?;
        }
        let mut my_ini_local = Ini::new();
        let _config_map_rs = my_ini_local
            .load(&my_local)
            .map_err(|e| anyhow!("Failed to load local ini: {e}"))?;
        if let Some(keyspace) = my_ini_local.get(SECTION_STORE, "cass_keyspace") {
            Ok(keyspace)
        } else {
            Ok("eloq_redis".to_string())
        }
    }

    pub fn get_keyspace(&self) -> Result<String> {
        self.get_redis_keyspace()
    }

    fn build_log_config(&self) -> HashMap<String, String> {
        let log_srv = self
            .log_service
            .as_ref()
            .expect("log_service is not configured");
        let replica_num = log_srv.log_replica();
        let all_members = log_srv.group_member_as_vec();

        // The txlog_service_list should contain all nodes in the order they appear in the grouping algorithm
        // This matches the order used in the -conf parameter for log service startup
        let node_group = all_members
            .iter()
            .map(|member| format!("{}:{}", member.member_host, member.port))
            .collect::<Vec<String>>()
            .join(",");

        let key_list = "txlog_service_list".to_string();
        let key_replica = "txlog_group_replica_num".to_string();
        HashMap::from([
            (key_replica, replica_num.to_string()),
            (key_list, node_group),
        ])
    }

    pub fn bootstrap_host(&self) -> anyhow::Result<String> {
        let all_hosts = self.tx_service.merge_hosts();
        if all_hosts.is_empty() {
            bail!("No hosts configured for bootstrap");
        }
        Ok(all_hosts.first().unwrap().to_string())
    }

    pub fn build_eloqkv_config(&self, set_ip_list: bool, port: String) -> anyhow::Result<Ini> {
        let mut ini = Ini::new();
        // config template does not have port suffix
        ini.load(cluster_config_template(
            &self.cluster_name,
            ELOQKV_TEMPLATE_INI,
        )?)
        .map_err(|e| anyhow!("Failed to load template ini: {e}"))?;

        // Each eloqkv process will store data in {tx_srv_home}/data/port-{}, need to differentiate between different ports in case multiple eloqkv processes are running on the same host
        ini.set(
            SECTION_LOCAL,
            "eloq_data_path",
            Some(format!("{}/data/port-{}", self.tx_srv_home(), port)),
        );

        if self.tx_service.requirepass.is_some() {
            ini.set(
                SECTION_LOCAL,
                "requirepass",
                self.tx_service.requirepass.clone(),
            );
        }

        if self.tx_service.enable_cache_replacement.is_some() {
            ini.set(
                SECTION_LOCAL,
                "enable_cache_replacement",
                self.tx_service.enable_cache_replacement.clone(),
            );
        } else {
            ini.set(
                SECTION_LOCAL,
                "enable_cache_replacement",
                Some("on".to_string()),
            );
        }

        // Only configure storage if storage_service is provided
        if let Some(storage) = self.storage_service.as_ref() {
            match storage.provider().unwrap() {
                StorageProvider::Rocksdb => match storage.rocksdb.clone().unwrap() {
                    RocksDB::LOCAL(local) => {
                        let rocks_path = match &local.path {
                            Some(path) => {
                                if port.is_empty() {
                                    format!("{}/{}/rocksdb", path, self.cluster_name)
                                } else {
                                    format!("{}/{}/rocksdb-{}", path, self.cluster_name, port)
                                }
                            }
                            None => {
                                if port.is_empty() {
                                    format!("{}/rocksdb", self.tx_srv_home())
                                } else {
                                    format!("{}/rocksdb-{}", self.tx_srv_home(), port)
                                }
                            }
                        };

                        ini.set(SECTION_STORE, "rocksdb_storage_path", Some(rocks_path));
                    }
                    RocksDB::S3(s3) => {
                        ini.set(
                            SECTION_STORE,
                            "aws_access_key_id",
                            Some(s3.aws_access_key_id.clone()),
                        );
                        ini.set(
                            SECTION_STORE,
                            "aws_secret_key",
                            Some(s3.aws_secret_key.clone()),
                        );
                        ini.set(SECTION_STORE, "rocksdb_cloud_region", Some(s3.region));
                        ini.set(
                            SECTION_STORE,
                            "rocksdb_cloud_bucket_name",
                            Some(s3.bucket_name),
                        );
                        ini.set(
                            SECTION_STORE,
                            "rocksdb_cloud_bucket_prefix",
                            Some(s3.bucket_prefix),
                        );
                        if let Some(endpoint) = &s3.endpoint {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_cloud_s3_endpoint_url",
                                Some(endpoint.clone()),
                            );
                        }
                        ini.set(
                            SECTION_STORE,
                            "rocksdb_target_file_size_base",
                            Some(s3.target_file_size_base),
                        );
                        ini.set(
                            SECTION_STORE,
                            "rocksdb_cloud_sst_file_cache_size",
                            Some(s3.sst_file_cache_size),
                        );
                        if let Some(val) = &s3.rocksdb_max_background_jobs {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_max_background_jobs",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &s3.rocksdb_max_background_flush {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_max_background_flush",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &s3.rocksdb_max_background_compaction {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_max_background_compaction",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &s3.rocksdb_level0_stop_writes_trigger {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_level0_stop_writes_trigger",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &s3.rocksdb_level0_slowdown_writes_trigger {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_level0_slowdown_writes_trigger",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &s3.rocksdb_level0_file_num_compaction_trigger {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_level0_file_num_compaction_trigger",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &s3.rocksdb_max_write_buffer_number {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_max_write_buffer_number",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &s3.rocksdb_write_buffer_size {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_write_buffer_size",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &s3.rocksdb_enable_stats {
                            ini.set(SECTION_STORE, "rocksdb_enable_stats", Some(val.clone()));
                        }
                        if let Some(val) = &s3.rocksdb_stats_dump_period_sec {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_stats_dump_period_sec",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &s3.rocksdb_periodic_compaction_seconds {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_periodic_compaction_seconds",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &s3.rocksdb_delete_obsolete_files_period_micros {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_delete_obsolete_files_period_micros",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &s3.rocksdb_storage_path {
                            ini.set(SECTION_STORE, "rocksdb_storage_path", Some(val.clone()));
                        }
                        if let Some(val) = &s3.object_path {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_cloud_object_path",
                                Some(val.clone()),
                            );
                        }
                    }
                    RocksDB::EloqDssRocksdb(_eloq_dss) => {
                        // DSS-specific RocksDB config is managed by DSS ini; no KV store fields here
                    }
                    RocksDB::MINIO(minio) => {
                        // MINIO uses S3-compatible API with endpoint and combined bucket name
                        ini.set(
                            SECTION_STORE,
                            "aws_access_key_id",
                            Some(minio.aws_access_key_id.clone()),
                        );
                        ini.set(
                            SECTION_STORE,
                            "aws_secret_key",
                            Some(minio.aws_secret_key.clone()),
                        );
                        ini.set(
                            SECTION_STORE,
                            "rocksdb_cloud_s3_endpoint_url",
                            Some(minio.endpoint),
                        );
                        ini.set(
                            SECTION_STORE,
                            "rocksdb_cloud_bucket_name",
                            Some(minio.bucket_name),
                        );
                        ini.set(
                            SECTION_STORE,
                            "rocksdb_cloud_bucket_prefix",
                            Some(minio.bucket_prefix),
                        );
                        if let Some(val) = &minio.rocksdb_enable_stats {
                            ini.set(SECTION_STORE, "rocksdb_enable_stats", Some(val.clone()));
                        }
                        if let Some(val) = &minio.rocksdb_stats_dump_period_sec {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_stats_dump_period_sec",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &minio.rocksdb_periodic_compaction_seconds {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_periodic_compaction_seconds",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &minio.rocksdb_delete_obsolete_files_period_micros {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_delete_obsolete_files_period_micros",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &minio.object_path {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_cloud_object_path",
                                Some(val.clone()),
                            );
                        }
                    }
                    RocksDB::GCS(gcs) => {
                        ini.set(SECTION_STORE, "rocksdb_cloud_region", Some(gcs.region));
                        ini.set(
                            SECTION_STORE,
                            "rocksdb_cloud_bucket_name",
                            Some(gcs.bucket_name),
                        );
                        ini.set(
                            SECTION_STORE,
                            "rocksdb_cloud_bucket_prefix",
                            Some(gcs.bucket_prefix),
                        );
                        ini.set(
                            SECTION_STORE,
                            "rocksdb_target_file_size_base",
                            Some(gcs.target_file_size_base),
                        );
                        ini.set(
                            SECTION_STORE,
                            "rocksdb_cloud_sst_file_cache_size",
                            Some(gcs.sst_file_cache_size),
                        );
                        if let Some(val) = &gcs.rocksdb_enable_stats {
                            ini.set(SECTION_STORE, "rocksdb_enable_stats", Some(val.clone()));
                        }
                        if let Some(val) = &gcs.rocksdb_stats_dump_period_sec {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_stats_dump_period_sec",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &gcs.rocksdb_periodic_compaction_seconds {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_periodic_compaction_seconds",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &gcs.rocksdb_delete_obsolete_files_period_micros {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_delete_obsolete_files_period_micros",
                                Some(val.clone()),
                            );
                        }
                        if let Some(val) = &gcs.object_path {
                            ini.set(
                                SECTION_STORE,
                                "rocksdb_cloud_object_path",
                                Some(val.clone()),
                            );
                        }
                    }
                },
                StorageProvider::EloqDSS => {
                    if let Some(dss) = storage.eloqdss.as_ref() {
                        // Only embed DataStoreService config in EloqKv.ini for Local mode
                        // Remote mode config will be written to EloqDss.ini via build_dss_config()
                        if dss.is_local_mode() {
                            match dss.backend_config() {
                                DataStoreServiceBackend::EloqStore(config) => {
                                    // Cloud access key and secret key (only for AWS/MinIO, not for GCS)
                                    if let Some(cloud_config) = &config.eloq_store_cloud_config {
                                        // Enforce credentials for AWS/MINIO providers
                                        cloud_config.validate_credentials()?;
                                        let provider =
                                            cloud_config.eloq_store_cloud_provider.as_str();
                                        if provider == "aws" || provider == "minio" {
                                            if let (Some(access_key), Some(secret_key)) = (
                                                cloud_config.eloq_store_cloud_access_key.as_ref(),
                                                cloud_config.eloq_store_cloud_secret_key.as_ref(),
                                            ) {
                                                ini.set(
                                                    SECTION_STORE,
                                                    "aws_secret_key",
                                                    Some(access_key.clone()),
                                                );
                                                ini.set(
                                                    SECTION_STORE,
                                                    "aws_access_key_id",
                                                    Some(secret_key.clone()),
                                                );
                                            }
                                        }
                                    }
                                    if let Some(worker_num) = config.eloq_store_worker_num {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_worker_num",
                                            Some(worker_num.to_string()),
                                        );
                                    }
                                    if let Some(data_path_list) = &config.eloq_store_data_path_list
                                    {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_data_path_list",
                                            Some(data_path_list.clone()),
                                        );
                                    } else {
                                        // Compute default value if not specified
                                        // Only write to ini file, don't modify memory
                                        use crate::config::storage_service_config::EloqStoreConfig;
                                        let eloq_data_path =
                                            format!("{}/data/port-{}", self.tx_srv_home(), port);
                                        let default_data_path =
                                            EloqStoreConfig::compute_default_eloq_store_data_path(
                                                &eloq_data_path,
                                            );
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_data_path_list",
                                            Some(default_data_path),
                                        );
                                    }
                                    // EloqStore Cloud mode configuration
                                    // When cloud_store_path is set, EloqStore operates in cloud mode
                                    // using direct S3/MinIO access
                                    if let Some(cloud_path) = &config.eloq_store_cloud_store_path {
                                        if !cloud_path.is_empty() {
                                            // Write bucket-name
                                            ini.set(
                                                SECTION_STORE,
                                                "eloq_store_cloud_store_path",
                                                Some(cloud_path.clone()),
                                            );
                                        }
                                    }
                                    // EloqStoreConfig additional fields
                                    if let Some(value) = config.eloq_store_open_files_limit {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_open_files_limit",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) =
                                        config.eloq_store_data_page_restart_interval
                                    {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_data_page_restart_interval",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) =
                                        config.eloq_store_index_page_restart_interval
                                    {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_index_page_restart_interval",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_init_page_count {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_init_page_count",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_skip_verify_checksum {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_skip_verify_checksum",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = &config.eloq_store_buffer_pool_size {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_buffer_pool_size",
                                            Some(value.clone()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_manifest_limit {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_manifest_limit",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_io_queue_size {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_io_queue_size",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_max_inflight_write {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_max_inflight_write",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_max_write_batch_pages {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_max_write_batch_pages",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_buf_ring_size {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_buf_ring_size",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_coroutine_stack_size {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_coroutine_stack_size",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_num_retained_archives {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_num_retained_archives",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_archive_interval_secs {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_archive_interval_secs",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_max_archive_tasks {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_max_archive_tasks",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_file_amplify_factor {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_file_amplify_factor",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_standby_max_concurrency {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_standby_max_concurrency",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = &config.eloq_store_local_space_limit {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_local_space_limit",
                                            Some(value.clone()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_reserve_space_ratio {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_reserve_space_ratio",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_data_page_size {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_data_page_size",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_pages_per_file_shift {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_pages_per_file_shift",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_overflow_pointers {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_overflow_pointers",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_enable_compression {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_enable_compression",
                                            Some(value.to_string()),
                                        );
                                    }
                                    if let Some(value) = config.eloq_store_max_upload_batch {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_max_upload_batch",
                                            Some(value.to_string()),
                                        );
                                    }
                                    // Write EloqStoreCloudConfig fields if cloud mode is enabled
                                    if let Some(cloud_config) = &config.eloq_store_cloud_config {
                                        // Enforce credentials for AWS/MINIO providers
                                        cloud_config.validate_credentials()?;
                                        let provider =
                                            cloud_config.eloq_store_cloud_provider.as_str();
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_cloud_provider",
                                            Some(cloud_config.eloq_store_cloud_provider.clone()),
                                        );
                                        // Only set access_key and secret_key for AWS/MinIO, not for GCS
                                        if provider == "aws" || provider == "minio" {
                                            if let (Some(access_key), Some(secret_key)) = (
                                                cloud_config.eloq_store_cloud_access_key.as_ref(),
                                                cloud_config.eloq_store_cloud_secret_key.as_ref(),
                                            ) {
                                                ini.set(
                                                    SECTION_STORE,
                                                    "eloq_store_cloud_access_key",
                                                    Some(access_key.clone()),
                                                );
                                                ini.set(
                                                    SECTION_STORE,
                                                    "eloq_store_cloud_secret_key",
                                                    Some(secret_key.clone()),
                                                );
                                            }
                                        }
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_cloud_endpoint",
                                            Some(cloud_config.eloq_store_cloud_endpoint.clone()),
                                        );
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_cloud_region",
                                            Some(cloud_config.eloq_store_cloud_region.clone()),
                                        );
                                        if let Some(value) =
                                            cloud_config.eloq_store_cloud_verify_ssl
                                        {
                                            ini.set(
                                                SECTION_STORE,
                                                "eloq_store_cloud_verify_ssl",
                                                Some(value.to_string()),
                                            );
                                        }
                                        if let Some(value) =
                                            cloud_config.eloq_store_max_cloud_concurrency
                                        {
                                            ini.set(
                                                SECTION_STORE,
                                                "eloq_store_max_cloud_concurrency",
                                                Some(value.to_string()),
                                            );
                                        }
                                        if let Some(value) =
                                            cloud_config.eloq_store_cloud_request_threads
                                        {
                                            ini.set(
                                                SECTION_STORE,
                                                "eloq_store_cloud_request_threads",
                                                Some(value.to_string()),
                                            );
                                        }
                                        if let Some(value) =
                                            cloud_config.eloq_store_prewarm_cloud_cache
                                        {
                                            ini.set(
                                                SECTION_STORE,
                                                "eloq_store_prewarm_cloud_cache",
                                                Some(value.to_string()),
                                            );
                                        }
                                        if let Some(value) =
                                            cloud_config.eloq_store_prewarm_task_count
                                        {
                                            ini.set(
                                                SECTION_STORE,
                                                "eloq_store_prewarm_task_count",
                                                Some(value.to_string()),
                                            );
                                        }
                                        if let Some(value) =
                                            cloud_config.eloq_store_reuse_local_files
                                        {
                                            ini.set(
                                                SECTION_STORE,
                                                "eloq_store_reuse_local_files",
                                                Some(value.to_string()),
                                            );
                                        }
                                    }
                                    if let Some(data_append_mode) =
                                        config.eloq_store_data_append_mode
                                    {
                                        ini.set(
                                            SECTION_STORE,
                                            "eloq_store_data_append_mode",
                                            Some(data_append_mode.to_string()),
                                        );
                                    }
                                } // For future backends, add appropriate handling here
                            }
                        } else {
                            // For Remote mode, only write the eloq_dss_peer_node to the [store] section
                            // Using the first peer_host_port as the eloq_dss_peer_node
                            // The backend configuration will be written to EloqDss.ini
                            if let Some(peer_host_ports) = &dss.peer_host_ports {
                                if !peer_host_ports.is_empty() {
                                    ini.set(
                                        SECTION_STORE,
                                        "eloq_dss_peer_node",
                                        Some(peer_host_ports[0].clone()),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        } else {
            let rocks_path = {
                if port.is_empty() {
                    format!("{}/rocksdb", self.tx_srv_home())
                } else {
                    format!("{}/rocksdb-{}", self.tx_srv_home(), port)
                }
            };

            ini.set(SECTION_STORE, "rocksdb_storage_path", Some(rocks_path));
        }

        let tx_host_ports = &self.tx_service.tx_host_ports;
        if set_ip_list {
            // Set the ip_port_list
            let tx_ip_port_list = tx_host_ports
                .iter()
                .flat_map(|host_str| {
                    host_str
                        .split(',') // Split on ','
                        .map(str::trim) // Trim any whitespace
                })
                .collect::<Vec<&str>>()
                .join(",");
            ini.set(SECTION_CLUSTER, "ip_port_list", Some(tx_ip_port_list));

            if let Some(standby_host_ports) = &self.tx_service.standby_host_ports {
                if standby_host_ports.is_empty() {
                    bail!("standby_host_ports is empty, but it was expected to contain values.");
                }

                // Process each string in the standby_host_ports vector
                let standby_ip_port_list = standby_host_ports
                    .iter()
                    .map(|host_str| {
                        let mut trimmed = String::new();
                        let mut current = String::new();
                        for c in host_str.chars() {
                            if c == ',' || c == '|' {
                                // Trim the current token and append the delimiter
                                trimmed.push_str(current.trim());
                                trimmed.push(c);
                                current.clear();
                            } else {
                                current.push(c);
                            }
                        }
                        // Append the last token after the loop
                        trimmed.push_str(current.trim());
                        trimmed
                    })
                    .collect::<Vec<String>>()
                    .join(","); // Use the appropriate delimiter to join multiple host_strs if needed

                ini.set(
                    SECTION_CLUSTER,
                    "standby_ip_port_list",
                    Some(standby_ip_port_list),
                );
            }

            if let Some(voter_host_ports) = &self.tx_service.voter_host_ports {
                if voter_host_ports.is_empty() {
                    bail!("voter_host_ports is empty, but it was expected to contain values.");
                }

                // Process each string in the voter_host_ports vector
                let voter_ip_port_list = voter_host_ports
                    .iter()
                    .map(|host_str| {
                        let mut trimmed = String::new();
                        let mut current = String::new();
                        for c in host_str.chars() {
                            if c == ',' || c == '|' {
                                // Trim the current token and append the delimiter
                                trimmed.push_str(current.trim());
                                trimmed.push(c);
                                current.clear();
                            } else {
                                current.push(c);
                            }
                        }
                        // Append the last token after the loop
                        trimmed.push_str(current.trim());
                        trimmed
                    })
                    .collect::<Vec<String>>()
                    .join(","); // Use the appropriate delimiter to join multiple host_strs if needed

                ini.set(
                    SECTION_CLUSTER,
                    "voter_ip_port_list",
                    Some(voter_ip_port_list),
                );
            }

            if let Some(max_standby_lag) = &self.tx_service.max_standby_lag {
                ini.set(
                    SECTION_CLUSTER,
                    "max_standby_lag",
                    Some(max_standby_lag.to_string()),
                );
            }
        } else {
            ini.set(
                SECTION_CLUSTER,
                "ip_port_list",
                Some(format!("{}:{}", "127.0.0.1", "6379")),
            );

            if let Some(max_standby_lag) = &self.tx_service.max_standby_lag {
                ini.set(
                    SECTION_CLUSTER,
                    "max_standby_lag",
                    Some(max_standby_lag.to_string()),
                );
            }
        }

        if self.is_empty(&ini, SECTION_METRIC, "enable_metrics") {
            ini.set(
                SECTION_METRIC,
                "enable_metrics",
                Some(
                    self.monitor
                        .as_ref()
                        .and_then(|monitor| {
                            if monitor.eloq_metrics.is_some() {
                                Some(true)
                            } else {
                                None
                            }
                        })
                        .is_some()
                        .to_string(),
                ),
            );
        }

        if self.is_empty(&ini, SECTION_LOCAL, "enable_data_store") {
            ini.set(
                SECTION_LOCAL,
                "enable_data_store",
                Some(self.storage_service.is_some().to_string()),
            );
        } else {
            println!("**WARNING:** Manually modifying `enable_data_store` in template `EloqKv.ini` is not recommended.");
        }

        if self.is_empty(&ini, SECTION_LOCAL, "enable_wal") {
            ini.set(
                SECTION_LOCAL,
                "enable_wal",
                Some(self.enable_wal.unwrap_or(false).to_string()),
            );
        } else {
            println!("**WARNING:** Manually modifying `enable_wal` in template `EloqKv.ini` is not recommended.");
        }

        if self.is_empty(&ini, SECTION_LOCAL, "enable_io_uring") {
            ini.set(
                SECTION_LOCAL,
                "enable_io_uring",
                Some(self.enable_io_uring.unwrap_or(false).to_string()),
            );
        } else {
            println!("**WARNING:** Manually modifying `enable_io_uring` in template `EloqKv.ini` is not recommended.");
        }

        if self.is_empty(&ini, SECTION_LOCAL, "checkpointer_interval") {
            ini.set(
                SECTION_LOCAL,
                "checkpointer_interval",
                Some(self.checkpoint_interval.unwrap_or(60).to_string()),
            );
        } else {
            println!("**WARNING:** Manually modifying `checkpointer_interval` in template `EloqKv.ini` is not recommended.");
        }

        if let Some(mode) = self.cluster_mode {
            if self.is_empty(&ini, SECTION_LOCAL, "cluster_mode") {
                ini.set(SECTION_LOCAL, "cluster_mode", Some(mode.to_string()));
            }
        }

        Ok(ini.clone())
    }

    fn is_empty(&self, ini: &Ini, section: &str, key: &str) -> bool {
        ini.get(section, key)
            .is_some_and(|value| value == "${OVERRIDE}")
    }

    pub fn gen_eloqkv_node_config(
        &self,
        host: Option<String>,
        port: Option<String>,
    ) -> Result<PathBuf> {
        let ini_name = ELOQKV_NODE_INI;

        let cnf_path;
        let mut ini;

        if let (Some(host_get), Some(port_get)) = (host.clone(), port.clone()) {
            // Create the path using the cluster_name and ip-addr format
            let dir = format!("{}/{}", self.cluster_name, host_get);
            cnf_path = create_upload_cluster_dir(&dir)
                .join(format!("{}-{}.{}", ini_name, port_get, "ini"));

            ini = self.build_eloqkv_config(true, port_get.clone())?;

            // If storage is S3/MINIO or EloqDSS with EloqStore in cloud mode, add txlog cloud settings to [local].
            if let Some(storage) = self.storage_service.as_ref() {
                if let Some(rocksdb) = storage.rocksdb.as_ref() {
                    match rocksdb {
                        RocksDB::S3(s3) => {
                            let bucket = format!("{}-{}", s3.bucket_prefix, s3.bucket_name);
                            if let Some(endpoint) = &s3.endpoint {
                                ini.set(
                                    SECTION_LOCAL,
                                    "txlog_rocksdb_cloud_s3_endpoint_url",
                                    Some(endpoint.clone()),
                                );
                            }
                            ini.set(
                                SECTION_LOCAL,
                                "txlog_rocksdb_cloud_bucket_name",
                                Some(bucket),
                            );
                            ini.set(
                                SECTION_LOCAL,
                                "txlog_rocksdb_cloud_region",
                                Some(s3.region.clone()),
                            );
                        }
                        RocksDB::MINIO(minio) => {
                            let bucket = format!("{}-{}", minio.bucket_prefix, minio.bucket_name);
                            ini.set(
                                SECTION_LOCAL,
                                "txlog_rocksdb_cloud_s3_endpoint_url",
                                Some(minio.endpoint.clone()),
                            );
                            ini.set(
                                SECTION_LOCAL,
                                "txlog_rocksdb_cloud_bucket_name",
                                Some(bucket),
                            );
                        }
                        _ => {}
                    }
                } else if let Some(dss) = storage.eloqdss.as_ref() {
                    use crate::config::storage_service_config::DataStoreServiceBackend;
                    match dss.backend_config() {
                        DataStoreServiceBackend::EloqStore(eloq_store_config) => {
                            if eloq_store_config.is_cloud_mode() && !self.log_service.is_some() {
                                if let Some(cloud_config) = eloq_store_config.get_cloud_config() {
                                    let provider = cloud_config.eloq_store_cloud_provider.as_str();
                                    let bucket_name = eloq_store_config
                                        .eloq_store_cloud_store_path
                                        .clone()
                                        .unwrap_or_else(|| "txlog-eloqkv".to_string());

                                    if provider == "aws" || provider == "minio" {
                                        // For AWS/MinIO: keep endpoint_url and bucket_name, add region
                                        ini.set(
                                            SECTION_LOCAL,
                                            "txlog_rocksdb_cloud_s3_endpoint_url",
                                            Some(cloud_config.eloq_store_cloud_endpoint.clone()),
                                        );
                                        ini.set(
                                            SECTION_LOCAL,
                                            "txlog_rocksdb_cloud_bucket_name",
                                            Some(bucket_name.clone()),
                                        );
                                        ini.set(
                                            SECTION_LOCAL,
                                            "txlog_rocksdb_cloud_region",
                                            Some(cloud_config.eloq_store_cloud_region.clone()),
                                        );
                                    } else if provider == "gcs" {
                                        // For GCS: only bucket_name and region, no endpoint_url
                                        ini.set(
                                            SECTION_LOCAL,
                                            "txlog_rocksdb_cloud_bucket_name",
                                            Some(bucket_name.clone()),
                                        );
                                        ini.set(
                                            SECTION_LOCAL,
                                            "txlog_rocksdb_cloud_region",
                                            Some(cloud_config.eloq_store_cloud_region.clone()),
                                        );
                                    }
                                }
                            }
                        } // Future backends can be handled here
                    }
                }
            }

            if self.log_service.is_some() {
                // If MINIO is used, skip setting txlog_service_list but keep other log configs
                let is_minio = self
                    .storage_service
                    .as_ref()
                    .and_then(|s| s.rocksdb.as_ref())
                    .map(|r| matches!(r, RocksDB::MINIO(_)))
                    .unwrap_or(false);

                self.build_log_config()
                    .into_iter()
                    .for_each(|(key, conf_val)| {
                        if is_minio && key == "txlog_service_list" {
                            // skip
                        } else {
                            ini.set(SECTION_CLUSTER, &key, Some(conf_val));
                        }
                    });
            }

            ini.set(SECTION_LOCAL, "ip", Some(host_get.clone()));
            ini.set(SECTION_LOCAL, "port", Some(port_get.clone()));

            if self.tls_enabled() {
                let (cert_name, key_name) = self.ensure_tls_cert_for_node(&host_get, &port_get)?;
                let cert_dir = self.tls_cert_install_dir();
                ini.set(SECTION_LOCAL, "enable_tls", Some("true".to_string()));
                ini.set(
                    SECTION_LOCAL,
                    "tls_cert_file",
                    Some(format!("{}/{}", cert_dir, cert_name)),
                );
                ini.set(
                    SECTION_LOCAL,
                    "tls_key_file",
                    Some(format!("{}/{}", cert_dir, key_name)),
                );
            }

            // Set maxclients if specified in deployment config
            if let Some(maxclients) = self.maxclients {
                let key = "maxclients";
                if set_by_user!(ini.get(SECTION_LOCAL, key), u32).is_none() {
                    ini.set(SECTION_LOCAL, key, Some(maxclients.to_string()));
                }
            }

            if let Some(hw) = self.get_hardware(&host_get, &port_get) {
                let key = "core_number";
                let mut core_tx = 1; // minimal value
                if let Some(val) = set_by_user!(ini.get(SECTION_LOCAL, key), u16) {
                    core_tx = val;
                } else {
                    if hw.cpu == 0 {
                        bail!("Hardware CPU count must be greater than 0");
                    }
                    core_tx = match hw.cpu {
                        1 | 2 => 1,
                        3 | 4 => 2,
                        _ => core_tx.max((hw.cpu * 4) / 5),
                    };
                    ini.set(SECTION_LOCAL, key, Some(core_tx.to_string()));
                }

                let key = "event_dispatcher_num";
                let val = set_by_user!(ini.get(SECTION_LOCAL, key), u16);
                if val.is_none() {
                    let core_io = hw.event_dispatcher_num.unwrap_or(core_tx.div_ceil(8));
                    ini.set(SECTION_LOCAL, key, Some(core_io.to_string()));
                }

                let union_cass = self
                    .topology()
                    .get(&host_get)
                    .unwrap()
                    .contains(&DeploymentPackage::Storage);
                let key = "node_memory_limit_mb";
                let val = set_by_user!(ini.get(SECTION_LOCAL, key), u32);
                if val.is_none() {
                    let mut limit = hw.memory * 4 / 5;
                    if union_cass {
                        limit /= 2;
                    }
                    if limit == 0 {
                        bail!("Memory limit must be greater than 0");
                    }
                    ini.set(SECTION_LOCAL, key, Some(limit.to_string()));
                }

                // Per-node base data directory override. Relocates the whole
                // node data tree so co-located nodes can use distinct paths.
                if let Some(data_path) = &hw.eloq_data_path {
                    ini.set(SECTION_LOCAL, "eloq_data_path", Some(data_path.clone()));
                }

                // Per-node EloqStore data directory resolution (precedence:
                // per-node store override > shared storage_service value >
                // derived from the effective base path). Overrides the value
                // already written by build_eloqkv_config.
                if let Some(data_path_list) = &hw.eloq_store_data_path_list {
                    ini.set(
                        SECTION_STORE,
                        "eloq_store_data_path_list",
                        Some(data_path_list.clone()),
                    );
                } else if let Some(base) = &hw.eloq_data_path {
                    // Base was redirected and no explicit store list given:
                    // keep the EloqStore data under the new base, unless a
                    // shared explicit value was configured.
                    if self.global_eloq_store_data_path_list().is_none() {
                        use crate::config::storage_service_config::EloqStoreConfig;
                        ini.set(
                            SECTION_STORE,
                            "eloq_store_data_path_list",
                            Some(EloqStoreConfig::compute_default_eloq_store_data_path(base)),
                        );
                    }
                }
            }

            // Fix the value of eloq_store_worker_num to be same as core_number
            if ini.get(SECTION_STORE, "eloq_store_worker_num").is_some() {
                if let Some(core_number_str) = ini.get(SECTION_LOCAL, "core_number") {
                    if let Ok(core_number) = core_number_str.parse::<u16>() {
                        ini.set(
                            SECTION_STORE,
                            "eloq_store_worker_num",
                            Some(core_number.to_string()),
                        );
                    }
                }
            }
        } else {
            // Create the redis_local.ini file in the cluster's monitor directory
            cnf_path = upload_dir()
                .join(&self.cluster_name)
                .join("redis_local.ini");
            ini = self.build_eloqkv_config(false, "".to_string())?;

            ini.set(SECTION_LOCAL, "ip", Some("127.0.0.1".to_owned()));
            ini.set(SECTION_LOCAL, "port", Some("6379".to_owned()));

            // Set maxclients if specified in deployment config
            if let Some(maxclients) = self.maxclients {
                let key = "maxclients";
                if set_by_user!(ini.get(SECTION_LOCAL, key), u32).is_none() {
                    ini.set(SECTION_LOCAL, key, Some(maxclients.to_string()));
                }
            }

            // If storage is S3/MINIO or EloqDSS with EloqStore in cloud mode, add txlog cloud settings to [local] for local ini as well
            if let Some(storage) = self.storage_service.as_ref() {
                if let Some(rocksdb) = storage.rocksdb.as_ref() {
                    match rocksdb {
                        RocksDB::S3(s3) => {
                            let bucket = format!("{}-{}", s3.bucket_prefix, s3.bucket_name);
                            if let Some(endpoint) = &s3.endpoint {
                                ini.set(
                                    SECTION_LOCAL,
                                    "txlog_rocksdb_cloud_s3_endpoint_url",
                                    Some(endpoint.clone()),
                                );
                            }
                            ini.set(
                                SECTION_LOCAL,
                                "txlog_rocksdb_cloud_bucket_name",
                                Some(bucket),
                            );
                            ini.set(
                                SECTION_LOCAL,
                                "txlog_rocksdb_cloud_region",
                                Some(s3.region.clone()),
                            );
                        }
                        RocksDB::MINIO(minio) => {
                            let bucket = format!("{}-{}", minio.bucket_prefix, minio.bucket_name);
                            ini.set(
                                SECTION_LOCAL,
                                "txlog_rocksdb_cloud_s3_endpoint_url",
                                Some(minio.endpoint.clone()),
                            );
                            ini.set(
                                SECTION_LOCAL,
                                "txlog_rocksdb_cloud_bucket_name",
                                Some(bucket),
                            );
                        }
                        _ => {}
                    }
                } else if let Some(dss) = storage.eloqdss.as_ref() {
                    use crate::config::storage_service_config::DataStoreServiceBackend;
                    match dss.backend_config() {
                        DataStoreServiceBackend::EloqStore(eloq_store_config) => {
                            if eloq_store_config.is_cloud_mode() {
                                if let Some(cloud_config) = eloq_store_config.get_cloud_config() {
                                    let provider = cloud_config.eloq_store_cloud_provider.as_str();
                                    let bucket_name = eloq_store_config
                                        .eloq_store_cloud_store_path
                                        .clone()
                                        .unwrap_or_else(|| "txlog-eloqkv".to_string());

                                    if provider == "aws" || provider == "minio" {
                                        // For AWS/MinIO: keep endpoint_url and bucket_name, add region
                                        ini.set(
                                            SECTION_LOCAL,
                                            "txlog_rocksdb_cloud_s3_endpoint_url",
                                            Some(cloud_config.eloq_store_cloud_endpoint.clone()),
                                        );
                                        ini.set(
                                            SECTION_LOCAL,
                                            "txlog_rocksdb_cloud_bucket_name",
                                            Some(bucket_name.clone()),
                                        );
                                        ini.set(
                                            SECTION_LOCAL,
                                            "txlog_rocksdb_cloud_region",
                                            Some(cloud_config.eloq_store_cloud_region.clone()),
                                        );
                                    } else if provider == "gcs" {
                                        // For GCS: only bucket_name and region, no endpoint_url
                                        ini.set(
                                            SECTION_LOCAL,
                                            "txlog_rocksdb_cloud_bucket_name",
                                            Some(bucket_name.clone()),
                                        );
                                        ini.set(
                                            SECTION_LOCAL,
                                            "txlog_rocksdb_cloud_region",
                                            Some(cloud_config.eloq_store_cloud_region.clone()),
                                        );
                                    }
                                }
                            }
                        } // Future backends can be handled here
                    }
                }
            }
        }

        // if let Some(parent) = cnf_path.parent() {
        //     if !parent.exists() {
        //         fs::create_dir_all(parent)?;
        //     }
        // }

        ini.write(cnf_path.as_path())?;
        Ok(cnf_path)
    }

    pub fn build_dss_config(&self, host: String, port: String) -> anyhow::Result<Ini> {
        let mut ini = Ini::new();
        ini.load(cluster_config_template(
            &self.cluster_name,
            ELOQDSS_TEMPLATE_INI,
        )?)
        .map_err(|e| anyhow!("Failed to load template ini: {e}"))?;

        ini.set("local", "ip", Some(host.clone()));
        ini.set("local", "port", Some(port.clone()));
        ini.set(
            "local",
            "log_dir",
            Some(format!("{}/logs/dss", self.tx_srv_home())),
        );
        ini.set(
            "local",
            "data_path",
            Some(format!("{}/data/dss-{}", self.tx_srv_home(), port)),
        );

        // Track whether any [store] fields are actually provided by launch yaml
        let mut store_fields_set = false;
        if let Some(storage) = &self.storage_service {
            if let Some(crate::config::storage_service_config::RocksDB::EloqDssRocksdb(s)) =
                &storage.rocksdb
            {
                if let Some(v) = &s.aws_access_key_id {
                    ini.set("store", "aws_access_key_id", Some(v.clone()));
                    store_fields_set = true;
                }
                if let Some(v) = &s.aws_secret_key {
                    ini.set("store", "aws_secret_key", Some(v.clone()));
                    store_fields_set = true;
                }
                if let Some(v) = &s.bucket_name {
                    ini.set("store", "rocksdb_cloud_bucket_name", Some(v.clone()));
                    store_fields_set = true;
                }
                if let Some(v) = &s.bucket_prefix {
                    ini.set("store", "rocksdb_cloud_bucket_prefix", Some(v.clone()));
                    store_fields_set = true;
                }
                if let Some(v) = &s.region {
                    ini.set("store", "rocksdb_cloud_region", Some(v.clone()));
                    store_fields_set = true;
                }
                if let Some(v) = &s.target_file_size_base {
                    ini.set("store", "rocksdb_target_file_size_base", Some(v.clone()));
                    store_fields_set = true;
                }
                if let Some(v) = &s.sst_file_cache_size {
                    ini.set(
                        "store",
                        "rocksdb_cloud_sst_file_cache_size",
                        Some(v.clone()),
                    );
                    store_fields_set = true;
                }
                if let Some(v) = &s.rocksdb_enable_stats {
                    ini.set("store", "rocksdb_enable_stats", Some(v.clone()));
                    store_fields_set = true;
                }
                if let Some(v) = &s.rocksdb_stats_dump_period_sec {
                    ini.set("store", "rocksdb_stats_dump_period_sec", Some(v.clone()));
                    store_fields_set = true;
                }
                if let Some(v) = &s.rocksdb_periodic_compaction_seconds {
                    ini.set(
                        "store",
                        "rocksdb_periodic_compaction_seconds",
                        Some(v.clone()),
                    );
                    store_fields_set = true;
                }
                if let Some(v) = &s.rocksdb_delete_obsolete_files_period_micros {
                    ini.set(
                        "store",
                        "rocksdb_delete_obsolete_files_period_micros",
                        Some(v.clone()),
                    );
                    store_fields_set = true;
                }
                if let Some(v) = &s.object_path {
                    ini.set("store", "rocksdb_cloud_object_path", Some(v.clone()));
                    store_fields_set = true;
                }
            }
            // Handle DataStoreService Remote mode (EloqStore backend)
            // Only generate config for internal (managed) mode, not external
            if let Some(dss) = &storage.eloqdss {
                if dss.is_remote_mode() && !dss.is_external() {
                    match dss.backend_config() {
                        DataStoreServiceBackend::EloqStore(config) => {
                            if let Some(worker_num) = config.eloq_store_worker_num {
                                ini.set(
                                    "store",
                                    "eloq_store_worker_num",
                                    Some(worker_num.to_string()),
                                );
                            }
                            if let Some(data_path_list) = &config.eloq_store_data_path_list {
                                ini.set(
                                    "store",
                                    "eloq_store_data_path_list",
                                    Some(data_path_list.clone()),
                                );
                                store_fields_set = true;
                            } else {
                                // Compute default value if not specified
                                // Only write to ini file, don't modify memory
                                // In Remote mode, use DSS port as EloqKV port to compute default path
                                use crate::config::storage_service_config::EloqStoreConfig;
                                let eloq_data_path =
                                    format!("{}/data/port-{}", self.tx_srv_home(), port);
                                let default_data_path =
                                    EloqStoreConfig::compute_default_eloq_store_data_path(
                                        &eloq_data_path,
                                    );
                                ini.set(
                                    "store",
                                    "eloq_store_data_path_list",
                                    Some(default_data_path),
                                );
                                store_fields_set = true;
                            }
                            // Per-node override (see gen_eloqkv_node_config):
                            // an ip:port-keyed hardware entry wins over the
                            // shared value and the default computed above.
                            if let Some(hw_path) = self
                                .get_hardware(&host, &port)
                                .and_then(|hw| hw.eloq_store_data_path_list.as_ref())
                            {
                                ini.set(
                                    "store",
                                    "eloq_store_data_path_list",
                                    Some(hw_path.clone()),
                                );
                                store_fields_set = true;
                            }
                            // EloqStore Cloud mode configuration
                            // When cloud_store_path is set, EloqStore operates in cloud mode
                            // using direct S3/MinIO access
                            if let Some(cloud_path) = &config.eloq_store_cloud_store_path {
                                if !cloud_path.is_empty() {
                                    ini.set(
                                        "store",
                                        "eloq_store_cloud_store_path",
                                        Some(cloud_path.clone()),
                                    );
                                    store_fields_set = true;
                                }
                            }
                            // EloqStoreConfig additional fields
                            if let Some(value) = config.eloq_store_open_files_limit {
                                ini.set(
                                    "store",
                                    "eloq_store_open_files_limit",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_data_page_restart_interval {
                                ini.set(
                                    "store",
                                    "eloq_store_data_page_restart_interval",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_index_page_restart_interval {
                                ini.set(
                                    "store",
                                    "eloq_store_index_page_restart_interval",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_init_page_count {
                                ini.set(
                                    "store",
                                    "eloq_store_init_page_count",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_skip_verify_checksum {
                                ini.set(
                                    "store",
                                    "eloq_store_skip_verify_checksum",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = &config.eloq_store_buffer_pool_size {
                                ini.set(
                                    "store",
                                    "eloq_store_buffer_pool_size",
                                    Some(value.clone()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_manifest_limit {
                                ini.set(
                                    "store",
                                    "eloq_store_manifest_limit",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_io_queue_size {
                                ini.set(
                                    "store",
                                    "eloq_store_io_queue_size",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_max_inflight_write {
                                ini.set(
                                    "store",
                                    "eloq_store_max_inflight_write",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_max_write_batch_pages {
                                ini.set(
                                    "store",
                                    "eloq_store_max_write_batch_pages",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_buf_ring_size {
                                ini.set(
                                    "store",
                                    "eloq_store_buf_ring_size",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_coroutine_stack_size {
                                ini.set(
                                    "store",
                                    "eloq_store_coroutine_stack_size",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_num_retained_archives {
                                ini.set(
                                    "store",
                                    "eloq_store_num_retained_archives",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_archive_interval_secs {
                                ini.set(
                                    "store",
                                    "eloq_store_archive_interval_secs",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_max_archive_tasks {
                                ini.set(
                                    "store",
                                    "eloq_store_max_archive_tasks",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_file_amplify_factor {
                                ini.set(
                                    "store",
                                    "eloq_store_file_amplify_factor",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_standby_max_concurrency {
                                ini.set(
                                    "store",
                                    "eloq_store_standby_max_concurrency",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = &config.eloq_store_local_space_limit {
                                ini.set(
                                    "store",
                                    "eloq_store_local_space_limit",
                                    Some(value.clone()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_reserve_space_ratio {
                                ini.set(
                                    "store",
                                    "eloq_store_reserve_space_ratio",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_data_page_size {
                                ini.set(
                                    "store",
                                    "eloq_store_data_page_size",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_pages_per_file_shift {
                                ini.set(
                                    "store",
                                    "eloq_store_pages_per_file_shift",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_overflow_pointers {
                                ini.set(
                                    "store",
                                    "eloq_store_overflow_pointers",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_enable_compression {
                                ini.set(
                                    "store",
                                    "eloq_store_enable_compression",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            if let Some(value) = config.eloq_store_max_upload_batch {
                                ini.set(
                                    "store",
                                    "eloq_store_max_upload_batch",
                                    Some(value.to_string()),
                                );
                                store_fields_set = true;
                            }
                            // Write EloqStoreCloudConfig fields if cloud mode is enabled
                            if let Some(cloud_config) = &config.eloq_store_cloud_config {
                                // Enforce credentials for AWS/MINIO providers
                                cloud_config.validate_credentials()?;
                                let provider = cloud_config.eloq_store_cloud_provider.as_str();
                                ini.set(
                                    "store",
                                    "eloq_store_cloud_provider",
                                    Some(cloud_config.eloq_store_cloud_provider.clone()),
                                );
                                // Only set access_key and secret_key for AWS/MinIO, not for GCS
                                if provider == "aws" || provider == "minio" {
                                    if let (Some(access_key), Some(secret_key)) = (
                                        cloud_config.eloq_store_cloud_access_key.as_ref(),
                                        cloud_config.eloq_store_cloud_secret_key.as_ref(),
                                    ) {
                                        ini.set(
                                            "store",
                                            "eloq_store_cloud_access_key",
                                            Some(access_key.clone()),
                                        );
                                        ini.set(
                                            "store",
                                            "eloq_store_cloud_secret_key",
                                            Some(secret_key.clone()),
                                        );
                                    }
                                }
                                ini.set(
                                    "store",
                                    "eloq_store_cloud_endpoint",
                                    Some(cloud_config.eloq_store_cloud_endpoint.clone()),
                                );
                                ini.set(
                                    "store",
                                    "eloq_store_cloud_region",
                                    Some(cloud_config.eloq_store_cloud_region.clone()),
                                );
                                if let Some(value) = cloud_config.eloq_store_cloud_verify_ssl {
                                    ini.set(
                                        "store",
                                        "eloq_store_cloud_verify_ssl",
                                        Some(value.to_string()),
                                    );
                                }
                                if let Some(value) = cloud_config.eloq_store_max_cloud_concurrency {
                                    ini.set(
                                        "store",
                                        "eloq_store_max_cloud_concurrency",
                                        Some(value.to_string()),
                                    );
                                }
                                if let Some(value) = cloud_config.eloq_store_cloud_request_threads {
                                    ini.set(
                                        "store",
                                        "eloq_store_cloud_request_threads",
                                        Some(value.to_string()),
                                    );
                                }
                                if let Some(value) = cloud_config.eloq_store_prewarm_cloud_cache {
                                    ini.set(
                                        "store",
                                        "eloq_store_prewarm_cloud_cache",
                                        Some(value.to_string()),
                                    );
                                }
                                if let Some(value) = cloud_config.eloq_store_prewarm_task_count {
                                    ini.set(
                                        "store",
                                        "eloq_store_prewarm_task_count",
                                        Some(value.to_string()),
                                    );
                                }
                                if let Some(value) = cloud_config.eloq_store_reuse_local_files {
                                    ini.set(
                                        "store",
                                        "eloq_store_reuse_local_files",
                                        Some(value.to_string()),
                                    );
                                }
                                store_fields_set = true;
                            }
                            if let Some(data_append_mode) = config.eloq_store_data_append_mode {
                                ini.set(
                                    "store",
                                    "eloq_store_data_append_mode",
                                    Some(data_append_mode.to_string()),
                                );
                                store_fields_set = true;
                            }
                        } // For future backends, add appropriate handling here
                    }
                }
            }
        }

        // After populating store section, remove any leftover ${OVERRIDE} placeholders.
        if let Some(store_section) = ini.get_map() {
            if let Some(store_map) = store_section.get("store") {
                let store_pairs: Vec<(String, Option<String>)> = store_map
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                let mut has_real_value = false;
                for (key, value) in store_pairs {
                    if value.as_deref() == Some("${OVERRIDE}") {
                        ini.remove_key("store", key.as_str());
                    } else {
                        has_real_value = true;
                    }
                }
                if !has_real_value {
                    ini.remove_section("store");
                }
            }
        }

        // If no store-related fields were set from yaml, drop the [store] section from the ini
        if !store_fields_set {
            ini.remove_section("store");
        }

        Ok(ini)
    }

    pub fn eloq_download_links(&self) -> anyhow::Result<HashMap<String, DownloadUrl>> {
        let mut links = HashMap::new();
        download_urls!(links,{ELOQ_FILE_KEY, self.tx_image()});
        if let Some(img) = self.log_image() {
            download_urls!(links,{ELOQ_LOG_FILE_KEY, img});
        }
        Ok(links)
    }

    pub fn all_download_links(&self) -> anyhow::Result<HashMap<String, DownloadUrl>> {
        let mut db_image_download_links = self.eloq_download_links()?;
        if let Some(monitor_srv) = self.monitor.as_ref() {
            db_image_download_links.extend(monitor_srv.download_links_as_map()?);
        }
        Ok(db_image_download_links)
    }

    fn get_host_list_internal(&self, host_port_entries: &Option<Vec<String>>) -> Vec<String> {
        host_port_entries
            .as_ref()
            .map(|hostports| {
                hostports
                    .iter()
                    .flat_map(|hostport| hostport.split(['|', ',']))
                    .map(|host| host.split(':').next().unwrap_or("")) // Take the part before ':' (i.e., the IP part)
                    .filter(|host| !host.is_empty())
                    .map(|host| host.to_string())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn get_host_list(&self, service: DeploymentPackage) -> Vec<String> {
        match service {
            DeploymentPackage::Storage => vec![],
            DeploymentPackage::EloqLog => {
                if let Some(ref log_srv) = self.log_service {
                    log_srv.log_host_unique()
                } else {
                    vec![]
                }
            }
            DeploymentPackage::EloqTx => {
                self.get_host_list_internal(&Some(self.tx_service.tx_host_ports.clone()))
            }
            DeploymentPackage::EloqStandby => {
                self.get_host_list_internal(&self.tx_service.standby_host_ports)
            }
            DeploymentPackage::EloqVoter => {
                self.get_host_list_internal(&self.tx_service.voter_host_ports)
            }
            DeploymentPackage::Prometheus => {
                extract_monitor_host!(self, prometheus)
            }
            DeploymentPackage::Alertmanager => {
                extract_monitor_host!(self, alertmanager)
            }
            DeploymentPackage::Grafana => {
                extract_monitor_host!(self, grafana)
            }
            DeploymentPackage::PrometheusAlert => {
                extract_monitor_host!(self, alertmanager)
            }
        }
    }

    fn get_host_port_list_internal(&self, host_port_entries: &Option<Vec<String>>) -> Vec<String> {
        host_port_entries
            .as_ref()
            .map(|hostports| {
                hostports
                    .iter()
                    .flat_map(|hostport| hostport.split(['|', ',']))
                    .filter(|hostport| !hostport.is_empty())
                    .map(|hostport| hostport.to_string())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn get_host_port_list(&self, service: DeploymentPackage) -> Vec<String> {
        match service {
            DeploymentPackage::Storage => vec![],
            DeploymentPackage::EloqLog => vec![],
            DeploymentPackage::EloqTx => {
                self.get_host_port_list_internal(&Some(self.tx_service.tx_host_ports.clone()))
            }
            DeploymentPackage::EloqStandby => {
                self.get_host_port_list_internal(&self.tx_service.standby_host_ports.clone())
            }
            DeploymentPackage::EloqVoter => {
                self.get_host_port_list_internal(&self.tx_service.voter_host_ports)
            }
            DeploymentPackage::Prometheus => self
                .monitor
                .as_ref()
                .and_then(|monitor| {
                    monitor
                        .prometheus
                        .as_ref()
                        .map(|component| vec![format!("{}:{}", component.host, component.port)])
                })
                .unwrap_or_default(),
            DeploymentPackage::Alertmanager => self
                .monitor
                .as_ref()
                .and_then(|monitor| {
                    monitor
                        .alertmanager
                        .as_ref()
                        .map(|component| vec![format!("{}:{}", component.host, component.port)])
                })
                .unwrap_or_default(),
            DeploymentPackage::Grafana => self
                .monitor
                .as_ref()
                .and_then(|monitor| {
                    monitor
                        .grafana
                        .as_ref()
                        .map(|component| vec![format!("{}:{}", component.host, component.port)])
                })
                .unwrap_or_default(),
            DeploymentPackage::PrometheusAlert => self
                .monitor
                .as_ref()
                .and_then(|monitor| {
                    monitor.alertmanager.as_ref().map(|component| {
                        vec![format!(
                            "{}:{}",
                            component.host, component.webhook_adapter_port
                        )]
                    })
                })
                .unwrap_or_default(),
        }
    }

    fn populate_topo(
        &self,
        topo: &mut HashMap<String, Vec<DeploymentPackage>>,
        pkg: DeploymentPackage,
    ) {
        self.get_host_list(pkg.clone())
            .into_iter()
            .for_each(|host| {
                if let Some(list) = topo.get_mut(&host) {
                    list.push(pkg.clone());
                } else {
                    topo.insert(host, vec![pkg.clone()]);
                }
            });
    }

    pub fn topology(&self) -> HashMap<String, Vec<DeploymentPackage>> {
        let mut topo: HashMap<String, Vec<DeploymentPackage>> = HashMap::new();
        self.populate_topo(&mut topo, DeploymentPackage::EloqTx);
        self.populate_topo(&mut topo, DeploymentPackage::EloqStandby);
        self.populate_topo(&mut topo, DeploymentPackage::EloqVoter);
        self.populate_topo(&mut topo, DeploymentPackage::Prometheus);
        self.populate_topo(&mut topo, DeploymentPackage::Alertmanager);
        self.populate_topo(&mut topo, DeploymentPackage::Grafana);
        self.populate_topo(&mut topo, DeploymentPackage::PrometheusAlert);
        self.populate_topo(&mut topo, DeploymentPackage::EloqLog);
        topo
    }

    /// Generate environment variable export statements from configuration
    pub fn gen_env_exports(&self) -> String {
        let mut env_exports = String::new();
        if let Some(env_vars) = &self.environment_variables {
            for (key, value) in env_vars {
                // Escape quotes in value to prevent shell injection
                let escaped_value = value.replace('"', "\\\"");
                env_exports.push_str(&format!("export {}=\"{}\"; ", key, escaped_value));
            }
        }
        env_exports
    }

    pub fn srv_start_cmd(&self, port: &str, server_type: ServerType) -> String {
        if server_type == ServerType::Node {
            unreachable!()
        }
        let ini_file = self.tx_srv_ini(port);
        let tx_dir = self.tx_srv_home();
        let tx_bin = self.tx_srv_bin();
        let logs_dir = self.node_srv_logs(port);

        let glog = format!(
            "mkdir -p {logs_dir} ; export GLOG_log_dir={logs_dir} ; export GLOG_max_log_size=1024"
        );
        let mut ld_lib = if let Some(Version::Debug) = self.version() {
            let fast_unwind_on_malloc = self.uses_eloqstore_storage();
            let detect_stack_use_after_return = !self.uses_eloqstore_storage();
            export_asan(
                &format!("{logs_dir}/asan"),
                fast_unwind_on_malloc,
                detect_stack_use_after_return,
            )
        } else {
            format!("export LD_PRELOAD={tx_dir}/lib/libmimalloc.so.2")
        };
        ld_lib.push_str(&format!(
            "; export LD_LIBRARY_PATH={tx_dir}/lib:$LD_LIBRARY_PATH"
        ));

        // Generate environment variable exports from configuration
        let env_exports = self.gen_env_exports();

        // Get the current datetime
        let now = Local::now();
        // Format the datetime as "YYYYMMDD-HHMMSS.microseconds"
        let datetime = now.format("%Y%m%d-%H%M%S.%6f").to_string();

        format!(
            "cd {tx_dir}; mkdir -p logs/std-output; {env_exports}{glog}; {ld_lib} ; {tx_bin} --config={ini_file} --graceful_quit_on_sigterm=true > logs/std-output/std-out-{port}-{datetime} 2>&1 & cd logs/std-output ; ln -sf std-out-{port}-{datetime} std-out-{port} "
        )
    }

    // only used in `start --nodes`
    pub fn srv_start_cmd_with_host(&self, port: &str, server_type: ServerType) -> String {
        let ini_file = match server_type {
            ServerType::Node => self.tx_srv_ini(port),
            _ => unreachable!(),
        };
        let tx_dir = self.tx_srv_home();
        let tx_bin = self.tx_srv_bin();
        let logs_dir = match server_type {
            ServerType::Node => self.node_srv_logs(port),
            _ => unreachable!(),
        };

        let glog = format!(
            "mkdir -p {logs_dir} ; export GLOG_log_dir={logs_dir} ; export GLOG_max_log_size=1024"
        );
        let mut ld_lib = if let Some(Version::Debug) = self.version() {
            let fast_unwind_on_malloc = self.uses_eloqstore_storage();
            let detect_stack_use_after_return = !self.uses_eloqstore_storage();
            export_asan(
                &format!("{logs_dir}/asan"),
                fast_unwind_on_malloc,
                detect_stack_use_after_return,
            )
        } else {
            format!("export LD_PRELOAD={tx_dir}/lib/libmimalloc.so.2")
        };
        ld_lib.push_str(&format!(
            "; export LD_LIBRARY_PATH={tx_dir}/lib:$LD_LIBRARY_PATH"
        ));

        // Generate environment variable exports from configuration
        let env_exports = self.gen_env_exports();

        // Get the current datetime
        let now = Local::now();
        // Format the datetime as "YYYYMMDD-HHMMSS.microseconds"
        let datetime = now.format("%Y%m%d-%H%M%S.%6f").to_string();

        format!(
            "cd {tx_dir}; mkdir -p logs/std-output; {env_exports}{glog}; {ld_lib} ; {tx_bin} --config={ini_file} --graceful_quit_on_sigterm=true > logs/std-output/std-out-{port}-{datetime} 2>&1 & cd logs/std-output ; ln -sf std-out-{port}-{datetime} std-out-{port} "
        )
    }
}

pub fn version_digits(s: &str) -> Result<[u32; 3]> {
    let mut version = vec![];
    for e in s.split('.') {
        let v = e.parse::<u32>()?;
        version.push(v);
    }
    version
        .try_into()
        .map_err(|digits| anyhow!("too many version digits {:?}", digits))
}

pub fn parse_version(v: &str) -> Version {
    match v {
        "nightly" => Version::Nightly,
        "debug" => Version::Debug,
        _ => {
            if let Ok(digits) = version_digits(v) {
                Version::Tag(digits)
            } else {
                Version::Devel(v.to_owned())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_version, Version};
    use crate::cli::{create_upload_cluster_dir, upload_dir, HOME_DIR};
    use crate::config::config_base::DeployConfig;
    use crate::config::{
        config_template, CONFIG_PATH_DIR, ELOQKV_TEMPLATE_INI, SECTION_LOCAL, SECTION_STORE,
        UPLOAD_PATH_DIR,
    };
    use configparser::ini::Ini;
    use indexmap::IndexMap;
    use itertools::Itertools;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn set_test_env() -> PathBuf {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let config_path = manifest_dir.join("config");
        std::env::set_var(CONFIG_PATH_DIR, config_path.to_str().unwrap());

        let uniq = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let home = std::env::temp_dir().join(format!("eloqctl-test-{uniq}"));
        std::env::set_var(HOME_DIR, home.to_str().unwrap());
        fs::create_dir_all(upload_dir()).unwrap();
        std::env::set_var(UPLOAD_PATH_DIR, upload_dir().to_str().unwrap());
        home
    }

    #[test]
    pub fn test_index_value_order() {
        let map = IndexMap::from([
            (3, "3".to_string()),
            (2, "2".to_string()),
            (1, "1".to_string()),
        ]);

        let ordered_map = map
            .into_iter()
            .sorted_by_key(|(key, _val)| *key)
            .collect::<IndexMap<i32, String>>();
        let node_group = Vec::from_iter(ordered_map.values()).into_iter().join(",");

        println!("{node_group:#?}");
    }

    #[test]
    pub fn test_parse_version() {
        let mut _v = parse_version("0.1.0");
        assert!(matches!(Version::Tag([0, 1, 0]), _v));
        _v = parse_version("1.999.2024");
        assert!(matches!(Version::Tag([1, 999, 2024]), _v));
        _v = parse_version("nightly");
        assert!(matches!(Version::Nightly, _v));
        _v = parse_version("debug");
        assert!(matches!(Version::Debug, _v));
        _v = parse_version("dev1");
        assert!(matches!(Version::Devel("dev1".to_owned()), _v));
        _v = parse_version("dev-b");
        assert!(matches!(Version::Devel("dev-b".to_owned()), _v));
        _v = parse_version("0.4.4.4");
        assert!(matches!(Version::Devel("0.4.4.4".to_owned()), _v));
        _v = parse_version("0.4.1-beta");
        assert!(matches!(Version::Devel("0.4.1-beta".to_owned()), _v));
    }

    #[test]
    pub fn test_s3_endpoint_written_to_store_and_txlog_ini() {
        let test_home = set_test_env();
        let cluster_name = format!(
            "test-s3-endpoint-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let config_yaml = format!(
            r#"
connection:
  username: "tester"
  auth_type: "keypair"
  auth:
    keypair:
deployment:
  cluster_name: "{cluster_name}"
  product: "EloqKV"
  version: "debug"
  install_dir: "/tmp"
  enable_wal: true
  enable_io_uring: false
  checkpointer_interval: 120
  tx_service:
    tx_host_ports: [127.0.0.1:6389]
  storage_service:
    rocksdb: !S3
      aws_access_key_id: "ak"
      aws_secret_key: "sk"
      region: "ap-northeast-1"
      bucket_name: "bucket"
      bucket_prefix: "prefix"
      endpoint: "http://127.0.0.1:9000"
      target_file_size_base: "64MB"
      sst_file_cache_size: "64MB"
"#
        );

        let config: DeployConfig = serde_yaml::from_str(&config_yaml).unwrap();
        let cluster_dir = create_upload_cluster_dir(&cluster_name);
        fs::copy(
            config_template(ELOQKV_TEMPLATE_INI).unwrap(),
            cluster_dir.join(ELOQKV_TEMPLATE_INI),
        )
        .unwrap();
        let ini_path = config
            .deployment
            .gen_eloqkv_node_config(Some("127.0.0.1".to_string()), Some("6389".to_string()))
            .unwrap();

        let mut ini = Ini::new();
        ini.load(ini_path.to_str().unwrap()).unwrap();

        assert_eq!(
            ini.get(SECTION_STORE, "rocksdb_cloud_s3_endpoint_url"),
            Some("http://127.0.0.1:9000".to_string())
        );
        assert_eq!(
            ini.get(SECTION_LOCAL, "txlog_rocksdb_cloud_s3_endpoint_url"),
            Some("http://127.0.0.1:9000".to_string())
        );
        assert_eq!(
            ini.get(SECTION_LOCAL, "txlog_rocksdb_cloud_region"),
            Some("ap-northeast-1".to_string())
        );
        assert_eq!(
            ini.get(SECTION_LOCAL, "txlog_rocksdb_cloud_bucket_name"),
            Some("prefix-bucket".to_string())
        );

        fs::remove_dir_all(test_home).unwrap();
    }
}
