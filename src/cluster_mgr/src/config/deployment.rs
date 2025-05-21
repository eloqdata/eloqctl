use crate::cli::task::monograph_tx_ctl_task::ServerType;
use crate::cli::{create_upload_cluster_dir, upload_dir};
use crate::config::config_base::{
    export_asan, CASSANDRA_FILE_KEY, LOG_SERVICE_HOME, MONOGRAPH_FILE_KEY, MONOGRAPH_LOG_FILE_KEY,
};
use crate::config::log_service::LogService;
use crate::config::monitor::Monitor;
use crate::config::storage_service_config::{Cassandra, RocksDB, StorageService};
use crate::config::ConfigErr::GenCassandraConfigErr;
use crate::config::{
    cluster_config_template, config_template, load_yaml_config_template, DeploymentPackage,
    DownloadUrl, StorageProvider, CASSANDRA_CONF_TEMPLATE, CASSANDRA_JVM_OPTION,
    CASSANDRA_JVM_TEMPLATE, CDN, CODIS_DASHBOARD_CNF, CODIS_PROXY_CNF, ELOQKV_NODE_INI,
    ELOQKV_TEMPLATE_INI, ELOQSQL_CLIENT_PORT, ELOQSQL_DYNAMO_TEMPLATE_INI, ELOQSQL_TEMPLATE_INI,
    JVM_SETTING_HOLDER, SECTION_CLUSTER, SECTION_LOCAL, SECTION_MARIADB, SECTION_METRIC,
    SECTION_STORE,
};
use anyhow::{anyhow, Result};
use chrono::Local;
use configparser::ini::Ini;
use core::panic;
use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
use strum_macros::Display;
use tracing::warn;

const GC_SETTING_CMS: &str = "
-XX:+UseConcMarkSweepGC
-XX:+CMSParallelRemarkEnabled
-XX:SurvivorRatio=8
-XX:MaxTenuringThreshold=1
-XX:CMSInitiatingOccupancyFraction=75
-XX:+UseCMSInitiatingOccupancyOnly
-XX:CMSWaitDuration=10000
-XX:+CMSParallelInitialMarkEnabled
-XX:+CMSEdenChunksRecordAlways
-XX:+CMSClassUnloadingEnabled
";
const GC_SETTING_G1: &str = "
-XX:+UseG1GC
-XX:+ParallelRefProcEnabled
-XX:MaxTenuringThreshold=1
-XX:G1HeapRegionSize=16m
-XX:G1RSetUpdatingPauseTimePercent=5
-XX:MaxGCPauseMillis=300
-XX:InitiatingHeapOccupancyPercent=70
";
const GB: u32 = 1024; // *MiB

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
            Some(v.parse::<$T>()?)
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
    pub monograph_port: Option<MonographPort>,
}

impl Port {
    pub fn contains(&self, p: u16) -> bool {
        if let Some(mono_port) = &self.monograph_port {
            if p >= mono_port.start && p <= mono_port.end {
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
pub struct MonographPort {
    pub start: u16,
    pub end: u16,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct MonographService {
    pub image: Option<String>,
    pub tx_host_ports: Vec<String>,
    pub standby_host_ports: Option<Vec<String>>,
    pub voter_host_ports: Option<Vec<String>>,
    pub requirepass: Option<String>,
    pub enable_cache_replacement: Option<String>,
    pub client_port: Option<u16>, // only used in mysql
}

impl MonographService {
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
    #[serde(alias = "eloqsql", alias = "eloq-sql", alias = "SQL")]
    EloqSQL,
    #[serde(alias = "eloqkv", alias = "eloq-kv", alias = "KV")]
    EloqKV,
}

impl Product {
    pub fn name(&self) -> &str {
        match self {
            Product::EloqSQL => "eloqsql",
            Product::EloqKV => "eloqkv",
        }
    }

    pub fn home(&self) -> &str {
        match self {
            Product::EloqSQL => "EloqSQL",
            Product::EloqKV => "EloqKV",
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Hardware {
    pub cpu: u16,
    pub memory: u32, // MiB
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Codis {
    pub dashboard: String,
    pub proxy: Vec<String>,
}

impl Codis {
    pub fn download_url() -> String {
        format!("{CDN}/codis/codis-linux-amd64.tar.gz")
    }
    pub fn dir(install_dir: &str) -> String {
        format!("{install_dir}/codis")
    }
    pub fn dashboard_cfg(config: &Deployment) -> anyhow::Result<String> {
        // Check if storage_service exists
        if let Some(storage) = config.storage_service.as_ref() {
            if let Some(coord) = storage.cassandra.as_ref() {
                let port = coord.client_port()?;
                let addr = coord.host.iter().map(|ip| format!("{ip}:{port}")).join(",");
                let keyspace = config.get_redis_keyspace()?;
                let cmds = format!(
                    "sed -i 's/coordinator_addr.*/coordinator_addr = \"{addr}\"/g ; s/coordinator_keyspace.*/coordinator_keyspace = \"{}\"/g' {}/dashboard.toml",
                    keyspace, Self::dir(&config.install_dir())
                );
                return Ok(cmds);
            }
        }
        Err(anyhow!("Codis requires storage_service with cassandra"))
    }
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
    pub tx_service: MonographService,
    pub log_service: Option<LogService>,
    pub storage_service: Option<StorageService>,
    pub monitor: Option<Monitor>,
    pub codis: Option<Codis>,
    pub hardware: Option<HashMap<String, Hardware>>,
    pub enable_wal: Option<bool>,
}

impl Deployment {
    pub fn tx_image(&self) -> &str {
        self.tx_service.image.as_ref().unwrap()
    }

    pub fn log_image(&self) -> Option<&str> {
        if let Some(srv) = &self.log_service {
            srv.image.as_deref()
        } else {
            None
        }
    }

    pub fn get_hardware(&self, host: &str) -> Option<&Hardware> {
        if let Some(all_hw) = self.hardware.as_ref() {
            all_hw.get(host)
        } else {
            None
        }
    }

    pub fn product(&self) -> Product {
        self.product.clone()
    }

    pub fn version_str(&self) -> &str {
        self.version.as_ref().unwrap()
    }

    pub fn version(&self) -> Option<Version> {
        self.version.as_ref().map(|ver| parse_version(ver))
    }

    pub fn client_port(&self) -> u16 {
        if self.tx_service.client_port.is_some() {
            self.tx_service.client_port.unwrap()
        } else {
            ELOQSQL_CLIENT_PORT
        }
    }

    pub fn install_dir(&self) -> String {
        format!("{}/{}", &self.install_dir, self.cluster_name)
    }

    pub fn tx_srv_home(&self) -> String {
        format!("{}/{}", &self.install_dir(), self.product().home())
    }

    pub fn log_srv_home(&self) -> String {
        format!("{}/{LOG_SERVICE_HOME}", &self.install_dir())
    }

    pub fn cassandra_home(&self) -> String {
        format!("{}/cassandra", &self.install_dir())
    }

    pub fn tx_srv_ini(&self, port: &str) -> String {
        let home = self.tx_srv_home();
        match self.product() {
            Product::EloqSQL => format!("{home}/{ELOQSQL_TEMPLATE_INI}"),
            Product::EloqKV => format!("{}/{}-{}.ini", &self.tx_srv_home(), ELOQKV_NODE_INI, port),
        }
    }

    pub fn asan_logs(&self) -> String {
        format!("{}/logs", &self.tx_srv_home())
    }

    pub fn node_srv_logs(&self, port: &str) -> String {
        format!("{}/logs/node-{}", &self.tx_srv_home(), port)
    }

    pub fn tx_srv_bin(&self) -> String {
        match self.product() {
            Product::EloqSQL => format!("{}/bin/mariadbd", &self.tx_srv_home()),
            Product::EloqKV => format!("{}/bin/eloqkv", &self.tx_srv_home()),
        }
    }

    pub fn client_bin(&self) -> String {
        let tx_home = self.tx_srv_home();
        match self.product() {
            Product::EloqSQL => format!("{tx_home}/bin/mariadb"),
            Product::EloqKV => format!("{tx_home}/bin/eloqkv-cli"),
        }
    }

    pub fn get_monograph_keyspace(&self) -> anyhow::Result<String> {
        let my_local = upload_dir().join("my_local.cnf");
        if !my_local.exists() {
            self.gen_eloqsql_config(None, None)?;
        }
        let mut my_ini_local = Ini::new();
        let _config_map_rs = my_ini_local.load(my_local).unwrap();
        if let Some(keyspace) = my_ini_local.get(SECTION_MARIADB, "eloq_keyspace_name") {
            Ok(keyspace)
        } else {
            Ok("mono".to_string())
        }
    }

    pub fn get_redis_keyspace(&self) -> anyhow::Result<String> {
        let my_local = upload_dir()
            .join(&self.cluster_name)
            .join("redis_local.ini");
        if !my_local.exists() {
            self.gen_eloqkv_node_config(None, None)?;
        }
        let mut my_ini_local = Ini::new();
        let _config_map_rs = my_ini_local.load(my_local).unwrap();
        if let Some(keyspace) = my_ini_local.get(SECTION_STORE, "cass_keyspace") {
            Ok(keyspace)
        } else {
            Ok("mono_redis".to_string())
        }
    }

    pub fn get_keyspace(&self) -> Result<String> {
        match self.product() {
            Product::EloqSQL => self.get_monograph_keyspace(),
            Product::EloqKV => self.get_redis_keyspace(),
        }
    }

    fn build_log_config(&self) -> HashMap<String, String> {
        let log_srv = self
            .log_service
            .as_ref()
            .expect("log_service is not configured");
        let replica_num = log_srv.log_replica();
        let all_members = log_srv.group_member_as_vec();
        let group_member_map = log_srv.group_member_config(all_members.as_slice());
        let ordered_members = group_member_map
            .into_iter()
            .sorted_by_key(|(key, _val)| *key)
            .collect::<IndexMap<usize, String>>();
        let node_group = Vec::from_iter(ordered_members.values())
            .into_iter()
            .join(",");
        let key_list = match self.product() {
            Product::EloqSQL => "eloq_txlog_service_list".to_string(),
            Product::EloqKV => "txlog_service_list".to_string(),
        };
        let key_replica = match self.product() {
            Product::EloqSQL => "eloq_txlog_group_replica_num".to_string(),
            Product::EloqKV => "txlog_group_replica_num".to_string(),
        };
        HashMap::from([
            (key_replica, replica_num.to_string()),
            (key_list, node_group),
        ])
    }

    pub fn bootstrap_host(&self) -> String {
        let all_hosts = self.tx_service.merge_hosts();
        assert!(!all_hosts.is_empty());
        all_hosts.first().unwrap().to_string()
    }

    pub fn build_eloqsql_config(&self, set_ip_list: bool) -> anyhow::Result<Ini> {
        let mut my_ini = Ini::new();
        let storage = self.storage_service.as_ref();
        if let Some(cass) = storage.and_then(|s| s.cassandra.as_ref()) {
            my_ini
                .load(config_template(ELOQSQL_TEMPLATE_INI)?.as_path())
                .unwrap();
            let hosts = cass.host.join(",");
            my_ini.set(SECTION_MARIADB, "eloq_cass_hosts", Some(hosts));
            let port = cass.client_port()?;
            my_ini.set(SECTION_MARIADB, "eloq_cass_port", Some(port.to_string()));
            if let Some(conn) = cass.external() {
                if let Some(user) = conn.user.clone() {
                    my_ini.set(SECTION_MARIADB, "eloq_cass_user", Some(user));
                }
                if let Some(pwd) = conn.password.clone() {
                    my_ini.set(SECTION_MARIADB, "eloq_cass_password", Some(pwd));
                }
            }
        } else {
            my_ini
                .load(config_template(ELOQSQL_DYNAMO_TEMPLATE_INI)?.as_path())
                .unwrap();

            let dynamodb = storage
                .expect("storage_service is required")
                .dynamodb
                .as_ref()
                .unwrap();
            my_ini.set(
                SECTION_MARIADB,
                "eloq_aws_access_key_id",
                Some(dynamodb.clone().access_key_id),
            );
            my_ini.set(
                SECTION_MARIADB,
                "eloq_aws_secret_key",
                Some(dynamodb.clone().secret_key),
            );
            my_ini.set(
                SECTION_MARIADB,
                "eloq_dynamodb_region",
                Some(dynamodb.clone().region),
            );
            my_ini.set(
                SECTION_MARIADB,
                "eloq_dynamodb_endpoint",
                Some(dynamodb.clone().endpoint),
            );
        }
        // mysql_ini.set(
        //     CONFIG_MARIADB_SECTION,
        //     "eloq_keyspace_name",
        //     Some(self.cluster_name.clone()),
        // );

        let txsrv_home = self.tx_srv_home();
        my_ini.set(
            SECTION_MARIADB,
            "datadir",
            Some(format!("{txsrv_home}/datafarm")),
        );
        my_ini.set(
            SECTION_MARIADB,
            "lc_messages_dir",
            Some(format!("{txsrv_home}/share")),
        );
        my_ini.set(
            SECTION_MARIADB,
            "plugin_dir",
            Some(format!("{txsrv_home}/lib/plugin",)),
        );
        my_ini.set(
            SECTION_MARIADB,
            "port",
            Some(self.client_port().to_string()),
        );
        my_ini.set(
            SECTION_MARIADB,
            "socket",
            Some(format!("/tmp/eloqsql{}.sock", self.client_port())),
        );

        let tx_host_ports = &self.tx_service.tx_host_ports;
        if set_ip_list {
            let ip_list = tx_host_ports
                .iter()
                .map(|host_port| host_port.clone().to_string())
                .join(",");
            my_ini.set(SECTION_MARIADB, "eloq_ip_list", Some(ip_list));
        } else {
            my_ini.set(
                SECTION_MARIADB,
                "eloq_ip_list",
                Some(format!("{}:{}", "127.0.0.1", "8000")),
            );
        }

        let enable_metric = if let Some(monitor) = &self.monitor {
            monitor.monograph_metrics.is_some() || monitor.eloq_metrics.is_some()
        } else {
            false
        };
        my_ini.set(
            SECTION_MARIADB,
            "eloq_enable_metrics",
            Some(enable_metric.to_string()),
        );
        Ok(my_ini.clone())
    }

    pub fn gen_eloqsql_config(
        &self,
        host: Option<String>,
        port: Option<String>,
    ) -> anyhow::Result<PathBuf> {
        let is_host = host.is_some();
        let mut my_ini = self.build_eloqsql_config(is_host)?;
        let (host, port, cnf_path) = if let (Some(host), Some(port)) = (host, port) {
            (
                host.clone(),
                port.clone(),
                create_upload_cluster_dir(&host).join(ELOQSQL_TEMPLATE_INI),
            )
        } else {
            my_ini.set(
                SECTION_MARIADB,
                "eloq_local_ip",
                Some("127.0.0.1:8000".to_string()),
            );
            my_ini.set(SECTION_MARIADB, "thread_pool_size", Some("1".to_owned()));
            my_ini.set(SECTION_MARIADB, "eloq_core_num", Some("1".to_owned()));
            my_ini.set(
                SECTION_MARIADB,
                "eloq_node_memory_limit_mb",
                Some("512".to_owned()),
            );
            let cnf_path = upload_dir().join("my_local.cnf");
            my_ini.write(&cnf_path)?;
            return Ok(cnf_path);
        };

        if self.log_service.is_some() {
            self.build_log_config()
                .into_iter()
                .for_each(|(key, conf_val)| {
                    my_ini.set(SECTION_MARIADB, &key, Some(conf_val));
                });
        }
        my_ini.set(
            SECTION_MARIADB,
            "eloq_local_ip",
            Some(format!("{}:{}", host, port)),
        );
        let opt_hw = self.get_hardware(&host);
        if opt_hw.is_none() {
            warn!("hardware information for {host} is missing");
        }

        let union_cass = self
            .topology()
            .get(&host)
            .unwrap()
            .contains(&DeploymentPackage::Storage);

        let mut core = 1;
        if let Some(hw) = opt_hw {
            if self.tx_service.tx_host_ports.len() == 1 {
                core = core.max((hw.cpu * 3) / 4);
            } else {
                core = core.max((hw.cpu * 2) / 5);
            }
            if union_cass {
                core = (core + 1) / 2;
            }
        }
        let key = "thread_pool_size";
        let val = set_by_user!(my_ini.get(SECTION_MARIADB, key), u16);
        if val.is_none() {
            my_ini.set(SECTION_MARIADB, key, Some(core.to_string()));
        }
        let key = "eloq_core_num";
        let val = set_by_user!(my_ini.get(SECTION_MARIADB, key), u16);
        if val.is_none() {
            my_ini.set(SECTION_MARIADB, key, Some(core.to_string()));
        }

        let key = "eloq_node_memory_limit_mb";
        let val = set_by_user!(my_ini.get(SECTION_MARIADB, key), u32);
        if val.is_none() {
            let mut limit = opt_hw.map(|hw| (hw.memory * 3) / 5).unwrap_or(GB);
            if union_cass {
                limit /= 2;
            }
            assert!(limit > 0);
            my_ini.set(SECTION_MARIADB, key, Some(limit.to_string()));
        }
        my_ini.write(cnf_path.as_path())?;
        Ok(cnf_path)
    }

    pub fn build_eloqkv_config(&self, set_ip_list: bool, port: String) -> anyhow::Result<Ini> {
        let mut ini = Ini::new();
        // config template does not have port suffix
        ini.load(cluster_config_template(
            &self.cluster_name,
            ELOQKV_TEMPLATE_INI,
        )?)
        .unwrap();

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
                StorageProvider::Cassandra => {
                    let cass = storage.cassandra.as_ref().unwrap();
                    let cassandra_hosts = cass.host.join(",");
                    ini.set(SECTION_STORE, "cass_hosts", Some(cassandra_hosts));
                    let port = cass.client_port()?;
                    ini.set(SECTION_STORE, "cass_port", Some(port.to_string()));
                    if let Some(conn) = cass.external() {
                        if let Some(user) = conn.user.clone() {
                            ini.set(SECTION_STORE, "cass_user", Some(user));
                        }
                        if let Some(pwd) = conn.password.clone() {
                            ini.set(SECTION_STORE, "cass_password", Some(pwd));
                        }
                    }
                    let factor = cass.host.len().min(3).to_string();
                    ini.set(SECTION_STORE, "cass_keyspace_replication", Some(factor));
                }
                StorageProvider::Dynamodb => panic!("not supported"),
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
                        ini.set(SECTION_STORE, "aws_access_key_id", Some(s3.aws_id));
                        ini.set(SECTION_STORE, "aws_secret_key", Some(s3.aws_secret));
                        ini.set(
                            SECTION_STORE,
                            "kv_store_rocksdb_cloud_region",
                            Some(s3.region),
                        );
                        ini.set(
                            SECTION_STORE,
                            "kv_store_rocksdb_cloud_bucket_name",
                            Some(s3.bucket_name),
                        );
                        ini.set(
                            SECTION_STORE,
                            "kv_store_rocksdb_cloud_bucket_prefix",
                            Some(s3.bucket_prefix),
                        );
                        ini.set(
                            SECTION_STORE,
                            "kv_store_rocksdb_target_file_size_base",
                            Some(s3.target_file_size_base),
                        );
                        ini.set(
                            SECTION_STORE,
                            "kv_store_rocksdb_cloud_sst_file_cache_size",
                            Some(s3.sst_file_cache_size),
                        );
                    }
                    RocksDB::GCS(gcs) => {
                        ini.set(
                            SECTION_STORE,
                            "kv_store_rocksdb_cloud_region",
                            Some(gcs.region),
                        );
                        ini.set(
                            SECTION_STORE,
                            "kv_store_rocksdb_cloud_bucket_name",
                            Some(gcs.bucket_name),
                        );
                        ini.set(
                            SECTION_STORE,
                            "kv_store_rocksdb_cloud_bucket_prefix",
                            Some(gcs.bucket_prefix),
                        );
                        ini.set(
                            SECTION_STORE,
                            "kv_store_rocksdb_target_file_size_base",
                            Some(gcs.target_file_size_base),
                        );
                        ini.set(
                            SECTION_STORE,
                            "kv_store_rocksdb_cloud_sst_file_cache_size",
                            Some(gcs.sst_file_cache_size),
                        );
                    }
                },
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
                    panic!("standby_host_ports is empty, but it was expected to contain values.");
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
                    panic!("voter_host_ports is empty, but it was expected to contain values.");
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
        } else {
            ini.set(
                SECTION_CLUSTER,
                "ip_port_list",
                Some(format!("{}:{}", "127.0.0.1", "6379")),
            );
        }

        if self.is_empty(&ini, SECTION_METRIC, "enable_metrics") {
            ini.set(
                SECTION_METRIC,
                "enable_metrics",
                Some(
                    self.monitor
                        .as_ref()
                        .and_then(|monitor| {
                            if monitor.monograph_metrics.is_some() || monitor.eloq_metrics.is_some()
                            {
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

        Ok(ini.clone())
    }

    fn is_empty(&self, ini: &Ini, section: &str, key: &str) -> bool {
        ini.get(section, key)
            .map_or(false, |value| value == "${OVERRIDE}")
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

            if self.log_service.is_some() {
                self.build_log_config()
                    .into_iter()
                    .for_each(|(key, conf_val)| {
                        ini.set(SECTION_CLUSTER, &key, Some(conf_val));
                    });
            }

            ini.set(SECTION_LOCAL, "ip", Some(host_get.clone()));
            ini.set(SECTION_LOCAL, "port", Some(port_get.clone()));

            if let Some(hw) = self.get_hardware(&host_get) {
                let key = "core_number";
                let mut core_tx = 1; // minimal value
                if let Some(val) = set_by_user!(ini.get(SECTION_LOCAL, key), u16) {
                    core_tx = val;
                } else {
                    assert!(hw.cpu > 0);
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
                    let core_io = (core_tx + 7) / 8;
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
                    assert!(limit > 0);
                    ini.set(SECTION_LOCAL, key, Some(limit.to_string()));
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
        }

        // if let Some(parent) = cnf_path.parent() {
        //     if !parent.exists() {
        //         fs::create_dir_all(parent)?;
        //     }
        // }

        ini.write(cnf_path.as_path())?;
        Ok(cnf_path)
    }

    // generate proxy config file
    pub fn codis_proxy_config(&self) -> anyhow::Result<PathBuf> {
        let temp = fs::read_to_string(config_template(CODIS_PROXY_CNF)?)?;
        let mut cnf = toml::Table::from_str(&temp)?;
        cnf.insert(
            "product_name".to_owned(),
            toml::Value::String(self.cluster_name.clone()),
        );
        // calculate backend_primary_parallel
        let ncpu = &self
            .tx_service
            .tx_host_ports
            .iter()
            .map(|host_port| {
                let parts: Vec<&str> = host_port.split(':').collect();
                let host = parts[0];
                if let Some(hw) = self.get_hardware(host) {
                    hw.cpu
                } else {
                    4
                }
            })
            .max()
            .expect("tx-service hosts can't be empty");
        cnf.insert(
            "backend_primary_parallel".to_owned(),
            toml::Value::Integer(*ncpu as i64),
        );
        // write toml
        let path_proxy = upload_dir().join(CODIS_PROXY_CNF);
        fs::File::create(path_proxy.as_path())?.write_all(cnf.to_string().as_bytes())?;
        Ok(path_proxy)
    }

    // generate dashboard config file
    pub fn codis_dashboard_config(&self) -> anyhow::Result<PathBuf> {
        let temp = fs::read_to_string(config_template(CODIS_DASHBOARD_CNF)?)?;
        let mut cnf = toml::Table::from_str(&temp)?;
        cnf.insert(
            "product_name".to_owned(),
            toml::Value::String(self.cluster_name.clone()),
        );

        // Check if storage_service exists
        if let Some(storage) = self.storage_service.as_ref() {
            if let Some(coord) = storage.cassandra.as_ref() {
                let port = coord.client_port()?;
                let addr = coord.host.iter().map(|ip| format!("{ip}:{port}")).join(",");
                let keyspace = self.get_redis_keyspace()?;
                cnf.insert("coordinator_addr".to_owned(), toml::Value::String(addr));
                cnf.insert(
                    "coordinator_keyspace".to_owned(),
                    toml::Value::String(keyspace),
                );
                // write toml
                let path_dashb = upload_dir().join(CODIS_DASHBOARD_CNF);
                fs::File::create(path_dashb.as_path())?.write_all(cnf.to_string().as_bytes())?;
                return Ok(path_dashb);
            }
        }
        Err(anyhow!("Codis requires storage_service with cassandra"))
    }

    pub fn monograph_download_links(&self) -> anyhow::Result<HashMap<String, DownloadUrl>> {
        let mut links = HashMap::new();
        download_urls!(links,{MONOGRAPH_FILE_KEY, self.tx_image()});
        if let Some(img) = self.log_image() {
            download_urls!(links,{MONOGRAPH_LOG_FILE_KEY, img});
        }
        if let Some(storage) = self.storage_service.as_ref() {
            if let Some(cass) = storage.cassandra.as_ref() {
                if let Some(cassdp) = cass.internal() {
                    download_urls!(links,
                        {CASSANDRA_FILE_KEY, &cassdp.image_url()}
                    );
                }
            }
        }
        Ok(links)
    }

    pub fn all_download_links(&self) -> anyhow::Result<HashMap<String, DownloadUrl>> {
        let mut db_image_download_links = self.monograph_download_links()?;
        if let Some(monitor_srv) = self.monitor.as_ref() {
            db_image_download_links.extend(monitor_srv.download_links_as_map()?);
        }
        if self.codis.is_some() {
            download_urls!(db_image_download_links, {"codis", &Codis::download_url()});
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
            DeploymentPackage::Storage => {
                if let Some(storage) = &self.storage_service {
                    if let Some(cassandra) = &storage.cassandra {
                        if cassandra.internal().is_some() {
                            return cassandra.host.to_vec();
                        }
                    }
                }
                vec![]
            }
            DeploymentPackage::MonographLog => {
                if let Some(ref log_srv) = self.log_service {
                    log_srv.log_host_unique()
                } else {
                    vec![]
                }
            }
            DeploymentPackage::MonographTx => {
                self.get_host_list_internal(&Some(self.tx_service.tx_host_ports.clone()))
            }
            DeploymentPackage::MonographStandby => {
                self.get_host_list_internal(&self.tx_service.standby_host_ports)
            }
            DeploymentPackage::MonographVoter => {
                self.get_host_list_internal(&self.tx_service.voter_host_ports)
            }
            DeploymentPackage::Prometheus => {
                extract_monitor_host!(self, prometheus)
            }
            DeploymentPackage::Grafana => {
                extract_monitor_host!(self, grafana)
            }
            DeploymentPackage::Codis => {
                if let Some(codis) = &self.codis {
                    let mut hosts = codis.proxy.clone();
                    hosts.push(codis.dashboard.clone());
                    hosts
                } else {
                    vec![]
                }
            }
            DeploymentPackage::Proxy => unreachable!(),
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
            DeploymentPackage::MonographLog => vec![],
            DeploymentPackage::MonographTx => {
                self.get_host_port_list_internal(&Some(self.tx_service.tx_host_ports.clone()))
            }
            DeploymentPackage::MonographStandby => {
                self.get_host_port_list_internal(&self.tx_service.standby_host_ports.clone())
            }
            DeploymentPackage::MonographVoter => {
                self.get_host_port_list_internal(&self.tx_service.voter_host_ports)
            }
            DeploymentPackage::Prometheus => vec![],
            DeploymentPackage::Grafana => vec![],
            DeploymentPackage::Codis => vec![],
            DeploymentPackage::Proxy => unreachable!(),
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
        self.populate_topo(&mut topo, DeploymentPackage::MonographTx);
        self.populate_topo(&mut topo, DeploymentPackage::MonographStandby);
        self.populate_topo(&mut topo, DeploymentPackage::MonographVoter);
        self.populate_topo(&mut topo, DeploymentPackage::Storage);
        self.populate_topo(&mut topo, DeploymentPackage::Prometheus);
        self.populate_topo(&mut topo, DeploymentPackage::Grafana);
        self.populate_topo(&mut topo, DeploymentPackage::MonographLog);
        self.populate_topo(&mut topo, DeploymentPackage::Codis);
        topo
    }

    // key is cassandra node IP, value config files path
    pub fn gen_cassandra_config(
        &self,
        install_dir: String,
        cluster_name: String,
    ) -> anyhow::Result<HashMap<String, Vec<PathBuf>>> {
        let storage = self.storage_service.as_ref();
        if storage.and_then(|s| s.cassandra.as_ref()).is_none() {
            return Err(anyhow!(GenCassandraConfigErr("NotCassandra".to_string())));
        }
        let mut configs = vec![];
        if let Some(monitor) = &self.monitor {
            if monitor.cassandra_collector.is_some() {
                let storage = storage.expect("storage_service exists since we checked above");
                let p = storage.gen_cassandra_env(&cluster_name, install_dir)?;
                configs.push(p);
            }
        }
        let jvm_temp = fs::read_to_string(config_template(CASSANDRA_JVM_TEMPLATE)?)?;
        let tune_jvm = jvm_temp.contains(JVM_SETTING_HOLDER);
        let cass = storage
            .expect("storage_service exists since we checked above")
            .cassandra
            .as_ref()
            .unwrap();
        // cassandra.yaml config object
        let mut cass_conf_map = load_yaml_config_template(CASSANDRA_CONF_TEMPLATE)?;
        let storage_cluster = if let Some(cassdp) = cass.internal() {
            cassdp.cluster_name.clone().unwrap_or(cluster_name.clone())
        } else {
            unreachable!()
        };
        let nodes_topo = self.topology();

        cass_conf_map.insert("cluster_name".to_string(), Value::String(storage_cluster));
        let seeds = cass.host.iter().take(Cassandra::MAX_SEED).join(",");
        let seed_values = format!(
            r#"
               - class_name: org.apache.cassandra.locator.SimpleSeedProvider
                 parameters:
                 - seeds: {seeds}"#,
        );
        let seed_yaml_value: Value = serde_yaml::from_str(seed_values.as_str())?;
        cass_conf_map.insert(String::from("seed_provider"), seed_yaml_value);
        let cass_config_vec = cass
            .host
            .iter()
            .map(|host| {
                let host_value = Value::String(host.to_string());
                cass_conf_map.insert(String::from("listen_address"), host_value.clone());
                cass_conf_map.insert(
                    String::from("rpc_address"),
                    Value::String("0.0.0.0".to_string()),
                );
                cass_conf_map.insert(String::from("broadcast_rpc_address"), host_value.clone());
                cass_conf_map.insert(String::from("broadcast_address"), host_value);
                // Store cassandra.yaml inside .eloqctl/upload/${cluster_name}/${host}/cassandra.yaml
                let cluster_host_path = format!("{}/{}", cluster_name, host);
                let config_path =
                    create_upload_cluster_dir(&cluster_host_path).join("cassandra.yaml");
                let new_config_file = File::create(config_path.as_path()).unwrap();
                let gen_config_write = serde_yaml::to_writer(new_config_file, &cass_conf_map);
                assert!(gen_config_write.is_ok());
                let mut host_configs = configs.clone();
                host_configs.push(config_path);

                // Tune JVM for each cassandra node
                // https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/operations/opsTuneJVM.html
                let jvm_opt = if tune_jvm {
                    let gc_setting;
                    if let Some(hw) = self.get_hardware(host) {
                        let union = nodes_topo
                            .get(host)
                            .unwrap()
                            .contains(&DeploymentPackage::MonographTx);
                        let heap = if union { hw.memory / 4 } else { hw.memory / 2 }.min(64 * GB);
                        let h_xm = format!("-Xms{}M\n-Xmx{}M", heap, heap);
                        if heap < 8 * GB {
                            gc_setting = format!("{GC_SETTING_CMS}\n{h_xm}");
                        } else {
                            gc_setting = format!("{GC_SETTING_G1}\n{h_xm}");
                        }
                    } else {
                        warn!("cass node hardware information for {} is missing", host);
                        gc_setting = GC_SETTING_CMS.to_owned();
                    }
                    jvm_temp.clone().replace(JVM_SETTING_HOLDER, &gc_setting)
                } else {
                    jvm_temp.clone()
                };
                // Store jvm11-server.options inside .eloqctl/upload/${cluster_name}/${host}/jvm11-server.options
                let opt_path =
                    create_upload_cluster_dir(&cluster_host_path).join(CASSANDRA_JVM_OPTION);
                File::create(opt_path.as_path())
                    .unwrap()
                    .write_all(jvm_opt.as_bytes())
                    .unwrap();
                host_configs.push(opt_path);
                (host.to_string(), host_configs)
            })
            .collect::<HashMap<String, Vec<PathBuf>>>();

        Ok(cass_config_vec)
    }

    pub fn srv_start_cmd(&self, port: &str, server_type: ServerType) -> String {
        if server_type == ServerType::Node {
            unreachable!()
        }
        let ini_file = self.tx_srv_ini(port);
        let tx_dir = self.tx_srv_home();
        let tx_bin = self.tx_srv_bin();
        let logs_dir = self.node_srv_logs(port);

        let mut txlog_flag = String::new();
        if self.log_service.is_some() {
            let txlog_service_list = self
                .log_service
                .as_ref()
                .unwrap()
                .nodes
                .iter()
                .map(|node| {
                    format!(
                        "{host_str}:{port_str}",
                        host_str = node.host,
                        port_str = node.port
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            let txlog_group_replica_num = txlog_service_list.len();

            txlog_flag = format!(
                "--txlog_service_list={} --txlog_group_replica_num={}",
                txlog_service_list, txlog_group_replica_num
            );
        }

        let glog = format!(
            "mkdir -p {logs_dir} ; export GLOG_log_dir={logs_dir} ; export GLOG_max_log_size=1024"
        );
        let mut ld_lib = if let Some(Version::Debug) = self.version() {
            export_asan(&format!("{logs_dir}/asan"))
        } else {
            format!("export LD_PRELOAD={tx_dir}/lib/libmimalloc.so.2")
        };
        ld_lib.push_str(&format!(
            "; export LD_LIBRARY_PATH={tx_dir}/lib:$LD_LIBRARY_PATH"
        ));

        // Get the current datetime
        let now = Local::now();
        // Format the datetime as "YYYYMMDD-HHMMSS.microseconds"
        let datetime = now.format("%Y%m%d-%H%M%S.%6f").to_string();

        match self.product() {
            Product::EloqSQL => {
                let mut logout = "/dev/null".to_owned();
                if let Some(Version::Tag(nums)) = self.version() {
                    if nums <= version_digits("0.4.2").unwrap() {
                        logout = format!("{tx_dir}/logs/eloqsql.log")
                    }
                }
                format!(
                    "cd {tx_dir}; {glog}; {ld_lib} ; {tx_bin} --defaults-file={ini_file} > {logout} 2>&1 &"
                )
            }
            Product::EloqKV => {
                format!(
                    "cd {tx_dir}; mkdir -p logs/std-output; {glog}; {ld_lib} ; {tx_bin} --config={ini_file} {txlog_flag} --graceful_quit_on_sigterm=true > logs/std-output/std-out-{port}-{datetime} 2>&1 & cd logs/std-output ; ln -sf std-out-{port}-{datetime} std-out-{port} "
                )
            }
        }
    }

    // only used in `start --nodes`
    pub fn srv_start_cmd_with_host(
        &self,
        port: &str,
        server_type: ServerType,
        host: &str,
    ) -> String {
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

        let mut txlog_flag = String::new();
        if self.log_service.is_some() {
            let txlog_service_list = self
                .log_service
                .as_ref()
                .unwrap()
                .nodes
                .iter()
                .map(|node| {
                    format!(
                        "{host_str}:{port_str}",
                        host_str = node.host,
                        port_str = node.port
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            let txlog_group_replica_num = txlog_service_list.len();

            txlog_flag = format!(
                "--txlog_service_list={} --txlog_group_replica_num={}",
                txlog_service_list, txlog_group_replica_num
            );
        }

        let glog = format!(
            "mkdir -p {logs_dir} ; export GLOG_log_dir={logs_dir} ; export GLOG_max_log_size=1024"
        );
        let mut ld_lib = if let Some(Version::Debug) = self.version() {
            export_asan(&format!("{logs_dir}/asan"))
        } else {
            format!("export LD_PRELOAD={tx_dir}/lib/libmimalloc.so.2")
        };
        ld_lib.push_str(&format!(
            "; export LD_LIBRARY_PATH={tx_dir}/lib:$LD_LIBRARY_PATH"
        ));

        // Get the current datetime
        let now = Local::now();
        // Format the datetime as "YYYYMMDD-HHMMSS.microseconds"
        let datetime = now.format("%Y%m%d-%H%M%S.%6f").to_string();

        match self.product() {
            Product::EloqSQL => {
                let mut logout = "/dev/null".to_owned();
                if let Some(Version::Tag(nums)) = self.version() {
                    if nums <= version_digits("0.4.2").unwrap() {
                        logout = format!("{tx_dir}/logs/eloqsql.log")
                    }
                }
                format!(
                    "cd {tx_dir}; {glog}; {ld_lib} ; {tx_bin} --defaults-file={ini_file} > {logout} 2>&1 &"
                )
            }
            Product::EloqKV => {
                format!(
                    "cd {tx_dir}; mkdir -p logs/std-output; {glog}; {ld_lib} ; {tx_bin} --config={ini_file} {txlog_flag} --graceful_quit_on_sigterm=true > logs/std-output/std-out-{port}-{datetime} 2>&1 & cd logs/std-output ; ln -sf std-out-{port}-{datetime} std-out-{port} "
                )
            }
        }
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
    use indexmap::IndexMap;
    use itertools::Itertools;

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
}
