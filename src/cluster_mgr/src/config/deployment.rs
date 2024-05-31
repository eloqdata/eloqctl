use crate::cli::{upload_dir, upload_host_dir};
use crate::config::config_base::CASSANDRA_FILE_KEY;
use crate::config::config_base::{
    MONOGRAPH_FILE_KEY, MONOGRAPH_LOG_FILE_KEY, MONOGRAPH_TX_SERVICE_DIR,
};
use crate::config::log_service::LogService;
use crate::config::monitor::Monitor;
use crate::config::storage_service_config::{Cassandra, RocksDB, StorageService};
use crate::config::ConfigErr::GenCassandraConfigErr;
use crate::config::{
    config_template, load_yaml_config_template, DeploymentPackage, DownloadUrl, StorageProvider,
    CASSANDRA_CONF_TEMPLATE, CASSANDRA_JVM_OPTION, CASSANDRA_JVM_TEMPLATE, CODIS_DASHBOARD_CNF,
    CODIS_PROXY_CNF, CONFIG_MARIADB_SECTION, DOWNLOAD_SRC, JVM_SETTING_HOLDER,
    MONOGRAPH_CONF_DYNAMO_TEMPLATE, MONOGRAPH_CONF_TEMPLATE, REDIS_CONF_TEMPLATE, SECTION_CLUSTER,
    SECTION_LOCAL, SECTION_METRIC, SECTION_STORE, SET_FOR_ME,
};
use anyhow::{anyhow, bail, Result};
use configparser::ini::Ini;
use core::panic;
use indexmap::IndexMap;
use itertools::Itertools;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;
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

#[macro_export]
macro_rules! download_urls {
    ($download_link:expr, $({$url_key:expr, $url_value:expr} $(,)?)*) => {
        $(
          $download_link.insert($url_key.to_string(), DownloadUrl::from_url_str($url_value.as_str()).unwrap());
        )*
    };
}

macro_rules! set_by_user {
    ($opt_val:expr, $T:ty) => {
        if let Some(v) = $opt_val {
            if v == SET_FOR_ME {
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
    ($deployment_ref:expr, $monitor_components:ident) => {{
        if let Some(monitor) = $deployment_ref.monitor.as_ref() {
            vec![monitor.$monitor_components.host.clone()]
        } else {
            vec![]
        }
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
    pub host: Vec<String>,
    pub port: Option<u16>,
    pub client_port: u16,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, clap::ValueEnum, Display)]
pub enum Product {
    #[serde(alias = "eloqsql", alias = "eloq-sql", alias = "SQL")]
    EloqSQL,
    #[serde(alias = "eloqkv", alias = "eloq-kv", alias = "KV")]
    EloqKV,
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
        format!("{}/codis/codis.tar.gz", DOWNLOAD_SRC.as_str())
    }
    pub fn dir(install_dir: &str) -> String {
        format!("{install_dir}/codis")
    }
    pub fn dashboard_cfg(config: &Deployment) -> anyhow::Result<String> {
        let coord = config
            .storage_service
            .cassandra
            .as_ref()
            .expect("codis only support cassandra coordinator");
        let port = coord.client_port()?;
        let addr = coord.host.iter().map(|ip| format!("{ip}:{port}")).join(",");
        let keyspace = config.get_redis_keyspace()?;
        let cmds = format!(
            "sed -i 's/coordinator_addr.*/coordinator_addr = \"{addr}\"/g ; s/coordinator_keyspace.*/coordinator_keyspace = \"{}\"/g' {}/dashboard.toml",
            keyspace, Self::dir(&config.install_dir())
        );
        Ok(cmds)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Deployment {
    pub product: Option<Product>,
    pub version: Option<String>,
    pub tx_image: Option<String>,
    pub log_image: Option<String>,
    pub cluster_name: String,
    pub install_dir: String,
    pub tx_service: MonographService,
    pub log_service: Option<LogService>,
    pub storage_service: StorageService,
    pub monitor: Option<Monitor>,
    pub codis: Option<Codis>,
    pub hardware: Option<HashMap<String, Hardware>>,
}

impl Deployment {
    // Populate tx_image and log_image according to version number
    pub fn set_image(&mut self) -> Result<()> {
        if self.version.is_none() {
            self.version = Some("latest".to_owned());
        }
        self.version.as_mut().unwrap().make_ascii_lowercase();
        let ver = self.version.as_ref().unwrap();
        if ver != "latest" && ver != "nightly" && ver != "debug" {
            let re = Regex::new(r"(0|[1-9][0-9]?)\.(0|[1-9][0-9]?)\.(0|[1-9][0-9]?)").unwrap();
            if !re.is_match(ver) {
                bail!("Invalid version {}", ver);
            }
        }

        let mut prefix = PathBuf::from(DOWNLOAD_SRC.as_str());
        let os_name = sysinfo::System::distribution_id();
        let os_version = sysinfo::System::os_version().unwrap().replace('.', "");
        let os_pretty = format!("{os_name}{os_version}");
        let arch = sysinfo::System::cpu_arch().unwrap();
        let arch = match arch.as_str() {
            "aarch64" | "arm64" => "arm64",
            "x86" | "x86_64" | "amd64" => "amd64",
            _ => bail!("unsupported cpu arch {arch}"),
        };
        let store = self.storage_service.pretty_name();
        let version = self.version.as_ref().unwrap();
        let log_tarball = format!("log-service-{version}-{arch}.tar.gz");
        let tx_tarball;
        match self.product() {
            Product::EloqSQL => {
                prefix.push("eloqsql");
                tx_tarball = format!("eloqsql-{version}-{arch}.tar.gz");
            }
            Product::EloqKV => {
                prefix.push("eloqkv");
                tx_tarball = format!("eloqkv-{version}-{arch}.tar.gz");
            }
        }
        prefix.push(os_pretty);
        let prefix = prefix.as_path().to_str().unwrap();
        if self.tx_image.is_none() {
            self.tx_image = Some(format!("{prefix}/{store}/{tx_tarball}"));
        }
        if self.log_image.is_none() && self.log_service.is_some() {
            self.log_image = Some(format!("{prefix}/logservice/{log_tarball}"));
        }
        Ok(())
    }

    pub fn get_tx_image(&self) -> String {
        self.tx_image.clone().unwrap()
    }

    pub fn get_hardware(&self, host: &str) -> Option<&Hardware> {
        if let Some(all_hw) = self.hardware.as_ref() {
            all_hw.get(host)
        } else {
            None
        }
    }

    pub fn product(&self) -> Product {
        if let Some(p) = self.product.clone() {
            p
        } else {
            Product::EloqSQL
        }
    }

    pub fn client_port(&self) -> u16 {
        self.tx_service.client_port
    }

    pub fn install_dir(&self) -> String {
        format!("{}/{}", &self.install_dir, self.cluster_name)
    }

    pub fn get_monograph_keyspace(&self) -> anyhow::Result<String> {
        let my_local = upload_dir().join("my_local.cnf");
        if !my_local.exists() {
            self.gen_eloqsql_config_by_host(None, self.install_dir())?;
        }
        let mut my_ini_local = Ini::new();
        let _config_map_rs = my_ini_local.load(my_local).unwrap();
        if let Some(keyspace) = my_ini_local.get(CONFIG_MARIADB_SECTION, "monograph_keyspace_name")
        {
            Ok(keyspace)
        } else {
            Ok("mono".to_string())
        }
    }

    pub fn get_redis_keyspace(&self) -> anyhow::Result<String> {
        let my_local = upload_dir().join("redis_local.ini");
        if !my_local.exists() {
            self.gen_eloqkv_config_by_host(None)?;
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
            Product::EloqSQL => "monograph_txlog_service_list".to_string(),
            Product::EloqKV => "txlog_service_list".to_string(),
        };
        let key_replica = match self.product() {
            Product::EloqSQL => "monograph_txlog_group_replica_num".to_string(),
            Product::EloqKV => "txlog_group_replica_num".to_string(),
        };
        HashMap::from([
            (key_replica, replica_num.to_string()),
            (key_list, node_group),
        ])
    }

    pub fn bootstrap_host(&self) -> String {
        let mut all_hosts = self.tx_service.host.clone();
        assert!(!all_hosts.is_empty());
        all_hosts.sort();
        all_hosts.first().unwrap().to_string()
    }

    pub fn build_eloqsql_config(
        &self,
        set_ip_list: bool,
        install_dir: String,
    ) -> anyhow::Result<Ini> {
        let mut my_ini = Ini::new();
        if let Some(cass) = self.storage_service.cassandra.as_ref() {
            my_ini
                .load(config_template(MONOGRAPH_CONF_TEMPLATE)?.as_path())
                .unwrap();
            let hosts = cass.host.join(",");
            my_ini.set(CONFIG_MARIADB_SECTION, "monograph_cass_hosts", Some(hosts));
            let port = cass.client_port()?;
            my_ini.set(
                CONFIG_MARIADB_SECTION,
                "monograph_cass_port",
                Some(port.to_string()),
            );
            if let Some(conn) = cass.external() {
                if let Some(user) = conn.user.clone() {
                    my_ini.set(CONFIG_MARIADB_SECTION, "monograph_cass_user", Some(user));
                }
                if let Some(pwd) = conn.password.clone() {
                    my_ini.set(CONFIG_MARIADB_SECTION, "monograph_cass_password", Some(pwd));
                }
            }
        } else {
            my_ini
                .load(config_template(MONOGRAPH_CONF_DYNAMO_TEMPLATE)?.as_path())
                .unwrap();

            let dynamodb = self.storage_service.dynamodb.as_ref().unwrap();
            my_ini.set(
                CONFIG_MARIADB_SECTION,
                "monograph_aws_access_key_id",
                Some(dynamodb.clone().access_key_id),
            );
            my_ini.set(
                CONFIG_MARIADB_SECTION,
                "monograph_aws_secret_key",
                Some(dynamodb.clone().secret_key),
            );
            my_ini.set(
                CONFIG_MARIADB_SECTION,
                "monograph_dynamodb_region",
                Some(dynamodb.clone().region),
            );
            my_ini.set(
                CONFIG_MARIADB_SECTION,
                "monograph_dynamodb_endpoint",
                Some(dynamodb.clone().endpoint),
            );
        }
        // mysql_ini.set(
        //     CONFIG_MARIADB_SECTION,
        //     "monograph_keyspace_name",
        //     Some(self.cluster_name.clone()),
        // );

        my_ini.set(
            CONFIG_MARIADB_SECTION,
            "datadir",
            Some(format!("{install_dir}/datafarm")),
        );
        my_ini.set(
            CONFIG_MARIADB_SECTION,
            "lc_messages_dir",
            Some(format!(
                "{install_dir}/{MONOGRAPH_TX_SERVICE_DIR}/install/share"
            )),
        );
        my_ini.set(
            CONFIG_MARIADB_SECTION,
            "plugin_dir",
            Some(format!(
                "{install_dir}/{MONOGRAPH_TX_SERVICE_DIR}/install/lib/plugin",
            )),
        );
        my_ini.set(
            CONFIG_MARIADB_SECTION,
            "port",
            Some(self.client_port().to_string()),
        );

        my_ini.set(
            CONFIG_MARIADB_SECTION,
            "socket",
            Some(format!("/tmp/mysql{}.sock", self.client_port())),
        );

        let use_port = self.tx_service.port.unwrap();
        let monograph_hosts = &self.tx_service.host;
        if set_ip_list {
            let ip_list = monograph_hosts
                .iter()
                .map(|host| format!("{}:{}", host.clone(), use_port))
                .join(",");
            my_ini.set(CONFIG_MARIADB_SECTION, "monograph_ip_list", Some(ip_list));
        } else {
            my_ini.set(
                CONFIG_MARIADB_SECTION,
                "monograph_ip_list",
                Some(format!("{}:{}", "127.0.0.1", use_port)),
            );
        }

        let enable_metric = if let Some(monitor) = &self.monitor {
            monitor.monograph_metrics.is_some()
        } else {
            false
        };
        my_ini.set(
            CONFIG_MARIADB_SECTION,
            "monograph_enable_metrics",
            Some(enable_metric.to_string()),
        );
        Ok(my_ini.clone())
    }

    pub fn gen_eloqsql_config_by_host(
        &self,
        tx_host: Option<String>,
        install_dir: String,
    ) -> anyhow::Result<PathBuf> {
        let port = self.tx_service.port.unwrap();
        let is_host = tx_host.is_some();
        let mut my_ini = self.build_eloqsql_config(is_host, install_dir)?;
        let (host, cnf_path) = if let Some(host) = tx_host {
            (host.clone(), upload_host_dir(&host).join("my.cnf"))
        } else {
            my_ini.set(
                CONFIG_MARIADB_SECTION,
                "monograph_local_ip",
                Some(format!("127.0.0.1:{port}")),
            );
            my_ini.set(
                CONFIG_MARIADB_SECTION,
                "thread_pool_size",
                Some("1".to_owned()),
            );
            my_ini.set(
                CONFIG_MARIADB_SECTION,
                "monograph_core_num",
                Some("1".to_owned()),
            );
            my_ini.set(
                CONFIG_MARIADB_SECTION,
                "monograph_node_memory_limit_mb",
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
                    my_ini.set(CONFIG_MARIADB_SECTION, &key, Some(conf_val));
                });
        }
        my_ini.set(
            CONFIG_MARIADB_SECTION,
            "monograph_local_ip",
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
            if union_cass {
                core = core.max((hw.cpu * 3) / 8);
            } else {
                core = core.max((hw.cpu * 3) / 4);
            }
        }
        let key = "thread_pool_size";
        let val = set_by_user!(my_ini.get(CONFIG_MARIADB_SECTION, key), u16);
        if val.is_none() {
            my_ini.set(CONFIG_MARIADB_SECTION, key, Some(core.to_string()));
        }
        let key = "monograph_core_num";
        let val = set_by_user!(my_ini.get(CONFIG_MARIADB_SECTION, key), u16);
        if val.is_none() {
            my_ini.set(CONFIG_MARIADB_SECTION, key, Some(core.to_string()));
        }

        let key = "monograph_node_memory_limit_mb";
        let val = set_by_user!(my_ini.get(CONFIG_MARIADB_SECTION, key), u32);
        if val.is_none() {
            let mut limit = opt_hw.map(|hw| (hw.memory * 3) / 5).unwrap_or(1 * GB);
            if union_cass {
                limit /= 2;
            }
            assert!(limit > 0);
            my_ini.set(CONFIG_MARIADB_SECTION, key, Some(limit.to_string()));
        }
        my_ini.write(cnf_path.as_path())?;
        Ok(cnf_path)
    }

    pub fn build_eloqkv_config(&self, set_ip_list: bool) -> anyhow::Result<Ini> {
        let mut redis_ini = Ini::new();
        redis_ini
            .load(config_template(REDIS_CONF_TEMPLATE)?)
            .unwrap();
        match self.storage_service.provider().unwrap() {
            StorageProvider::Cassandra => {
                let cass = self.storage_service.cassandra.as_ref().unwrap();
                let cassandra_hosts = cass.host.join(",");
                redis_ini.set(SECTION_STORE, "cass_hosts", Some(cassandra_hosts));
                let port = cass.client_port()?;
                redis_ini.set(SECTION_STORE, "cass_port", Some(port.to_string()));
                if let Some(conn) = cass.external() {
                    if let Some(user) = conn.user.clone() {
                        redis_ini.set(SECTION_STORE, "cass_user", Some(user));
                    }
                    if let Some(pwd) = conn.password.clone() {
                        redis_ini.set(SECTION_STORE, "cass_password", Some(pwd));
                    }
                }
            }
            StorageProvider::Dynamo => panic!("not supported"),
            StorageProvider::Rocks => match self.storage_service.rocksdb.clone().unwrap() {
                RocksDB::Local => {}
                RocksDB::S3(s3) => {
                    redis_ini.set(SECTION_STORE, "aws_access_key_id", Some(s3.aws_id));
                    redis_ini.set(SECTION_STORE, "aws_secret_key", Some(s3.aws_secret));
                    redis_ini.set(
                        SECTION_STORE,
                        "kv_store_rocksdb_cloud_region",
                        Some(s3.region),
                    );
                    redis_ini.set(
                        SECTION_STORE,
                        "kv_store_rocksdb_cloud_bucket_name",
                        Some(s3.bucket_name),
                    );
                    redis_ini.set(
                        SECTION_STORE,
                        "kv_store_rocksdb_cloud_bucket_prefix",
                        Some(s3.bucket_prefix),
                    );
                    redis_ini.set(
                        SECTION_STORE,
                        "kv_store_rocksdb_target_file_size_base",
                        Some(s3.target_file_size_base),
                    );
                    redis_ini.set(
                        SECTION_STORE,
                        "kv_store_rocksdb_cloud_sst_file_cache_size",
                        Some(s3.sst_file_cache_size),
                    );
                }
                RocksDB::GCS(gcs) => {
                    redis_ini.set(
                        SECTION_STORE,
                        "kv_store_rocksdb_cloud_region",
                        Some(gcs.region),
                    );
                    redis_ini.set(
                        SECTION_STORE,
                        "kv_store_rocksdb_cloud_bucket_name",
                        Some(gcs.bucket_name),
                    );
                    redis_ini.set(
                        SECTION_STORE,
                        "kv_store_rocksdb_cloud_bucket_prefix",
                        Some(gcs.bucket_prefix),
                    );
                    redis_ini.set(
                        SECTION_STORE,
                        "kv_store_rocksdb_target_file_size_base",
                        Some(gcs.target_file_size_base),
                    );
                    redis_ini.set(
                        SECTION_STORE,
                        "kv_store_rocksdb_cloud_sst_file_cache_size",
                        Some(gcs.sst_file_cache_size),
                    );
                }
            },
        }
        let use_port = self.client_port();
        let monograph_hosts = &self.tx_service.host;
        if set_ip_list {
            let ip_list = monograph_hosts
                .iter()
                .map(|host| format!("{}:{}", host.clone(), use_port))
                .join(",");
            redis_ini.set(SECTION_CLUSTER, "ip_port_list", Some(ip_list));
        } else {
            redis_ini.set(
                SECTION_CLUSTER,
                "ip_port_list",
                Some(format!("{}:{}", "127.0.0.1", use_port)),
            );
        }

        let enable_metric = if let Some(monitor) = &self.monitor {
            monitor.monograph_metrics.is_some()
        } else {
            false
        };
        redis_ini.set(
            SECTION_METRIC,
            "enable_metrics",
            Some(enable_metric.to_string()),
        );
        Ok(redis_ini.clone())
    }

    pub fn gen_eloqkv_config_by_host(&self, tx_host: Option<String>) -> anyhow::Result<PathBuf> {
        let is_host = tx_host.is_some();
        let mut ini = self.build_eloqkv_config(is_host)?;
        let (host, cnf_path) = if let Some(host) = tx_host {
            (host.clone(), upload_host_dir(&host).join("redis.ini"))
        } else {
            ini.set(SECTION_LOCAL, "ip", Some("127.0.0.1".to_owned()));
            ini.set(SECTION_LOCAL, "port", Some(self.client_port().to_string()));
            ini.set(SECTION_LOCAL, "core_number", Some("1".to_owned()));
            ini.set(SECTION_LOCAL, "event_dispatcher_num", Some("1".to_owned()));
            ini.set(
                SECTION_LOCAL,
                "node_memory_limit_mb",
                Some("512".to_owned()),
            );
            let cnf_path = upload_dir().join("redis_local.ini");
            ini.write(cnf_path.as_path())?;
            return Ok(cnf_path);
        };
        if self.log_service.is_some() {
            self.build_log_config()
                .into_iter()
                .for_each(|(key, conf_val)| {
                    ini.set(SECTION_CLUSTER, &key, Some(conf_val));
                });
        }
        ini.set(SECTION_LOCAL, "ip", Some(host.clone()));
        ini.set(SECTION_LOCAL, "port", Some(self.client_port().to_string()));

        let opt_hw = self.get_hardware(&host);
        if opt_hw.is_none() {
            warn!("hardware information for {host} is missing");
        }
        let key = "core_number";
        let mut core_tx = 1; // minimal value
        if let Some(val) = set_by_user!(ini.get(SECTION_LOCAL, key), u16) {
            core_tx = val;
        } else {
            if let Some(hw) = opt_hw {
                assert!(hw.cpu > 0);
                core_tx = core_tx.max((hw.cpu * 4) / 5);
            }
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
            .get(&host)
            .unwrap()
            .contains(&DeploymentPackage::Storage);
        let key = "node_memory_limit_mb";
        let val = set_by_user!(ini.get(SECTION_LOCAL, key), u32);
        if val.is_none() {
            let mut limit = opt_hw.map(|hw| (hw.memory * 4) / 5).unwrap_or(1 * GB);
            if union_cass {
                limit /= 2;
            }
            assert!(limit > 0);
            ini.set(SECTION_LOCAL, key, Some(limit.to_string()));
        }

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
            .host
            .iter()
            .map(|host| {
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
        let coord = self
            .storage_service
            .cassandra
            .as_ref()
            .expect("codis only support cassandra coordinator");
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
        Ok(path_dashb)
    }

    pub fn monograph_download_links(&self) -> anyhow::Result<HashMap<String, DownloadUrl>> {
        let mut links = HashMap::new();
        download_urls!(links,
                {MONOGRAPH_FILE_KEY, self.get_tx_image()}
        );
        if let Some(log_image_url) = self.log_image.as_ref() {
            download_urls!(links,
                {MONOGRAPH_LOG_FILE_KEY, log_image_url.to_string()}
            );
        }
        if let Some(cass) = self.storage_service.cassandra.as_ref() {
            if let Some(cassdp) = cass.internal() {
                download_urls!(links,
                    {CASSANDRA_FILE_KEY, cassdp.download_url}
                );
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
            download_urls!(db_image_download_links, {"codis", Codis::download_url()});
        }
        Ok(db_image_download_links)
    }

    pub fn get_host_list(&self, service: DeploymentPackage) -> Vec<String> {
        match service {
            DeploymentPackage::Storage => {
                if let Some(cassandra) = &self.storage_service.cassandra {
                    if cassandra.internal().is_some() {
                        return cassandra.host.to_vec();
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
            DeploymentPackage::MonographTx => self.tx_service.host.to_vec(),
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
        if self.storage_service.cassandra.is_none() {
            return Err(anyhow!(GenCassandraConfigErr("Dynamodb".to_string())));
        }
        let has_cassandra_monitor = self
            .storage_service
            .gen_cassandra_env(install_dir, self.monitor.as_ref())?;
        let cass_env_sh = if has_cassandra_monitor {
            Some(upload_dir().join("cassandra-env.sh"))
        } else {
            None
        };
        let jvm_temp = fs::read_to_string(config_template(CASSANDRA_JVM_TEMPLATE)?)?;
        let tune_jvm = jvm_temp.contains(JVM_SETTING_HOLDER);
        let cass = self.storage_service.cassandra.as_ref().unwrap();
        // cassandra.yaml config object
        let mut cass_conf_map = load_yaml_config_template(CASSANDRA_CONF_TEMPLATE)?;
        let storage_cluster = if let Some(cassdp) = cass.internal() {
            cassdp.cluster_name.clone().unwrap_or(cluster_name)
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
                let config_path = upload_host_dir(host).join("cassandra.yaml");
                let new_config_file = File::create(config_path.as_path()).unwrap();
                let gen_config_write = serde_yaml::to_writer(new_config_file, &cass_conf_map);
                assert!(gen_config_write.is_ok());
                let mut config_path_vec = vec![config_path];
                if let Some(env_sh) = &cass_env_sh {
                    config_path_vec.push(env_sh.clone());
                }

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
                let opt_path = upload_host_dir(host).join(CASSANDRA_JVM_OPTION);
                File::create(opt_path.as_path())
                    .unwrap()
                    .write_all(jvm_opt.as_bytes())
                    .unwrap();
                config_path_vec.push(opt_path);
                (host.to_string(), config_path_vec)
            })
            .collect::<HashMap<String, Vec<PathBuf>>>();

        Ok(cass_config_vec)
    }
}

#[cfg(test)]
mod tests {
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
}
