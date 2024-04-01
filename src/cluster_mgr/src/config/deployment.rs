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
    config_template, get_cassandra_port, load_yaml_config_template, DownloadUrl, StorageProvider,
    CASSANDRA_CONF_TEMPLATE, CASSANDRA_JVM_OPTION, CASSANDRA_JVM_TEMPLATE, CODIS_DASHBOARD_CNF,
    CODIS_PROXY_CNF, CONFIG_MARIADB_SECTION, CONFIG_SECTION_CLUSTER, CONFIG_SECTION_LOCAL,
    CONFIG_SECTION_STORE, JVM_SETTING_HOLDER, MONOGRAPH_CONF_DYNAMO_TEMPLATE,
    MONOGRAPH_CONF_TEMPLATE, REDIS_CONF_TEMPLATE, RESOURCE_REPO,
};
use anyhow::anyhow;
use configparser::ini::Ini;
use core::panic;
use indexmap::IndexMap;
use itertools::Itertools;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::cmp;
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
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, clap::ValueEnum, Display)]
pub enum Product {
    #[serde(alias = "eloqsql", alias = "eloq-sql", alias = "SQL", alias = "sql")]
    EloqSQL,
    #[serde(alias = "eloqkv", alias = "eloq-kv", alias = "KV", alias = "kv")]
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
        format!("{}/codis/codis.tar.gz", RESOURCE_REPO)
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
        let port = get_cassandra_port()?;
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
    pub port: Port,
    pub tx_service: MonographService,
    pub log_service: Option<LogService>,
    pub storage_service: StorageService,
    pub monitor: Option<Monitor>,
    pub codis: Option<Codis>,
    pub hardware: Option<HashMap<String, Hardware>>,
}

impl Deployment {
    // Populate tx_image and log_image according to version number
    pub fn image_by_version(&mut self) {
        if self.version.is_none() || self.version.as_ref().unwrap().to_lowercase() == "latest" {
            self.version = Some("latest".to_owned());
        } else if self.version.as_ref().unwrap().to_lowercase() == "nightly" {
            self.version = Some("nightly".to_owned());
        } else {
            let re = Regex::new(r"(0|[1-9][0-9]?)\.(0|[1-9][0-9]?)\.(0|[1-9][0-9]?)").unwrap();
            if !re.is_match(self.version.as_ref().unwrap()) {
                panic!("Invalid version {}", self.version.as_ref().unwrap());
            }
        }
        let mut prefix = PathBuf::from(RESOURCE_REPO);
        let os_name = sysinfo::System::distribution_id();
        let os_version = sysinfo::System::os_version().unwrap().replace('.', "");
        let os_pretty = format!("{os_name}{os_version}");
        let arch = match sysinfo::System::cpu_arch().unwrap().as_str() {
            "aarch64" | "arm64" => "arm64",
            "x86" | "x86_64" | "amd64" => "amd64",
            _ => unreachable!(),
        };
        let store = self.storage_service.pretty_name();
        let version = self.version.as_ref().unwrap();
        match self.product() {
            Product::EloqSQL => {
                prefix.push("eloqsql");
                prefix.push(os_pretty);
                prefix.push(store);
                prefix.push(version);
                let prefix = prefix.as_path().to_str().unwrap();
                if self.tx_image.is_none() {
                    self.tx_image = Some(format!("{prefix}/eloqsql-{arch}.tar.gz"));
                }
                if self.log_image.is_none() && self.log_service.is_some() {
                    self.log_image = Some(format!("{prefix}/log-service-{arch}.tar.gz"));
                }
            }
            Product::EloqKV => {
                prefix.push("eloqkv");
                prefix.push(os_pretty);
                prefix.push(store);
                prefix.push(version);
                let prefix = prefix.as_path().to_str().unwrap();
                if self.tx_image.is_none() {
                    self.tx_image = Some(format!("{prefix}/eloqkv-{arch}.tar.gz"));
                }
                if self.log_image.is_none() && self.log_service.is_some() {
                    self.log_image = Some(format!("{prefix}/log-service-{arch}.tar.gz"));
                }
            }
        }
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

    pub fn cs_conn_port(&self) -> u16 {
        if let Some(p) = self.port.cs_conn {
            p
        } else {
            match self.product() {
                Product::EloqSQL => 3306,
                Product::EloqKV => 6379,
            }
        }
    }

    pub fn install_dir(&self) -> String {
        format!("{}/{}", &self.install_dir, self.cluster_name)
    }

    pub fn get_monograph_keyspace(&self) -> anyhow::Result<String> {
        let my_local = upload_dir().join("my_local.cnf");
        if !my_local.exists() {
            self.gen_monograph_config_by_host(None, self.install_dir())?;
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
            self.gen_redis_config_by_host(None)?;
        }
        let mut my_ini_local = Ini::new();
        let _config_map_rs = my_ini_local.load(my_local).unwrap();
        if let Some(keyspace) = my_ini_local.get(CONFIG_SECTION_STORE, "cass_keyspace") {
            Ok(keyspace)
        } else {
            Ok("mono_redis".to_string())
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
        // println!("group_member_map={group_member_map:#?}");
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

    pub fn build_monograph_config(
        &self,
        set_ip_list: bool,
        install_dir: String,
    ) -> anyhow::Result<Ini> {
        let mut mysql_ini = Ini::new();
        if let Some(cassandra) = self.storage_service.cassandra.as_ref() {
            mysql_ini
                .load(config_template(MONOGRAPH_CONF_TEMPLATE)?.as_path())
                .unwrap();

            let cassandra_hosts = cassandra.host.join(",");
            mysql_ini.set(
                CONFIG_MARIADB_SECTION,
                "monograph_cass_hosts",
                Some(cassandra_hosts),
            );
        } else {
            mysql_ini
                .load(config_template(MONOGRAPH_CONF_DYNAMO_TEMPLATE)?.as_path())
                .unwrap();

            let dynamodb = self.storage_service.dynamodb.as_ref().unwrap();
            mysql_ini.set(
                CONFIG_MARIADB_SECTION,
                "monograph_aws_access_key_id",
                Some(dynamodb.clone().access_key_id),
            );
            mysql_ini.set(
                CONFIG_MARIADB_SECTION,
                "monograph_aws_secret_key",
                Some(dynamodb.clone().secret_key),
            );
            mysql_ini.set(
                CONFIG_MARIADB_SECTION,
                "monograph_dynamodb_region",
                Some(dynamodb.clone().region),
            );
            mysql_ini.set(
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

        mysql_ini.set(
            CONFIG_MARIADB_SECTION,
            "datadir",
            Some(format!("{install_dir}/datafarm")),
        );
        mysql_ini.set(
            CONFIG_MARIADB_SECTION,
            "lc_messages_dir",
            Some(format!(
                "{install_dir}/{MONOGRAPH_TX_SERVICE_DIR}/install/share"
            )),
        );
        mysql_ini.set(
            CONFIG_MARIADB_SECTION,
            "plugin_dir",
            Some(format!(
                "{install_dir}/{MONOGRAPH_TX_SERVICE_DIR}/install/lib/plugin",
            )),
        );
        mysql_ini.set(
            CONFIG_MARIADB_SECTION,
            "port",
            Some(self.cs_conn_port().to_string()),
        );

        mysql_ini.set(
            CONFIG_MARIADB_SECTION,
            "socket",
            Some(format!("/tmp/mysql{}.sock", self.cs_conn_port())),
        );

        let use_port = self.port.monograph_port.as_ref().unwrap().start;
        let monograph_hosts = &self.tx_service.host;
        if set_ip_list {
            let ip_list = monograph_hosts
                .iter()
                .map(|host| format!("{}:{}", host.clone(), use_port))
                .join(",");
            mysql_ini.set(CONFIG_MARIADB_SECTION, "monograph_ip_list", Some(ip_list));
        } else {
            mysql_ini.set(
                CONFIG_MARIADB_SECTION,
                "monograph_ip_list",
                Some(format!("{}:{}", "127.0.0.1", use_port)),
            );
        }
        Ok(mysql_ini.clone())
    }

    pub fn gen_monograph_config_by_host(
        &self,
        tx_host: Option<String>,
        install_dir: String,
    ) -> anyhow::Result<PathBuf> {
        let port = self.port.monograph_port.as_ref().unwrap().start;
        let is_host = tx_host.is_some();
        let mut my_ini = self.build_monograph_config(is_host, install_dir)?;

        let (host, db_config_location) = if let Some(host) = tx_host {
            (host.clone(), upload_host_dir(&host).join("my.cnf"))
        } else {
            ("127.0.0.1".to_string(), upload_dir().join("my_local.cnf"))
        };
        if is_host && self.log_service.is_some() {
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
        let key = "thread_pool_size";
        if my_ini.get(CONFIG_MARIADB_SECTION, key).is_none() {
            let mut v;
            if let Some(hw) = opt_hw {
                v = (hw.cpu * 3) / 8;
                if v == 0 {
                    v = 1;
                }
            } else {
                v = 1;
            }
            my_ini.set(CONFIG_MARIADB_SECTION, key, Some(v.to_string()));
        }
        let key = "monograph_core_num";
        if my_ini.get(CONFIG_MARIADB_SECTION, key).is_none() {
            let mut v;
            if let Some(hw) = opt_hw {
                v = (hw.cpu * 3) / 8;
                if v == 0 {
                    v = 1;
                }
            } else {
                v = 1;
            }
            my_ini.set(CONFIG_MARIADB_SECTION, key, Some(v.to_string()));
        }
        let key = "monograph_node_memory_limit_mb";
        if my_ini.get(CONFIG_MARIADB_SECTION, key).is_none() {
            let v = if let Some(hw) = opt_hw {
                (hw.memory * 6) / 10
            } else {
                GB
            };
            my_ini.set(CONFIG_MARIADB_SECTION, key, Some(v.to_string()));
        }
        my_ini.write(db_config_location.as_path())?;
        Ok(db_config_location)
    }

    pub fn build_redis_config(&self, set_ip_list: bool) -> anyhow::Result<Ini> {
        let mut redis_ini = Ini::new();
        redis_ini
            .load(config_template(REDIS_CONF_TEMPLATE)?)
            .unwrap();
        match self.storage_service.provider().unwrap() {
            StorageProvider::Cassandra => {
                let cassandra_hosts = self
                    .storage_service
                    .cassandra
                    .as_ref()
                    .unwrap()
                    .host
                    .join(",");
                redis_ini.set(CONFIG_SECTION_STORE, "cass_hosts", Some(cassandra_hosts));
            }
            StorageProvider::DynamoDB => panic!("not supported"),
            StorageProvider::RocksDB => match self.storage_service.rocksdb.clone().unwrap() {
                RocksDB::Local => {}
                RocksDB::S3(s3) => {
                    redis_ini.set(CONFIG_SECTION_STORE, "aws_access_key_id", Some(s3.aws_id));
                    redis_ini.set(CONFIG_SECTION_STORE, "aws_secret_key", Some(s3.aws_secret));
                    redis_ini.set(
                        CONFIG_SECTION_STORE,
                        "kv_store_rocksdb_cloud_region",
                        Some(s3.region),
                    );
                    redis_ini.set(
                        CONFIG_SECTION_STORE,
                        "kv_store_rocksdb_cloud_bucket_name",
                        Some(s3.bucket_name),
                    );
                    redis_ini.set(
                        CONFIG_SECTION_STORE,
                        "kv_store_rocksdb_cloud_bucket_prefix",
                        Some(s3.bucket_prefix),
                    );
                    redis_ini.set(
                        CONFIG_SECTION_STORE,
                        "kv_store_rocksdb_target_file_size_base",
                        Some(s3.target_file_size_base),
                    );
                    redis_ini.set(
                        CONFIG_SECTION_STORE,
                        "kv_store_rocksdb_cloud_sst_file_cache_size",
                        Some(s3.sst_file_cache_size),
                    );
                }
                RocksDB::GCS(gcs) => {
                    redis_ini.set(
                        CONFIG_SECTION_STORE,
                        "kv_store_rocksdb_cloud_region",
                        Some(gcs.region),
                    );
                    redis_ini.set(
                        CONFIG_SECTION_STORE,
                        "kv_store_rocksdb_cloud_bucket_name",
                        Some(gcs.bucket_name),
                    );
                    redis_ini.set(
                        CONFIG_SECTION_STORE,
                        "kv_store_rocksdb_cloud_bucket_prefix",
                        Some(gcs.bucket_prefix),
                    );
                    redis_ini.set(
                        CONFIG_SECTION_STORE,
                        "kv_store_rocksdb_target_file_size_base",
                        Some(gcs.target_file_size_base),
                    );
                    redis_ini.set(
                        CONFIG_SECTION_STORE,
                        "kv_store_rocksdb_cloud_sst_file_cache_size",
                        Some(gcs.sst_file_cache_size),
                    );
                }
            },
        }
        let use_port = self.cs_conn_port();
        let monograph_hosts = &self.tx_service.host;
        if set_ip_list {
            let ip_list = monograph_hosts
                .iter()
                .map(|host| format!("{}:{}", host.clone(), use_port))
                .join(",");
            redis_ini.set(CONFIG_SECTION_CLUSTER, "ip_port_list", Some(ip_list));
        } else {
            redis_ini.set(
                CONFIG_SECTION_CLUSTER,
                "ip_port_list",
                Some(format!("{}:{}", "127.0.0.1", use_port)),
            );
        }
        Ok(redis_ini.clone())
    }

    pub fn gen_redis_config_by_host(&self, tx_host: Option<String>) -> anyhow::Result<PathBuf> {
        let is_host = tx_host.is_some();
        let mut my_ini = self.build_redis_config(is_host)?;
        let (host, db_config_location) = if let Some(host) = tx_host {
            (host.clone(), upload_host_dir(&host).join("redis.ini"))
        } else {
            (
                "127.0.0.1".to_string(),
                upload_dir().join("redis_local.ini"),
            )
        };
        if is_host && self.log_service.is_some() {
            self.build_log_config()
                .into_iter()
                .for_each(|(key, conf_val)| {
                    my_ini.set(CONFIG_SECTION_CLUSTER, &key, Some(conf_val));
                });
        }
        my_ini.set(CONFIG_SECTION_LOCAL, "ip", Some(host.clone()));
        my_ini.set(
            CONFIG_SECTION_LOCAL,
            "port",
            Some(self.cs_conn_port().to_string()),
        );

        let opt_hw = self.get_hardware(&host);
        if opt_hw.is_none() {
            warn!("hardware information for {host} is missing");
        }
        const MIN_CORE_TX: u16 = 1;
        let key = "core_number";
        let mut core_tx = MIN_CORE_TX;
        if let Some(v) = my_ini.get(CONFIG_SECTION_LOCAL, key) {
            core_tx = v.parse()?;
        } else if let Some(hw) = opt_hw {
            assert!(hw.cpu > 0);
            core_tx = (hw.cpu * 4 + 4) / 5;
        }
        if core_tx < MIN_CORE_TX {
            warn!("bad config {}={} for {} {:?}", key, core_tx, host, opt_hw);
            core_tx = MIN_CORE_TX;
        }
        my_ini.set(CONFIG_SECTION_LOCAL, key, Some(core_tx.to_string()));

        let key = "event_dispatcher_num";
        if my_ini.get(CONFIG_SECTION_LOCAL, key).is_none() {
            let core_io = (core_tx + 7) / 8;
            my_ini.set(CONFIG_SECTION_LOCAL, key, Some(core_io.to_string()));
        }

        let key = "node_memory_limit_mb";
        if my_ini.get(CONFIG_SECTION_LOCAL, key).is_none() {
            let v = if let Some(hw) = opt_hw {
                (hw.memory * 4) / 5
            } else {
                GB
            };
            my_ini.set(CONFIG_SECTION_LOCAL, key, Some(v.to_string()));
        }

        my_ini.write(db_config_location.as_path())?;
        Ok(db_config_location)
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
        let port = get_cassandra_port()?;
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
            download_urls!(links,
                {CASSANDRA_FILE_KEY, cass.download_url}
            );
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
        let storage_cluster = if cass.storage_cluster.is_none() {
            format!("{cluster_name}_cass_cluster")
        } else {
            cass.clone().storage_cluster.unwrap()
        };

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
                    let mut gc_setting;
                    if let Some(hw) = self.get_hardware(host) {
                        let h = if hw.memory < 16 * GB {
                            gc_setting = GC_SETTING_CMS.to_owned();
                            cmp::max(cmp::min(hw.memory / 2, GB), cmp::min(hw.memory / 4, 8 * GB))
                        } else {
                            gc_setting = GC_SETTING_G1.to_owned();
                            if hw.memory > 256 * GB {
                                64 * GB
                            } else {
                                hw.memory / 4
                            }
                        };
                        let h_xm = format!("\n-Xms{}M\n-Xmx{}M\n", h, h);
                        gc_setting.push_str(&h_xm);
                    } else {
                        warn!("hardware information for {} is missing", host);
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
