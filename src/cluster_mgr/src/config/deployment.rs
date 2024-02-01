use crate::cli::{upload_dir, upload_host_dir};
use crate::config::config_base::CASSANDRA_FILE_KEY;
use crate::config::config_base::{
    MONOGRAPH_FILE_KEY, MONOGRAPH_LOG_FILE_KEY, MONOGRAPH_TX_SERVICE_DIR,
};
use crate::config::log_service::LogService;
use crate::config::monitor::Monitor;
use crate::config::storage_service_config::StorageService;
use crate::config::ConfigErr::GenCassandraConfigErr;
use crate::config::{
    config_template, load_yaml_config_template, DownloadUrl, CASSANDRA_CONF_TEMPLATE,
    CASSANDRA_JVM_OPTION, CASSANDRA_JVM_TEMPLATE, CONFIG_MARIADB_SECTION, CONFIG_SECTION_CLUSTER,
    CONFIG_SECTION_LOCAL, CONFIG_SECTION_STORE, JVM_SETTING_HOLDER, MONOGRAPH_CONF_DYNAMO_TEMPLATE,
    MONOGRAPH_CONF_TEMPLATE, REDIS_CONF_TEMPLATE, RESOURCE_REPO,
};
use anyhow::{anyhow, bail};
use configparser::ini::Ini;
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
    pub mysql_port: u16,
    pub monograph_port: MonographPort,
}

impl Port {
    pub fn contains(&self, p: u16) -> bool {
        p == self.mysql_port || (p >= self.monograph_port.start && p <= self.monograph_port.end)
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

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Eq)]
pub enum Product {
    #[serde(alias = "monograph", alias = "MONOGRAPH")]
    Monograph,
    #[serde(alias = "redis", alias = "REDIS")]
    Redis,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Hardware {
    pub cpu: u16,
    pub memory: u32, // MiB
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
    pub hardware: Option<HashMap<String, Hardware>>,
}

impl Deployment {
    // Populate tx_image and log_image according to version number
    pub fn image_by_version(&mut self) {
        if self.version.is_none() || self.version.as_ref().unwrap().to_lowercase() == "latest" {
            self.version = Some("latest".to_owned());
        } else {
            let re = Regex::new(r"(0|[1-9][0-9]?)\.(0|[1-9][0-9]?)\.(0|[1-9][0-9]?)").unwrap();
            if !re.is_match(self.version.as_ref().unwrap()) {
                panic!("Invalid version {}", self.version.as_ref().unwrap());
            }
        }
        let version = self.version.as_ref().unwrap();
        match self.product() {
            Product::Monograph => {
                if self.tx_image.is_none() {
                    let store = self.storage_service.provider().unwrap().to_string();
                    self.tx_image = Some(format!(
                        "{}/main_tagged_range_ubuntu2004/{}/{}/monographdb-tx-release-bin.tar.gz",
                        RESOURCE_REPO, store, version
                    ));
                }
                if self.log_image.is_none() && self.log_service.is_some() {
                    let store = self.storage_service.provider().unwrap().to_string();
                    self.log_image = Some(format!(
                        "{}/main_tagged_range_ubuntu2004/{}/{}/monographdb-log-release-bin.tar.gz",
                        RESOURCE_REPO, store, version
                    ));
                }
            }
            Product::Redis => {
                if self.tx_image.is_none() {
                    self.tx_image = Some(format!(
                        "{}/mono-release-ci-redis/monograph_redis.tar.gz",
                        RESOURCE_REPO
                    ));
                }
                if self.log_image.is_none() && self.log_service.is_some() {
                    self.log_image = Some(format!(
                        "{}/mono-release-ci-redis/log_service.tar.gz",
                        RESOURCE_REPO
                    ));
                }
            }
        }
    }

    pub fn get_tx_image(&self) -> String {
        self.tx_image.clone().unwrap()
    }

    pub fn get_hardware(&self, host: &str) -> Option<&Hardware> {
        self.hardware.as_ref().unwrap().get(host)
    }

    pub fn product(&self) -> Product {
        if let Some(p) = self.product.clone() {
            p
        } else {
            Product::Monograph
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
            Product::Monograph => "monograph_txlog_service_list".to_string(),
            Product::Redis => "txlog_service_list".to_string(),
        };
        let key_replica = match self.product() {
            Product::Monograph => "monograph_txlog_group_replica_num".to_string(),
            Product::Redis => "txlog_group_replica_num".to_string(),
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
            Some(self.port.mysql_port.to_string()),
        );

        mysql_ini.set(
            CONFIG_MARIADB_SECTION,
            "socket",
            Some(format!("/tmp/mysql{}.sock", self.port.mysql_port)),
        );

        let use_port = self.port.monograph_port.start;
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
        let port = self.port.monograph_port.start;
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
        if let Some(cassandra) = self.storage_service.cassandra.as_ref() {
            let cassandra_hosts = cassandra.host.join(",");
            redis_ini.set(CONFIG_SECTION_STORE, "host", Some(cassandra_hosts));
        } else if redis_ini.get(CONFIG_SECTION_STORE, "host").is_none() {
            return Err(anyhow!("cassandra host is not confiured"));
        }
        let use_port = self.port.monograph_port.start;
        let monograph_hosts = &self.tx_service.host;
        if set_ip_list {
            let ip_list = monograph_hosts
                .iter()
                .map(|host| format!("{}:{}", host.clone(), use_port))
                .join(",");
            redis_ini.set(CONFIG_SECTION_CLUSTER, "ip_list", Some(ip_list));
        } else {
            redis_ini.set(
                CONFIG_SECTION_CLUSTER,
                "ip_list",
                Some(format!("{}:{}", "127.0.0.1", use_port)),
            );
        }
        Ok(redis_ini.clone())
    }

    pub fn gen_redis_config_by_host(&self, tx_host: Option<String>) -> anyhow::Result<PathBuf> {
        let port = self.port.monograph_port.start;
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
        my_ini.set(
            CONFIG_SECTION_LOCAL,
            "ip",
            Some(format!("{}:{}", host, port)),
        );

        let opt_hw = self.get_hardware(&host);
        if opt_hw.is_none() {
            warn!("hardware information for {host} is missing");
        }
        let key = "core_number";
        let core_tx;
        if let Some(v) = my_ini.get(CONFIG_SECTION_LOCAL, key) {
            core_tx = v.parse()?;
            if core_tx == 0 || (opt_hw.is_some() && opt_hw.unwrap().cpu < core_tx) {
                bail!("invalid config {}={} for host {}", key, core_tx, host);
            }
        } else {
            if let Some(hw) = opt_hw {
                assert!(hw.cpu > 0);
                core_tx = (hw.cpu + 3) / 4;
            } else {
                core_tx = 1;
            };
            my_ini.set(CONFIG_SECTION_LOCAL, key, Some(core_tx.to_string()));
        }
        assert!(core_tx > 0);

        let key = "bthread_concurrency";
        let mut core_bt = 0;
        if let Some(v) = my_ini.get(CONFIG_SECTION_LOCAL, key) {
            core_bt = v.parse()?;
            if core_bt == 0 || (opt_hw.is_some() && opt_hw.unwrap().cpu < core_bt) {
                bail!("invalid config {}={}for host {}", key, core_bt, host);
            }
        } else {
            if let Some(hw) = opt_hw {
                assert!(core_tx <= hw.cpu);
                core_bt = hw.cpu - core_tx;
            }
            if core_bt == 0 {
                core_bt = 1;
            }
            my_ini.set(CONFIG_SECTION_LOCAL, key, Some(core_bt.to_string()));
        }
        assert!(core_bt > 0);

        let key = "event_dispatcher_num";
        if let Some(v) = my_ini.get(CONFIG_SECTION_LOCAL, key) {
            let core_io = v.parse()?;
            if core_io == 0 || (opt_hw.is_some() && opt_hw.unwrap().cpu < core_io) {
                bail!("invalid config {}={} for host {}", key, core_io, host);
            }
        } else {
            let core_io = (core_bt + 5) / 6;
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
            db_image_download_links.extend(monitor_srv.download_links_as_amp()?);
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
        let cassandra_hosts = cass.clone().host;
        let storage_cluster = if cass.storage_cluster.is_none() {
            format!("{cluster_name}_cass_cluster")
        } else {
            cass.clone().storage_cluster.unwrap()
        };

        cass_conf_map.insert("cluster_name".to_string(), Value::String(storage_cluster));
        let seeds = cassandra_hosts.join(",");
        let seed_values = format!(
            r#"
               - class_name: org.apache.cassandra.locator.SimpleSeedProvider
                 parameters:
                 - seeds: {seeds}"#,
        );
        let seed_yaml_value: Value = serde_yaml::from_str(seed_values.as_str())?;
        cass_conf_map.insert(String::from("seed_provider"), seed_yaml_value);
        let cass_config_vec = cassandra_hosts
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
