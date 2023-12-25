use crate::cli::download_dir;
use std::collections::HashMap;

use crate::config::config_base::CASSANDRA_FILE_KEY;
use crate::config::config_base::{
    MONOGRAPH_FILE_KEY, MONOGRAPH_LOG_FILE_KEY, MONOGRAPH_TX_SERVICE_DIR,
};
use crate::config::log_service::LogService;
use crate::config::monitor::Monitor;
use crate::config::storage_service_config::StorageService;
use crate::config::{
    config_template, DownloadUrl, CONFIG_MARIADB_SECTION, MONOGRAPH_CONF_DYNAMO_TEMPLATE,
    MONOGRAPH_CONF_TEMPLATE,
};
use anyhow::anyhow;
use configparser::ini::Ini;
use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
    Monograph,
    Redis,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Deployment {
    pub product: Product,
    pub tx_image: String,
    pub log_image: Option<String>,
    pub cluster_name: String,
    pub install_dir: String,
    pub port: Port,
    pub tx_service: MonographService,
    pub log_service: Option<LogService>,
    pub storage_service: StorageService,
    pub monitor: Option<Monitor>,
}

impl Deployment {
    fn build_log_config(&self) -> Option<HashMap<String, String>> {
        if let Some(ref log_srv) = self.log_service {
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

            Some(HashMap::from([
                (
                    "monograph_txlog_group_replica_num".to_string(),
                    replica_num.to_string(),
                ),
                ("monograph_txlog_service_list".to_string(), node_group),
            ]))
        } else {
            None
        }
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
        let set_ip_list = tx_host.is_some();
        let my_ini_rs = self.build_monograph_config(set_ip_list, install_dir);

        let host_and_file_tuple = if let Some(host) = tx_host {
            (host.clone(), host)
        } else {
            ("127.0.0.1".to_string(), "local".to_string())
        };
        let file_suffix = host_and_file_tuple.1;
        let db_config_location = download_dir().join(format!("my_{file_suffix}.cnf"));
        let log_member_config = self.build_log_config();
        if let Ok(mut my_ini) = my_ini_rs {
            if !file_suffix.eq("local") {
                if let Some(config_map) = log_member_config {
                    config_map.iter().for_each(|(key, conf_val)| {
                        my_ini.set(CONFIG_MARIADB_SECTION, key, Some(conf_val.to_string()));
                    });
                }
            }
            my_ini.set(
                CONFIG_MARIADB_SECTION,
                "monograph_local_ip",
                Some(format!("{}:{}", host_and_file_tuple.0, port)),
            );

            if let Err(err) = my_ini.write(db_config_location.clone()) {
                Err(anyhow!(err))
            } else {
                Ok(db_config_location)
            }
        } else {
            Err(my_ini_rs.err().unwrap())
        }
    }

    pub fn monograph_download_links(&self) -> anyhow::Result<HashMap<String, DownloadUrl>> {
        let mut links = HashMap::new();
        download_urls!(links,
                {MONOGRAPH_FILE_KEY, self.tx_image}
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
