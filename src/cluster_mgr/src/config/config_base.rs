use crate::cli::MONOGRAPH_INSTALL_SCRIPT;
use crate::cli::START_MONOGRAPH_SCRIPT;
use crate::cli::{
    download_dir, CASSANDRA_CONF_TEMPLATE, MONOGRAPH_CONF_DYNAMO_TEMPLATE, MONOGRAPH_CONF_TEMPLATE,
    MONOGRAPH_INSTALL_TEMPLATE, START_MONOGRAPH_TEMPLATE,
};
use crate::config::connection::Connection;
use crate::config::deployment::Deployment;
use crate::config::ConfigErr;
use crate::config::{
    config_path_string, DeploymentService, StorageProvider, CONFIG_MARIADB_SECTION, CONFIG_PATH_DIR,
};
use crate::gen_db_misc_files;
use anyhow::anyhow;
use configparser::ini::Ini;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use sysinfo::SystemExt;
use tracing::{error, info};
use url::Url;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct DeploymentConfig {
    pub connection: Connection,
    pub deployment: Deployment,
}

impl DeploymentConfig {
    /// key is host, value is tarball
    pub fn unpack_files_map(&self) -> HashMap<String, Vec<String>> {
        let hosts = self.get_host_as_map();
        let deployment_cloned = self.deployment.clone();
        let storage = deployment_cloned.storage_service.cassandra;
        let monograph_download_url = deployment_cloned.install_image;
        let storage_provider = self.get_monograph_storage();
        let provider = storage_provider.as_ref().unwrap().clone();
        hosts
            .into_iter()
            .map(|entry| {
                let hosts = entry.1;
                let service = entry.0;
                let file_name =
                    if service == DeploymentService::Storage && storage_provider.as_ref().is_ok() {
                        if provider == StorageProvider::Cassandra {
                            let storage = storage.as_ref().unwrap();
                            self.extract_file_name(storage.download_url.as_str())
                                .unwrap()
                        } else {
                            "".to_string()
                        }
                    } else {
                        self.extract_file_name(monograph_download_url.as_str())
                            .unwrap()
                    };
                (file_name, hosts)
            })
            .filter(|rs_entry| !rs_entry.0.is_empty() && rs_entry.0.contains("tar.gz"))
            .collect::<HashMap<String, Vec<String>>>()
    }

    pub fn gen_install_db_script(&self) -> anyhow::Result<PathBuf> {
        gen_db_misc_files!(
            self,
            build_install_monograph_script,
            MONOGRAPH_INSTALL_SCRIPT
        )
    }

    pub fn gen_db_start_script(&self) -> anyhow::Result<PathBuf> {
        gen_db_misc_files!(self, build_start_monograph_script, START_MONOGRAPH_SCRIPT)
    }

    pub fn gen_monograph_config(&self, db_host: Option<String>) -> anyhow::Result<PathBuf> {
        let port = self.deployment.clone().port.monograph_port.start;
        let set_ip_list = db_host.is_some();
        let my_ini_rs = self.build_monograph_config(set_ip_list);

        let host_and_file_tuple = if let Some(host) = db_host {
            (host.clone(), host)
        } else {
            ("127.0.0.1".to_string(), "local".to_string())
        };
        let db_config_location = download_dir().join(format!("my_{}.cnf", host_and_file_tuple.1));
        if let Ok(mut my_ini) = my_ini_rs {
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

    pub fn get_monograph_keyspace(&self) -> anyhow::Result<String> {
        let download_dir = download_dir();
        let my_local = download_dir.join("my_local.cnf");
        if !my_local.exists() {
            self.gen_monograph_config(None)?;
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

    pub fn install_dir(&self) -> String {
        format!(
            "{}/{}",
            self.deployment.clone().install_dir,
            self.deployment.cluster_name
        )
    }

    pub fn download_file_as_map(&self) -> anyhow::Result<HashMap<DeploymentService, String>> {
        let deployment_cloned = self.deployment.clone();
        let monograph_download_file_rs =
            self.extract_file_name(deployment_cloned.install_image.as_str());
        if monograph_download_file_rs.is_err() {
            return Err(monograph_download_file_rs.err().unwrap());
        }
        let mut download_files = HashMap::from([(
            DeploymentService::Monograph,
            monograph_download_file_rs.unwrap(),
        )]);

        if let Some(cassandra) = deployment_cloned.storage_service.cassandra {
            let cassandra_download_file = self.extract_file_name(cassandra.download_url.as_str());
            if cassandra_download_file.is_err() {
                return Err(cassandra_download_file.err().unwrap());
            }
            download_files.insert(DeploymentService::Storage, cassandra_download_file.unwrap());
        }
        Ok(download_files)
    }

    fn extract_file_name(&self, url_str: &str) -> anyhow::Result<String> {
        let url_rs = Url::parse(url_str);
        if let Err(url_parse_err) = url_rs {
            let parser_err = url_parse_err.to_string();
            Err(anyhow!(parser_err))
        } else {
            let url = url_rs.unwrap();
            let path_segments = url.path_segments();
            if path_segments.is_none() {
                return Err(anyhow!(
                    "get url path segments error {}",
                    self.deployment.install_image
                ));
            }
            let file_name = path_segments.unwrap().last();
            if let Some(file_name_str) = file_name {
                Ok(file_name_str.to_string())
            } else {
                Err(anyhow!(
                    "extract file name error. MonographDB install image={}, Cassandra download url={:?}",
                    self.deployment.install_image,self.deployment.storage_service.cassandra
                ))
            }
        }
    }

    fn config_template(file_name: &str) -> anyhow::Result<PathBuf> {
        let config_path_var_rs = std::env::var(CONFIG_PATH_DIR);
        assert!(config_path_var_rs.is_ok());
        let config_path = config_path_var_rs.unwrap();
        let path_buf = PathBuf::from(config_path.as_str()).join(file_name);
        if path_buf.exists() {
            Ok(path_buf)
        } else {
            Err(anyhow!(
                "MonographDB config not found in the {:?}",
                path_buf
            ))
        }
    }

    pub fn build_install_monograph_script(&self) -> anyhow::Result<String> {
        let install_db_template = DeploymentConfig::config_template(MONOGRAPH_INSTALL_TEMPLATE)?;
        let remote_install_dir = self.install_dir();

        let rs = std::fs::read_to_string(install_db_template.as_path())?;
        let final_script = rs
            .replace(
                "_CASSANDRA_STORAGE_BIN",
                format!("{}/{}", remote_install_dir, "apache-cassandra/bin").as_str(),
            )
            .replace(
                "_MONOGRAPH_DB_HOME",
                format!("{}/{}/install", remote_install_dir, "monographdb-release").as_str(),
            )
            .replace(
                "_MY_CONF",
                format!("{remote_install_dir}/my_local.cnf").as_str(),
            )
            .replace("_MY_CLUSTER_HOME", remote_install_dir.as_str());
        Ok(final_script)
    }

    pub fn build_start_monograph_script(&self) -> anyhow::Result<String> {
        let script_path = DeploymentConfig::config_template(START_MONOGRAPH_TEMPLATE)?;
        let rs = std::fs::read_to_string(script_path.as_path())?;
        Ok(rs.replace(
            "_MY_INSTALL_DIR",
            format!("{}/monographdb-release/install", self.install_dir()).as_str(),
        ))
    }

    pub fn build_monograph_config(&self, set_ip_list: bool) -> anyhow::Result<Ini> {
        let storage_provider = self.get_monograph_storage()?;
        let deployment = self.deployment.clone();
        let mut mysql_ini = Ini::new();
        match storage_provider {
            StorageProvider::Cassandra => {
                mysql_ini
                    .load(DeploymentConfig::config_template(MONOGRAPH_CONF_TEMPLATE)?.as_path())
                    .unwrap();

                let cassandra_hosts = self.get_host_list(DeploymentService::Storage).join(",");
                mysql_ini.set(
                    CONFIG_MARIADB_SECTION,
                    "monograph_cass_hosts",
                    Some(cassandra_hosts),
                );
            }
            StorageProvider::DynamoDB => {
                mysql_ini
                    .load(
                        DeploymentConfig::config_template(MONOGRAPH_CONF_DYNAMO_TEMPLATE)?
                            .as_path(),
                    )
                    .unwrap();

                let dynamodb = deployment.storage_service.dynamodb.unwrap();
                mysql_ini.set(
                    CONFIG_MARIADB_SECTION,
                    "monograph_aws_access_key_id",
                    Some(dynamodb.access_key_id),
                );
                mysql_ini.set(
                    CONFIG_MARIADB_SECTION,
                    "monograph_aws_secret_key",
                    Some(dynamodb.secret_key),
                );
                mysql_ini.set(
                    CONFIG_MARIADB_SECTION,
                    "monograph_dynamodb_region",
                    Some(dynamodb.region),
                );
                mysql_ini.set(
                    CONFIG_MARIADB_SECTION,
                    "monograph_dynamodb_endpoint",
                    Some(dynamodb.endpoint),
                );
            }
        };
        mysql_ini.set(
            CONFIG_MARIADB_SECTION,
            "datadir",
            Some(format!("{}/datafarm", self.install_dir())),
        );
        mysql_ini.set(
            CONFIG_MARIADB_SECTION,
            "lc_messages_dir",
            Some(format!(
                "{}/monographdb-release/install/share",
                self.install_dir()
            )),
        );
        mysql_ini.set(
            CONFIG_MARIADB_SECTION,
            "plugin_dir",
            Some(format!(
                "{}/monographdb-release/install/lib/plugin",
                self.install_dir()
            )),
        );
        mysql_ini.set(
            CONFIG_MARIADB_SECTION,
            "port",
            Some(deployment.port.mysql_port.to_string()),
        );

        mysql_ini.set(
            CONFIG_MARIADB_SECTION,
            "socket",
            Some(format!("/tmp/mysql{}.sock", deployment.port.mysql_port)),
        );

        let use_port = deployment.port.monograph_port.start;
        if set_ip_list {
            let ip_list = self
                .get_host_list(DeploymentService::Monograph)
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

    fn read_config_from_file(path: String) -> anyhow::Result<Self> {
        let open_file_handler = File::open(Path::new(path.as_str()))?;
        let deployment_config =
            serde_yaml::from_reader::<File, DeploymentConfig>(open_file_handler)?;
        Ok(deployment_config)
    }

    pub fn get_host_as_map(&self) -> HashMap<DeploymentService, Vec<String>> {
        HashMap::from([
            (
                DeploymentService::Monograph,
                self.get_host_list(DeploymentService::Monograph),
            ),
            (
                DeploymentService::Storage,
                self.get_host_list(DeploymentService::Storage),
            ),
        ])
    }

    pub fn get_unique_host_list(&self) -> Vec<String> {
        let mut hosts_vec = self.get_host_list(DeploymentService::Monograph);
        let storage_hosts = self.get_host_list(DeploymentService::Storage);
        hosts_vec.extend(storage_hosts.into_iter());
        hosts_vec.into_iter().unique().collect_vec()
    }

    pub fn get_host_list(&self, service: DeploymentService) -> Vec<String> {
        match service {
            DeploymentService::Storage => {
                if let Some(cassandra) = self.deployment.clone().storage_service.cassandra {
                    cassandra.host
                } else {
                    vec![]
                }
            }
            DeploymentService::Monograph => self.clone().deployment.mono_service.host,
        }
    }

    pub fn load_from_string(config_content: String) -> anyhow::Result<Self> {
        let deployment_config_rs =
            serde_yaml::from_str::<DeploymentConfig>(config_content.as_str());
        if let Ok(deployment_config) = deployment_config_rs {
            Ok(deployment_config)
        } else {
            Err(anyhow!(deployment_config_rs.err().unwrap().to_string()))
        }
    }

    pub fn validate_storage_service(&self) -> bool {
        let storage_service = &self.deployment.storage_service;
        storage_service.dynamodb.is_some() || storage_service.cassandra.is_some()
    }

    pub fn get_monograph_storage(&self) -> anyhow::Result<StorageProvider> {
        let storage = &self.deployment.storage_service;
        if !self.validate_storage_service() {
            let err_msg = format!(
                "DynamoDB Option={}, Cassandra option={}",
                storage.cassandra.is_some(),
                storage.cassandra.is_some()
            );
            Err(anyhow!(ConfigErr::StorageConfigErr(err_msg)))
        } else {
            if storage.cassandra.is_some() {
                return Ok(StorageProvider::Cassandra);
            }
            Ok(StorageProvider::DynamoDB)
        }
    }

    pub fn config_to_string(&self) -> String {
        serde_yaml::to_string(self).unwrap()
    }

    pub fn load_cassandra_config_template(&self) -> anyhow::Result<HashMap<String, Value>> {
        let cass_template_path_buf = DeploymentConfig::config_template(CASSANDRA_CONF_TEMPLATE)?;
        let cass_opened_file = File::open(cass_template_path_buf.as_path())?;
        // cassandra.yaml config object
        let cass_conf_map =
            serde_yaml::from_reader::<File, HashMap<String, Value>>(cass_opened_file)?;
        Ok(cass_conf_map)
    }

    // key is cassandra host, value is cassandra.yaml config
    pub fn gen_cassandra_config(&self) -> anyhow::Result<HashMap<String, PathBuf>> {
        if self.deployment.storage_service.cassandra.is_none() {
            let storage_provider = self.get_monograph_storage()?;
            return Err(anyhow!(ConfigErr::GenCassandraConfigErr(
                storage_provider.as_ref().to_string()
            )));
        }
        let cass = self.deployment.clone().storage_service.cassandra.unwrap();
        // cassandra.yaml config object
        let mut cass_conf_map = self.load_cassandra_config_template()?;

        let cassandra_hosts = self.get_host_list(DeploymentService::Storage);

        let storage_cluster = if cass.storage_cluster.is_none() {
            format!("{}_cass_cluster", self.deployment.cluster_name)
        } else {
            cass.storage_cluster.unwrap()
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
                let config_path = download_dir().join(format!("cassandra_{host}.yaml"));
                let new_config_file = File::create(config_path.as_path()).unwrap();
                let gen_config_write = serde_yaml::to_writer(new_config_file, &cass_conf_map);
                assert!(gen_config_write.is_ok());
                (host.to_string(), config_path)
            })
            .collect::<HashMap<String, PathBuf>>();

        Ok(cass_config_vec)
    }

    /// Returns the runtime dependencies of MonographDB, with different return values depending on the installation platform.
    /// for example: ubuntu_runtime_deps, centos_runtime_deps
    pub fn load_runtime_deps_by_os(os_name: Option<String>) -> anyhow::Result<(String, String)> {
        let config_path_var_rs = std::env::var(CONFIG_PATH_DIR);
        assert!(config_path_var_rs.is_ok());
        let curr_os_name = if let Some(short_os_name) = os_name {
            short_os_name.to_lowercase()
        } else {
            let system = sysinfo::System::new();
            let curr_os_name = system.name();
            assert!(curr_os_name.is_some());
            let os_name = curr_os_name.unwrap().to_lowercase();
            if os_name.starts_with("ubuntu") {
                "ubuntu".to_string()
            } else if os_name.starts_with("centos") {
                "centos".to_string()
            } else {
                unreachable!()
            }
        };
        let runtime_deps_file = format!("{curr_os_name}_runtime_deps");
        let config_path = config_path_var_rs.unwrap();
        let deps_path = PathBuf::from(config_path.as_str()).join(runtime_deps_file);
        let deps_file_opened = File::open(deps_path.as_path())?;
        let lines = BufReader::new(deps_file_opened).lines();

        let deps_string = lines
            .filter_map(|line_rs| {
                if let Ok(line) = line_rs {
                    Some(line)
                } else {
                    None
                }
            })
            .join(" ");
        Ok((curr_os_name, deps_string))
    }

    pub fn load(path: Option<String>) -> anyhow::Result<Self> {
        let path_string = config_path_string(path)?;
        info!("DeploymentConfig load file from {}", path_string);
        let config_rs = DeploymentConfig::read_config_from_file(path_string);
        if let Ok(config) = config_rs {
            Ok(config)
        } else {
            let config_err = config_rs.err().unwrap().to_string();
            error!(
                "DeploymentConfig load error cause by {:?}",
                config_err.as_str()
            );
            Err(anyhow!(config_err))
        }
    }
}
