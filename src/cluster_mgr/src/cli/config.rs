use crate::cli::{
    download_dir, CASSANDRA_CONF_TEMPLATE, MONOGRAPH_CONF_DYNAMO_TEMPLATE, MONOGRAPH_CONF_TEMPLATE,
    MONOGRAPH_INSTALL_SCRIPT, MONOGRAPH_INSTALL_TEMPLATE, START_MONOGRAPH_SCRIPT,
    START_MONOGRAPH_TEMPLATE,
};
use anyhow::anyhow;
use configparser::ini::Ini;
use itertools::Itertools;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use strum_macros::AsRefStr;
use thiserror::Error;
use tracing::{error, info};

#[macro_export]
macro_rules! gen_db_script {
    ($script_name:expr, $build_script_func:expr) => {{
        let script_rs = $build_script_func;
        if let Ok(script) = script_rs {
            let script_location = $crate::cli::download_dir().join($script_name);
            std::fs::write(script_location.clone(), script).unwrap();
            Ok(script_location)
        } else {
            Err(script_rs.err().unwrap())
        }
    }};
}

#[derive(PartialEq, Eq, Clone, Error, Debug)]
pub enum ConfigErr {
    #[error("MonographDB storage provider config error [{0}].For now only support Cassandra or DynamoDB, \
    You can choose either one.")]
    StorageConfigErr(String),
    #[error("The current configuration of the storage provider is not Cassandra. Storage Provider is {0}")]
    GenCassandraConfigErr(String),
}

pub const CONFIG_PATH_DIR: &str = "CLUSTER_MGR_CLI_CONFIG";
pub const CONFIG_MARIADB_SECTION: &str = "mariadb";

#[derive(Hash, Debug, Clone, PartialEq, Eq, AsRefStr)]
pub enum DeploymentService {
    #[strum(serialize = "monograph")]
    Monograph,
    #[strum(serialize = "storage")]
    Storage,
}

#[derive(Hash, Debug, Clone, PartialEq, Eq, AsRefStr)]
pub enum StorageProvider {
    #[strum(serialize = "cassandra")]
    Cassandra,
    #[strum(serialize = "dynamodb")]
    DynamoDB,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct DeploymentConfig {
    pub connection: Connection,
    pub deployment: Deployment,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Connection {
    pub username: String,
    pub auth_type: String,
    pub auth: Auth,
    pub port: Option<u16>,
}

impl Connection {
    pub fn ssh_port(&self) -> u16 {
        if let Some(ssh_port) = self.port {
            ssh_port
        } else {
            22_u16
        }
    }

    pub fn ssh_auth_key(&self) -> Option<String> {
        self.auth.clone().keypair
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Auth {
    pub password: Option<String>,
    pub keypair: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Deployment {
    pub install_image: String,
    pub cluster_name: String,
    pub install_dir: String,
    pub port: Port,
    pub mono_service: MonographService,
    pub storage_service: StorageService,
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

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct StorageService {
    pub cassandra: Option<Cassandra>,
    pub dynamodb: Option<Dynamodb>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Cassandra {
    pub host: Vec<String>,
    pub download_url: String,
    pub storage_cluster: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Dynamodb {
    pub access_key_id: String,
    pub secret_key: String,
    pub region: String,
    pub endpoint: String,
}

#[macro_export]
macro_rules! gen_db_misc_files {
    ($self:ident,$build_func:ident, $script_template:expr) => {{
        let script = $self.$build_func()?;
        let script_location = download_dir().join($script_template);
        std::fs::write(script_location.clone(), script).unwrap();
        Ok(script_location)
    }};
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
            .filter(|rs_entry| !rs_entry.0.is_empty())
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
                    "Database image url error {}",
                    self.deployment.install_image
                ));
            }
            let file_name = path_segments.unwrap().last();
            if let Some(file_name_str) = file_name {
                Ok(file_name_str.to_string())
            } else {
                Err(anyhow!(
                    "Database image url parse error. url={}",
                    self.deployment.install_image
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
                format!("{}/my_local.cnf", remote_install_dir).as_str(),
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

    pub fn config_string(&self) -> String {
        serde_yaml::to_string(self).unwrap()
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
        let cass_template_path_buf = DeploymentConfig::config_template(CASSANDRA_CONF_TEMPLATE)?;
        let cass_opened_file = File::open(cass_template_path_buf.as_path())?;

        // cassandra.yaml config object
        let mut cass_conf_map =
            serde_yaml::from_reader::<File, HashMap<String, Value>>(cass_opened_file)?;

        let storage_port_opt = cass_conf_map.get("storage_port");
        assert!(storage_port_opt.is_some());

        let storage_port = storage_port_opt.unwrap().as_i64().unwrap();
        let cassandra_hosts = self.get_host_list(DeploymentService::Storage);

        let storage_cluster = if cass.storage_cluster.is_none() {
            format!("{}_cass_cluster", self.deployment.cluster_name)
        } else {
            cass.storage_cluster.unwrap()
        };

        cass_conf_map.insert("cluster_name".to_string(), Value::String(storage_cluster));

        let seeds = cassandra_hosts
            .iter()
            .map(|host| format!("{}:{}", host, storage_port))
            .collect_vec()
            .join(",");

        let seed_values = format!(
            r#"
           - class_name: org.apache.cassandra.locator.SimpleSeedProvider
             parameters:
             - seeds: {}"#,
            seeds
        );
        let seed_yaml_value: Value = serde_yaml::from_str(seed_values.as_str())?;
        cass_conf_map.insert(String::from("seed_provider"), seed_yaml_value);

        let cass_config_vec = cassandra_hosts
            .iter()
            .map(|host| {
                let host_value = Value::String(host.to_string());
                cass_conf_map.insert(String::from("listen_address"), host_value.clone());
                cass_conf_map.insert(String::from("rpc_address"), host_value);
                let config_path = download_dir().join(format!("cassandra_{}.yaml", host));
                let new_config_file = File::create(config_path.as_path()).unwrap();
                let gen_config_write = serde_yaml::to_writer(new_config_file, &cass_conf_map);
                assert!(gen_config_write.is_ok());
                (host.to_string(), config_path)
            })
            .collect::<HashMap<String, PathBuf>>();

        Ok(cass_config_vec)
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

pub fn config_path_string(path: Option<String>) -> anyhow::Result<String> {
    if let Some(path_string) = path {
        Ok(path_string)
    } else {
        Ok(std::env::var(CONFIG_PATH_DIR)?)
    }
}

pub fn load_remote_env(path: Option<String>) -> anyhow::Result<HashMap<String, String>> {
    let path_string = config_path_string(path)?;
    let file = File::open(PathBuf::from(path_string).join("remote_env"))?;
    let mut reader = BufReader::new(file);
    let mut file_content_buf = String::new();
    reader.read_to_string(&mut file_content_buf)?;

    let env_props = file_content_buf
        .lines()
        .filter(|line| line.contains('='))
        .map(|line| {
            let splits = line.split('=').collect_vec();
            assert_eq!(splits.len(), 2);
            (splits[0].to_string(), splits[1].to_string())
        })
        .collect::<HashMap<String, String>>();

    Ok(env_props)
}

#[cfg(test)]
mod tests {
    use crate::cli::config::{load_remote_env, DeploymentConfig, CONFIG_PATH_DIR};
    use crate::cli::CASSANDRA_CONF_TEMPLATE;
    use serde_yaml::Value;
    use std::collections::HashMap;
    use std::env::set_var;
    use std::fs;
    use std::fs::File;
    use std::path::PathBuf;

    fn deployment_file_path() -> String {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config");
        set_var(CONFIG_PATH_DIR, manifest_dir.to_str().unwrap());
        let deployment_file_path = manifest_dir.join("deployment.yaml");
        deployment_file_path.to_str().unwrap().to_string()
    }

    #[test]
    pub fn test_load_config() {
        let path_string = deployment_file_path();
        let deployment_config = DeploymentConfig::load(Some(path_string));
        assert!(deployment_config.is_ok());
        println!("{:#?}", deployment_config);
    }

    #[test]
    pub fn test_gen_db_script() {
        use crate::cli::MONOGRAPH_INSTALL_SCRIPT;
        let path_string = deployment_file_path();
        let deployment_config = DeploymentConfig::load(Some(path_string));
        assert!(deployment_config.is_ok());
        let config = deployment_config.unwrap();
        let path_buf_rs = gen_db_script!(
            MONOGRAPH_INSTALL_SCRIPT,
            config.build_install_monograph_script()
        );
        println!("start_script_path ={:?}", path_buf_rs);
        assert!(path_buf_rs.is_ok());
        let start_script_path_buf = path_buf_rs.unwrap();
        assert!(start_script_path_buf.exists());
    }

    #[test]
    pub fn test_db_image_local_filename() {
        let path_string = deployment_file_path();
        let deployment_config = DeploymentConfig::load(Some(path_string));
        assert!(deployment_config.is_ok());
        let deployment = deployment_config.unwrap();
        let db_download_url = deployment.deployment.install_image.as_str();
        let file_name_rs = deployment.extract_file_name(db_download_url);
        assert!(file_name_rs.is_ok());
        println!("file_name = {:?}", file_name_rs.unwrap());
    }

    #[test]
    pub fn test_unpack_files_map() {
        let path_string = deployment_file_path();
        let deployment_config = DeploymentConfig::load(Some(path_string));
        assert!(deployment_config.is_ok());
        let config = deployment_config.unwrap();
        let unpack = config.unpack_files_map();
        println!("unpack_files = {:?}", unpack);
        assert_eq!(unpack.len(), 2);
    }

    #[test]
    pub fn test_load_remote_env() {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config");
        set_var(CONFIG_PATH_DIR, manifest_dir.to_str().unwrap());
        let rs = load_remote_env(None);
        println!("rs {:?}", rs);
        assert!(rs.is_ok());
        println!("remote env props = {:?}", rs.unwrap());
    }

    #[test]
    pub fn test_gen_cass_config() {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config");
        set_var(CONFIG_PATH_DIR, manifest_dir.to_str().unwrap());
        let cass_template_path_rs = DeploymentConfig::config_template(CASSANDRA_CONF_TEMPLATE);
        assert!(cass_template_path_rs.is_ok());
        let cass_template_path = cass_template_path_rs.unwrap();
        let cass_map_rs = serde_yaml::from_reader::<File, HashMap<String, Value>>(
            File::open(cass_template_path.as_path()).unwrap(),
        );
        assert!(cass_map_rs.is_ok());
        let mut cass_map = cass_map_rs.unwrap();
        //println!("{:#?}", cass_map);
        cass_map.insert(
            "listen_address".to_string(),
            Value::String("127.0.0.1".to_string()),
        );

        let seed_provider_str = format!(
            r#"
           - class_name: org.apache.cassandra.locator.SimpleSeedProvider
             parameters:
             - seeds: {}"#,
            "172.172.172.172:7070"
        );

        let seed_provider_value: Value = serde_yaml::from_str(seed_provider_str.as_str()).unwrap();
        println!("seed_provider = {:#?}", seed_provider_value);
        cass_map.insert("seed_provider".to_string(), seed_provider_value);

        let config_path = manifest_dir.join("cassandra_127.0.0.1.yaml");
        let config_file = File::create(config_path.as_path()).unwrap();
        let write_config_rs = serde_yaml::to_writer(config_file, &cass_map);
        assert!(write_config_rs.is_ok());

        let updated_file = serde_yaml::from_reader::<File, HashMap<String, Value>>(
            File::open(config_path.as_path()).unwrap(),
        );
        assert!(updated_file.is_ok());

        let final_cass_map = updated_file.unwrap();

        let listen_address_value = final_cass_map.get("listen_address").unwrap();

        println!("get listen_address_value={:?}", listen_address_value);
        assert_eq!(
            "127.0.0.1".to_string(),
            listen_address_value.as_str().unwrap().to_string()
        );
        let del_config_path = fs::remove_file(config_path.as_path());
        assert!(del_config_path.is_ok());
    }
}
