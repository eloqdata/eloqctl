use crate::all_hosts_merge;
use crate::cli::{create_upload_cluster_dir, upload_dir, HOME_DIR};
use crate::config::connection::Connection;
use crate::config::deployment::{Codis, Deployment, Product, Version};
use crate::config::log_service::LogProcessKey;
use crate::config::{
    config_path_string, config_template, DeploymentPackage, StorageProvider,
    MONOGRAPH_INSTALL_SCRIPT, START_LOG_TEMPLATE,
};
use crate::config::{ConfigErr, DownloadUrl};
use crate::gen_db_misc_files;
use anyhow::{anyhow, Result};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use tracing::{error, info};

pub const LOG_SERVICE_HOME: &str = "LogServer";

pub const MONOGRAPH_FILE_KEY: &str = "monograph_tx";
pub const MONOGRAPH_LOG_FILE_KEY: &str = "monograph_log";
pub const CASSANDRA_FILE_KEY: &str = "cassandra";
pub const PROMETHEUS_FILE_KEY: &str = "prometheus";
pub const GRAFANA_FILE_KEY: &str = "grafana";
pub const NODE_EXPORTER_FILE_KEY: &str = "node_exporter";
pub const MYSQL_EXPORTER_FILE_KEY: &str = "mysqld_exporter";
pub const CASSANDRA_COLLECTOR_AGENT_FILE_KEY: &str = "datastax-mcac-agent";
pub const DEPLOYMENT_CHECK_SUCCESS_TASK: &str = "deploy_check_success_task";
pub const SCALED_CLUSTER_CONFIG: &str = "cluster_config";

pub const ASAN_OPTIONS: &str = "abort_on_error=1:disable_coredump=0:halt_on_error=0:fast_unwind_on_malloc=0:leak_check_at_exit=0";

pub fn export_asan(log: &str) -> String {
    format!("export ASAN_OPTIONS={ASAN_OPTIONS}:log_path={log}")
}

macro_rules! extract_monitor_link {
    ($monitor_links:expr, $monitor_fil_key:expr, $links_vec:expr) => {
        if let Some(download_url) = $monitor_links.get($monitor_fil_key) {
            $links_vec.push(download_url.clone());
        }
    };
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct UploadFile {
    pub source: String,
    pub dest: String,
    pub extension: String,
    pub host: String,
    pub copy_dir: bool,
}

#[serde_with::skip_serializing_none]
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct DeployConfig {
    pub connection: Connection,
    pub deployment: Deployment,
    pub conf_opts: Option<HashMap<String, bool>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnPackFileLocation {
    pub file: DownloadUrl,
    pub host: String,
}

impl DeployConfig {
    fn unpack_links_map(&self) -> HashMap<DeploymentPackage, Vec<DownloadUrl>> {
        let all_hosts = self.get_host_as_map();
        let cassandra_opt = self
            .deployment
            .storage_service
            .as_ref()
            .and_then(|s| s.cassandra.as_ref());
        let monitor_opt = self.deployment.monitor.as_ref();
        let tx_image = self.deployment.tx_image();
        let log_image = self.deployment.log_image();
        let monitor_link = if let Some(monitor) = monitor_opt {
            monitor.download_links_as_map().unwrap()
        } else {
            HashMap::default()
        };
        all_hosts
            .keys()
            .map(|pkg| {
                let mut unpack_files = vec![];
                match pkg {
                    DeploymentPackage::Storage => {
                        if let Some(cassandra) = cassandra_opt {
                            if let Some(cassdply) = cassandra.internal() {
                                let cass_url =
                                    DownloadUrl::from_url_str(&cassdply.image_url()).unwrap();
                                unpack_files.push(cass_url);
                                extract_monitor_link!(
                                    monitor_link,
                                    NODE_EXPORTER_FILE_KEY,
                                    unpack_files
                                );
                                extract_monitor_link!(
                                    monitor_link,
                                    CASSANDRA_COLLECTOR_AGENT_FILE_KEY,
                                    unpack_files
                                );
                            }
                        }
                    }
                    DeploymentPackage::MonographTx => {
                        extract_monitor_link!(monitor_link, NODE_EXPORTER_FILE_KEY, unpack_files);
                        extract_monitor_link!(monitor_link, MYSQL_EXPORTER_FILE_KEY, unpack_files);
                        let tx_image = DownloadUrl::from_url_str(tx_image).unwrap();
                        unpack_files.push(tx_image);
                    }
                    DeploymentPackage::MonographStandby => {
                        extract_monitor_link!(monitor_link, NODE_EXPORTER_FILE_KEY, unpack_files);
                        extract_monitor_link!(monitor_link, MYSQL_EXPORTER_FILE_KEY, unpack_files);
                        let tx_image = DownloadUrl::from_url_str(tx_image).unwrap();
                        unpack_files.push(tx_image);
                    }
                    DeploymentPackage::MonographVoter => {
                        extract_monitor_link!(monitor_link, NODE_EXPORTER_FILE_KEY, unpack_files);
                        extract_monitor_link!(monitor_link, MYSQL_EXPORTER_FILE_KEY, unpack_files);
                        let tx_image = DownloadUrl::from_url_str(tx_image).unwrap();
                        unpack_files.push(tx_image);
                    }
                    DeploymentPackage::MonographLog => {
                        if let Some(img) = log_image {
                            let log_image_link = DownloadUrl::from_url_str(img).unwrap();
                            unpack_files.push(log_image_link);
                            extract_monitor_link!(
                                monitor_link,
                                NODE_EXPORTER_FILE_KEY,
                                unpack_files
                            );
                        }
                    }
                    DeploymentPackage::Prometheus => {
                        extract_monitor_link!(monitor_link, PROMETHEUS_FILE_KEY, unpack_files);
                    }
                    DeploymentPackage::Grafana => {
                        extract_monitor_link!(monitor_link, GRAFANA_FILE_KEY, unpack_files);
                    }
                    DeploymentPackage::Codis => {
                        extract_monitor_link!(monitor_link, NODE_EXPORTER_FILE_KEY, unpack_files);
                        let link = DownloadUrl::from_url_str(&Codis::download_url()).unwrap();
                        unpack_files.push(link);
                    }
                    DeploymentPackage::Proxy => unreachable!(),
                }

                (pkg.clone(), unpack_files)
            })
            .collect::<HashMap<DeploymentPackage, Vec<DownloadUrl>>>()
    }

    // key is file, value is hosts
    pub fn unpack_files_map(&self) -> Vec<UnPackFileLocation> {
        let pkg_links = self.unpack_links_map();
        let all_hosts = self.get_host_as_map();
        let mut result = vec![];
        for (pkg, hosts) in &all_hosts {
            let links = pkg_links.get(pkg).unwrap();
            links.iter().for_each(|link| {
                let unpack_files = hosts
                    .clone()
                    .iter()
                    .map(|host| UnPackFileLocation {
                        file: link.clone(),
                        host: host.to_string(),
                    })
                    .collect_vec();
                result.extend(unpack_files.into_iter());
            })
        }
        result
    }

    pub fn gen_all_monograph_configs(&self) -> anyhow::Result<Vec<PathBuf>> {
        let mut path_vec = match self.product() {
            Product::EloqSQL => vec![self.deployment.gen_eloqsql_config(None, None)?],
            Product::EloqKV => {
                vec![self.deployment.gen_eloqkv_node_config(None, None)?]
            }
        };
        let tx_host_ports = &self.deployment.tx_service.tx_host_ports;
        let standby_host_ports = &self.deployment.tx_service.standby_host_ports;
        let voter_host_ports = &self.deployment.tx_service.voter_host_ports;

        let all_config_path = match self.product() {
            Product::EloqSQL => tx_host_ports
                .iter()
                .flat_map(|hostports| {
                    hostports.split(',').map(|hostport| {
                        // Split `hostport` into host and port using ':'
                        let parts: Vec<&str> = hostport.split(':').collect();

                        // Ensure that both host and port exist
                        let host = parts
                            .first()
                            .expect("Error: Host part is missing")
                            .to_string();
                        let port = parts
                            .get(1)
                            .expect("Error: Port part is missing")
                            .to_string();

                        // Panic with a comment if host or port is empty
                        if host.is_empty() {
                            panic!("Error: Host in tx_host_ports cannot be empty");
                        }
                        if port.is_empty() {
                            panic!("Error: Port in tx_host_ports cannot be empty");
                        }

                        // Generate config using non-empty host and port
                        self.deployment
                            .gen_eloqsql_config(Some(host), Some(port))
                            .unwrap()
                    })
                })
                .collect_vec(),

            Product::EloqKV => tx_host_ports
                .iter()
                .flat_map(|hostports| {
                    hostports.split(',').map(|hostport| {
                        // Split `hostport` into host and port using ':'
                        let parts: Vec<&str> = hostport.split(':').collect();

                        // Ensure that both host and port exist
                        let host = parts
                            .first()
                            .expect("Error: Host part is missing")
                            .to_string();
                        let port = parts
                            .get(1)
                            .expect("Error: Port part is missing")
                            .to_string();

                        // Panic with a comment if host or port is empty
                        if host.is_empty() {
                            panic!("Error: Host in tx_host_ports cannot be empty");
                        }
                        if port.is_empty() {
                            panic!("Error: Port in tx_host_ports cannot be empty");
                        }

                        // Generate config using non-empty host and port
                        self.deployment
                            .gen_eloqkv_node_config(Some(host), Some(port))
                            .unwrap()
                    })
                })
                .collect(),
        };
        path_vec.extend(all_config_path);

        if let Some(standby_host_ports) = &standby_host_ports {
            if standby_host_ports.is_empty() {
                panic!("standby_host_ports is empty, but it was expected to contain values.");
            }

            let all_standby_config_path = match self.product() {
                Product::EloqSQL => vec![],
                Product::EloqKV => standby_host_ports
                    .iter()
                    .flat_map(|hosts_str| {
                        // Split `hosts_str` by '|' or ','
                        hosts_str.split(['|', ',']).map(|hostport| {
                            // Split `hostport` into host and port
                            let parts: Vec<&str> = hostport.split(':').collect();
                            let host = parts.first().unwrap_or(&"").to_string();
                            let port = parts.get(1).unwrap_or(&"").to_string();
                            self.deployment
                                .gen_eloqkv_node_config(Some(host), Some(port))
                                .unwrap()
                        })
                    })
                    .collect(),
            };
            path_vec.extend(all_standby_config_path);
        }

        if let Some(voter_host_ports) = &voter_host_ports {
            if voter_host_ports.is_empty() {
                panic!("voter_host_ports is empty, but it was expected to contain values.");
            }

            let all_voter_config_path = match self.product() {
                Product::EloqSQL => vec![],
                Product::EloqKV => voter_host_ports
                    .iter()
                    .flat_map(|hosts_str| {
                        // Split `hosts_str` by '|' or ','
                        hosts_str.split(['|', ',']).map(|hostport| {
                            // Split `hostport` into host and port
                            let parts: Vec<&str> = hostport.split(':').collect();
                            let host = parts.first().unwrap_or(&"").to_string();
                            let port = parts.get(1).unwrap_or(&"").to_string();
                            self.deployment
                                .gen_eloqkv_node_config(Some(host), Some(port))
                                .unwrap()
                        })
                    })
                    .collect(),
            };
            path_vec.extend(all_voter_config_path);
        }

        Ok(path_vec)
    }

    pub fn gen_all_mysql_exporter_config(&self) -> anyhow::Result<Option<Vec<PathBuf>>> {
        let deployment_ref = &self.deployment;
        if let Some(monitor) = deployment_ref.monitor.as_ref() {
            let mysql_port = deployment_ref.client_port();
            let tx_host_ports = &deployment_ref.tx_service.tx_host_ports;
            let config_path = tx_host_ports
                .iter()
                .map(|host_port| {
                    let parts: Vec<&str> = host_port.split(':').collect();
                    let host = parts[0];
                    monitor
                        .gen_mysql_exporter_connect_config(
                            &self.deployment.cluster_name,
                            host.to_string(),
                            mysql_port,
                        )
                        .unwrap()
                })
                .collect_vec();
            Ok(Some(config_path))
        } else {
            Ok(None)
        }
    }

    pub fn gen_bootstrap_db_script(&self) -> anyhow::Result<PathBuf> {
        gen_db_misc_files!(
            self,
            build_install_monograph_script,
            MONOGRAPH_INSTALL_SCRIPT,
            self.deployment.cluster_name.clone()
        )
    }

    pub fn gen_log_start_script(&self) -> anyhow::Result<Option<Vec<PathBuf>>> {
        let log_cmd_map_opt = self.build_log_start_script()?;
        if let Some(log_scripts) = log_cmd_map_opt.as_ref() {
            let log_cmd_locations = log_scripts
                .iter()
                .map(|(key, cmd)| {
                    let host = &key.host;
                    let port = key.port;
                    let cmd_file_name = format!("start_tx_log_{}.bash", port);
                    let dir = format!("{}/{}", self.deployment.cluster_name, host);
                    let script_location = create_upload_cluster_dir(&dir).join(cmd_file_name);
                    if let Err(write_err) = fs::write(script_location.clone(), cmd) {
                        error!("Failed gen Log start command. cause by {write_err:#?}");
                        panic!("Failed gen Log start command");
                    }
                    script_location
                })
                .collect_vec();
            Ok(Some(log_cmd_locations))
        } else {
            Ok(None)
        }
    }

    pub fn install_dir(&self) -> String {
        self.deployment.install_dir()
    }

    pub fn client_conn(&self) -> String {
        let bin = self.deployment.client_bin();
        match self.product() {
            Product::EloqSQL => format!(
                "LD_LIBRARY_PATH={}/lib:$LD_LIBRARY_PATH {bin} --user={} -S /tmp/eloqsql{}.sock",
                self.deployment.tx_srv_home(),
                self.connection.username,
                self.deployment.client_port()
            ),
            Product::EloqKV => {
                let (host, port) = if let Some(codis) = &self.deployment.codis {
                    (codis.proxy.first().unwrap().as_str(), "19000")
                } else {
                    let host_port = self.deployment.tx_service.tx_host_ports.first().unwrap();
                    let parts: Vec<&str> = host_port.split(':').collect();
                    let host = parts[0];
                    let port = parts[1];
                    (host, port)
                };
                format!("{bin} -h {} -p {}", host, port)
            }
        }
    }

    pub fn product(&self) -> Product {
        self.deployment.product()
    }

    pub fn build_install_monograph_script(&self) -> anyhow::Result<String> {
        let install_db_template = config_template(MONOGRAPH_INSTALL_SCRIPT)?;
        let install_dir = self.install_dir();
        let tx_home = self.deployment.tx_srv_home();
        let malloc = if let Some(Version::Debug) = self.deployment.version() {
            export_asan(&self.deployment.asan_logs())
        } else {
            format!("export LD_PRELOAD={tx_home}/lib/libmimalloc.so.2")
        };
        let rs = fs::read_to_string(install_db_template.as_path())?;
        let final_script = rs
            .replace("${INSTALL_DIR}", &tx_home)
            .replace("${MALLOC}", &malloc)
            .replace(
                "${BS_INI}",
                &format!(
                    "{install_dir}/{}/my_local.cnf",
                    self.deployment.cluster_name
                ),
            )
            .replace("${DATA_DIR}", &format!("{tx_home}/datafarm"));
        Ok(final_script)
    }

    pub fn build_log_start_script(&self) -> anyhow::Result<Option<HashMap<LogProcessKey, String>>> {
        if let Some(log_srv) = self.deployment.log_service.as_ref() {
            let log_start_template_path = config_template(START_LOG_TEMPLATE)?;
            let log_start_template = fs::read_to_string(log_start_template_path.as_path())?;
            let all_start_cmd_by_hosts = log_srv.log_start_cmd();
            let log_home_dir = self.deployment.log_srv_home();
            let version = self.deployment.version.as_ref().unwrap();
            let cmd_scripts = all_start_cmd_by_hosts
                .iter()
                .flat_map(|(host, cmd_items)| {
                    cmd_items
                        .iter()
                        .map(|cmd_items| {
                            let curr_member = &cmd_items.log_member;
                            let bthread_concurrency = log_srv.bthread_concurrency.unwrap_or(6);
                            let cmd_script = log_start_template
                                .replace("${LOG_INSTALL_DIR}", log_home_dir.as_str())
                                .replace("${GROUP_MEMBERS}", &cmd_items.group_members_config)
                                .replace("${NODE_ID}", curr_member.node_id.to_string().as_str())
                                .replace(
                                    "${LOG_SERVER_PORT}",
                                    curr_member.port.to_string().as_str(),
                                )
                                .replace("${STORAGE_DIR}", curr_member.storage_path.as_str())
                                .replace("${ASAN_OPTS}", ASAN_OPTIONS)
                                .replace("${VERSION}", version)
                                .replace(
                                    "${LOG_GROUP_REPLICA_NUM}",
                                    &log_srv.log_replica().to_string(),
                                )
                                .replace(
                                    "${BTHREAD_CONCURRENCY}",
                                    &bthread_concurrency.to_string(),
                                );
                            (
                                LogProcessKey {
                                    host: host.clone(),
                                    port: curr_member.port,
                                },
                                cmd_script,
                            )
                        })
                        .collect::<HashMap<LogProcessKey, String>>()
                })
                .collect::<HashMap<LogProcessKey, String>>();
            Ok(Some(cmd_scripts))
        } else {
            Ok(None)
        }
    }

    fn read_config_from_file(path: String) -> anyhow::Result<Self> {
        let content = fs::read_to_string(path)?
            .replace("${USER}", &whoami::username())
            .replace(&format!("${{{HOME_DIR}}}"), &std::env::var(HOME_DIR)?);
        let deployment_config = serde_yaml::from_str::<DeployConfig>(&content)?;
        Ok(deployment_config)
    }

    pub fn get_host_as_map(&self) -> HashMap<DeploymentPackage, Vec<String>> {
        HashMap::from([
            (
                DeploymentPackage::MonographTx,
                self.get_host_list(DeploymentPackage::MonographTx),
            ),
            (
                DeploymentPackage::MonographStandby,
                self.get_host_list(DeploymentPackage::MonographStandby),
            ),
            (
                DeploymentPackage::MonographVoter,
                self.get_host_list(DeploymentPackage::MonographVoter),
            ),
            (
                DeploymentPackage::MonographLog,
                self.get_host_list(DeploymentPackage::MonographLog),
            ),
            (
                DeploymentPackage::Storage,
                self.get_host_list(DeploymentPackage::Storage),
            ),
            (
                DeploymentPackage::Prometheus,
                self.get_host_list(DeploymentPackage::Prometheus),
            ),
            (
                DeploymentPackage::Grafana,
                self.get_host_list(DeploymentPackage::Grafana),
            ),
            (
                DeploymentPackage::Codis,
                self.get_host_list(DeploymentPackage::Codis),
            ),
        ])
    }

    pub fn get_unique_host_list(&self) -> Vec<String> {
        let all_hosts = all_hosts_merge!(
            self,
            MonographTx,
            MonographStandby,
            MonographVoter,
            MonographLog,
            Storage,
            Grafana,
            Prometheus,
            Codis
        );
        all_hosts.iter().unique().cloned().collect_vec()
    }

    pub fn get_host_list(&self, service: DeploymentPackage) -> Vec<String> {
        self.deployment.get_host_list(service)
    }

    pub fn get_host_port_list(&self, service: DeploymentPackage) -> Vec<String> {
        let result = self.deployment.get_host_port_list(service.clone());
        info!("get_host_port_list for service {:?}: {:?}", service, result);
        result
    }

    pub fn merge_and_deduplicate(&self, mut vec1: Vec<String>, vec2: Vec<String>) -> Vec<String> {
        let mut seen = HashSet::new();
        // Remove duplicates from vec1 in place, keeping the first occurrence
        vec1.retain(|s| seen.insert(s.clone()));
        // Append unique elements from vec2 that aren't already in vec1
        for s in vec2 {
            if seen.insert(s.clone()) {
                vec1.push(s);
            }
        }
        vec1
    }

    pub fn load_from_string(config_content: String) -> anyhow::Result<Self> {
        let deployment_config_rs = serde_yaml::from_str::<DeployConfig>(config_content.as_str());
        if let Ok(deployment_config) = deployment_config_rs {
            Ok(deployment_config)
        } else {
            Err(anyhow!(deployment_config_rs.err().unwrap().to_string()))
        }
    }

    pub fn get_monograph_storage(&self) -> anyhow::Result<StorageProvider> {
        if let Some(storage) = &self.deployment.storage_service {
            if let Some(sp) = storage.provider() {
                Ok(sp)
            } else {
                Err(anyhow!(ConfigErr::StorageConfigErr(
                    "storage provider is missing".to_owned()
                )))
            }
        } else {
            Err(anyhow!(ConfigErr::StorageConfigErr(
                "storage service is missing".to_owned()
            )))
        }
    }

    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(self).unwrap()
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }

    pub fn to_flat_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    /// Returns the runtime dependencies of MonographDB, with different return values depending on the installation platform.
    /// for example: ubuntu_runtime_deps, centos_runtime_deps
    pub fn load_runtime_deps_by_os(os: &str) -> Result<Vec<String>> {
        let deps_path = config_template(&format!("runtime_deps_{os}"))?;
        let deps_file_opened = File::open(deps_path)?;
        let deps_string = BufReader::new(deps_file_opened)
            .lines()
            .filter_map(|line_rs| match line_rs {
                Ok(line) => Some(line),
                Err(err) => {
                    error!("invalid config line: {err}");
                    None
                }
            })
            .collect();
        Ok(deps_string)
    }

    pub fn load(path: Option<String>) -> anyhow::Result<Self> {
        let path_string = config_path_string(path)?;
        info!("DeploymentConfig load file from {}", path_string);
        let config = DeployConfig::read_config_from_file(path_string.clone())
            .map_err(|err| anyhow!("{path_string}: {err}"))?;
        config.connection.auth.check_keypair()?;
        Ok(config)
    }

    /// By default, the directory where cluster_mgr is located includes config dir.
    /// If it does not exist, users need to specify the config location through environment variables (MONO_CLUSTER_MGR_CONF).
    /// Recursively traverse all dashboard files
    pub fn load_monitor_dashboard(&self) -> Vec<String> {
        let base = config_template("dashboard").expect("dashbord config not found");
        info!("dashboard_path {base:?}");
        let mut paths = vec![base.join("node")];
        match self.deployment.product {
            Product::EloqSQL => {
                paths.push(base.join("eloqsql"));
                paths.push(base.join("mysql"));
            }
            Product::EloqKV => {
                paths.push(base.join("eloqkv"));
            }
        }
        if let Some(storage) = &self.deployment.storage_service {
            if storage.inner_cass().is_some() {
                paths.push(base.join("cassandra"));
            }
        }
        paths
            .into_iter()
            .flat_map(|p| {
                walkdir::WalkDir::new(p)
                    .into_iter()
                    .filter_map(|curr_path| curr_path.ok())
                    .filter(|entry| entry.path().is_file())
                    .map(|entry| entry.path().to_str().unwrap().to_string())
            })
            .collect_vec()
    }

    pub fn abstract_info(&self) -> DeployAbstract {
        let store = if let Some(storage) = &self.deployment.storage_service {
            storage.pretty_name()
        } else {
            // if not set, use rocksdb as default
            "rocksdb".to_string()
        };

        DeployAbstract {
            name: self.deployment.cluster_name.clone(),
            product: self.deployment.product(),
            store,
            version: self.deployment.version.clone().unwrap_or_default(),
            user: self.connection.username.clone(),
        }
    }
}

#[derive(tabled::Tabled, Clone, Debug)]
pub struct DeployAbstract {
    name: String,
    product: Product,
    store: String,
    version: String,
    user: String,
}

#[derive(tabled::Tabled, Clone, Debug)]
pub struct VersionRow {
    pub product: String,
    pub store: String,
    pub version: String,
}

#[cfg(test)]
mod tests {
    use crate::config::config_base::DeployConfig;
    use crate::config::monitor::MONITOR_JOB_NAME;
    use crate::config::{DeploymentPackage, CONFIG_PATH_DIR};
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn set_config_path_env_and_get() -> String {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let config_path = manifest_dir.join("config");
        std::env::set_var(CONFIG_PATH_DIR, config_path.to_str().unwrap());
        config_path.to_str().unwrap().to_string()
    }

    #[test]
    pub fn test_cass_env_gen() {
        let config_path_string = set_config_path_env_and_get();
        let config_rs = DeployConfig::load(Some(format!("{config_path_string}/deployment.yaml")));
        assert!(config_rs.is_ok());
        let config = config_rs.unwrap();
        let monitor_opt = config.clone().deployment.monitor;
        let monitor = monitor_opt.as_ref();
        assert!(monitor.is_some());

        let mono_host_list = config.get_host_list(DeploymentPackage::MonographTx);
        let monitor = monitor.unwrap();
        let pro_rs = monitor.gen_prometheus_config(
            &config.deployment.cluster_name,
            HashMap::from([(MONITOR_JOB_NAME.to_string(), mono_host_list)]),
        );
        println!("pro_rs={pro_rs:#?}")
    }
}
