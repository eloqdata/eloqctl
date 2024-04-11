use crate::cli::{ssh, upload_dir, upload_host_dir, HOME_DIR};
use crate::config::connection::Connection;
use crate::config::deployment::{Codis, Deployment, Hardware, Product};
use crate::config::log_service::LogProcessKey;
use crate::config::{
    config_path_string, config_template, DeploymentPackage, StorageProvider, CONFIG_PATH_DIR,
    MONOGRAPH_INSTALL_SCRIPT, MONOGRAPH_INSTALL_TEMPLATE, START_LOG_TEMPLATE,
};
use crate::config::{ConfigErr, DownloadUrl};
use crate::gen_db_misc_files;
use anyhow::{anyhow, bail};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env::current_exe;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use tracing::{error, info};

pub const MONOGRAPH_TX_SERVICE_DIR: &str = "monograph-tx-service-release";
pub const REDIS_TX_SERVICE_DIR: &str = "monograph_redis";
pub const MONOGRAPH_LOG_SERVICE_DIR: &str = "monograph-log-service-release";

pub const MONOGRAPH_FILE_KEY: &str = "monograph_tx";
pub const MONOGRAPH_LOG_FILE_KEY: &str = "monograph_log";
pub const CASSANDRA_FILE_KEY: &str = "cassandra";
pub const PROMETHEUS_FILE_KEY: &str = "prometheus";
pub const GRAFANA_FILE_KEY: &str = "grafana";
pub const NODE_EXPORTER_FILE_KEY: &str = "node_exporter";
pub const MYSQL_EXPORTER_FILE_KEY: &str = "mysqld_exporter";
pub const CASSANDRA_COLLECTOR_AGENT_FILE_KEY: &str = "datastax-mcac-agent";
pub const DEPLOYMENT_CHECK_SUCCESS_TASK: &str = "deploy_check_success_task";

macro_rules! all_hosts_merge {
    ($config_ref:expr, $($pkg_name:ident $(,)?)*) => {{
        let mut all_hosts = vec![];
        $(
           let host_vec = $config_ref.get_host_list(DeploymentPackage::$pkg_name);
           if !host_vec.is_empty(){
               all_hosts.extend(host_vec.into_iter());
           }
        )*
        all_hosts
    }};
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

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct DeploymentConfig {
    pub connection: Connection,
    pub deployment: Deployment,
    pub conf_opts: Option<HashMap<String, bool>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnPackFileLocation {
    pub file: DownloadUrl,
    pub host: String,
}

impl DeploymentConfig {
    fn unpack_links_map(&self) -> HashMap<DeploymentPackage, Vec<DownloadUrl>> {
        let all_hosts = self.get_host_as_map();
        let cassandra_opt = self.deployment.storage_service.cassandra.as_ref();
        let monitor_opt = self.deployment.monitor.as_ref();
        let tx_image = &self.deployment.get_tx_image();
        let log_image = &self.deployment.log_image;
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
                            let cass_url =
                                DownloadUrl::from_url_str(cassandra.download_url.as_str()).unwrap();
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
                    DeploymentPackage::MonographTx => {
                        extract_monitor_link!(monitor_link, NODE_EXPORTER_FILE_KEY, unpack_files);
                        extract_monitor_link!(monitor_link, MYSQL_EXPORTER_FILE_KEY, unpack_files);
                        let tx_image = DownloadUrl::from_url_str(tx_image.as_str()).unwrap();
                        unpack_files.push(tx_image);
                    }
                    DeploymentPackage::MonographLog => {
                        if let Some(log_img_val) = log_image {
                            let log_image_link =
                                DownloadUrl::from_url_str(log_img_val.as_str()).unwrap();
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
        let install_dir = self.install_dir();
        let mut path_vec = match self.product() {
            Product::EloqSQL => vec![self
                .deployment
                .gen_monograph_config_by_host(None, install_dir.clone())?],
            Product::EloqKV => vec![self.deployment.gen_redis_config_by_host(None)?],
        };
        let db_hosts = &self.deployment.tx_service.host;
        let all_config_path = match self.product() {
            Product::EloqSQL => db_hosts
                .iter()
                .map(|host| {
                    self.deployment
                        .gen_monograph_config_by_host(Some(host.to_string()), install_dir.clone())
                        .unwrap()
                })
                .collect_vec(),
            Product::EloqKV => db_hosts
                .iter()
                .map(|host| {
                    self.deployment
                        .gen_redis_config_by_host(Some(host.to_string()))
                        .unwrap()
                })
                .collect_vec(),
        };
        path_vec.extend(all_config_path);
        Ok(path_vec)
    }

    pub fn gen_all_mysql_exporter_config(&self) -> anyhow::Result<Option<Vec<PathBuf>>> {
        let deployment_ref = &self.deployment;
        if let Some(monitor) = deployment_ref.monitor.as_ref() {
            let mysql_port = deployment_ref.cs_conn_port();
            let db_hosts = &deployment_ref.tx_service.host;
            let config_path = db_hosts
                .iter()
                .map(|host| {
                    monitor
                        .gen_mysql_exporter_connect_config(host.to_string(), mysql_port)
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
            MONOGRAPH_INSTALL_SCRIPT
        )
    }

    pub fn log_home_dir(&self) -> String {
        let cluster_install_dir = self.install_dir();
        format!("{cluster_install_dir}/monograph-log-service-release")
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
                    let script_location = upload_host_dir(host).join(cmd_file_name);
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
        match self.product() {
            Product::EloqSQL => format!(
                "{}/{}/install/bin/mariadb --user={} -S /tmp/mysql{}.sock",
                self.install_dir(),
                MONOGRAPH_TX_SERVICE_DIR,
                self.connection.username,
                self.deployment.cs_conn_port()
            ),
            Product::EloqKV => {
                let (host, port) = if let Some(codis) = &self.deployment.codis {
                    (codis.proxy.first().unwrap(), 19000)
                } else {
                    (
                        self.deployment.tx_service.host.first().unwrap(),
                        self.deployment.cs_conn_port(),
                    )
                };
                let redis_dir = format!("{}/{}", self.install_dir(), REDIS_TX_SERVICE_DIR);
                format!(
                    "LD_LIBRARY_PATH=$LD_LIBRARY_PATH:{}/lib {}/redis_cli -server {}:{}",
                    redis_dir, redis_dir, host, port
                )
            }
        }
    }

    pub fn product(&self) -> Product {
        self.deployment.product()
    }

    pub fn build_install_monograph_script(&self) -> anyhow::Result<String> {
        let install_db_template = config_template(MONOGRAPH_INSTALL_TEMPLATE)?;
        let remote_install_dir = self.install_dir();

        let rs = fs::read_to_string(install_db_template.as_path())?;
        let final_script = rs
            .replace(
                "_CASSANDRA_STORAGE_BIN",
                format!("{}/{}", remote_install_dir, "apache-cassandra/bin").as_str(),
            )
            .replace(
                "_MONOGRAPH_DB_HOME",
                format!("{remote_install_dir}/{MONOGRAPH_TX_SERVICE_DIR}/install",).as_str(),
            )
            .replace(
                "_MY_CONF",
                format!("{remote_install_dir}/my_local.cnf").as_str(),
            )
            .replace("_MY_CLUSTER_HOME", remote_install_dir.as_str());
        Ok(final_script)
    }

    pub fn build_log_start_script(&self) -> anyhow::Result<Option<HashMap<LogProcessKey, String>>> {
        if let Some(log_srv) = self.deployment.log_service.as_ref() {
            let log_start_template_path = config_template(START_LOG_TEMPLATE)?;
            let log_start_template = fs::read_to_string(log_start_template_path.as_path())?;
            let all_start_cmd_by_hosts = log_srv.log_start_cmd();
            let log_home_dir = self.log_home_dir();
            let cmd_scripts = all_start_cmd_by_hosts
                .iter()
                .flat_map(|(host, cmd_items)| {
                    cmd_items
                        .iter()
                        .map(|cmd_items| {
                            let curr_member = &cmd_items.log_member;
                            let cmd_script = log_start_template
                                .replace("_MY_LOG_INSTALL_DIR", log_home_dir.as_str())
                                .replace("_GROUP_MEMBERS", &cmd_items.group_members_config)
                                .replace("_GROUP_ID", curr_member.group_id.to_string().as_str())
                                .replace("_NODE_ID", curr_member.node_id.to_string().as_str())
                                .replace("_STORAGE_DIR", curr_member.storage_path.as_str());

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
        let deployment_config = serde_yaml::from_str::<DeploymentConfig>(&content)?;
        Ok(deployment_config)
    }

    pub fn get_host_as_map(&self) -> HashMap<DeploymentPackage, Vec<String>> {
        HashMap::from([
            (
                DeploymentPackage::MonographTx,
                self.get_host_list(DeploymentPackage::MonographTx),
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
            MonographLog,
            Storage,
            Grafana,
            Prometheus,
            Codis
        );
        all_hosts.iter().unique().cloned().collect_vec()
    }

    pub fn get_host_list(&self, service: DeploymentPackage) -> Vec<String> {
        let deployment = &self.deployment;
        match service {
            DeploymentPackage::Storage => {
                if let Some(cassandra) = &deployment.storage_service.cassandra {
                    cassandra.host.to_vec()
                } else {
                    vec![]
                }
            }
            DeploymentPackage::MonographLog => {
                if let Some(ref log_srv) = deployment.log_service {
                    log_srv.log_host_unique()
                } else {
                    vec![]
                }
            }
            DeploymentPackage::MonographTx => deployment.tx_service.host.to_vec(),
            DeploymentPackage::Prometheus => {
                extract_monitor_host!(deployment, prometheus)
            }
            DeploymentPackage::Grafana => {
                extract_monitor_host!(deployment, grafana)
            }
            DeploymentPackage::Codis => {
                if let Some(codis) = &deployment.codis {
                    let mut hosts = codis.proxy.clone();
                    hosts.push(codis.dashboard.clone());
                    hosts
                } else {
                    vec![]
                }
            }
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

    pub fn get_monograph_storage(&self) -> anyhow::Result<StorageProvider> {
        let storage = &self.deployment.storage_service;
        if let Some(sp) = storage.provider() {
            Ok(sp)
        } else {
            Err(anyhow!(ConfigErr::StorageConfigErr(format!(
                "storage provider is missing"
            ))))
        }
    }

    pub fn config_to_string(&self) -> String {
        serde_yaml::to_string(self).unwrap()
    }

    /// Returns the runtime dependencies of MonographDB, with different return values depending on the installation platform.
    /// for example: ubuntu_runtime_deps, centos_runtime_deps
    pub fn load_runtime_deps_by_os(
        os_name: Option<String>,
        os_version: Option<String>,
    ) -> anyhow::Result<(String, String, String)> {
        let config_path_var_rs = std::env::var(CONFIG_PATH_DIR);
        assert!(config_path_var_rs.is_ok());
        let (curr_os_name, curr_os_version) = if let Some(short_os_name) = os_name {
            let os_version_input = os_version.unwrap();
            (short_os_name.to_lowercase(), os_version_input)
        } else {
            let curr_os_name = sysinfo::System::name();
            let os_version = sysinfo::System::os_version();
            assert!(curr_os_name.is_some());
            assert!(os_version.is_some());
            let os_name = curr_os_name.unwrap().to_lowercase();
            if os_name.starts_with("ubuntu") {
                ("ubuntu".to_string(), os_version.unwrap())
            } else if os_name.starts_with("centos") {
                ("centos".to_string(), os_version.unwrap())
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
        Ok((curr_os_name, curr_os_version, deps_string))
    }

    pub fn load(path: Option<String>) -> anyhow::Result<Self> {
        let path_string = config_path_string(path)?;
        info!("DeploymentConfig load file from {}", path_string);
        let mut config = DeploymentConfig::read_config_from_file(path_string.clone())
            .map_err(|err| anyhow!("{path_string}: {err}"))?;
        if let Some(sshkey) = &config.connection.auth.keypair {
            if !Path::new(sshkey).exists() {
                bail!("ssh key {sshkey} not exist");
            }
        }
        config.deployment.image_by_version()?;
        Ok(config)
    }

    /// By default, the directory where cluster_mgr is located includes config dir.
    /// If it does not exist, users need to specify the config location through environment variables (MONO_CLUSTER_MGR_CONF).
    /// Recursively traverse all dashboard files
    pub fn load_monitor_dashboard(&self, path_buf_opt: Option<PathBuf>) -> Vec<String> {
        let dashboard_path = if let Some(spec_path_buf) = path_buf_opt {
            spec_path_buf
        } else if let Ok(curr) = current_exe() {
            let parent = curr.parent().unwrap();
            parent.join("config/dashboard")
        } else {
            return vec![];
        };
        println!("dashboard_path {dashboard_path:?}");
        walkdir::WalkDir::new(dashboard_path)
            .into_iter()
            .filter_map(|curr_path| curr_path.ok())
            .filter(|dir_entry| {
                let path = dir_entry.path();
                path.is_file()
            })
            .map(|file_entry| {
                let path = file_entry.path();
                let path_str = path.to_str().unwrap();
                path_str.to_string()
            })
            .collect_vec()
    }

    pub async fn scan_hardware(&mut self) -> anyhow::Result<()> {
        let hw_sh = tokio::fs::read_to_string(config_template("hardware.sh")?).await?;
        let mut hw_info = ssh::SSHSession::parallel(
            self.connection.ssh_auth_key().unwrap(),
            &self.connection.username,
            self.connection.ssh_port() as usize,
            self.get_unique_host_list(),
            &hw_sh,
        )
        .await?
        .into_iter()
        .map(|(host, out)| {
            let hw = out.trim().split(',').collect::<Vec<&str>>();
            info!("{} hardware: cpu={}, memory={}Mib", host, hw[0], hw[1]);
            let hw = Hardware {
                cpu: hw[0].parse().unwrap(),
                memory: hw[1].parse().unwrap(),
            };
            (host, hw)
        })
        .collect::<HashMap<String, Hardware>>();

        // user configured hardware info will override scan result
        if let Some(hw) = self.deployment.hardware.take() {
            hw_info.extend(hw);
        }
        self.deployment.hardware = Some(hw_info);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::config_base::DeploymentConfig;
    use crate::config::monitor::MONOGRAPH_TX_JOB_NAME;
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
        let config_rs =
            DeploymentConfig::load(Some(format!("{config_path_string}/deployment.yaml")));
        assert!(config_rs.is_ok());
        let config = config_rs.unwrap();
        let monitor_opt = config.clone().deployment.monitor;
        let monitor = monitor_opt.as_ref();
        assert!(monitor.is_some());

        let mono_host_list = config.get_host_list(DeploymentPackage::MonographTx);
        let monitor = monitor.unwrap();
        let pro_rs = monitor.gen_prometheus_config(HashMap::from([(
            MONOGRAPH_TX_JOB_NAME.to_string(),
            mono_host_list,
        )]));
        println!("pro_rs={pro_rs:#?}")
    }
}
