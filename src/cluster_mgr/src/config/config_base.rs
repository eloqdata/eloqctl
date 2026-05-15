use crate::all_hosts_merge;
use crate::cli::{create_upload_cluster_dir, HOME_DIR};
use crate::config::connection::Connection;
use crate::config::deployment::{Deployment, Product};
use crate::config::log_service::LogProcessKey;
use crate::config::{
    config_path_string, config_template, DeploymentPackage, StorageProvider, START_LOG_TEMPLATE,
};
use crate::config::{ConfigErr, DownloadUrl};
use anyhow::{anyhow, bail, Context, Result};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use tracing::{error, info};

pub const LOG_SERVICE_HOME: &str = "LogServer";

pub const ELOQ_FILE_KEY: &str = "eloq_tx";
pub const ELOQ_LOG_FILE_KEY: &str = "eloq_log";
pub const PROMETHEUS_FILE_KEY: &str = "prometheus";
pub const GRAFANA_FILE_KEY: &str = "grafana";
pub const NODE_EXPORTER_FILE_KEY: &str = "node_exporter";
pub const DEPLOYMENT_CHECK_SUCCESS_TASK: &str = "deploy_check_success_task";
pub const SCALED_CLUSTER_CONFIG: &str = "cluster_config";

pub const ASAN_OPTIONS: &str = "abort_on_error=1:disable_coredump=0:halt_on_error=0:fast_unwind_on_malloc=0:leak_check_at_exit=0:detect_stack_use_after_return=1";

pub fn export_asan(
    log: &str,
    fast_unwind_on_malloc: bool,
    detect_stack_use_after_return: bool,
) -> String {
    let asan_options = build_asan_options(fast_unwind_on_malloc, detect_stack_use_after_return);
    format!("export ASAN_OPTIONS={asan_options}:log_path={log}")
}

fn build_asan_options(fast_unwind_on_malloc: bool, detect_stack_use_after_return: bool) -> String {
    let mut options = ASAN_OPTIONS.to_string();
    if fast_unwind_on_malloc {
        options = options.replacen("fast_unwind_on_malloc=0", "fast_unwind_on_malloc=1", 1);
    }
    if !detect_stack_use_after_return {
        options = options.replacen(
            "detect_stack_use_after_return=1",
            "detect_stack_use_after_return=0",
            1,
        );
    }
    options
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
    #[serde(skip_serializing, skip_deserializing, default)]
    pub tx_version_override: Option<String>,
    #[serde(skip_serializing, skip_deserializing, default)]
    pub tx_image_override: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnPackFileLocation {
    pub file: DownloadUrl,
    pub host: String,
}

impl DeployConfig {
    fn validate_host_port_entry(
        field: &str,
        entry: &str,
        allow_group_separator: bool,
    ) -> Result<()> {
        let separators: &[_] = if allow_group_separator {
            &['|', ',']
        } else {
            &[',']
        };
        for token in entry.split(separators) {
            let token = token.trim();
            if token.is_empty() {
                continue;
            }
            let Some((host, port)) = token.rsplit_once(':') else {
                return Err(anyhow!("{field} entry '{token}' must use host:port format"));
            };
            if host.trim().is_empty() {
                return Err(anyhow!("{field} entry '{token}' has empty host"));
            }
            if port.parse::<u16>().is_err() {
                return Err(anyhow!("{field} entry '{token}' has invalid port '{port}'"));
            }
        }
        Ok(())
    }

    pub fn validate_topology(&self) -> Result<()> {
        if self.deployment.tx_service.tx_host_ports.is_empty() {
            return Err(anyhow!(
                "deployment.tx_service.tx_host_ports must not be empty"
            ));
        }
        for entry in &self.deployment.tx_service.tx_host_ports {
            Self::validate_host_port_entry("deployment.tx_service.tx_host_ports", entry, false)?;
        }
        if let Some(standby_host_ports) = &self.deployment.tx_service.standby_host_ports {
            if standby_host_ports.is_empty() {
                return Err(anyhow!(
                    "deployment.tx_service.standby_host_ports must not be empty when provided"
                ));
            }
            for entry in standby_host_ports {
                Self::validate_host_port_entry(
                    "deployment.tx_service.standby_host_ports",
                    entry,
                    true,
                )?;
            }
        }
        if let Some(voter_host_ports) = &self.deployment.tx_service.voter_host_ports {
            if voter_host_ports.is_empty() {
                return Err(anyhow!(
                    "deployment.tx_service.voter_host_ports must not be empty when provided"
                ));
            }
            for entry in voter_host_ports {
                Self::validate_host_port_entry(
                    "deployment.tx_service.voter_host_ports",
                    entry,
                    true,
                )?;
            }
        }

        if let Some(storage) = &self.deployment.storage_service {
            let provider_count = usize::from(storage.dynamodb.is_some())
                + usize::from(storage.rocksdb.is_some())
                + usize::from(storage.eloqdss.is_some());
            if provider_count == 0 {
                return Err(anyhow!(
                    "deployment.storage_service is provided but no storage provider is configured"
                ));
            }
            if provider_count > 1 {
                return Err(anyhow!(
                    "deployment.storage_service must configure only one provider among dynamodb, rocksdb, and eloqdss"
                ));
            }
        }

        if let Some(log_service) = &self.deployment.log_service {
            if log_service.nodes.is_empty() {
                return Err(anyhow!("deployment.log_service.nodes must not be empty"));
            }
            if log_service.replica == 0 {
                return Err(anyhow!(
                    "deployment.log_service.replica must be greater than 0"
                ));
            }
            for (idx, node) in log_service.nodes.iter().enumerate() {
                if node.host.trim().is_empty() {
                    return Err(anyhow!(
                        "deployment.log_service.nodes[{idx}].host must not be empty"
                    ));
                }
                if node.data_dir.is_empty() {
                    return Err(anyhow!(
                        "deployment.log_service.nodes[{idx}].data_dir must not be empty"
                    ));
                }
            }
        }

        if self.deployment.enable_wal.unwrap_or(false) && self.deployment.log_service.is_none() {
            return Err(anyhow!(
                "deployment.enable_wal=true requires deployment.log_service to be configured"
            ));
        }

        Ok(())
    }

    pub fn redis_password(&self, cli_password: Option<String>) -> Option<String> {
        cli_password.or_else(|| self.deployment.tx_service.requirepass.clone())
    }

    pub fn service_endpoint(&self, host: &str, port: u16) -> (String, u16) {
        self.connection.service_endpoint(host, port)
    }

    pub fn service_endpoint_url(&self, scheme: &str, host: &str, port: u16) -> String {
        let (endpoint_host, endpoint_port) = self.service_endpoint(host, port);
        format!("{scheme}://{endpoint_host}:{endpoint_port}")
    }

    pub fn service_host_port(&self, host_port: &str) -> String {
        let Some((host, port)) = host_port.rsplit_once(':') else {
            return host_port.to_string();
        };
        let Ok(port) = port.parse::<u16>() else {
            return host_port.to_string();
        };
        let (endpoint_host, endpoint_port) = self.service_endpoint(host, port);
        format!("{endpoint_host}:{endpoint_port}")
    }

    pub fn service_host_ports(&self, host_ports: Vec<String>) -> Vec<String> {
        host_ports
            .into_iter()
            .map(|host_port| self.service_host_port(&host_port))
            .collect()
    }

    fn unpack_links_map(&self) -> HashMap<DeploymentPackage, Vec<DownloadUrl>> {
        let all_hosts = self.get_host_as_map();
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
                    DeploymentPackage::Storage => {}
                    DeploymentPackage::EloqTx => {
                        extract_monitor_link!(monitor_link, NODE_EXPORTER_FILE_KEY, unpack_files);
                        let tx_image = DownloadUrl::from_url_str(tx_image).unwrap();
                        unpack_files.push(tx_image);
                    }
                    DeploymentPackage::EloqStandby => {
                        extract_monitor_link!(monitor_link, NODE_EXPORTER_FILE_KEY, unpack_files);
                        let tx_image = DownloadUrl::from_url_str(tx_image).unwrap();
                        unpack_files.push(tx_image);
                    }
                    DeploymentPackage::EloqVoter => {
                        extract_monitor_link!(monitor_link, NODE_EXPORTER_FILE_KEY, unpack_files);
                        let tx_image = DownloadUrl::from_url_str(tx_image).unwrap();
                        unpack_files.push(tx_image);
                    }
                    DeploymentPackage::EloqLog => {
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
                result.extend(unpack_files);
            })
        }
        result
    }

    pub fn gen_all_eloq_configs(&self) -> anyhow::Result<Vec<PathBuf>> {
        let mut path_vec = vec![self.deployment.gen_eloqkv_node_config(None, None)?];
        let tx_host_ports = &self.deployment.tx_service.tx_host_ports;
        let standby_host_ports = &self.deployment.tx_service.standby_host_ports;
        let voter_host_ports = &self.deployment.tx_service.voter_host_ports;

        let all_config_path = tx_host_ports
            .iter()
            .flat_map(|hostports| hostports.split(','))
            .map(|hostport| {
                let (host, port) = hostport.split_once(':').ok_or_else(|| {
                    anyhow::anyhow!("invalid deployment.tx_service.tx_host_ports entry: {hostport}")
                })?;
                if host.is_empty() || port.is_empty() {
                    anyhow::bail!("invalid deployment.tx_service.tx_host_ports entry: {hostport}");
                }
                self.deployment
                    .gen_eloqkv_node_config(Some(host.to_string()), Some(port.to_string()))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        path_vec.extend(all_config_path);

        if let Some(standby_host_ports) = &standby_host_ports {
            if standby_host_ports.is_empty() {
                bail!("standby_host_ports is empty, but it was expected to contain values.");
            }

            let all_standby_config_path = standby_host_ports
                .iter()
                .flat_map(|hosts_str| hosts_str.split(['|', ',']))
                .map(|hostport| {
                    let (host, port) = hostport.split_once(':').ok_or_else(|| {
                        anyhow::anyhow!(
                            "invalid deployment.tx_service.standby_host_ports entry: {hostport}"
                        )
                    })?;
                    if host.is_empty() || port.is_empty() {
                        anyhow::bail!(
                            "invalid deployment.tx_service.standby_host_ports entry: {hostport}"
                        );
                    }
                    self.deployment
                        .gen_eloqkv_node_config(Some(host.to_string()), Some(port.to_string()))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            path_vec.extend(all_standby_config_path);
        }

        if let Some(voter_host_ports) = &voter_host_ports {
            if voter_host_ports.is_empty() {
                bail!("voter_host_ports is empty, but it was expected to contain values.");
            }

            let all_voter_config_path = voter_host_ports
                .iter()
                .flat_map(|hosts_str| hosts_str.split(['|', ',']))
                .map(|hostport| {
                    let (host, port) = hostport.split_once(':').ok_or_else(|| {
                        anyhow::anyhow!(
                            "invalid deployment.tx_service.voter_host_ports entry: {hostport}"
                        )
                    })?;
                    if host.is_empty() || port.is_empty() {
                        anyhow::bail!(
                            "invalid deployment.tx_service.voter_host_ports entry: {hostport}"
                        );
                    }
                    self.deployment
                        .gen_eloqkv_node_config(Some(host.to_string()), Some(port.to_string()))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            path_vec.extend(all_voter_config_path);
        }

        Ok(path_vec)
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
                        bail!("Failed gen Log start command");
                    }
                    Ok(script_location)
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
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
        let host_port = self.deployment.tx_service.tx_host_ports.first().unwrap();
        let parts: Vec<&str> = host_port.split(':').collect();
        format!("{bin} -h {} -p {}", parts[0], parts[1])
    }

    pub fn product(&self) -> Product {
        self.deployment.product()
    }

    pub fn build_log_start_script(&self) -> anyhow::Result<Option<HashMap<LogProcessKey, String>>> {
        if let Some(log_srv) = self.deployment.log_service.as_ref() {
            let log_start_template_path = config_template(START_LOG_TEMPLATE)?;
            let log_start_template = fs::read_to_string(log_start_template_path.as_path())?;
            let all_start_cmd_by_hosts = log_srv.log_start_cmd();
            let log_home_dir = self.deployment.log_srv_home();
            let version = self.deployment.version.as_ref().unwrap();
            let fast_unwind_on_malloc = self.deployment.uses_eloqstore_storage();
            let detect_stack_use_after_return = !self.deployment.uses_eloqstore_storage();
            let asan_options =
                build_asan_options(fast_unwind_on_malloc, detect_stack_use_after_return);
            let cmd_scripts = all_start_cmd_by_hosts
                .iter()
                .flat_map(|(host, cmd_items)| {
                    cmd_items
                        .iter()
                        .map(|cmd_items| {
                            let curr_member = &cmd_items.log_member;
                            let bthread_concurrency = log_srv.bthread_concurrency.unwrap_or(6);
                            let rocks_flag = log_srv.rocks_cloud_flag();
                            let cmd_script = log_start_template
                                .replace("${LOG_INSTALL_DIR}", log_home_dir.as_str())
                                .replace("${GROUP_MEMBERS}", &cmd_items.group_members_config)
                                .replace("${NODE_ID}", curr_member.node_id.to_string().as_str())
                                .replace(
                                    "${LOG_SERVER_PORT}",
                                    curr_member.port.to_string().as_str(),
                                )
                                .replace("${STORAGE_DIR}", curr_member.storage_path.as_str())
                                .replace("${ASAN_OPTS}", &asan_options)
                                .replace("${VERSION}", version)
                                .replace(
                                    "${LOG_GROUP_REPLICA_NUM}",
                                    &log_srv.log_replica().to_string(),
                                )
                                .replace("${BTHREAD_CONCURRENCY}", &bthread_concurrency.to_string())
                                .replace("${ROCKS_CLOUD_FLAG}", rocks_flag.as_str());
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
                DeploymentPackage::EloqTx,
                self.get_host_list(DeploymentPackage::EloqTx),
            ),
            (
                DeploymentPackage::EloqStandby,
                self.get_host_list(DeploymentPackage::EloqStandby),
            ),
            (
                DeploymentPackage::EloqVoter,
                self.get_host_list(DeploymentPackage::EloqVoter),
            ),
            (
                DeploymentPackage::EloqLog,
                self.get_host_list(DeploymentPackage::EloqLog),
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
        ])
    }

    pub fn get_unique_host_list(&self) -> Vec<String> {
        let all_hosts = all_hosts_merge!(
            self,
            EloqTx,
            EloqStandby,
            EloqVoter,
            EloqLog,
            Storage,
            Grafana,
            Prometheus
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
        let deployment_config = serde_yaml::from_str::<DeployConfig>(config_content.as_str())
            .context("failed to parse deployment config")?;
        deployment_config.validate_topology()?;
        Ok(deployment_config)
    }

    pub fn get_eloq_storage(&self) -> anyhow::Result<StorageProvider> {
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

    pub fn to_yaml(&self) -> anyhow::Result<String> {
        Ok(serde_yaml::to_string(self)?)
    }

    /// Returns the runtime dependencies of EloqDB, with different return values depending on the installation platform.
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
        config.validate_topology()?;
        config.connection.auth.check_keypair()?;
        Ok(config)
    }

    /// By default, the directory where cluster_mgr is located includes config dir.
    /// If it does not exist, users need to specify the config location through environment variables (ELOQ_CLUSTER_MGR_CONF).
    /// Recursively traverse all dashboard files
    pub fn load_monitor_dashboard(&self) -> Vec<String> {
        let base = config_template("dashboard").expect("dashbord config not found");
        info!("dashboard_path {base:?}");
        let paths = vec![base.join("node"), base.join("eloqkv")];
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
    use std::fs;
    use std::path::PathBuf;

    fn set_config_path_env_and_get() -> String {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let config_path = manifest_dir.join("config");
        std::env::set_var(CONFIG_PATH_DIR, config_path.to_str().unwrap());
        config_path.to_str().unwrap().to_string()
    }

    fn load_test_config(example_name: &str) -> DeployConfig {
        let config_path_string = set_config_path_env_and_get();
        let example_path = format!("{config_path_string}/examples/{example_name}");
        let content = fs::read_to_string(example_path)
            .unwrap()
            .replace("${USER}", &whoami::username())
            .replace("${HOME}", &std::env::var("HOME").unwrap_or_default());
        serde_yaml::from_str::<DeployConfig>(&content).unwrap()
    }

    fn load_yaml(yaml: &str) -> anyhow::Result<DeployConfig> {
        DeployConfig::load_from_string(yaml.to_string())
    }

    fn base_yaml(tx_service: &str, extra: &str) -> String {
        format!(
            r#"
connection:
  username: test
  auth_type: keypair
  auth:
    keypair: /tmp/nonexistent-test-key
deployment:
  cluster_name: test-cluster
  product: EloqKV
  version: latest
  install_dir: /tmp
{tx_service}
{extra}
"#
        )
    }

    fn default_tx_service() -> &'static str {
        "  tx_service:\n    tx_host_ports: [127.0.0.1:6379]"
    }

    #[test]
    fn validate_rejects_invalid_tx_host_port() {
        let yaml = base_yaml(
            r#"  tx_service:
    tx_host_ports: [127.0.0.1]
"#,
            "",
        );
        let err = load_yaml(&yaml).unwrap_err().to_string();
        assert!(err.contains("host:port"), "unexpected error: {err}");
    }

    #[test]
    fn validate_rejects_enable_wal_without_log_service() {
        let yaml = base_yaml(default_tx_service(), "  enable_wal: true\n");
        let err = load_yaml(&yaml).unwrap_err().to_string();
        assert!(err.contains("enable_wal=true"));
    }

    #[test]
    fn validate_rejects_multiple_storage_providers() {
        let yaml = base_yaml(
            default_tx_service(),
            r#"  storage_service:
    dynamodb:
      access_key_id: id
      secret_key: secret
      region: us-east-1
      endpoint: http://localhost:8000
    rocksdb: !LOCAL {}
"#,
        );
        let err = load_yaml(&yaml).unwrap_err().to_string();
        assert!(err.contains("only one provider"));
    }

    #[test]
    fn validate_accepts_grouped_standby_and_voter_ports() {
        let yaml = base_yaml(
            r#"  tx_service:
    tx_host_ports: [127.0.0.1:6379]
    standby_host_ports: [127.0.0.2:6379|127.0.0.3:6379]
    voter_host_ports: [127.0.0.4:6379,127.0.0.5:6379]
"#,
            "",
        );
        load_yaml(&yaml).unwrap();
    }

    #[test]
    pub fn test_prometheus_config_gen() {
        let config = load_test_config("eloqkv_rocks_s3.yaml");
        let monitor_opt = config.clone().deployment.monitor;
        let monitor = monitor_opt.as_ref();
        assert!(monitor.is_some());

        let eloq_host_list = config.get_host_list(DeploymentPackage::EloqTx);
        let monitor = monitor.unwrap();
        let pro_rs = monitor.gen_prometheus_config(
            &config.deployment.cluster_name,
            HashMap::from([(MONITOR_JOB_NAME.to_string(), eloq_host_list)]),
        );
        println!("pro_rs={pro_rs:#?}")
    }

    #[test]
    pub fn test_redis_password_falls_back_to_requirepass() {
        let mut config = load_test_config("eloqkv_rocksdb.yaml");

        config.deployment.tx_service.requirepass = Some("requirepass-from-config".to_string());

        assert_eq!(
            config.redis_password(None),
            Some("requirepass-from-config".to_string())
        );
        assert_eq!(
            config.redis_password(Some("password-from-cli".to_string())),
            Some("password-from-cli".to_string())
        );
    }
}
