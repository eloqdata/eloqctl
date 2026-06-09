use crate::cli::reconcile::{
    ObservedCluster, ObservedServiceStatus, ReconcileAction, ReconcilePlan,
};
use crate::cli::task::backup_utils::split_manifests;
use crate::cli::task::download_task::DownloadTask;
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::Config;
use crate::cli::task::local_extract_task::LocalExtractTask;
use crate::cli::task::monitor_ctl_task::MonitorCtlTask;
use crate::cli::task::task_base::{
    set_verbose_task_output, TaskExecutionContext, TaskId, TaskMgr, TaskResultPair,
};
use crate::cli::task::task_controller::task_action_summary;
use crate::cli::task::unpack_file_task::UnpackFileTask;
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, upload_tasks, UploadTaskBuilderType,
};
use crate::cli::util::{cpu_arch, file_pg_bar, os_id, os_major_version};
use crate::cli::{
    download_dir, upload_dir, MonitorCommand, SubCommand, UpdateMonitorComponent, HOME_DIR,
};
use crate::cli::{BackupCommand, ProxyCommand};
use crate::config::config_base::{DeployConfig, UploadFile, VersionRow};
use crate::config::config_base::{
    ALERTMANAGER_FILE_KEY, GRAFANA_FILE_KEY, NODE_EXPORTER_FILE_KEY, PROMETHEUSALERT_FILE_KEY,
    PROMETHEUS_FILE_KEY,
};
use crate::config::deployment::{version_digits, Deployment, Product};
use crate::config::monitor::{
    Alertmanager, Exporter, Grafana, Monitor, Prometheus, ALERTMANAGER_WEBHOOK_ADAPTER_DEFAULT_URL,
    ALERTMANAGER_WEBHOOK_ADAPTER_PORT,
};
use crate::config::proxy_config_base::ProxyConfig;
use crate::config::storage_service_config::{
    DataStoreServiceBackend, DataStoreServiceMode, RocksDB, RocksLocal, StorageService,
};
use crate::config::{
    DeploymentPackage, DownloadUrl, StorageProvider, CDN, CONFIG_PATH_DIR, UPLOAD_PATH_DIR,
};
use crate::github_release::{
    fetch_eloqkv_releases, find_eloqkv_asset, find_product_asset, list_versions_from_releases,
};
use crate::state::proxy_operation::{ProxyEntity, ProxyOperation};
use crate::state::state_base::{QueryCondition, StateOperation};
use crate::state::state_mgr::{StateMgr, PROXY_STATE, STATE_MGR};
use crate::StateValue;
use anyhow::{anyhow, bail, Result};
use futures::StreamExt;
use itertools::Itertools;
use owo_colors::OwoColorize;
use serde_yaml::Value as YamlValue;
use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use std::{env, fs};
use tracing::{info, warn};

#[derive(tabled::Tabled)]
struct StatusSummaryRow {
    host: String,
    service: String,
    port: String,
    status: String,
    detail: String,
}

pub static NOT_PRINT_TASK_RESULT: &str = "NOT_PRINT_TASK_RESULT";

pub static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(60))
        .http1_only()
        .tcp_keepalive(Duration::from_secs(60))
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(4)
        .build()
        .expect("can't init http client")
});

pub static HTTP_INTERNAL: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(10))
        .no_proxy()
        .build()
        .expect("can't init http client for internal use")
});

const DEFAULT_PROMETHEUS_URL: &str =
    "https://github.com/prometheus/prometheus/releases/download/v2.42.0/prometheus-2.42.0.linux-amd64.tar.gz";
const DEFAULT_ALERTMANAGER_URL: &str =
    "https://github.com/prometheus/alertmanager/releases/download/v0.32.1/alertmanager-0.32.1.linux-amd64.tar.gz";
const DEFAULT_GRAFANA_URL: &str =
    "https://dl.grafana.com/oss/release/grafana-9.3.6.linux-amd64.tar.gz";
const DEFAULT_PROMETHEUSALERT_URL: &str = ALERTMANAGER_WEBHOOK_ADAPTER_DEFAULT_URL;
const DEFAULT_NODE_EXPORTER_URL: &str =
    "https://github.com/prometheus/node_exporter/releases/download/v1.5.0/node_exporter-1.5.0.linux-amd64.tar.gz";

pub struct CmdExecutor {
    task_mgr: Arc<TaskMgr>,
    state_mgr: Arc<StateMgr>,
    pub home: PathBuf,
}

struct ClusterMutationLock {
    path: PathBuf,
}

impl ClusterMutationLock {
    fn sanitize_cluster_name(cluster: &str) -> String {
        cluster
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    }

    fn acquire(lock_dir: &Path, cluster: &str, command: &str) -> Result<Self> {
        std::fs::create_dir_all(lock_dir)?;
        let lock_name = Self::sanitize_cluster_name(cluster);
        let path = lock_dir.join(format!("{lock_name}.lock"));
        let content = format!(
            "pid={}\ncluster={}\ncommand={}\ncreated_at={}\n",
            std::process::id(),
            cluster,
            command,
            chrono::Utc::now().to_rfc3339()
        );
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(mut file) => {
                file.write_all(content.as_bytes())?;
                Ok(Self { path })
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                if Self::is_stale(&path) {
                    fs::remove_file(&path)?;
                    return Self::acquire(lock_dir, cluster, command);
                }
                let detail = fs::read_to_string(&path).unwrap_or_default();
                bail!(
                    "cluster '{cluster}' is already being modified. lock={} {}",
                    path.display(),
                    detail.replace('\n', "; ")
                )
            }
            Err(err) => Err(err.into()),
        }
    }

    fn parse_pid(lock_content: &str) -> Option<u32> {
        lock_content.lines().find_map(|line| {
            line.strip_prefix("pid=")
                .and_then(|pid| pid.trim().parse::<u32>().ok())
        })
    }

    /// Check whether a process with the given pid exists by examining /proc/<pid>.
    /// This is Linux-specific; on other platforms we conservatively assume the process
    /// still exists to avoid incorrectly reclaiming a live lock.
    #[cfg(target_os = "linux")]
    fn process_exists(pid: u32) -> bool {
        Path::new("/proc").join(pid.to_string()).exists()
    }

    #[cfg(not(target_os = "linux"))]
    fn process_exists(_pid: u32) -> bool {
        true
    }

    fn is_stale(path: &Path) -> bool {
        let Ok(content) = fs::read_to_string(path) else {
            return false;
        };
        let Some(pid) = Self::parse_pid(&content) else {
            return false;
        };
        !Self::process_exists(pid)
    }
}

impl Drop for ClusterMutationLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl CmdExecutor {
    fn release_asset_url(
        product: &str,
        version: &str,
        store: &str,
        os: &str,
        arch: &str,
    ) -> String {
        format!(
            "https://github.com/eloqdata/eloqkv/releases/download/{version}/{product}-{version}-{store}-{os}-{arch}.tar.gz"
        )
    }

    pub fn new(home: PathBuf) -> Self {
        Self {
            task_mgr: Arc::new(TaskMgr::new()),
            state_mgr: Arc::new(STATE_MGR.clone()),
            home,
        }
    }

    pub fn home_init(home: Option<PathBuf>) -> Result<PathBuf> {
        let home = match home {
            Some(home) => {
                env::set_var(HOME_DIR, &home);
                home
            }
            None => match env::var(HOME_DIR) {
                Ok(v) => PathBuf::from(v),
                Err(_) => {
                    let home = env::current_dir()?;
                    env::set_var(HOME_DIR, &home);
                    home
                }
            },
        };
        // check config directory
        let cnf_dir = home.join("config");
        if !cnf_dir.exists() {
            bail!("config path not exist: {} ", cnf_dir.display());
        }
        env::set_var(CONFIG_PATH_DIR, cnf_dir);
        let down_dir = home.join("download");
        if !down_dir.exists() {
            std::fs::create_dir(down_dir)?;
        }
        let up_dir = home.join("upload");
        if !up_dir.exists() {
            std::fs::create_dir(up_dir.clone())?;
        }
        env::set_var(UPLOAD_PATH_DIR, up_dir);
        let log_dir = home.join("logs");
        if !log_dir.exists() {
            std::fs::create_dir(log_dir)?;
        }
        Ok(home)
    }

    pub fn task_mgr(&self) -> &Arc<TaskMgr> {
        &self.task_mgr
    }

    pub fn state_mgr(&self) -> &Arc<StateMgr> {
        &self.state_mgr
    }

    pub fn os_vers(&self) -> String {
        format!("{}{}", os_id(), os_major_version())
    }

    fn summarize_status_rows(task_results: &[TaskResultPair]) -> Vec<StatusSummaryRow> {
        ObservedCluster::from_task_results("status".to_string(), task_results)
            .services
            .iter()
            .map(Self::status_row_from_observed)
            .collect_vec()
    }

    fn summarize_monitor_status_rows(task_results: &[TaskResultPair]) -> Vec<StatusSummaryRow> {
        ObservedCluster::from_task_results("monitor".to_string(), task_results)
            .services
            .iter()
            .filter(|service| {
                matches!(
                    service.service.as_str(),
                    "prometheus"
                        | "alertmanager"
                        | "grafana"
                        | "alertmanager_webhook_adapter"
                        | "node_exporter"
                )
            })
            .map(Self::status_row_from_observed)
            .collect_vec()
    }

    fn status_row_from_observed(service: &ObservedServiceStatus) -> StatusSummaryRow {
        StatusSummaryRow {
            host: service.host.clone(),
            service: service.service.clone(),
            port: service.port.clone(),
            status: service.status.to_string(),
            detail: if service.detail.len() > 120 {
                format!("{}...", &service.detail[..120])
            } else {
                service.detail.clone()
            },
        }
    }

    fn print_status_summary(task_results: &[TaskResultPair]) {
        let rows = Self::summarize_status_rows(task_results);
        if rows.is_empty() {
            println!("No status rows found.");
            return;
        }
        println!("{}", tabled::Table::new(rows));
    }

    fn print_monitor_status_summary(task_results: &[TaskResultPair]) {
        let rows = Self::summarize_monitor_status_rows(task_results);
        if rows.is_empty() {
            println!("No monitor status rows found.");
            return;
        }
        println!("{}", tabled::Table::new(rows));
    }

    fn print_monitor_log_hints() {
        println!("Control-node execution log:");
        println!("\t~/.eloqctl/logs/last-monitor.log");
        println!("Remote component runtime logs:");
        println!("\tprometheus -> /tmp/eloq_prometheus.log");
        println!("\talertmanager -> /tmp/eloq_alertmanager.log");
        println!("\tgrafana -> /tmp/eloq_grafana_server.log");
        println!("\talertmanager-webhook-adapter -> /tmp/eloq_alertmanager_webhook_adapter.log");
        println!("\tnode_exporter -> /tmp/eloq_node_exporter.log");
    }

    fn shell_single_quote(value: &str) -> String {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }

    fn redis_cli_connect_command(
        config: &DeployConfig,
        cli_password: Option<&str>,
    ) -> Option<String> {
        let host_port = config.deployment.tx_service.tx_host_ports.first()?;
        let parts: Vec<&str> = host_port.split(':').collect();
        let port = parts.get(1)?.parse::<u16>().ok()?;
        let (host, port) = config.service_endpoint(parts[0], port);

        let mut command = format!("redis-cli -h {host} -p {port}");
        if let Some(password) = config.redis_password(cli_password.map(ToOwned::to_owned)) {
            command.push_str(" -a ");
            command.push_str(&Self::shell_single_quote(&password));
        }
        Some(command)
    }

    fn print_status_connect_hint(config: &DeployConfig, cli_password: Option<&str>) {
        if let Some(command) = Self::redis_cli_connect_command(config, cli_password) {
            println!("Connect with redis-cli:\n\t{command}");
        }
    }

    fn print_status_output(
        cmd: &SubCommand,
        deploy_config: &DeployConfig,
        task_results: &[TaskResultPair],
        verbose: bool,
    ) {
        if let SubCommand::Status {
            detail, password, ..
        } = cmd
        {
            if !detail {
                Self::print_status_summary(task_results);
                Self::print_status_connect_hint(deploy_config, password.as_deref());
                if !verbose {
                    println!("Tip: use `--verbose` to show per-task execution details.");
                }
            } else {
                Self::print_status_connect_hint(deploy_config, password.as_deref());
            }
        } else if let SubCommand::Monitor {
            command: MonitorCommand::Status { .. },
            ..
        } = cmd
        {
            Self::print_monitor_status_summary(task_results);
            Self::print_monitor_log_hints();
            if !verbose {
                println!("Tip: use `--verbose` to show per-operation execution details.");
            }
        }
    }

    async fn observe_cluster(
        &'static self,
        config: &DeployConfig,
        wait: Option<u16>,
    ) -> Result<ObservedCluster> {
        let status_cmd = SubCommand::Status {
            cluster: config.deployment.cluster_name.clone(),
            user: None,
            password: None,
            wait,
            detail: false,
        };
        let results = self
            .task_mgr
            .run_tasks_with_error_break(status_cmd, Config::Cluster(config.clone()), false)
            .await?;
        self.task_mgr.drain_task_results().await?;
        Ok(ObservedCluster::from_task_results(
            config.deployment.cluster_name.clone(),
            &results,
        ))
    }

    async fn cluster_has_running_tx(&'static self, config: &DeployConfig) -> Result<bool> {
        let observed = self.observe_cluster(config, None).await?;
        Ok(observed.has_running_service("tx"))
    }

    async fn ensure_critical_services_healthy(
        &'static self,
        config: &DeployConfig,
        operation: &str,
    ) -> Result<()> {
        use crate::cli::task::eloq_tx_ctl_task::RedisProbe;
        let observed = self.observe_cluster(config, Some(30)).await?;
        if observed.has_errors() || !observed.unavailable_services().is_empty() {
            observed.print();
            bail!(
                "cannot {operation}: live cluster '{}' is not healthy",
                config.deployment.cluster_name
            );
        }
        // Verify standby and voter nodes are actually serving Redis, not just running.
        let password = config.redis_password(None);
        let mut redis_nodes = config.get_host_port_list(DeploymentPackage::EloqStandby);
        redis_nodes.extend(config.get_host_port_list(DeploymentPackage::EloqVoter));
        let probes = redis_nodes.iter().map(|node| {
            let (host, port_str) = node.split_once(':').unwrap_or((node, "6379"));
            let port: u16 = port_str.parse().unwrap_or(6379);
            let tls_enabled = config.deployment.tls_enabled();
            let (endpoint_host, endpoint_port) = config.service_endpoint(host, port);
            let probe = RedisProbe::with_password_and_tls(
                endpoint_host,
                endpoint_port,
                password.clone(),
                tls_enabled,
            );
            let node = node.clone();
            let operation = operation.to_string();
            async move {
                probe.probe(10).await.map_err(|e| {
                    anyhow!(
                        "cannot {operation}: node {node} is not serving Redis after waiting: {e}"
                    )
                })
            }
        });
        let results = futures::future::join_all(probes).await;
        for result in results {
            result?;
        }
        Ok(())
    }

    fn idempotent_noop_message(cmd: &SubCommand, config: &DeployConfig) -> Option<String> {
        match cmd {
            SubCommand::Scale {
                add_nodes,
                remove_nodes,
                ..
            } => {
                let mut existing = config.get_host_port_list(DeploymentPackage::EloqTx);
                existing.extend(config.get_host_port_list(DeploymentPackage::EloqStandby));
                existing.extend(config.get_host_port_list(DeploymentPackage::EloqVoter));
                if !add_nodes.is_empty() && add_nodes.iter().all(|node| existing.contains(node)) {
                    Some("All requested nodes already exist; scale add is a no-op.".to_string())
                } else if !remove_nodes.is_empty()
                    && remove_nodes.iter().all(|node| !existing.contains(node))
                {
                    Some(
                        "All requested nodes are already absent; scale remove is a no-op."
                            .to_string(),
                    )
                } else {
                    None
                }
            }
            SubCommand::ScaleLog {
                add_nodes,
                remove_nodes,
                ..
            } => {
                let log_nodes = config
                    .deployment
                    .log_service
                    .as_ref()
                    .map(|log| {
                        log.nodes
                            .iter()
                            .map(|node| format!("{}:{}", node.host, node.port))
                            .collect_vec()
                    })
                    .unwrap_or_default();
                if !add_nodes.is_empty() && add_nodes.iter().all(|node| log_nodes.contains(node)) {
                    Some(
                        "All requested log nodes already exist; scalelog add is a no-op."
                            .to_string(),
                    )
                } else if !remove_nodes.is_empty()
                    && remove_nodes.iter().all(|node| !log_nodes.contains(node))
                {
                    Some(
                        "All requested log nodes are already absent; scalelog remove is a no-op."
                            .to_string(),
                    )
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub async fn list_cluster_names(&self) -> Result<Vec<String>> {
        let deployments = self.state_mgr.list_deployments().await?;
        Ok(deployments
            .iter()
            .map(|deploy| deploy.deployment.cluster_name.clone())
            .sorted()
            .collect_vec())
    }

    fn dir_home(&self) -> &str {
        self.home.to_str().expect("invalid home directory")
    }

    fn dir_config(&self) -> PathBuf {
        self.home.join("config")
    }

    fn lock_dir(&self) -> PathBuf {
        self.home.join("locks")
    }

    fn mutation_cluster_from_cmd(cmd: &SubCommand) -> Option<String> {
        match cmd {
            SubCommand::Demo { product, .. } => Some(format!("demo-{product}")),
            SubCommand::Launch { topology_file, .. }
            | SubCommand::Deploy { topology_file }
            | SubCommand::RunDeps { topology_file } => {
                DeployConfig::load(Some(topology_file.clone()))
                    .ok()
                    .map(|config| config.deployment.cluster_name)
            }
            SubCommand::Apply { topology_file } => DeployConfig::load(Some(topology_file.clone()))
                .ok()
                .map(|config| config.deployment.cluster_name),
            SubCommand::Install { cluster }
            | SubCommand::Start { cluster, .. }
            | SubCommand::Stop { cluster, .. }
            | SubCommand::Restart { cluster }
            | SubCommand::UpdateConf { cluster, .. }
            | SubCommand::Remove { cluster, .. }
            | SubCommand::LogService { cluster, .. }
            | SubCommand::Update {
                cluster: Some(cluster),
                ..
            }
            | SubCommand::Scale { cluster, .. }
            | SubCommand::ScaleLog { cluster, .. }
            | SubCommand::Backup { cluster, .. }
            | SubCommand::Failover { cluster, .. } => Some(cluster.clone()),
            SubCommand::Monitor { cluster, command } => {
                Self::monitor_cluster(cluster, command).ok()
            }
            _ => None,
        }
    }

    fn acquire_mutation_lock(&self, cmd: &SubCommand) -> Result<Option<ClusterMutationLock>> {
        if let Some(cluster) = Self::mutation_cluster_from_cmd(cmd) {
            Ok(Some(ClusterMutationLock::acquire(
                &self.lock_dir(),
                &cluster,
                cmd.as_ref(),
            )?))
        } else {
            Ok(None)
        }
    }

    fn dir_download(&self) -> PathBuf {
        self.home.join("download")
    }

    fn monitor_cluster(
        parent_cluster: &Option<String>,
        command: &MonitorCommand,
    ) -> anyhow::Result<String> {
        let child_cluster = match command {
            MonitorCommand::Start { cluster, .. }
            | MonitorCommand::Stop { cluster, .. }
            | MonitorCommand::Restart { cluster, .. }
            | MonitorCommand::Status { cluster, .. }
            | MonitorCommand::Update { cluster, .. } => cluster.clone(),
        };

        match (parent_cluster.clone(), child_cluster) {
            (Some(parent), Some(child)) if parent != child => Err(anyhow!(
                "monitor cluster is ambiguous: parent --cluster is '{}' but subcommand cluster is '{}'",
                parent, child
            )),
            (Some(parent), _) => Ok(parent),
            (_, Some(child)) => Ok(child),
            (None, None) => Err(anyhow!(
                "monitor cluster is required; use `eloqctl monitor --cluster <cluster> ...` or `eloqctl monitor ... <cluster>`"
            )),
        }
    }

    fn update_monitor_component_name(component: &UpdateMonitorComponent) -> &'static str {
        match component {
            UpdateMonitorComponent::Grafana => "grafana",
            UpdateMonitorComponent::Prometheus => "prometheus",
            UpdateMonitorComponent::Alertmanager => "alertmanager",
            UpdateMonitorComponent::Prometheusalert => "alertmanager-webhook-adapter",
            UpdateMonitorComponent::NodeExporter => "node_exporter",
        }
    }

    fn monitor_update_component_key(component: &UpdateMonitorComponent) -> &'static str {
        match component {
            UpdateMonitorComponent::Grafana => GRAFANA_FILE_KEY,
            UpdateMonitorComponent::Prometheus => PROMETHEUS_FILE_KEY,
            UpdateMonitorComponent::Alertmanager => ALERTMANAGER_FILE_KEY,
            UpdateMonitorComponent::Prometheusalert => PROMETHEUSALERT_FILE_KEY,
            UpdateMonitorComponent::NodeExporter => NODE_EXPORTER_FILE_KEY,
        }
    }

    fn monitor_update_component_home(component: &UpdateMonitorComponent) -> &'static str {
        match component {
            UpdateMonitorComponent::Grafana => GRAFANA_FILE_KEY,
            UpdateMonitorComponent::Prometheus => PROMETHEUS_FILE_KEY,
            UpdateMonitorComponent::Alertmanager => ALERTMANAGER_FILE_KEY,
            UpdateMonitorComponent::Prometheusalert => PROMETHEUSALERT_FILE_KEY,
            UpdateMonitorComponent::NodeExporter => NODE_EXPORTER_FILE_KEY,
        }
    }

    fn monitor_update_uses_staged_dir(component: &UpdateMonitorComponent) -> bool {
        matches!(component, UpdateMonitorComponent::Prometheusalert)
    }

    fn monitor_update_components(
        component: &UpdateMonitorComponent,
    ) -> Vec<UpdateMonitorComponent> {
        match component {
            UpdateMonitorComponent::Alertmanager => vec![
                UpdateMonitorComponent::Alertmanager,
                UpdateMonitorComponent::Prometheusalert,
            ],
            other => vec![other.clone()],
        }
    }

    fn monitor_update_component_hosts(
        &self,
        config: &DeployConfig,
        component: &UpdateMonitorComponent,
    ) -> Vec<String> {
        let monitor_hosts = config.get_host_as_map();
        match component {
            UpdateMonitorComponent::Grafana => monitor_hosts
                .get(&DeploymentPackage::Grafana)
                .cloned()
                .unwrap_or_default(),
            UpdateMonitorComponent::Prometheus => monitor_hosts
                .get(&DeploymentPackage::Prometheus)
                .cloned()
                .unwrap_or_default(),
            UpdateMonitorComponent::Alertmanager => monitor_hosts
                .get(&DeploymentPackage::Alertmanager)
                .cloned()
                .unwrap_or_default(),
            UpdateMonitorComponent::Prometheusalert => monitor_hosts
                .get(&DeploymentPackage::PrometheusAlert)
                .cloned()
                .unwrap_or_default(),
            UpdateMonitorComponent::NodeExporter => config.get_unique_host_list(),
        }
    }

    fn monitor_update_stop_tasks(
        &self,
        cmd: SubCommand,
        config: &DeployConfig,
        component: &UpdateMonitorComponent,
    ) -> indexmap::IndexMap<TaskId, crate::cli::task::task_base::TaskInstance> {
        match component {
            UpdateMonitorComponent::Grafana => MonitorCtlTask::grafana_ctl_task(cmd, config),
            UpdateMonitorComponent::Prometheus => MonitorCtlTask::prometheus_ctl_task(cmd, config),
            UpdateMonitorComponent::Alertmanager => {
                MonitorCtlTask::alertmanager_ctl_task(cmd, config)
            }
            UpdateMonitorComponent::Prometheusalert => {
                MonitorCtlTask::prometheusalert_ctl_task(cmd, config)
            }
            UpdateMonitorComponent::NodeExporter => MonitorCtlTask::exporter_ctl_task(cmd, config),
        }
    }

    fn monitor_default_host(deployment: &Deployment) -> String {
        deployment
            .monitor
            .as_ref()
            .and_then(|monitor| {
                monitor
                    .prometheus
                    .as_ref()
                    .map(|component| component.host.clone())
                    .or_else(|| {
                        monitor
                            .grafana
                            .as_ref()
                            .map(|component| component.host.clone())
                    })
                    .or_else(|| {
                        monitor
                            .alertmanager
                            .as_ref()
                            .map(|component| component.host.clone())
                    })
            })
            .or_else(|| {
                deployment
                    .tx_service
                    .tx_host_ports
                    .first()
                    .and_then(|host_port| host_port.split(':').next().map(ToOwned::to_owned))
            })
            .unwrap_or_else(|| "127.0.0.1".to_string())
    }

    fn configure_monitor_update(
        config: &mut DeployConfig,
        component: &UpdateMonitorComponent,
        monitor_url: Option<String>,
        feishu_robot_urls: Vec<String>,
    ) -> Result<()> {
        let default_host = Self::monitor_default_host(&config.deployment);
        let prometheus_preferred_host = config
            .deployment
            .monitor
            .as_ref()
            .and_then(|monitor| {
                monitor
                    .prometheus
                    .as_ref()
                    .map(|component| component.host.clone())
            })
            .unwrap_or_else(|| default_host.clone());
        let monitor = config.deployment.monitor.get_or_insert(Monitor {
            data_dir: None,
            prometheus: None,
            alertmanager: None,
            grafana: None,
            node_exporter: None,
            eloq_metrics: None,
        });

        match component {
            UpdateMonitorComponent::Grafana => {
                let grafana = monitor.grafana.get_or_insert_with(|| Grafana {
                    download_url: DEFAULT_GRAFANA_URL.to_string(),
                    port: 3301,
                    host: default_host.clone(),
                });
                if let Some(url) = monitor_url {
                    grafana.download_url = url;
                }
            }
            UpdateMonitorComponent::Prometheus => {
                let prometheus = monitor.prometheus.get_or_insert_with(|| Prometheus {
                    download_url: DEFAULT_PROMETHEUS_URL.to_string(),
                    port: 9500,
                    host: default_host.clone(),
                    retention_time: None,
                    retention_size: None,
                    remote_write_urls: None,
                    alertmanager_targets: None,
                    alert_thresholds: None,
                });
                if let Some(url) = monitor_url {
                    prometheus.download_url = url;
                }
            }
            UpdateMonitorComponent::Alertmanager => {
                let alertmanager = monitor.alertmanager.get_or_insert_with(|| Alertmanager {
                    download_url: DEFAULT_ALERTMANAGER_URL.to_string(),
                    port: 9093,
                    host: prometheus_preferred_host.clone(),
                    feishu_robot_urls: None,
                    webhook_adapter_download_url: DEFAULT_PROMETHEUSALERT_URL.to_string(),
                    webhook_adapter_port: ALERTMANAGER_WEBHOOK_ADAPTER_PORT,
                });
                if let Some(url) = monitor_url {
                    alertmanager.download_url = url;
                }
                if !feishu_robot_urls.is_empty() {
                    alertmanager.feishu_robot_urls = Some(feishu_robot_urls);
                }
            }
            UpdateMonitorComponent::Prometheusalert => {
                let alertmanager = monitor.alertmanager.get_or_insert_with(|| Alertmanager {
                    download_url: DEFAULT_ALERTMANAGER_URL.to_string(),
                    port: 9093,
                    host: prometheus_preferred_host.clone(),
                    feishu_robot_urls: None,
                    webhook_adapter_download_url: DEFAULT_PROMETHEUSALERT_URL.to_string(),
                    webhook_adapter_port: ALERTMANAGER_WEBHOOK_ADAPTER_PORT,
                });
                if let Some(url) = monitor_url {
                    alertmanager.webhook_adapter_download_url = url;
                }
                if !feishu_robot_urls.is_empty() {
                    alertmanager.feishu_robot_urls = Some(feishu_robot_urls);
                }
            }
            UpdateMonitorComponent::NodeExporter => {
                let exporter = monitor.node_exporter.get_or_insert_with(|| Exporter {
                    url: DEFAULT_NODE_EXPORTER_URL.to_string(),
                    port: 9200,
                });
                if let Some(url) = monitor_url {
                    exporter.url = url;
                }
            }
        }
        Ok(())
    }

    fn monitor_update_context(
        &self,
        cmd: &SubCommand,
        config: &DeployConfig,
        component: &UpdateMonitorComponent,
    ) -> TaskExecutionContext {
        let mut executable = indexmap::IndexMap::new();
        let mut barrier = vec![];
        let components = Self::monitor_update_components(component);

        let download = DownloadTask::instances(DownloadTask::from_urls(
            components
                .iter()
                .map(|component| self.monitor_update_component_url(config, component))
                .collect(),
        ));
        if !download.is_empty() {
            barrier.push(download.len());
            executable.extend(download);
        }

        let staged_entries = components
            .iter()
            .filter(|component| Self::monitor_update_uses_staged_dir(component))
            .map(|component| {
                (
                    Self::monitor_update_component_key(component).to_string(),
                    DownloadUrl::from_url_str(
                        &self.monitor_update_component_url(config, component),
                    )
                    .unwrap(),
                    Self::monitor_update_component_home(component).to_string(),
                )
            })
            .collect_vec();
        if !staged_entries.is_empty() {
            let extract = LocalExtractTask::from_urls(staged_entries);
            if !extract.is_empty() {
                barrier.push(extract.len());
                executable.extend(extract);
            }
        }

        let mut upload_before_clean = indexmap::IndexMap::new();
        for component in &components {
            if !Self::monitor_update_uses_staged_dir(component) {
                upload_before_clean.extend(self.monitor_update_package_uploads(config, component));
            }
        }
        if !upload_before_clean.is_empty() {
            barrier.push(upload_before_clean.len());
            executable.extend(upload_before_clean);
        }

        let stop_cmd = SubCommand::Monitor {
            cluster: Some(match cmd {
                SubCommand::Update {
                    cluster: Some(cluster),
                    ..
                } => cluster.clone(),
                SubCommand::Monitor { cluster, .. } => {
                    cluster.clone().expect("monitor cluster is resolved")
                }
                _ => unreachable!(),
            }),
            command: MonitorCommand::Stop {
                cluster: None,
                components: vec![],
            },
        };
        let start_cmd = SubCommand::Monitor {
            cluster: Some(match cmd {
                SubCommand::Update {
                    cluster: Some(cluster),
                    ..
                } => cluster.clone(),
                SubCommand::Monitor { cluster, .. } => {
                    cluster.clone().expect("monitor cluster is resolved")
                }
                _ => unreachable!(),
            }),
            command: MonitorCommand::Start {
                cluster: None,
                components: vec![],
            },
        };

        let mut stop_tasks = indexmap::IndexMap::new();
        for component in &components {
            stop_tasks.extend(self.monitor_update_stop_tasks(stop_cmd.clone(), config, component));
        }
        if !stop_tasks.is_empty() {
            barrier.push(stop_tasks.len());
            executable.extend(stop_tasks);
        }

        let mut clean_tasks = indexmap::IndexMap::new();
        for component in &components {
            clean_tasks.extend(self.monitor_update_clean_tasks(config, component));
        }
        if !clean_tasks.is_empty() {
            barrier.push(clean_tasks.len());
            executable.extend(clean_tasks);
        }

        let mut upload_after_clean = indexmap::IndexMap::new();
        for component in &components {
            if Self::monitor_update_uses_staged_dir(component) {
                upload_after_clean.extend(self.monitor_update_package_uploads(config, component));
            }
        }
        if !upload_after_clean.is_empty() {
            barrier.push(upload_after_clean.len());
            executable.extend(upload_after_clean);
        }

        let mut unpack_tasks = indexmap::IndexMap::new();
        for component in &components {
            if !Self::monitor_update_uses_staged_dir(component) {
                unpack_tasks.extend(
                    self.monitor_update_component_hosts(config, component)
                        .into_iter()
                        .map(|host| {
                            UnpackFileTask::make_task_pair(
                                config,
                                &host,
                                &self.monitor_update_component_file_name(config, component),
                                Self::monitor_update_component_home(component),
                                vec![],
                            )
                        })
                        .collect::<indexmap::IndexMap<_, _>>(),
                );
            }
        }
        if !unpack_tasks.is_empty() {
            barrier.push(unpack_tasks.len());
            executable.extend(unpack_tasks);
        }

        let monitor_conf_upload = upload_tasks(
            UploadTaskBuilderType::MonitorConf,
            &Config::Cluster(config.clone()),
        );
        if !monitor_conf_upload.is_empty() {
            barrier.push(monitor_conf_upload.len());
            executable.extend(monitor_conf_upload);
        }

        let mut start_tasks = indexmap::IndexMap::new();
        for component in &components {
            start_tasks.extend(self.monitor_update_stop_tasks(
                start_cmd.clone(),
                config,
                component,
            ));
        }
        if !start_tasks.is_empty() {
            barrier.push(start_tasks.len());
            executable.extend(start_tasks);
        }

        TaskExecutionContext {
            task_group: format!(
                "update-monitor-{}",
                Self::update_monitor_component_name(component)
            ),
            barrier: Some(barrier),
            executable,
        }
    }

    fn monitor_update_package_uploads(
        &self,
        config: &DeployConfig,
        component: &UpdateMonitorComponent,
    ) -> indexmap::IndexMap<
        crate::cli::task::task_base::TaskId,
        crate::cli::task::task_base::TaskInstance,
    > {
        let file_name = self.monitor_update_component_file_name(config, component);
        let file_key = Self::monitor_update_component_key(component);
        let hosts = self.monitor_update_component_hosts(config, component);
        let source = if Self::monitor_update_uses_staged_dir(component) {
            LocalExtractTask::staged_dir_for(
                &DownloadUrl::from_url_str(&self.monitor_update_component_url(config, component))
                    .unwrap(),
                Self::monitor_update_component_home(component),
            )
            .to_string_lossy()
            .to_string()
        } else {
            download_dir()
                .join(&file_name)
                .to_string_lossy()
                .to_string()
        };
        let source_host = get_source_host(None);
        let config_ref = Config::Cluster(config.clone());

        hosts
            .into_iter()
            .map(|host| {
                let upload = UploadFile {
                    source: source.clone(),
                    dest: if Self::monitor_update_uses_staged_dir(component) {
                        format!(
                            "{}/{}",
                            config.install_dir(),
                            Self::monitor_update_component_home(component)
                        )
                    } else {
                        format!("{}/{}", config.install_dir(), file_name)
                    },
                    extension: if Self::monitor_update_uses_staged_dir(component) {
                        "dir".to_string()
                    } else {
                        file_name
                            .split('.')
                            .next_back()
                            .unwrap_or("pkg")
                            .to_string()
                    },
                    host: host.clone(),
                    copy_dir: Self::monitor_update_uses_staged_dir(component),
                    delete_remote: Self::monitor_update_uses_staged_dir(component),
                };
                build_task_instance(
                    source_host.clone(),
                    upload,
                    &config_ref,
                    "deploy",
                    &format!("upload_monitor_pkg_{}_{}", file_key, host),
                )
            })
            .collect()
    }

    fn monitor_update_clean_tasks(
        &self,
        config: &DeployConfig,
        component: &UpdateMonitorComponent,
    ) -> indexmap::IndexMap<
        crate::cli::task::task_base::TaskId,
        crate::cli::task::task_base::TaskInstance,
    > {
        let home = Self::monitor_update_component_home(component).to_string();
        let hosts = self.monitor_update_component_hosts(config, component);
        let task_name = format!(
            "clean_monitor_{}",
            Self::update_monitor_component_name(component)
        );

        let clean_cmd = format!(
            "rm -rf {install_dir}/{home} && mkdir -p {install_dir}/{home}",
            install_dir = config.install_dir(),
        );
        ExecCustomCommand::build_task_by_host(
            clean_cmd,
            &Config::Cluster(config.clone()),
            hosts,
            Some(task_name),
        )
    }

    fn monitor_update_component_url(
        &self,
        config: &DeployConfig,
        component: &UpdateMonitorComponent,
    ) -> String {
        match component {
            UpdateMonitorComponent::Grafana => config
                .deployment
                .monitor
                .as_ref()
                .and_then(|m| m.grafana.as_ref())
                .map(|g| g.download_url.clone())
                .unwrap(),
            UpdateMonitorComponent::Prometheus => config
                .deployment
                .monitor
                .as_ref()
                .and_then(|m| m.prometheus.as_ref())
                .map(|p| p.download_url.clone())
                .unwrap(),
            UpdateMonitorComponent::Alertmanager => config
                .deployment
                .monitor
                .as_ref()
                .and_then(|m| m.alertmanager.as_ref())
                .map(|a| a.download_url.clone())
                .unwrap(),
            UpdateMonitorComponent::Prometheusalert => config
                .deployment
                .monitor
                .as_ref()
                .and_then(|m| m.alertmanager.as_ref())
                .map(|a| a.webhook_adapter_download_url.clone())
                .unwrap(),
            UpdateMonitorComponent::NodeExporter => config
                .deployment
                .monitor
                .as_ref()
                .and_then(|m| m.node_exporter.as_ref())
                .map(|n| n.url.clone())
                .unwrap(),
        }
    }

    fn monitor_update_component_file_name(
        &self,
        config: &DeployConfig,
        component: &UpdateMonitorComponent,
    ) -> String {
        self.monitor_update_component_url(config, component)
            .split('/')
            .next_back()
            .unwrap()
            .to_string()
    }

    async fn run_update_command(
        &'static self,
        cmd: SubCommand,
        config: Config,
        quiet: bool,
        verbose: bool,
    ) -> Result<()> {
        let Config::Cluster(cfg) = config.clone() else {
            unreachable!();
        };

        let download_only = match &cmd {
            SubCommand::Update { download_only, .. } => *download_only,
            SubCommand::Monitor {
                cluster: _,
                command: MonitorCommand::Update { .. },
            } => false,
            _ => unreachable!(),
        };

        if download_only {
            let tasks = DownloadTask::from_config(&cfg)?;
            let context = TaskExecutionContext {
                task_group: "update-download".to_string(),
                barrier: Some(vec![tasks.len()]),
                executable: tasks,
            };
            let outfile = if quiet {
                let f = fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .open(self.home.join("task-result"))?;
                Some(f)
            } else {
                None
            };
            let task_mgr = self.task_mgr.clone();
            let recv_rs_and_print_join = tokio::task::spawn(async move {
                task_mgr
                    .write_task_result(outfile, verbose)
                    .await
                    .expect("write task result failed");
            });
            self.task_mgr.run_context(context, config, true).await?;
            recv_rs_and_print_join.await?;
            println!("required tarballs downloaded into cache");
            return Ok(());
        }

        if let SubCommand::Monitor {
            cluster: _,
            command: MonitorCommand::Update { component, .. },
        } = &cmd
        {
            let context = self.monitor_update_context(&cmd, &cfg, component);
            let outfile = if quiet {
                let f = fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .open(self.home.join("task-result"))?;
                Some(f)
            } else {
                None
            };
            let task_mgr = self.task_mgr.clone();
            let recv_rs_and_print_join = tokio::task::spawn(async move {
                task_mgr
                    .write_task_result(outfile, verbose)
                    .await
                    .expect("write task result failed");
            });
            self.task_mgr.run_context(context, config, true).await?;
            recv_rs_and_print_join.await?;
            self.save_deployment_config(&cfg, true).await?;
            println!(
                "cluster {} monitor component {} is updated!",
                cfg.deployment.cluster_name,
                Self::update_monitor_component_name(component)
            );
            return Ok(());
        }

        self.run_impl_default(cmd, Some(config), quiet, verbose)
            .await
    }

    async fn run_impl_default(
        &'static self,
        mut cmd: SubCommand,
        option_config: Option<Config>,
        quiet: bool,
        verbose: bool,
    ) -> Result<()> {
        let config = match option_config {
            Some(config) => config,
            None => self.get_config(cmd.clone()).await?,
        };

        match config {
            Config::Cluster(mut deploy_config) => {
                let cmd_for_match = cmd.clone();
                match cmd_for_match {
                    SubCommand::Connect { .. } => {
                        println!("{}", deploy_config.client_conn());
                    }
                    SubCommand::Export { cluster, output } => {
                        let yaml_str = deploy_config.to_yaml()?;
                        if let Some(path) = output {
                            std::fs::write(path.clone(), &yaml_str)
                                .map_err(|e| anyhow!("failed to write {}: {}", path, e))?;
                            println!("Exported cluster '{}' topology to {}", cluster, path);
                        } else {
                            println!("{}", yaml_str);
                        }
                    }
                    _ => {
                        if let SubCommand::Scale {
                            version: requested_version,
                            add_nodes,
                            remove_nodes,
                            ..
                        } = &mut cmd
                        {
                            if let Some(version_value) = requested_version.clone() {
                                if add_nodes.is_empty() {
                                    bail!("--version requires at least one entry in --add-nodes");
                                }
                                if !remove_nodes.is_empty() {
                                    bail!("--version cannot be combined with --remove-nodes");
                                }

                                let mut version_config = deploy_config.clone();
                                version_config.deployment.version = Some(version_value.clone());
                                version_config.deployment.tx_service.image = None;
                                if let Some(logsrv) = version_config.deployment.log_service.as_mut()
                                {
                                    logsrv.image = None;
                                }

                                self.resolve_version(&mut version_config.deployment).await?;
                                let resolved_version =
                                    version_config.deployment.version.clone().unwrap();
                                let resolved_image =
                                    version_config.deployment.tx_service.image.clone().unwrap();

                                deploy_config.tx_version_override = Some(resolved_version.clone());
                                deploy_config.tx_image_override = Some(resolved_image);

                                *requested_version = Some(resolved_version);
                            }
                        }

                        if let Some(noop_msg) = Self::idempotent_noop_message(&cmd, &deploy_config)
                        {
                            println!("{noop_msg}");
                            return Ok(());
                        }

                        let task_mgr = self.task_mgr.clone();
                        let outfile = if quiet {
                            let f = fs::OpenOptions::new()
                                .create(true)
                                .truncate(true)
                                .open(self.home.join("operation-result"))?;
                            Some(f)
                        } else {
                            None
                        };

                        let recv_rs_and_print_join = tokio::task::spawn(async move {
                            task_mgr
                                .write_task_result(outfile, verbose)
                                .await
                                .expect("write operation result failed");
                        });

                        let rs = self
                            .task_mgr
                            .run_tasks(cmd.clone(), Config::Cluster(deploy_config.clone()))
                            .await?;
                        recv_rs_and_print_join.await?;
                        info!(r#"all tasks complete. task_size={}"#, rs.len());
                        Self::print_status_output(&cmd, &deploy_config, &rs, verbose);

                        let should_verify_after_finish = matches!(
                            &cmd,
                            SubCommand::Update {
                                cluster: Some(_),
                                ..
                            } | SubCommand::UpdateConf { .. }
                                | SubCommand::Scale { .. }
                                | SubCommand::ScaleLog { .. }
                        );
                        let final_config = deploy_config.clone();
                        let verify_cluster = match &cmd {
                            SubCommand::Scale { cluster, .. }
                            | SubCommand::ScaleLog { cluster, .. } => Some(cluster.clone()),
                            _ => None,
                        };
                        self.finishing(cmd, Config::Cluster(deploy_config)).await?;
                        if should_verify_after_finish {
                            let verify_config = if let Some(cluster) = verify_cluster {
                                self.state_mgr
                                    .load_deployment_from_state(&cluster)
                                    .await?
                                    .unwrap_or(final_config)
                            } else {
                                final_config
                            };
                            self.ensure_critical_services_healthy(
                                &verify_config,
                                "verify mutation",
                            )
                            .await?;
                        }
                    }
                }
            }
            Config::Proxy(proxy_config) => {
                proxy_config.connection.auth.check_keypair()?;
                match cmd.clone() {
                    SubCommand::Proxy { .. } => {
                        let task_mgr = self.task_mgr.clone();
                        let outfile = if quiet {
                            let f = fs::OpenOptions::new()
                                .create(true)
                                .truncate(true)
                                .open(self.home.join("operation-result"))?;
                            Some(f)
                        } else {
                            None
                        };

                        let recv_rs_and_print_join = tokio::task::spawn(async move {
                            task_mgr
                                .write_task_result(outfile, verbose)
                                .await
                                .expect("write operation result failed");
                        });

                        let rs = self
                            .task_mgr
                            .run_tasks(cmd.clone(), Config::Proxy(proxy_config.clone()))
                            .await?;
                        recv_rs_and_print_join.await?;
                        info!(r#"all tasks complete. task_size={}"#, rs.len());

                        self.finishing(cmd, Config::Proxy(proxy_config)).await?;
                    }
                    _ => unreachable!(),
                }
            }
        }

        Ok(())
    }

    async fn save_deployment_config(&self, config: &DeployConfig, upsert: bool) -> Result<()> {
        let cluster = &config.deployment.cluster_name;
        let all_hosts = config.get_unique_host_list().join(";");
        self.state_mgr
            .save_deployment_config(config, upsert)
            .await?;
        info!("DeploymentConfig saved: cluster={cluster} @ {all_hosts}");
        Ok(())
    }

    async fn save_proxy_config(&self, config: &ProxyConfig, upsert: bool) -> Result<()> {
        println!("save_proxy_config for ProxyConfig");
        let proxy_operation = self
            .state_mgr
            .get_state_operation::<ProxyOperation>(PROXY_STATE);

        let proxy_name = config.proxy_service.proxy_name.clone();
        let proxy_entity = proxy_operation
            .load(|| -> Option<QueryCondition> {
                Some(QueryCondition {
                    cond_text: "proxy_name = $1".to_string(),
                    bind_values: vec![StateValue::Varchar(proxy_name.to_string())],
                })
            })
            .await?;
        if !proxy_entity.is_empty() && !upsert {
            bail!("Proxy {proxy_name} already exists");
        }
        // Extract and concatenate hosts
        let all_hosts = config
            .proxy_service
            .proxy_addrs
            .iter()
            .map(|addr| addr.split(':').next().unwrap())
            .collect::<Vec<&str>>()
            .join(";");
        let config_string = config.to_yaml();
        info!("ProxyConfig saved: proxy_name={proxy_name} @ {all_hosts}");
        let default_timestamp = chrono::DateTime::default();
        proxy_operation
            .put(ProxyEntity {
                proxy_name: proxy_name.to_string(),
                proxy_config: config_string,
                proxy_host_list: all_hosts,
                create_timestamp: default_timestamp,
                update_timestamp: default_timestamp,
            })
            .await?;
        Ok(())
    }

    fn validate_metrics_config(config: &DeployConfig) -> Result<()> {
        if let Some(monitor) = &config.deployment.monitor {
            let has_eloq = monitor.eloq_metrics.is_some();

            if !has_eloq {
                bail!("Monitor configuration is provided but eloq_metrics is not specified");
            }
        }

        Ok(())
    }

    fn normalize_config_for_apply_diff(config: &DeployConfig) -> DeployConfig {
        let mut normalized = config.clone();
        if normalized.deployment.version.is_none()
            || normalized.deployment.version.as_deref() == Some("latest")
        {
            normalized.deployment.version = Some("__ignored__".to_string());
        }
        normalized.deployment.tx_service.image = None;
        if let Some(logsrv) = &mut normalized.deployment.log_service {
            logsrv.image = None;
        }
        normalized
    }

    fn push_yaml_diff(
        path: String,
        current: Option<&YamlValue>,
        desired: Option<&YamlValue>,
        diffs: &mut Vec<String>,
    ) {
        match (current, desired) {
            (Some(YamlValue::Mapping(current_map)), Some(YamlValue::Mapping(desired_map))) => {
                let keys = current_map
                    .keys()
                    .chain(desired_map.keys())
                    .filter_map(|k| match k {
                        YamlValue::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect::<BTreeSet<_>>();
                for key in keys {
                    let next_path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{path}.{key}")
                    };
                    Self::push_yaml_diff(
                        next_path,
                        current_map.get(YamlValue::String(key.clone())),
                        desired_map.get(YamlValue::String(key)),
                        diffs,
                    );
                }
            }
            (Some(YamlValue::Sequence(current_seq)), Some(YamlValue::Sequence(desired_seq))) => {
                if current_seq != desired_seq {
                    diffs.push(path);
                }
            }
            _ => {
                if current != desired {
                    diffs.push(path);
                }
            }
        }
    }

    fn build_reconcile_plan(
        current: &DeployConfig,
        desired: &DeployConfig,
        observed: Option<ObservedCluster>,
    ) -> Result<ReconcilePlan> {
        let mut merged = current.clone();
        let mut tx_field_updates = Vec::new();
        let mut changes = Vec::new();
        let mut monitor_restart_required = false;
        let mut store_config_restart_required = false;
        let mut desired_for_diff = desired.clone();

        if current.deployment.cluster_name != desired.deployment.cluster_name {
            bail!(
                "cluster_name mismatch: state has '{}' but YAML has '{}'",
                current.deployment.cluster_name,
                desired.deployment.cluster_name
            );
        }

        if desired.deployment.enable_tls.is_some()
            && desired.deployment.enable_tls != current.deployment.enable_tls
        {
            merged.deployment.enable_tls = desired.deployment.enable_tls;
            store_config_restart_required = true;
            changes.push(format!(
                "deployment.enable_tls: {:?} -> {:?}",
                current.deployment.enable_tls, desired.deployment.enable_tls
            ));
        }
        desired_for_diff.deployment.enable_tls = current.deployment.enable_tls;

        if let Some(interval) = desired.deployment.checkpoint_interval {
            if current.deployment.checkpoint_interval != Some(interval) {
                merged.deployment.checkpoint_interval = Some(interval);
                tx_field_updates.push(format!("checkpointer_interval:{interval}"));
                changes.push(format!(
                    "deployment.checkpointer_interval: {:?} -> {:?}",
                    current.deployment.checkpoint_interval,
                    Some(interval)
                ));
            }
        } else {
            desired_for_diff.deployment.checkpoint_interval =
                current.deployment.checkpoint_interval;
        }

        if let Some(mode) = desired.deployment.cluster_mode {
            if current.deployment.cluster_mode != Some(mode) {
                merged.deployment.cluster_mode = Some(mode);
                tx_field_updates.push(format!("cluster_mode:{mode}"));
                changes.push(format!(
                    "deployment.cluster_mode: {:?} -> {:?}",
                    current.deployment.cluster_mode,
                    Some(mode)
                ));
            }
        } else {
            desired_for_diff.deployment.cluster_mode = current.deployment.cluster_mode;
        }

        let current_prom = current
            .deployment
            .monitor
            .as_ref()
            .and_then(|m| m.prometheus.as_ref());
        let desired_prom = desired
            .deployment
            .monitor
            .as_ref()
            .and_then(|m| m.prometheus.as_ref());

        if let (Some(current_prom), Some(desired_prom)) = (current_prom, desired_prom) {
            if let Some(retention_time) = desired_prom.retention_time.clone() {
                if current_prom.retention_time != Some(retention_time.clone()) {
                    if let Some(monitor) = &mut merged.deployment.monitor {
                        if let Some(prometheus) = &mut monitor.prometheus {
                            prometheus.retention_time = Some(retention_time.clone());
                        }
                    }
                    monitor_restart_required = true;
                    changes.push(format!(
                        "deployment.monitor.prometheus.retention_time: {:?} -> {:?}",
                        current_prom.retention_time,
                        Some(retention_time)
                    ));
                }
            } else if let Some(monitor) = &mut desired_for_diff.deployment.monitor {
                if let Some(prometheus) = &mut monitor.prometheus {
                    prometheus.retention_time = current_prom.retention_time.clone();
                }
            }

            if let Some(retention_size) = desired_prom.retention_size.clone() {
                if current_prom.retention_size != Some(retention_size.clone()) {
                    if let Some(monitor) = &mut merged.deployment.monitor {
                        if let Some(prometheus) = &mut monitor.prometheus {
                            prometheus.retention_size = Some(retention_size.clone());
                        }
                    }
                    monitor_restart_required = true;
                    changes.push(format!(
                        "deployment.monitor.prometheus.retention_size: {:?} -> {:?}",
                        current_prom.retention_size,
                        Some(retention_size)
                    ));
                }
            } else if let Some(monitor) = &mut desired_for_diff.deployment.monitor {
                if let Some(prometheus) = &mut monitor.prometheus {
                    prometheus.retention_size = current_prom.retention_size.clone();
                }
            }

            if let Some(remote_write_urls) = desired_prom.remote_write_urls.clone() {
                if current_prom.remote_write_urls != Some(remote_write_urls.clone()) {
                    if let Some(monitor) = &mut merged.deployment.monitor {
                        if let Some(prometheus) = &mut monitor.prometheus {
                            prometheus.remote_write_urls = Some(remote_write_urls.clone());
                        }
                    }
                    monitor_restart_required = true;
                    changes.push(format!(
                        "deployment.monitor.prometheus.remote_write_urls: {:?} -> {:?}",
                        current_prom.remote_write_urls,
                        Some(remote_write_urls)
                    ));
                }
            } else if let Some(monitor) = &mut desired_for_diff.deployment.monitor {
                if let Some(prometheus) = &mut monitor.prometheus {
                    prometheus.remote_write_urls = current_prom.remote_write_urls.clone();
                }
            }
        } else if let Some(monitor) = &mut desired_for_diff.deployment.monitor {
            if let Some(prometheus) = &mut monitor.prometheus {
                prometheus.retention_time = current_prom.and_then(|p| p.retention_time.clone());
                prometheus.retention_size = current_prom.and_then(|p| p.retention_size.clone());
                prometheus.remote_write_urls =
                    current_prom.and_then(|p| p.remote_write_urls.clone());
            }
        }

        let current_normalized = Self::normalize_config_for_apply_diff(current);
        let desired_normalized = Self::normalize_config_for_apply_diff(&desired_for_diff);
        let current_yaml = serde_yaml::to_value(&current_normalized)?;
        let desired_yaml = serde_yaml::to_value(&desired_normalized)?;
        let mut diffs = Vec::new();
        Self::push_yaml_diff(
            String::new(),
            Some(&current_yaml),
            Some(&desired_yaml),
            &mut diffs,
        );

        let store_prefix = "deployment.storage_service";
        let (store_diffs, other_diffs): (Vec<_>, Vec<_>) = diffs
            .into_iter()
            .filter(|path| !path.is_empty())
            .partition(|path| path.starts_with(store_prefix));

        if !store_diffs.is_empty() {
            store_config_restart_required = true;
            for diff in &store_diffs {
                changes.push(diff.clone());
            }
        }

        let supported_paths = [
            "deployment.checkpointer_interval",
            "deployment.cluster_mode",
            "deployment.monitor.prometheus.retention_time",
            "deployment.monitor.prometheus.retention_size",
            "deployment.monitor.prometheus.remote_write_urls",
        ];
        let unsupported_changes = other_diffs
            .into_iter()
            .filter(|path| !supported_paths.contains(&path.as_str()))
            .unique()
            .collect();

        let mut plan = ReconcilePlan::new(current.deployment.cluster_name.clone(), merged);
        plan.observed = observed;
        plan.changes = changes;
        plan.unsupported_changes = unsupported_changes;
        plan.tx_field_updates = tx_field_updates;
        if !plan.changes.is_empty() {
            plan.add_action(ReconcileAction::SaveClusterIndex);
        }
        if store_config_restart_required {
            plan.add_action(ReconcileAction::RegenerateEloqKvNodeConfig);
            plan.add_action(ReconcileAction::RestartTxWithUpdatedConfig);
        }
        if !plan.tx_field_updates.is_empty() {
            plan.add_action(ReconcileAction::RestartTxWithUpdatedConfig);
        }
        if monitor_restart_required {
            plan.add_action(ReconcileAction::RestartMonitor);
        }
        if !plan.actions.is_empty() {
            plan.add_action(ReconcileAction::VerifyClusterStatus);
        }

        Ok(plan)
    }

    async fn apply_topology(
        &'static self,
        topology_file: &str,
        quiet: bool,
        verbose: bool,
    ) -> Result<()> {
        let desired = DeployConfig::load(Some(topology_file.to_string()))?;
        Self::validate_metrics_config(&desired)?;

        let cluster = desired.deployment.cluster_name.clone();
        let current = self
            .state_mgr
            .load_deployment_from_state(&cluster)
            .await?
            .ok_or_else(|| anyhow!("cluster {} not found", cluster))?;

        let observed = self.observe_cluster(&current, None).await?;
        if observed.has_errors() {
            observed.print();
            bail!(
                "cannot apply: failed to observe live cluster state for '{}'",
                cluster
            );
        }
        let unavailable = observed.unavailable_services();
        if !unavailable.is_empty() {
            observed.print();
            bail!(
                "cannot apply: live cluster '{}' is not healthy; fix status before applying changes",
                cluster
            );
        }

        let plan = Self::build_reconcile_plan(&current, &desired, Some(observed))?;
        plan.print();

        if plan.is_empty() {
            return Ok(());
        }

        if plan.actions.contains(&ReconcileAction::SaveClusterIndex) {
            self.save_deployment_config(&plan.merged_config, true)
                .await?;
        }

        if plan
            .actions
            .contains(&ReconcileAction::RegenerateEloqKvNodeConfig)
        {
            let deployment = &plan.merged_config.deployment;
            let mut all_host_ports = deployment.get_host_port_list(DeploymentPackage::EloqTx);
            all_host_ports.extend(deployment.get_host_port_list(DeploymentPackage::EloqStandby));
            all_host_ports.extend(deployment.get_host_port_list(DeploymentPackage::EloqVoter));

            for host_port in &all_host_ports {
                if let Some((host, port)) = host_port.split_once(':') {
                    deployment
                        .gen_eloqkv_node_config(Some(host.to_string()), Some(port.to_string()))?;
                }
            }
        }

        if plan
            .actions
            .contains(&ReconcileAction::RestartTxWithUpdatedConfig)
        {
            Box::pin(self.run_impl(
                SubCommand::UpdateConf {
                    cluster: cluster.clone(),
                    restart: true,
                    password: None,
                    fields: plan.tx_field_updates.clone(),
                    tx_node_id: None,
                },
                None,
                quiet,
                verbose,
                false,
            ))
            .await?;
        }

        if plan.actions.contains(&ReconcileAction::RestartMonitor) {
            Box::pin(self.run_impl(
                SubCommand::Monitor {
                    cluster: Some(cluster.clone()),
                    command: MonitorCommand::Stop {
                        cluster: None,
                        components: vec![],
                    },
                },
                None,
                quiet,
                verbose,
                false,
            ))
            .await?;
            Box::pin(self.run_impl(
                SubCommand::Monitor {
                    cluster: Some(cluster.clone()),
                    command: MonitorCommand::Start {
                        cluster: None,
                        components: vec![],
                    },
                },
                None,
                quiet,
                verbose,
                false,
            ))
            .await?;
        }

        if plan.actions.contains(&ReconcileAction::VerifyClusterStatus) {
            let verified = self.observe_cluster(&plan.merged_config, None).await?;
            verified.print();
            if verified.has_errors() || !verified.unavailable_services().is_empty() {
                bail!(
                    "apply completed actions but final live status is not healthy for '{}'",
                    cluster
                );
            }
        }

        Ok(())
    }

    async fn plan_topology(&'static self, topology_file: &str) -> Result<()> {
        let desired = DeployConfig::load(Some(topology_file.to_string()))?;
        Self::validate_metrics_config(&desired)?;

        let cluster = desired.deployment.cluster_name.clone();
        let current = self
            .state_mgr
            .load_deployment_from_state(&cluster)
            .await?
            .ok_or_else(|| anyhow!("cluster {} not found", cluster))?;

        let observed = self.observe_cluster(&current, None).await?;
        let plan = Self::build_reconcile_plan(&current, &desired, Some(observed))?;
        plan.print();

        Ok(())
    }

    async fn get_config(&self, cmd: SubCommand) -> anyhow::Result<Config> {
        match cmd {
            SubCommand::Plan { topology_file } => {
                let config = DeployConfig::load(Some(topology_file))?;
                Self::validate_metrics_config(&config)?;
                Ok(Config::Cluster(config))
            }
            SubCommand::Apply { topology_file } => {
                let config = DeployConfig::load(Some(topology_file))?;
                Self::validate_metrics_config(&config)?;
                Ok(Config::Cluster(config))
            }
            SubCommand::Deploy { topology_file }
            | SubCommand::Launch {
                topology_file,
                skip_deps: _,
            } => {
                let mut config = DeployConfig::load(Some(topology_file))?;
                Self::validate_metrics_config(&config)?;

                self.resolve_version(&mut config.deployment).await?;
                self.save_deployment_config(&config, true).await?;
                info!("CmdExecutor Save DeploymentConfig successfully.");
                Ok(Config::Cluster(config))
            }
            SubCommand::Demo { .. } => self.gen_demo_config(cmd).await,
            SubCommand::Install { cluster }
            | SubCommand::Stop { cluster, .. }
            | SubCommand::Start { cluster, nodes: _ }
            | SubCommand::LogService {
                cluster,
                command: _,
            }
            | SubCommand::Restart { cluster }
            | SubCommand::UpdateConf {
                cluster,
                restart: _,
                fields: _,
                tx_node_id: _,
                password: _,
            }
            | SubCommand::Status {
                cluster,
                user: _,
                password: _,
                wait: _,
                detail: _,
            }
            | SubCommand::Export { cluster, .. }
            | SubCommand::Remove { cluster, force: _ }
            | SubCommand::Connect { cluster }
            | SubCommand::Backup { cluster, .. }
            | SubCommand::Failover { cluster, .. }
            | SubCommand::Scale { cluster, .. }
            | SubCommand::ScaleLog { cluster, .. } => {
                let config = self
                    .state_mgr
                    .load_deployment_from_state(&cluster)
                    .await?
                    .ok_or(anyhow!("cluster {} not found", cluster))?;
                Self::validate_metrics_config(&config)?;

                Ok(Config::Cluster(config))
            }
            SubCommand::RunDeps { topology_file }
            | SubCommand::Check { topology_file }
            | SubCommand::Exec {
                command: _,
                topology_file,
            } => {
                let config = DeployConfig::load(Some(topology_file))?;
                Self::validate_metrics_config(&config)?;

                Ok(Config::Cluster(config))
            }
            SubCommand::Update {
                cluster: Some(cluster),
                version,
                download_only,
                ..
            } => {
                if download_only && version.is_none() {
                    bail!(
                        "`update --download-only` requires a target version. Use `eloqctl update {cluster} <version> --download-only`"
                    );
                }
                if version
                    .as_deref()
                    .is_some_and(|v| v.eq_ignore_ascii_case("download-only"))
                {
                    bail!(
                        "`download-only` is a flag, not a version. Use `eloqctl update {cluster} <version> --download-only`"
                    );
                }
                let mut config = self
                    .state_mgr
                    .load_deployment_from_state(&cluster)
                    .await?
                    .ok_or(anyhow!("cluster {} not found", cluster))?;
                if let Some(v) = version {
                    if config.deployment.version.is_some() && config.deployment.version_str() == v {
                        warn!("cluster version not changed")
                    }
                    config.deployment.version = Some(v);
                    config.deployment.tx_service.image = None;
                    if let Some(logsrv) = &mut config.deployment.log_service {
                        logsrv.image = None;
                    }
                    self.resolve_version(&mut config.deployment).await?;
                }
                Self::validate_metrics_config(&config)?;

                Ok(Config::Cluster(config))
            }
            SubCommand::Monitor {
                cluster,
                command:
                    ref command @ MonitorCommand::Update {
                        ref component,
                        ref url,
                        ref feishu_robot_url,
                        ..
                    },
            } => {
                let cluster = Self::monitor_cluster(&cluster, command)?;
                let mut config = self
                    .state_mgr
                    .load_deployment_from_state(&cluster)
                    .await?
                    .ok_or(anyhow!("cluster {} not found", cluster))?;
                Self::configure_monitor_update(
                    &mut config,
                    component,
                    url.clone(),
                    feishu_robot_url.clone(),
                )?;
                Self::validate_metrics_config(&config)?;

                Ok(Config::Cluster(config))
            }
            SubCommand::Monitor { cluster, command } => {
                let cluster = Self::monitor_cluster(&cluster, &command)?;
                let config = self
                    .state_mgr
                    .load_deployment_from_state(&cluster)
                    .await?
                    .ok_or(anyhow!("cluster {} not found", cluster))?;
                Self::validate_metrics_config(&config)?;

                Ok(Config::Cluster(config))
            }
            SubCommand::Proxy { command } => {
                match &command {
                    ProxyCommand::Start { config } => {
                        // Load and handle the Start command with the provided config
                        let mut proxy_config = ProxyConfig::load(Some(config.to_string()))?;
                        self.resolve_proxy_version(&mut proxy_config);
                        self.save_proxy_config(&proxy_config, true).await?;
                        Ok(Config::Proxy(proxy_config))
                    }
                    ProxyCommand::Stop { proxy_name } => {
                        let proxy_config = self
                            .state_mgr
                            .load_proxy_from_state(Some(proxy_name.clone()))
                            .await?
                            .ok_or(anyhow!("proxy config not found"))?;
                        Ok(Config::Proxy(proxy_config))
                    }
                    ProxyCommand::List { proxy_name } => {
                        let proxy_config = self
                            .state_mgr
                            .load_proxy_from_state(proxy_name.clone())
                            .await?
                            .ok_or_else(|| anyhow!("proxy config not found"))?;
                        Ok(Config::Proxy(proxy_config))
                    }
                    ProxyCommand::Add { .. } | ProxyCommand::Remove { .. } => {
                        todo!()
                    }
                }
            }

            _ => Err(anyhow!("unexpected command: {cmd:?}")),
        }
    }

    pub async fn run(
        &'static self,
        cmd: SubCommand,
        option_config: Option<Config>,
        quiet: bool,
        verbose: bool,
    ) -> Result<()> {
        self.run_impl(cmd, option_config, quiet, verbose, true)
            .await
    }

    async fn run_impl(
        &'static self,
        mut cmd: SubCommand,
        option_config: Option<Config>,
        quiet: bool,
        verbose: bool,
        acquire_lock: bool,
    ) -> Result<()> {
        set_verbose_task_output(verbose);
        let _mutation_lock = if acquire_lock {
            self.acquire_mutation_lock(&cmd)?
        } else {
            None
        };
        match &cmd {
            SubCommand::List => return self.list_clusters().await,
            SubCommand::Versions { product, store } => {
                return self.list_versions(product.clone(), store.clone()).await
            }
            SubCommand::Update { cluster: None, .. } => return self.update().await,
            SubCommand::Apply { topology_file } => {
                return self.apply_topology(topology_file, quiet, verbose).await;
            }
            SubCommand::Plan { topology_file } => {
                return self.plan_topology(topology_file).await;
            }
            SubCommand::Update {
                cluster: Some(cluster),
                ..
            }
            | SubCommand::UpdateConf { cluster, .. }
            | SubCommand::Scale { cluster, .. }
            | SubCommand::ScaleLog { cluster, .. } => {
                let config = self
                    .state_mgr
                    .load_deployment_from_state(cluster)
                    .await?
                    .ok_or(anyhow!("cluster {} not found", cluster))?;
                self.ensure_critical_services_healthy(&config, cmd.as_ref())
                    .await?;
            }
            SubCommand::Backup {
                cluster,
                command: BackupCommand::Start { .. },
            } => {
                let config = self
                    .state_mgr
                    .load_deployment_from_state(cluster)
                    .await?
                    .ok_or(anyhow!("cluster {} not found", cluster))?;
                self.ensure_critical_services_healthy(&config, "backup start")
                    .await?;
            }
            SubCommand::Install { cluster } => {
                let config = self
                    .state_mgr
                    .load_deployment_from_state(cluster)
                    .await?
                    .ok_or(anyhow!("cluster {} not found", cluster))?;
                if self.cluster_has_running_tx(&config).await? {
                    println!(
                        "Cluster {cluster} already has running tx service; skipping bootstrap."
                    );
                    return Ok(());
                }
            }
            SubCommand::Remove { cluster, force: _ } => {
                let upload_path = upload_dir().join(cluster);
                if upload_path.exists() {
                    std::fs::remove_dir_all(upload_path)?;
                }
            }
            SubCommand::Upgrade => {
                self.state_mgr.upgrade_schema().await?;
                println!("Schema and local cluster topology upgrade complete");
                return Ok(());
            }
            _ => {}
        }

        if matches!(
            &cmd,
            SubCommand::Update {
                cluster: Some(_),
                ..
            } | SubCommand::Monitor {
                cluster: _,
                command: MonitorCommand::Update { .. },
            }
        ) {
            let config = match option_config {
                Some(config) => config,
                None => self.get_config(cmd.clone()).await?,
            };
            return self.run_update_command(cmd, config, quiet, verbose).await;
        }

        // Extract cluster_config from option_config or load it
        let config = match option_config {
            Some(config) => match config {
                Config::Cluster(mut deploy_config) => {
                    deploy_config.connection.auth.check_keypair()?;
                    self.resolve_version(&mut deploy_config.deployment).await?;
                    self.save_deployment_config(&deploy_config, true).await?;
                    Config::Cluster(deploy_config)
                }
                Config::Proxy(proxy_config) => Config::Proxy(proxy_config),
            },
            None => self.get_config(cmd.clone()).await?,
        };

        match config {
            Config::Cluster(mut deploy_config) => {
                let cmd_for_match = cmd.clone();
                match cmd_for_match {
                    SubCommand::Connect { .. } => {
                        println!("{}", deploy_config.client_conn());
                    }
                    SubCommand::Export { cluster, output } => {
                        let yaml_str = deploy_config.to_yaml()?;
                        if let Some(path) = output {
                            std::fs::write(path.clone(), &yaml_str)
                                .map_err(|e| anyhow!("failed to write {}: {}", path, e))?;
                            println!("Exported cluster '{}' topology to {}", cluster, path);
                        } else {
                            println!("{}", yaml_str);
                        }
                    }
                    _ => {
                        if let SubCommand::Scale {
                            version: requested_version,
                            add_nodes,
                            remove_nodes,
                            ..
                        } = &mut cmd
                        {
                            if let Some(version_value) = requested_version.clone() {
                                if add_nodes.is_empty() {
                                    bail!("--version requires at least one entry in --add-nodes");
                                }
                                if !remove_nodes.is_empty() {
                                    bail!("--version cannot be combined with --remove-nodes");
                                }

                                let mut version_config = deploy_config.clone();
                                version_config.deployment.version = Some(version_value.clone());
                                version_config.deployment.tx_service.image = None;
                                if let Some(logsrv) = version_config.deployment.log_service.as_mut()
                                {
                                    logsrv.image = None;
                                }

                                self.resolve_version(&mut version_config.deployment).await?;
                                let resolved_version =
                                    version_config.deployment.version.clone().unwrap();
                                let resolved_image =
                                    version_config.deployment.tx_service.image.clone().unwrap();

                                deploy_config.tx_version_override = Some(resolved_version.clone());
                                deploy_config.tx_image_override = Some(resolved_image);

                                *requested_version = Some(resolved_version);
                            }
                        }

                        if let Some(noop_msg) = Self::idempotent_noop_message(&cmd, &deploy_config)
                        {
                            println!("{noop_msg}");
                            return Ok(());
                        }

                        let task_mgr = self.task_mgr.clone();
                        let outfile = if quiet {
                            let f = fs::OpenOptions::new()
                                .create(true)
                                .truncate(true)
                                .open(self.home.join("task-result"))?;
                            Some(f)
                        } else {
                            None
                        };

                        let recv_rs_and_print_join = tokio::task::spawn(async move {
                            task_mgr
                                .write_task_result(outfile, verbose)
                                .await
                                .expect("write task result failed");
                        });

                        // Generate and run tasks
                        let rs = self
                            .task_mgr
                            .run_tasks(cmd.clone(), Config::Cluster(deploy_config.clone()))
                            .await?;
                        recv_rs_and_print_join.await?;
                        info!(r#"all tasks complete. task_size={}"#, rs.len());
                        Self::print_status_output(&cmd, &deploy_config, &rs, verbose);

                        // Using cluster_config again without moving it
                        let should_verify_after_finish = matches!(
                            &cmd,
                            SubCommand::Update {
                                cluster: Some(_),
                                ..
                            } | SubCommand::UpdateConf { .. }
                                | SubCommand::Scale { .. }
                                | SubCommand::ScaleLog { .. }
                        );
                        let final_config = deploy_config.clone();
                        let verify_cluster = match &cmd {
                            SubCommand::Scale { cluster, .. }
                            | SubCommand::ScaleLog { cluster, .. } => Some(cluster.clone()),
                            _ => None,
                        };
                        self.finishing(cmd, Config::Cluster(deploy_config)).await?;
                        if should_verify_after_finish {
                            let verify_config = if let Some(cluster) = verify_cluster {
                                self.state_mgr
                                    .load_deployment_from_state(&cluster)
                                    .await?
                                    .unwrap_or(final_config)
                            } else {
                                final_config
                            };
                            self.ensure_critical_services_healthy(
                                &verify_config,
                                "verify mutation",
                            )
                            .await?;
                        }
                    }
                }
            }
            Config::Proxy(proxy_config) => {
                proxy_config.connection.auth.check_keypair()?;
                match cmd.clone() {
                    SubCommand::Proxy { .. } => {
                        let task_mgr = self.task_mgr.clone();
                        let outfile = if quiet {
                            let f = fs::OpenOptions::new()
                                .create(true)
                                .truncate(true)
                                .open(self.home.join("task-result"))?;
                            Some(f)
                        } else {
                            None
                        };

                        let recv_rs_and_print_join = tokio::task::spawn(async move {
                            task_mgr
                                .write_task_result(outfile, verbose)
                                .await
                                .expect("write task result failed");
                        });

                        // Generate and run tasks
                        let rs = self
                            .task_mgr
                            .run_tasks(cmd.clone(), Config::Proxy(proxy_config.clone()))
                            .await?;
                        recv_rs_and_print_join.await?;
                        info!(r#"all tasks complete. task_size={}"#, rs.len());

                        // Using cluster_config again without moving it
                        self.finishing(cmd, Config::Proxy(proxy_config)).await?;
                    }
                    _ => unreachable!(),
                }
            }
        }

        Ok(())
    }
    async fn finishing(&self, cmd: SubCommand, config: Config) -> Result<()> {
        // After all tasks finished
        match config {
            Config::Cluster(cfg) => match cmd {
                SubCommand::Launch { .. } | SubCommand::Demo { .. } => {
                    println!("Launch cluster finished, Enjoy!");
                    println!("Connect to server: \n\t{}", cfg.client_conn());
                    if let Some(moni) = &cfg.deployment.monitor {
                        if let Some(prometheus) = &moni.prometheus {
                            println!("Prometheus: http://{}:{}", prometheus.host, prometheus.port);
                        }
                        if let Some(grafana) = &moni.grafana {
                            println!("Grafana: http://{}:{}", grafana.host, grafana.port);
                        }
                    }

                    // Display metrics information
                    if let Some(monitor) = &cfg.deployment.monitor {
                        if let Some(eloq_metrics) = &monitor.eloq_metrics {
                            if let (Some(path), Some(port)) =
                                (&eloq_metrics.path, eloq_metrics.port)
                            {
                                println!("Eloq Metrics: http://<host>:{}{}", port, path);
                            }
                        }
                    }
                }
                SubCommand::Remove { cluster, force } => {
                    let failed_tasks = self
                        .state_mgr
                        .load_task_status_from_state(
                            cluster.clone(),
                            Some(1),
                            Some(vec!["remove".to_string()]),
                        )
                        .await?;
                    if !failed_tasks.is_empty() {
                        eprintln!(
                            "Remove finished with {} cleanup task(s) still incomplete:",
                            failed_tasks.len()
                        );
                        for task in &failed_tasks {
                            let action = serde_json::from_str::<TaskId>(&task.task)
                                .ok()
                                .map(|id| task_action_summary(&id.cmd, &id.task))
                                .unwrap_or_else(|| "finish cleanup".to_string());
                            eprintln!("  - {}: {}", task.task_host, action);
                        }
                        eprintln!(
                            "If those hosts are reachable, retry `eloqctl remove {cluster}`. Otherwise clean them manually."
                        );
                        eprintln!(
                            "Manual cleanup command: rm -rf {}",
                            cfg.deployment.install_dir()
                        );
                    }
                    if force {
                        eprintln!(
                            "Force remove enabled: local state will be cleared even if remote cleanup is incomplete."
                        );
                        let hosts = cfg.get_unique_host_list();
                        if !hosts.is_empty() {
                            eprintln!(
                                "Manual cleanup may still be required on: {}",
                                hosts.join(", ")
                            );
                            eprintln!(
                                "Example command: ssh <host> 'rm -rf {}'",
                                cfg.deployment.install_dir()
                            );
                        }
                    }
                    let n = self.state_mgr.delete_cluster(&cluster).await?;
                    info!("cluster state cleared rows={n}");
                }
                SubCommand::Update {
                    cluster: Some(cluster),
                    ..
                } => {
                    self.save_deployment_config(&cfg, true).await?;
                    println!("cluster {cluster} is updated!");
                }
                SubCommand::Backup { cluster, command } => match &command {
                    BackupCommand::Start { .. } => {}
                    BackupCommand::List {} => {
                        let success_task_entity =
                            STATE_MGR.list_snapshots(cluster.to_string()).await?;

                        // Try to load cluster config to determine storage type
                        let cluster_config = self
                            .state_mgr
                            .load_deployment_from_state(&cluster)
                            .await?
                            .ok_or_else(|| anyhow!("cluster {} not found", cluster))?;

                        let is_eloqstore_cloud = cluster_config
                            .deployment
                            .storage_service
                            .as_ref()
                            .map(|s| {
                                s.eloqdss
                                    .as_ref()
                                    .map(|dss| {
                                        matches!(
                                            dss.backend_config(),
                                            DataStoreServiceBackend::EloqStore(config)
                                                if config.is_cloud_mode()
                                        )
                                    })
                                    .unwrap_or(false)
                            })
                            .unwrap_or(false);

                        let success_task_vec = success_task_entity
                            .iter()
                            .filter(|snapshot_info_entity| {
                                snapshot_info_entity.snapshot_status == 0
                            })
                            .map(|snapshot_info_entity| {
                                let cluster_name = &snapshot_info_entity.cluster_name;
                                let create_timestamp = &snapshot_info_entity.snapshot_ts;
                                let snapshot_path = &snapshot_info_entity.snapshot_path;
                                let dest_host = &snapshot_info_entity.dest_host;
                                let dest_user = &snapshot_info_entity.dest_user;

                                // Determine storage type
                                let storage_type = if dest_host.is_empty() {
                                    "cloud (S3)"
                                } else {
                                    "local"
                                };

                                // For cloud storage, parse and display appropriately
                                let display_path: String = if dest_host.is_empty() {
                                    if is_eloqstore_cloud {
                                        // EloqStore: snapshot_path stores backup_ts (timestamp)
                                        if snapshot_path.trim().is_empty() {
                                            "backup_ts: (empty)".to_string()
                                        } else {
                                            format!("backup_ts: {}", snapshot_path.trim())
                                        }
                                    } else {
                                        // RocksDB S3: show comma-separated list or formatted list
                                        let manifests = split_manifests(snapshot_path);
                                        if manifests.is_empty() {
                                            "[0 manifests]: ".to_string()
                                        } else if manifests.len() == 1 {
                                            snapshot_path.clone() // Single manifest: show as-is
                                        } else {
                                            // Multiple manifests: show count and list
                                            format!(
                                                "[{} manifests]: {}",
                                                manifests.len(),
                                                manifests.join(", ")
                                            )
                                        }
                                    }
                                } else {
                                    // Local: show path as-is
                                    snapshot_path.clone()
                                };

                                (
                                    cluster_name,
                                    create_timestamp,
                                    display_path,
                                    dest_host,
                                    dest_user,
                                    storage_type,
                                )
                            })
                            .collect_vec();

                        println!("available snapshots: {:#?}", success_task_vec);
                    }
                    BackupCommand::Remove { .. } => {}
                    BackupCommand::DumpAOF { .. } => {}
                    BackupCommand::DumpRDB { .. } => {}
                    BackupCommand::Restore { .. } => {}
                },
                _ => {}
            },
            Config::Proxy(..) => match cmd {
                SubCommand::Proxy { command } => match &command {
                    ProxyCommand::Start { .. } => {
                        println!("Launch proxy finished, Enjoy!");
                    }
                    ProxyCommand::Stop { .. } => {
                        println!("Proxy stopped.");
                    }
                    ProxyCommand::List { proxy_name } => {
                        let success_task_entity = STATE_MGR.list_proxy(proxy_name).await?;

                        let success_task_vec = success_task_entity
                            .iter()
                            .map(|proxy_info_entity| {
                                let proxy_name = &proxy_info_entity.proxy_name;
                                let proxy_config = &proxy_info_entity.proxy_config;
                                (proxy_name, proxy_config)
                            })
                            .collect_vec();

                        // Iterate over each proxy configuration
                        for (proxy_name, proxy_config) in success_task_vec {
                            // Parse the proxy_config string as YAML
                            let proxy_config: ProxyConfig = serde_yaml::from_str(proxy_config)
                                .map_err(|e| {
                                    anyhow!(
                                        "Failed to parse proxy_config for '{}': {}",
                                        proxy_name,
                                        e
                                    )
                                })?;

                            // Extract eloqkv_cluster_addr
                            println!(
                                "Proxy Name: {}\neloqkv_cluster_addr: {:#?}\n",
                                proxy_name, proxy_config.proxy_service.eloqkv_cluster_addr
                            );
                        }
                    }
                    ProxyCommand::Add { cluster_name, .. } => {
                        println!("Cluster {cluster_name} is added to proxy service.");
                    }
                    ProxyCommand::Remove { cluster_name, .. } => {
                        println!("Cluster {cluster_name} is removed from proxy service.");
                    }
                },
                _ => unreachable!(),
            },
        }
        Ok(())
    }

    async fn list_clusters(&self) -> Result<()> {
        let list = self
            .state_mgr
            .list_deployments()
            .await?
            .iter()
            .map(|cluster| cluster.abstract_info())
            .collect_vec();

        let table = tabled::Table::new(list);
        println!("{table}\n");
        Ok(())
    }

    async fn list_versions(
        &self,
        product: Option<Product>,
        store: Option<StorageProvider>,
    ) -> Result<()> {
        let store_name = store.as_ref().map(|s| s.to_string());
        let releases = fetch_eloqkv_releases(&HTTP_CLIENT).await?;
        let list = list_versions_from_releases(&releases, product, store_name.as_deref());
        let table = tabled::Table::new(list);
        println!("{table}\n");
        Ok(())
    }

    pub async fn resolve_version(&self, cnf: &mut Deployment) -> Result<()> {
        let os = self.os_vers();
        let arch = cpu_arch();

        // Get store name once and reuse it, if not set, use rocksdb as default
        let store = cnf
            .storage_service
            .as_ref()
            .map_or("rocksdb".to_string(), |s| s.pretty_name());

        let needs_latest_resolution =
            cnf.version.is_some() && cnf.version_str().eq_ignore_ascii_case("latest");
        let releases = if needs_latest_resolution {
            Some(fetch_eloqkv_releases(&HTTP_CLIENT).await?)
        } else {
            None
        };

        if needs_latest_resolution {
            let latest = list_versions_from_releases(
                releases.as_ref().unwrap(),
                Some(cnf.product()),
                Some(store.as_str()),
            )
            .into_iter()
            .map(|row: VersionRow| row.version)
            .max_by(|a, b| version_digits(a).ok().cmp(&version_digits(b).ok()))
            .ok_or_else(|| anyhow!("no available GitHub release found for store={store}"))?;
            info!("latest release version = {latest}");
            cnf.version = Some(latest);
        }

        if cnf.tx_service.image.is_none() {
            let vers = cnf.version.as_deref().expect("version is missing");
            let img = if let Some(releases) = releases.as_ref() {
                find_eloqkv_asset(releases, cnf.product(), vers, &store, &os, &arch)?.url
            } else {
                Self::release_asset_url(cnf.product().name(), vers, &store, &os, &arch)
            };
            info!("tx service image is set: {img}");
            cnf.tx_service.image = Some(img);
        }
        if let Some(logsrv) = &mut cnf.log_service {
            if logsrv.image.is_none() {
                let vers = cnf.version.as_deref().expect("version is missing");
                let img = if let Some(releases) = releases.as_ref() {
                    find_product_asset(releases, "log-service", vers, &store, &os, &arch)?.url
                } else {
                    Self::release_asset_url("log-service", vers, &store, &os, &arch)
                };
                info!("log service image is set: {img}");
                logsrv.image = Some(img);
            }
        }
        Ok(())
    }

    pub fn resolve_proxy_version(&self, cnf: &mut ProxyConfig) {
        let arch = cpu_arch();
        let os = self.os_vers();

        // Bind the PathBuf to a variable to extend its lifetime
        let path_buf = PathBuf::from(CDN);
        let prefix = path_buf.as_path().to_str().unwrap();

        // Rest of your code remains the same
        let url = format!("{prefix}/eloqkv/tools/{arch}/{os}/eloqkv-proxy");
        info!("proxy service binary is set: {url}");
        cnf.proxy_service.bin_download_url = Some(url);
    }

    async fn gen_demo_config(&self, cmd: SubCommand) -> Result<Config> {
        match cmd {
            SubCommand::Demo {
                product,
                store,
                version,
                skip_deps: _,
                unlimited,
                no_monitor,
                joint_wal,
            } => {
                let topology = format!(
                    "{}/demo-{product}.yaml",
                    self.dir_config().to_string_lossy()
                );
                let mut config = DeployConfig::load(Some(topology))?;
                let deploy = &mut config.deployment;
                // set storage
                match store {
                    StorageProvider::Dynamodb => unimplemented!(),
                    StorageProvider::Rocksdb => {
                        deploy
                            .storage_service
                            .get_or_insert(StorageService {
                                dynamodb: None,
                                rocksdb: None,
                                eloqdss: None,
                            })
                            .rocksdb = Some(RocksDB::LOCAL(RocksLocal {
                            path: Some("/tmp".to_string()),
                        }));
                    }
                    StorageProvider::EloqDSS => {
                        // Create a default DataStoreService with Local mode and EloqStore backend
                        use crate::config::storage_service_config::{
                            DataStoreService, DataStoreServiceBackend, EloqStoreConfig,
                        };

                        // Try to get values from existing YAML config, otherwise use defaults
                        let (worker_num, data_path_list) = if let Some(existing_dss) = deploy
                            .storage_service
                            .as_ref()
                            .and_then(|s| s.eloqdss.as_ref())
                        {
                            // Extract values from existing config if available
                            match existing_dss.backend_config() {
                                DataStoreServiceBackend::EloqStore(config) => (
                                    config.eloq_store_worker_num,
                                    config.eloq_store_data_path_list.clone(),
                                ),
                            }
                        } else {
                            // Use defaults if not in YAML
                            (Some(4), Some("/tmp".to_string()))
                        };

                        let default_dss = DataStoreService {
                            backend: DataStoreServiceBackend::EloqStore(EloqStoreConfig {
                                eloq_store_worker_num: worker_num,
                                eloq_store_data_path_list: data_path_list,
                                eloq_store_open_files_limit: None,
                                eloq_store_data_page_restart_interval: None,
                                eloq_store_index_page_restart_interval: None,
                                eloq_store_init_page_count: None,
                                eloq_store_skip_verify_checksum: None,
                                eloq_store_buffer_pool_size: None,
                                eloq_store_manifest_limit: None,
                                eloq_store_io_queue_size: None,
                                eloq_store_max_inflight_write: None,
                                eloq_store_max_write_batch_pages: None,
                                eloq_store_buf_ring_size: None,
                                eloq_store_coroutine_stack_size: None,
                                eloq_store_num_retained_archives: None,
                                eloq_store_archive_interval_secs: None,
                                eloq_store_max_archive_tasks: None,
                                eloq_store_file_amplify_factor: None,
                                eloq_store_local_space_limit: None,
                                eloq_store_reserve_space_ratio: None,
                                eloq_store_data_page_size: None,
                                eloq_store_pages_per_file_shift: None,
                                eloq_store_overflow_pointers: None,
                                eloq_store_enable_compression: None,
                                eloq_store_max_upload_batch: None,
                                eloq_store_cloud_store_path: None,
                                eloq_store_cloud_config: None,
                                eloq_store_data_append_mode: None,
                            }),
                            peer_host_ports: None,
                            mode: DataStoreServiceMode::Internal, // Default: eloqctl manages dss_server
                        };
                        deploy
                            .storage_service
                            .get_or_insert(StorageService {
                                dynamodb: None,
                                rocksdb: None,
                                eloqdss: None,
                            })
                            .eloqdss = Some(default_dss);
                    }
                }
                // deploy log-service jointly
                if joint_wal {
                    deploy.log_service = None;
                } else if let Some(log) = deploy.log_service.as_mut() {
                    // add an unique number (pid) to WAL directory
                    let pid = std::process::id().to_string();
                    log.nodes
                        .first_mut()
                        .unwrap()
                        .data_dir
                        .first_mut()
                        .unwrap()
                        .push_str(&pid);
                }
                // set monitor
                if no_monitor {
                    deploy.monitor = None;
                }
                if let Some(monitor) = &mut deploy.monitor {
                    // Check metrics configuration
                    let has_eloq = monitor.eloq_metrics.is_some();

                    // If neither is present, report an error
                    if !has_eloq {
                        bail!(
                            "Monitor configuration is provided but eloq_metrics is not specified"
                        );
                    }
                }
                // set version
                deploy.version.replace(version);
                // set image URL
                self.resolve_version(deploy).await?;
                // add kv-store name to cluster name suffix
                let name_suffix = format!("-{store}");
                deploy.cluster_name.push_str(&name_suffix);
                if unlimited {
                    deploy.hardware = None;
                }
                self.save_deployment_config(&config, false).await?;
                Ok(Config::Cluster(config))
            }
            _ => Err(anyhow!("unexpected command: {cmd:?}")),
        }
    }

    async fn update(&self) -> Result<()> {
        const UPDATE_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
        const UPDATE_PROGRESS_TIMEOUT: Duration = Duration::from_secs(120);
        const SELF_UPDATE_FLUSH_THRESHOLD: usize = 1024 * 1024;
        let os = self.os_vers();
        let arch = cpu_arch();
        let filename = format!("eloqctl-main-{os}-{arch}.tar.gz");
        let url = format!("{CDN}/eloqctl/{arch}/main/{filename}");
        info!("Fetching latest package {url}");
        let resp = HTTP_CLIENT
            .get(&url)
            .timeout(UPDATE_REQUEST_TIMEOUT)
            .send()
            .await?;
        if !resp.status().is_success() {
            bail!("Fetch package failed: {}", resp.status());
        }
        let len = resp
            .content_length()
            .ok_or_else(|| anyhow!("can't know package size"))?;
        let mut cached = self.dir_download();
        cached.push(filename);
        if cached.exists() {
            let local_len = std::fs::metadata(&cached)?.len();
            info!("latest package length {len}, local package length {local_len}");
            if len == local_len {
                println!("eloqctl is already latest");
                return Ok(());
            }
        }
        let pg_bar = file_pg_bar();
        pg_bar.set_length(len);
        pg_bar.set_message("downloading");
        let mut file = std::fs::File::create(&cached)?;
        let mut stream = resp.bytes_stream();
        let mut write_buf: Vec<u8> = Vec::with_capacity(SELF_UPDATE_FLUSH_THRESHOLD);
        loop {
            let next_chunk = tokio::time::timeout(UPDATE_PROGRESS_TIMEOUT, stream.next()).await;
            let Some(stream_chunk) = (match next_chunk {
                Ok(chunk) => chunk,
                Err(_) => {
                    bail!(
                        "download stalled for more than {:?} while fetching {}",
                        UPDATE_PROGRESS_TIMEOUT,
                        url
                    );
                }
            }) else {
                break;
            };
            let chunk = stream_chunk.map_err(|e| anyhow!("download failed: {e}"))?;
            write_buf.extend_from_slice(&chunk);
            pg_bar.inc(chunk.len() as u64);
            if write_buf.len() >= SELF_UPDATE_FLUSH_THRESHOLD {
                let data = std::mem::replace(
                    &mut write_buf,
                    Vec::with_capacity(SELF_UPDATE_FLUSH_THRESHOLD),
                );
                file = tokio::task::spawn_blocking(move || {
                    file.write_all(&data).map(|()| file).map_err(|e| anyhow!(e))
                })
                .await
                .map_err(|e| anyhow!(e))??;
            }
        }
        if !write_buf.is_empty() {
            tokio::task::spawn_blocking(move || file.write_all(&write_buf).map_err(|e| anyhow!(e)))
                .await
                .map_err(|e| anyhow!(e))??;
        } else {
            drop(file);
        }
        pg_bar.finish_with_message("downloaded");
        let tar_cmd = format!(
            "tar -xzvf {} -C {} --strip-components 1 --overwrite",
            cached.to_string_lossy(),
            self.dir_home()
        );
        println!(
            "Execute this command to complete the update:\n {}",
            tar_cmd.bold()
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::CmdExecutor;
    use crate::cli::reconcile::{ObservedCluster, ObservedStatus, ReconcileAction};
    use crate::cli::task::task_base::{TaskArgValue, TaskResultEnum, TaskResultPair};
    use crate::cli::{SubCommand, CMD_OUTPUT, CMD_STATUS};
    use crate::config::config_base::DeployConfig;
    use std::collections::HashMap;

    #[test]
    fn summarize_status_rows_includes_monitor_components() {
        let execution = HashMap::from([
            (CMD_STATUS.to_string(), TaskArgValue::Number(0)),
            (
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str("grafana is running, pid: 123".to_string()),
            ),
        ]);
        let rows = CmdExecutor::summarize_status_rows(&[TaskResultPair {
            task_id: "host=127.0.0.1,cmd=monitor,task=grafana-status-3301".to_string(),
            result: TaskResultEnum::Success(Some(execution)),
        }]);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].host, "127.0.0.1");
        assert_eq!(rows[0].service, "grafana");
        assert_eq!(rows[0].port, "3301");
        assert_eq!(rows[0].status, "UP");
    }

    #[test]
    fn observed_cluster_parses_critical_down_status() {
        let execution = HashMap::from([
            (CMD_STATUS.to_string(), TaskArgValue::Number(0)),
            (
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str("eloqkv service is down".to_string()),
            ),
        ]);
        let observed = ObservedCluster::from_task_results(
            "c1".to_string(),
            &[TaskResultPair {
                task_id: "host=127.0.0.1,cmd=status,task=tx-status-6379".to_string(),
                result: TaskResultEnum::Success(Some(execution)),
            }],
        );

        assert_eq!(observed.services.len(), 1);
        assert_eq!(observed.services[0].status, ObservedStatus::Down);
        assert_eq!(observed.unavailable_services().len(), 1);
        assert!(!observed.has_running_service("tx"));
    }

    #[test]
    fn observed_cluster_ignores_monitor_down_for_apply_gate() {
        let execution = HashMap::from([
            (CMD_STATUS.to_string(), TaskArgValue::Number(0)),
            (
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str("grafana service is down".to_string()),
            ),
        ]);
        let observed = ObservedCluster::from_task_results(
            "c1".to_string(),
            &[TaskResultPair {
                task_id: "host=127.0.0.1,cmd=monitor,task=grafana-status-3301".to_string(),
                result: TaskResultEnum::Success(Some(execution)),
            }],
        );

        assert_eq!(observed.services.len(), 1);
        assert_eq!(observed.services[0].status, ObservedStatus::Down);
        assert!(observed.unavailable_services().is_empty());
    }

    #[test]
    fn observed_cluster_detects_running_tx() {
        let execution = HashMap::from([
            (CMD_STATUS.to_string(), TaskArgValue::Number(0)),
            (
                CMD_OUTPUT.to_string(),
                TaskArgValue::Str("eloqkv service is running".to_string()),
            ),
        ]);
        let observed = ObservedCluster::from_task_results(
            "c1".to_string(),
            &[TaskResultPair {
                task_id: "host=127.0.0.1,cmd=status,task=tx-status-6379".to_string(),
                result: TaskResultEnum::Success(Some(execution)),
            }],
        );

        assert!(observed.has_running_service("tx"));
    }

    #[test]
    fn scale_duplicate_add_is_noop() {
        let config = load_apply_test_config(Some(60), Some("15d"));
        let cmd = SubCommand::Scale {
            cluster: "apply-test".to_string(),
            add_nodes: vec!["127.0.0.1:6389".to_string()],
            remove_nodes: Vec::new(),
            ng_id: Some(0),
            is_candidate: Some(vec![true]),
            password: None,
            version: None,
        };

        assert!(CmdExecutor::idempotent_noop_message(&cmd, &config)
            .unwrap()
            .contains("no-op"));
    }

    #[test]
    fn scale_duplicate_remove_is_noop() {
        let config = load_apply_test_config(Some(60), Some("15d"));
        let cmd = SubCommand::Scale {
            cluster: "apply-test".to_string(),
            add_nodes: Vec::new(),
            remove_nodes: vec!["127.0.0.9:6389".to_string()],
            ng_id: None,
            is_candidate: None,
            password: None,
            version: None,
        };

        assert!(CmdExecutor::idempotent_noop_message(&cmd, &config)
            .unwrap()
            .contains("no-op"));
    }

    #[test]
    fn cluster_mutation_lock_is_exclusive() {
        let dir = std::env::temp_dir().join(format!("eloqctl-lock-test-{}", uuid::Uuid::new_v4()));
        let first = super::ClusterMutationLock::acquire(&dir, "cluster/a", "apply").unwrap();
        let second = super::ClusterMutationLock::acquire(&dir, "cluster/a", "scale");

        assert!(second.is_err());
        drop(first);
        assert!(super::ClusterMutationLock::acquire(&dir, "cluster/a", "scale").is_ok());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn cluster_mutation_lock_reclaims_stale_lock() {
        let dir = std::env::temp_dir().join(format!("eloqctl-lock-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cluster_a.lock");
        std::fs::write(
            &path,
            "pid=99999999\ncluster=cluster/a\ncommand=apply\ncreated_at=2024-01-01T00:00:00Z\n",
        )
        .unwrap();

        let lock = super::ClusterMutationLock::acquire(&dir, "cluster/a", "scale").unwrap();

        assert!(path.exists());
        drop(lock);
        assert!(!path.exists());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn cluster_mutation_lock_preserves_live_lock() {
        let dir = std::env::temp_dir().join(format!("eloqctl-lock-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cluster_a.lock");
        std::fs::write(
            &path,
            format!(
                "pid={}\ncluster=cluster/a\ncommand=apply\ncreated_at=2024-01-01T00:00:00Z\n",
                std::process::id()
            ),
        )
        .unwrap();

        let lock = super::ClusterMutationLock::acquire(&dir, "cluster/a", "scale");

        assert!(lock.is_err());
        assert!(path.exists());
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir_all(dir);
    }

    fn load_apply_test_config(
        checkpoint_interval: Option<u32>,
        retention_time: Option<&str>,
    ) -> DeployConfig {
        let checkpoint_line = checkpoint_interval
            .map(|value| format!("  checkpointer_interval: {value}\n"))
            .unwrap_or_default();
        let retention_line = retention_time
            .map(|value| format!("      retention_time: \"{value}\"\n"))
            .unwrap_or_default();
        let yaml = format!(
            r#"connection:
  username: "eloq"
  auth_type: "keypair"
  auth:
    keypair: "/tmp/id_rsa"
deployment:
  cluster_name: "apply-test"
  product: "EloqKV"
  version: "1.0.6"
  install_dir: "/tmp/eloq"
{checkpoint_line}  tx_service:
    tx_host_ports: ["127.0.0.1:6389"]
    image: "eloqkv-image"
  log_service:
    nodes:
      - host: "127.0.0.1"
        port: 9000
        data_dir: ["/tmp/log"]
    replica: 1
    image: "log-image"
  storage_service:
  monitor:
    data_dir: ""
    eloq_metrics:
      path: "/eloq_metrics"
      port: 18081
    prometheus:
      download_url: "https://example.com/prometheus.tar.gz"
      port: 9500
      host: "127.0.0.1"
{retention_line}    grafana:
      download_url: "https://example.com/grafana.tar.gz"
      port: 3301
      host: "127.0.0.1"
    node_exporter:
      url: "https://example.com/node_exporter.tar.gz"
      port: 9200
"#
        );
        DeployConfig::load_from_string(yaml).unwrap()
    }

    #[test]
    fn build_reconcile_plan_detects_supported_and_ignored_changes() {
        let current = load_apply_test_config(Some(60), Some("15d"));
        let mut desired = load_apply_test_config(Some(120), Some("30d"));
        desired.deployment.tx_service.tx_host_ports = vec!["127.0.0.2:6389".to_string()];
        let observed = ObservedCluster {
            cluster: "apply-test".to_string(),
            services: Vec::new(),
        };

        let plan = CmdExecutor::build_reconcile_plan(&current, &desired, Some(observed)).unwrap();

        assert_eq!(
            plan.tx_field_updates,
            vec!["checkpointer_interval:120".to_string()]
        );
        assert!(plan.actions.contains(&ReconcileAction::SaveClusterIndex));
        assert!(plan
            .actions
            .contains(&ReconcileAction::RestartTxWithUpdatedConfig));
        assert!(plan.actions.contains(&ReconcileAction::RestartMonitor));
        assert!(plan.actions.contains(&ReconcileAction::VerifyClusterStatus));
        assert!(plan
            .changes
            .iter()
            .any(|change| change.contains("deployment.checkpointer_interval")));
        assert!(plan
            .changes
            .iter()
            .any(|change| change.contains("retention_time")));
        assert!(plan
            .unsupported_changes
            .contains(&"deployment.tx_service.tx_host_ports".to_string()));
        assert!(plan.observed.is_some());
        assert_eq!(plan.merged_config.deployment.checkpoint_interval, Some(120));
        assert_eq!(
            plan.merged_config
                .deployment
                .monitor
                .as_ref()
                .and_then(|m| m.prometheus.as_ref())
                .and_then(|p| p.retention_time.clone()),
            Some("30d".to_string())
        );
    }

    #[test]
    fn build_reconcile_plan_ignores_omitted_supported_fields() {
        let current = load_apply_test_config(Some(120), Some("30d"));
        let desired = load_apply_test_config(None, None);

        let plan = CmdExecutor::build_reconcile_plan(&current, &desired, None).unwrap();

        assert!(plan.is_empty());
        assert_eq!(plan.merged_config.deployment.checkpoint_interval, Some(120));
        assert_eq!(
            plan.merged_config
                .deployment
                .monitor
                .as_ref()
                .and_then(|m| m.prometheus.as_ref())
                .and_then(|p| p.retention_time.clone()),
            Some("30d".to_string())
        );
    }
}
