use crate::cli::task::backup_utils::split_manifests;
use crate::cli::task::group::Config;
use crate::cli::task::task_base::{
    set_verbose_task_output, TaskArgValue, TaskMgr, TaskResultEnum, TaskResultPair,
};
use crate::cli::util::{cpu_arch, file_pg_bar, os_id, os_major_version};
use crate::cli::{upload_dir, SubCommand, CMD_OUTPUT, CMD_STATUS, HOME_DIR};
use crate::cli::{BackupCommand, ProxyCommand};
use crate::config::config_base::{DeployConfig, VersionRow};
use crate::config::deployment::{Deployment, Product};
use crate::config::proxy_config_base::ProxyConfig;
use crate::config::storage_service_config::{
    CassConnect, CassDeploy, CassKind, Cassandra, DataStoreServiceBackend, DataStoreServiceMode,
    RocksDB, RocksLocal, StorageService,
};
use crate::config::{StorageProvider, TopoFormat, CDN, CONFIG_PATH_DIR, UPLOAD_PATH_DIR};
use crate::state::deployment_operation::{DeploymentEntity, DeploymentOperation};
use crate::state::proxy_operation::{ProxyEntity, ProxyOperation};
use crate::state::state_base::{QueryCondition, StateOperation};
use crate::state::state_mgr::{StateMgr, DEPLOYMENT_STATE, PROXY_STATE, STATE_MGR};
use crate::StateValue;
use anyhow::{anyhow, bail, Result};
use futures::StreamExt;
use itertools::Itertools;
use owo_colors::OwoColorize;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;
use std::{env, fs};
use tokio_postgres::config::SslMode;
use tokio_postgres::NoTls;
use tracing::{error, info, warn};

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
        .connect_timeout(Duration::from_secs(30))
        .build()
        .expect("can't init http client")
});

pub static HTTP_INTERNAL: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .no_proxy()
        .build()
        .expect("can't init http client for internal use")
});

pub struct CmdExecutor {
    task_mgr: Arc<TaskMgr>,
    state_mgr: Arc<StateMgr>,
    pg_client: OnceLock<tokio_postgres::Client>,
    pub home: PathBuf,
}

impl CmdExecutor {
    pub fn new(home: PathBuf) -> Self {
        Self {
            task_mgr: Arc::new(TaskMgr::new()),
            state_mgr: Arc::new(STATE_MGR.clone()),
            pg_client: OnceLock::new(),
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

    async fn pg_client(&self) -> Result<&tokio_postgres::Client> {
        if let Some(client) = self.pg_client.get() {
            Ok(client)
        } else {
            let (client, conn) = tokio_postgres::Config::new()
                .user("readonly_user")
                .password("eloq_readonly123!")
                .host("18.177.72.104")
                .port(5432)
                .dbname("eloq_release")
                .ssl_mode(SslMode::Prefer)
                .connect(NoTls)
                .await
                .map_err(|e| anyhow!("connect postgres failed: {e}"))?;
            // The connection object performs the actual communication with the database,
            // so spawn it off to run on its own.
            tokio::spawn(async move {
                if let Err(e) = conn.await {
                    error!("PG connection error: {}", e);
                }
            });
            self.pg_client.set(client).unwrap();
            Ok(self.pg_client.get().unwrap())
        }
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

    fn parse_task_id_parts(task_id: &str) -> (String, String, String) {
        let mut host = String::new();
        let mut cmd = String::new();
        let mut task = String::new();
        for part in task_id.split(',') {
            if let Some((k, v)) = part.split_once('=') {
                match k.trim() {
                    "host" => host = v.trim().to_string(),
                    "cmd" => cmd = v.trim().to_string(),
                    "task" => task = v.trim().to_string(),
                    _ => {}
                }
            }
        }
        (host, cmd, task)
    }

    fn summarize_status_rows(task_results: &[TaskResultPair]) -> Vec<StatusSummaryRow> {
        task_results
            .iter()
            .filter_map(|pair| {
                let (host, cmd, task) = Self::parse_task_id_parts(&pair.task_id);
                if host.is_empty() {
                    return None;
                }
                let tx_prefix = ["tx-status-", "txservice-status-"]
                    .into_iter()
                    .find(|prefix| task.starts_with(prefix));
                let (service, port) = if let Some(prefix) = tx_prefix {
                    (
                        "tx".to_string(),
                        task.trim_start_matches(prefix).to_string(),
                    )
                } else if task.starts_with("standby-status-") {
                    (
                        "standby".to_string(),
                        task.trim_start_matches("standby-status-").to_string(),
                    )
                } else if task.starts_with("voter-status-") {
                    (
                        "voter".to_string(),
                        task.trim_start_matches("voter-status-").to_string(),
                    )
                } else if cmd.starts_with("monograph_log_") && task == "status" {
                    ("log".to_string(), "-".to_string())
                } else if cmd.starts_with("dss_") && task == "status" {
                    ("dss".to_string(), "-".to_string())
                } else {
                    return None;
                };

                match &pair.result {
                    TaskResultEnum::Error(err) => Some(StatusSummaryRow {
                        host,
                        service,
                        port,
                        status: "ERROR".to_string(),
                        detail: err.replace('\n', " "),
                    }),
                    TaskResultEnum::Success(Some(ev)) => {
                        let status_code = ev
                            .get(CMD_STATUS)
                            .map(|v| TaskArgValue::into_inner_value::<i32>(v.clone()))
                            .unwrap_or(0);
                        let detail = ev
                            .get(CMD_OUTPUT)
                            .map(|v| TaskArgValue::into_inner_value::<String>(v.clone()))
                            .unwrap_or_default()
                            .replace('\n', " ")
                            .trim()
                            .to_string();
                        let low = detail.to_ascii_lowercase();
                        let status = if status_code != 0 {
                            "ERROR".to_string()
                        } else if low.contains("running") {
                            "UP".to_string()
                        } else if low.contains("down") {
                            "DOWN".to_string()
                        } else if low.contains("unknown") {
                            "UNKNOWN".to_string()
                        } else {
                            "OK".to_string()
                        };
                        Some(StatusSummaryRow {
                            host,
                            service,
                            port,
                            status,
                            detail: if detail.len() > 120 {
                                format!("{}...", &detail[..120])
                            } else {
                                detail
                            },
                        })
                    }
                    TaskResultEnum::Success(None) => None,
                }
            })
            .collect_vec()
    }

    fn print_status_summary(task_results: &[TaskResultPair]) {
        let rows = Self::summarize_status_rows(task_results);
        if rows.is_empty() {
            println!("No status rows found.");
            return;
        }
        println!("{}", tabled::Table::new(rows));
    }

    fn dir_home(&self) -> &str {
        self.home.to_str().expect("invalid home directory")
    }

    fn dir_config(&self) -> PathBuf {
        self.home.join("config")
    }

    fn dir_download(&self) -> PathBuf {
        self.home.join("download")
    }

    async fn save_deployment_config(&self, config: &DeployConfig, upsert: bool) -> Result<()> {
        let deployment_operation = self
            .state_mgr
            .get_state_operation::<DeploymentOperation>(DEPLOYMENT_STATE);

        let cluster = &config.deployment.cluster_name;
        let deployment_entity = deployment_operation
            .load(|| -> Option<QueryCondition> {
                Some(QueryCondition {
                    cond_text: "cluster_name = $1".to_string(),
                    bind_values: vec![StateValue::Varchar(cluster.clone())],
                })
            })
            .await?;
        if !deployment_entity.is_empty() && !upsert {
            bail!("cluster {cluster} already exists");
        }
        let all_hosts = config.get_unique_host_list().join(";");
        let config_string = config.to_yaml();
        info!("DeploymentConfig saved: cluster={cluster} @ {all_hosts}");
        let default_timestamp = chrono::DateTime::default();
        deployment_operation
            .put(DeploymentEntity {
                cluster_name: config.deployment.clone().cluster_name,
                deployment_config: config_string,
                host_list: all_hosts,
                create_timestamp: default_timestamp,
                update_timestamp: default_timestamp,
            })
            .await?;
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

    async fn get_config(&self, cmd: SubCommand) -> anyhow::Result<Config> {
        match cmd {
            SubCommand::Deploy { topology_file }
            | SubCommand::Launch {
                topology_file,
                skip_deps: _,
            } => {
                let mut config = DeployConfig::load(Some(topology_file))?;

                // Validate metrics configuration
                if let Some(monitor) = &config.deployment.monitor {
                    let has_monograph = monitor.monograph_metrics.is_some();
                    let has_eloq = monitor.eloq_metrics.is_some();

                    if !has_monograph && !has_eloq {
                        bail!("Monitor configuration is provided but neither monograph_metrics nor eloq_metrics is specified");
                    }

                    if has_monograph && has_eloq {
                        bail!("Cannot specify both monograph_metrics and eloq_metrics simultaneously; choose one");
                    }
                }

                self.resolve_version(&mut config.deployment).await?;
                self.save_deployment_config(&config, false).await?;
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
            | SubCommand::Monitor {
                command: _,
                cluster,
            }
            | SubCommand::Inspect { cluster, .. }
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

                // Validate metrics configuration
                if let Some(monitor) = &config.deployment.monitor {
                    let has_monograph = monitor.monograph_metrics.is_some();
                    let has_eloq = monitor.eloq_metrics.is_some();

                    if !has_monograph && !has_eloq {
                        bail!("Monitor configuration is provided but neither monograph_metrics nor eloq_metrics is specified");
                    }

                    if has_monograph && has_eloq {
                        bail!("Cannot specify both monograph_metrics and eloq_metrics simultaneously; choose one");
                    }
                }

                Ok(Config::Cluster(config))
            }
            SubCommand::RunDeps { topology_file }
            | SubCommand::Check { topology_file }
            | SubCommand::Exec {
                command: _,
                topology_file,
            } => {
                let config = DeployConfig::load(Some(topology_file))?;

                // Validate metrics configuration
                if let Some(monitor) = &config.deployment.monitor {
                    let has_monograph = monitor.monograph_metrics.is_some();
                    let has_eloq = monitor.eloq_metrics.is_some();

                    if !has_monograph && !has_eloq {
                        bail!("Monitor configuration is provided but neither monograph_metrics nor eloq_metrics is specified");
                    }

                    if has_monograph && has_eloq {
                        bail!("Cannot specify both monograph_metrics and eloq_metrics simultaneously; choose one");
                    }
                }

                Ok(Config::Cluster(config))
            }
            SubCommand::Update {
                cluster: Some(cluster),
                version,
                cassandra,
                cass_mirror,
                ..
            } => {
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
                if cassandra.is_some() || cass_mirror.is_some() {
                    let cass = &mut config
                        .deployment
                        .storage_service
                        .as_mut()
                        .expect("storage_service is required")
                        .cassandra;
                    if cass.is_none() {
                        bail!("do not have cassandra");
                    }
                    if cass.is_none() {
                        bail!("do not have cassandra");
                    }
                    if let CassKind::Internal(cass) = &mut cass.as_mut().unwrap().kind {
                        if let Some(v) = cassandra {
                            if v == cass.version {
                                warn!("cassandra version not changed")
                            }
                            cass.version = v;
                        }
                        if let Some(mi) = cass_mirror {
                            cass.mirror = Some(mi);
                        }
                    } else {
                        bail!("can not update cassandra");
                    }
                }

                // Validate metrics configuration
                if let Some(monitor) = &config.deployment.monitor {
                    let has_monograph = monitor.monograph_metrics.is_some();
                    let has_eloq = monitor.eloq_metrics.is_some();

                    if !has_monograph && !has_eloq {
                        bail!("Monitor configuration is provided but neither monograph_metrics nor eloq_metrics is specified");
                    }

                    if has_monograph && has_eloq {
                        bail!("Cannot specify both monograph_metrics and eloq_metrics simultaneously; choose one");
                    }
                }

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

            _ => unreachable!(),
        }
    }

    pub async fn run(
        &'static self,
        mut cmd: SubCommand,
        option_config: Option<Config>,
        quiet: bool,
        verbose: bool,
    ) -> Result<()> {
        set_verbose_task_output(verbose);
        match &cmd {
            SubCommand::List => return self.list_clusters().await,
            SubCommand::Versions { product, store } => {
                return self.list_versions(product.clone(), store.clone()).await
            }
            SubCommand::Update { cluster: None, .. } => return self.update().await,
            SubCommand::Remove { cluster, force: _ } => {
                let upload_path = upload_dir().join(cluster);
                if upload_path.exists() {
                    std::fs::remove_dir_all(upload_path)?;
                }
            }
            SubCommand::Upgrade => {
                self.state_mgr.upgrade_schema().await?;
                println!("Schema upgrade complete");
                return Ok(());
            }
            _ => {}
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
                    SubCommand::Inspect { cluster: _, format } => match format {
                        Some(fmt) => match fmt {
                            TopoFormat::Yaml => println!("{}", deploy_config.to_yaml()),
                            TopoFormat::Json => println!("{}", deploy_config.to_json()),
                        },
                        None => println!("{:#?}", deploy_config),
                    },
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
                        if let SubCommand::Status { detail, .. } = &cmd {
                            if !detail {
                                Self::print_status_summary(&rs);
                                if !verbose {
                                    println!(
                                        "Tip: use `--verbose` to show per-task execution details."
                                    );
                                }
                            }
                        }

                        // Using cluster_config again without moving it
                        self.finishing(cmd, Config::Cluster(deploy_config)).await?;
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
                        if let Some(monograph_metrics) = &monitor.monograph_metrics {
                            if let (Some(path), Some(port)) =
                                (&monograph_metrics.path, monograph_metrics.port)
                            {
                                println!("Monograph Metrics: http://<host>:{}{}", port, path);
                            }
                        }

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
                        let failed_hosts = failed_tasks
                            .iter()
                            .map(|t| t.task_host.clone())
                            .unique()
                            .collect_vec();
                        eprintln!(
                            "Remove completed with {} failed task(s) on host(s): {}",
                            failed_tasks.len(),
                            failed_hosts.join(", ")
                        );
                        eprintln!(
                            "Please clean unreachable hosts manually, e.g.: rm -rf {}",
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
        let client = self.pg_client().await?;
        let mut sql = "SELECT * FROM tx_release WHERE arch=$1 AND os=$2".to_owned();
        if let Some(p) = product {
            sql.push_str(&format!(" AND product='{}'", p.name()));
        }
        if let Some(s) = store {
            sql.push_str(&format!(" AND store='{s}'"));
        }

        let list = client
            .query(&sql, &[&cpu_arch(), &self.os_vers()])
            .await?
            .into_iter()
            .map(|row| {
                let product: String = row.get("product");
                let store: String = row.get("store");
                let major: i32 = row.get("version_major");
                let minor: i32 = row.get("version_minor");
                let build: i32 = row.get("version_build");
                let version: String = format!("{major}.{minor}.{build}");
                VersionRow {
                    product,
                    store,
                    version,
                }
            })
            .collect_vec();
        let table = tabled::Table::new(list);
        println!("{table}\n");
        Ok(())
    }

    pub async fn resolve_version(&self, cnf: &mut Deployment) -> Result<()> {
        let product = cnf.product().name().to_owned();
        let arch = cpu_arch();
        let os = self.os_vers();

        // Get store name once and reuse it, if not set, use rocksdb as default
        let store = cnf
            .storage_service
            .as_ref()
            .map_or("rocksdb".to_string(), |s| s.pretty_name());

        if cnf.version.is_some() && cnf.version_str().eq_ignore_ascii_case("latest") {
            let client = self.pg_client().await?;

            // Use the correct query based on storage_service existence
            let row = if cnf.storage_service.is_some() {
                client.query_one(
                    "SELECT * FROM tx_release WHERE product=$1 AND arch=$2 AND os=$3 AND store=$4
                     ORDER BY version_major DESC,version_minor DESC,version_build DESC LIMIT 1",
                    &[&product, &arch, &os, &store]
                ).await
            } else {
                client
                    .query_one(
                        "SELECT * FROM tx_release WHERE product=$1 AND arch=$2 AND os=$3
                     ORDER BY version_major DESC,version_minor DESC,version_build DESC LIMIT 1",
                        &[&product, &arch, &os],
                    )
                    .await
            }
            .map_err(|e| anyhow!("fetch latest version failed: {e}"))?;

            if row.is_empty() {
                bail!("no available release found")
            }
            let major: i32 = row.get("version_major");
            let minor: i32 = row.get("version_minor");
            let build: i32 = row.get("version_build");
            let latest: String = format!("{major}.{minor}.{build}");
            info!("latest release version = {latest}");
            cnf.version = Some(latest);
        }

        let mut prefix = PathBuf::from(CDN);
        prefix.push(&product);
        let prefix = prefix.as_path().to_str().unwrap();
        if cnf.tx_service.image.is_none() {
            let vers = cnf.version.as_deref().expect("version is missing");
            let img = format!("{prefix}/{store}/{product}-{vers}-{os}-{arch}.tar.gz");
            info!("tx service image is set: {img}");
            cnf.tx_service.image = Some(img);
        }
        if let Some(logsrv) = &mut cnf.log_service {
            if logsrv.image.is_none() {
                let vers = cnf.version.as_deref().expect("version is missing");
                let img =
                    format!("{prefix}/logservice/{store}/log-service-{vers}-{os}-{arch}.tar.gz");
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
                ext_cass,
                cass_port,
                cass_auth,
            } => {
                let topology = format!(
                    "{}/demo-{product}.yaml",
                    self.dir_config().to_string_lossy()
                );
                let mut config = DeployConfig::load(Some(topology))?;
                let deploy = &mut config.deployment;
                // set storage
                match store {
                    StorageProvider::Cassandra => {
                        let host;
                        let kind;
                        if !ext_cass.is_empty() {
                            host = ext_cass;
                            let mut user = None;
                            let mut password = None;
                            if let Some(auth) = cass_auth {
                                if let Some((u, p)) = auth.split_once(':') {
                                    if !u.is_empty() {
                                        user = Some(u.to_owned());
                                    }
                                    if !p.is_empty() {
                                        password = Some(p.to_owned());
                                    }
                                }
                            }
                            kind = CassKind::External(CassConnect {
                                port: cass_port,
                                user,
                                password,
                            });
                        } else {
                            host = vec!["127.0.0.1".to_owned()];
                            kind = CassKind::Internal(CassDeploy {
                                mirror: Some(CDN.to_owned()),
                                version: "4.1.3".to_owned(),
                                cluster_name: None,
                            });
                        };
                        deploy
                            .storage_service
                            .get_or_insert(StorageService {
                                cassandra: None,
                                dynamodb: None,
                                rocksdb: None,
                                eloqdss: None,
                            })
                            .cassandra = Some(Cassandra { host, kind });
                    }
                    StorageProvider::Dynamodb => unimplemented!(),
                    StorageProvider::Rocksdb => {
                        deploy
                            .storage_service
                            .get_or_insert(StorageService {
                                cassandra: None,
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
                                cassandra: None,
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
                    if store != StorageProvider::Cassandra {
                        monitor.cassandra_collector = None
                    }

                    // Check metrics configuration
                    let has_monograph = monitor.monograph_metrics.is_some();
                    let has_eloq = monitor.eloq_metrics.is_some();

                    // If neither is present, report an error
                    if !has_monograph && !has_eloq {
                        bail!("Monitor configuration is provided but neither monograph_metrics nor eloq_metrics is specified");
                    }

                    // If both are present, remove one based on product type to maintain consistency
                    if has_monograph && has_eloq {
                        match deploy.product {
                            Product::EloqSQL => {
                                monitor.eloq_metrics = None;
                            }
                            Product::EloqKV => {
                                monitor.monograph_metrics = None;
                            }
                        }
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
            _ => unreachable!(),
        }
    }

    async fn update(&self) -> Result<()> {
        let os = self.os_vers();
        let arch = cpu_arch();
        let filename = format!("eloqctl-main-{os}-{arch}.tar.gz");
        let url = format!("{CDN}/eloqctl/{arch}/main/{filename}");
        info!("Fetching latest package {url}");
        let resp = HTTP_CLIENT.get(&url).send().await?;
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
        // start downloading new package
        let pg_bar = file_pg_bar();
        pg_bar.set_length(len);
        pg_bar.set_message("downloading");
        let mut file = pg_bar.wrap_write(std::fs::File::create(&cached)?);
        let mut stream = resp.bytes_stream();
        while let Some(stream_chunk) = stream.next().await {
            let chunk = stream_chunk.map_err(|e| anyhow!("download failed: {e}"))?;
            file.write_all(&chunk)
                .map_err(|e| anyhow!("write file failed: {e}"))?;
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
