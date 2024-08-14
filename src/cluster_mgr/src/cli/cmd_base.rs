use crate::cli::task::task_base::TaskMgr;
use crate::cli::util::{cpu_arch, file_pg_bar, os_id, os_major_version};
use crate::cli::{upload_dir, SubCommand, HOME_DIR};
use crate::config::config_base::{DeployConfig, VersionRow};
use crate::config::deployment::{Deployment, Product};
use crate::config::storage_service_config::{
    CassConnect, CassDeploy, CassKind, Cassandra, RocksDB,
};
use crate::config::{StorageProvider, TopoFormat, CDN, CONFIG_PATH_DIR};
use crate::state::deployment_operation::{DeploymentEntity, DeploymentOperation};
use crate::state::state_base::{QueryCondition, StateOperation};
use crate::state::state_mgr::{StateMgr, DEPLOYMENT_STATE, STATE_MGR};
use crate::StateValue;
use anyhow::{anyhow, bail, Result};
use futures::StreamExt;
use itertools::Itertools;
use owo_colors::OwoColorize;
use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;
use std::{env, fs};
use tokio_postgres::config::SslMode;
use tokio_postgres::NoTls;
use tracing::{error, info, warn};

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
            std::fs::create_dir(up_dir)?;
        }
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
                .user("postgres")
                .password("eloq-pub-service-postgresql")
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

    async fn get_config(&self, cmd: SubCommand) -> anyhow::Result<DeployConfig> {
        match cmd {
            SubCommand::Deploy { topology_file }
            | SubCommand::Launch {
                topology_file,
                skip_deps: _,
            } => {
                let mut config = DeployConfig::load(Some(topology_file))?;
                self.resolve_version(&mut config.deployment).await?;
                config.scan_hardware().await?;
                self.save_deployment_config(&config, false).await?;
                info!("CmdExecutor Save DeploymentConfig successfully.");
                Ok(config)
            }
            SubCommand::Demo { .. } => self.gen_demo_config(cmd).await,
            SubCommand::Install { cluster }
            | SubCommand::Stop { cluster, .. }
            | SubCommand::Start { cluster }
            | SubCommand::LogService {
                cluster,
                command: _,
            }
            | SubCommand::Restart { cluster }
            | SubCommand::UpdateConf {
                cluster,
                restart: _,
            }
            | SubCommand::Status {
                cluster,
                user: _,
                password: _,
                wait: _,
            }
            | SubCommand::Monitor {
                command: _,
                cluster,
            }
            | SubCommand::Inspect { cluster, .. }
            | SubCommand::Remove { cluster }
            | SubCommand::Connect { cluster }
            | SubCommand::Scale {
                cluster,
                add_tx_node: _,
                del_tx_node: _,
            } => {
                let config = self
                    .state_mgr
                    .load_deployment_from_state(&cluster)
                    .await?
                    .ok_or(anyhow!("cluster {} not found", cluster))?;
                Ok(config)
            }
            SubCommand::RunDeps { topology_file }
            | SubCommand::Check { topology_file }
            | SubCommand::Exec {
                command: _,
                topology_file,
            } => Ok(DeployConfig::load(Some(topology_file))?),
            SubCommand::Update {
                cluster: Some(cluster),
                version,
                cassandra,
                cass_mirror,
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
                    let cass = &mut config.deployment.storage_service.cassandra;
                    if cass.is_none() {
                        bail!("do not have cassandra");
                    }
                    if let CassKind::Internal(cass) = &mut cass.as_mut().unwrap().kind {
                        if let Some(v) = cassandra {
                            if v == cass.version {
                                bail!("cassandra version not changed")
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
                Ok(config)
            }
            _ => unreachable!(),
        }
    }

    pub async fn run(
        &'static self,
        cmd: SubCommand,
        config: Option<DeployConfig>,
        quiet: bool,
    ) -> Result<()> {
        match &cmd {
            SubCommand::List => return self.list_clusters().await,
            SubCommand::Versions { product, store } => {
                return self.list_versions(product.clone(), store.clone()).await
            }
            SubCommand::Update { cluster: None, .. } => return self.update().await,
            SubCommand::Launch { .. }
            | SubCommand::Demo { .. }
            | SubCommand::Deploy { .. }
            | SubCommand::Update {
                cluster: Some(_), ..
            }
            | SubCommand::UpdateConf { .. } => {
                std::fs::remove_dir_all(upload_dir())?;
                std::fs::create_dir(upload_dir())?;
            }
            _ => {}
        }

        // fetch config from file or database
        let config = match config {
            Some(mut config) => {
                config.connection.auth.check_keypair()?;
                self.resolve_version(&mut config.deployment).await?;
                config.scan_hardware().await?;
                self.save_deployment_config(&config, true).await?;
                config
            }
            None => self.get_config(cmd.clone()).await?,
        };

        match cmd.clone() {
            SubCommand::Connect { .. } => {
                println!("{}", config.client_conn());
            }
            SubCommand::Inspect { cluster: _, format } => match format {
                Some(fmt) => match fmt {
                    TopoFormat::Yaml => println!("{}", config.to_yaml()),
                    TopoFormat::Json => println!("{}", config.to_json()),
                },
                None => println!("{:#?}", config),
            },
            SubCommand::Scale {
                cluster: _,
                add_tx_node,
                del_tx_node,
            } => {
                let add: HashSet<String> = HashSet::from_iter(add_tx_node.clone().into_iter());
                let del: HashSet<String> = HashSet::from_iter(del_tx_node.clone().into_iter());
                if add.intersection(&del).count() > 0 {
                    bail!("add_tx_node is overlaped with del_tx_node")
                }
                let hosts: HashSet<String> =
                    HashSet::from_iter(config.deployment.tx_service.host.clone().into_iter());
                if add.intersection(&hosts).count() > 0 {
                    bail!("can't add node already in cluster")
                }
                if !del.is_subset(&hosts) {
                    bail!("deleted node not found")
                }
                if add.is_empty() && del.is_empty() {
                    warn!("scale do nothing");
                    return Ok(());
                }
                // TODO(zhanghao): scale cluster
                let mut config = config;
                let tx_hosts = &mut config.deployment.tx_service.host;
                tx_hosts.retain(|h| !del_tx_node.contains(h));
                tx_hosts.extend(add_tx_node);
                // self.save_deployment_config(&config, true).await?;
                // println!("cluster {cluster} is scaled!");
                unimplemented!()
            }
            _ => {
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
                        .write_task_result(outfile)
                        .await
                        .expect("write task result failed");
                });
                // Generate and run tasks
                let rs = self.task_mgr.run_tasks(cmd.clone(), config.clone()).await?;
                recv_rs_and_print_join.await?;
                info!(r#"all tasks complete.task_size={}"#, rs.len());
                self.finishing(cmd, config).await?;
            }
        }
        Ok(())
    }

    async fn finishing(&self, cmd: SubCommand, config: DeployConfig) -> Result<()> {
        // After all tasks finished
        match cmd {
            SubCommand::Launch { .. } | SubCommand::Demo { .. } => {
                println!("Launch cluster finished, Enjoy!");
                println!("Connect to server: \n\t{}", config.client_conn());
                if let Some(moni) = &config.deployment.monitor {
                    println!(
                        "Prometheus: http://{}:{}",
                        moni.prometheus.host, moni.prometheus.port
                    );
                    println!(
                        "Grafana: http://{}:{}",
                        moni.grafana.host, moni.grafana.port
                    );
                }
            }
            SubCommand::Remove { cluster } => {
                let n = self.state_mgr.delete_cluster(&cluster).await?;
                info!("cluster state cleared rows={n}");
            }
            SubCommand::Update {
                cluster: Some(cluster),
                ..
            } => {
                self.save_deployment_config(&config, true).await?;
                println!("cluster {cluster} is updated!");
            }
            _ => {}
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
        let store = cnf.storage_service.pretty_name();
        if cnf.version.is_some() && cnf.version_str().to_ascii_lowercase() == "latest" {
            // request latest release version ID
            let client = self.pg_client().await?;
            let row = client
                .query_one(
                    "SELECT * FROM tx_release WHERE product=$1 AND arch=$2 AND os=$3 AND store=$4
                         ORDER BY version_major DESC,version_minor DESC,version_build DESC LIMIT 1",
                    &[&product, &arch, &os, &store],
                )
                .await
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
        // cnf.version.as_mut().unwrap().make_ascii_lowercase();

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
                let img = format!("{prefix}/logservice/log-service-{vers}-{os}-{arch}.tar.gz");
                info!("log service image is set: {img}");
                logsrv.image = Some(img);
            }
        }
        Ok(())
    }

    async fn gen_demo_config(&self, cmd: SubCommand) -> Result<DeployConfig> {
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
                                mirror: None,
                                version: "4.1.3".to_owned(),
                                cluster_name: None,
                            });
                        };
                        deploy.storage_service.cassandra = Some(Cassandra { host, kind });
                    }
                    StorageProvider::Dynamodb => unimplemented!(),
                    StorageProvider::Rocksdb => {
                        deploy.storage_service.rocksdb = Some(RocksDB::Local);
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
                    config.scan_hardware().await?;
                }
                self.save_deployment_config(&config, false).await?;
                Ok(config)
            }
            _ => unreachable!(),
        }
    }

    async fn update(&self) -> Result<()> {
        let os = self.os_vers();
        let arch = cpu_arch();
        let filename = format!("eloqctl-{os}-{arch}.tar.gz");
        let url = format!("{CDN}/eloqctl/{filename}");
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
