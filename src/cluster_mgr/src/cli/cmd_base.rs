use crate::cli::task::task_base::TaskMgr;
use crate::cli::{download_dir, upload_dir, CommandArgs, HOME_DIR};
use crate::config::config_base::{DeploymentConfig, VersionRow};
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
use indicatif::ProgressBar;
use itertools::Itertools;
use std::collections::HashSet;
use std::env;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, OnceLock};
use tokio_postgres::config::SslMode;
use tokio_postgres::NoTls;
use tracing::{error, info, warn};

pub static NOT_PRINT_TASK_RESULT: &str = "NOT_PRINT_TASK_RESULT";

pub static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .build()
        .expect("can't init http client")
});

pub struct CommandExecutor {
    task_mgr: Arc<TaskMgr>,
    state_mgr: Arc<StateMgr>,
    cpu_arch: String,
    os_id: String,
    os_version: String,
    home: PathBuf,
    pg_client: OnceLock<tokio_postgres::Client>,
}

impl Default for CommandExecutor {
    fn default() -> Self {
        Self::new(None)
    }
}

impl CommandExecutor {
    pub fn new(home: Option<PathBuf>) -> Self {
        info!("CommandExecutor init.");
        let home = match home {
            Some(home) => {
                env::set_var(HOME_DIR, &home);
                home
            }
            None => match env::var(HOME_DIR) {
                Ok(v) => PathBuf::from(v),
                Err(_) => {
                    let home = env::current_dir().expect("can't know current directory");
                    env::set_var(HOME_DIR, &home);
                    home
                }
            },
        };
        // check config directory
        let cnf_dir = home.join("config");
        if !cnf_dir.exists() {
            panic!("config path not exist: {} ", cnf_dir.display());
        }
        env::set_var(CONFIG_PATH_DIR, cnf_dir);
        if !download_dir().exists() {
            std::fs::create_dir(download_dir()).expect("can't init download directory");
        }
        if !upload_dir().exists() {
            std::fs::create_dir(upload_dir()).expect("can't init upload directory");
        }

        let mut cpu_arch = sysinfo::System::cpu_arch().expect("can't know CPU arch info");
        cpu_arch = match cpu_arch.as_str() {
            "aarch64" | "arm64" => "arm64",
            "x86" | "x86_64" | "amd64" => "amd64",
            _ => panic!("unsupported cpu arch {cpu_arch}"),
        }
        .to_owned();
        let os_id = sysinfo::System::distribution_id();
        let os_version = sysinfo::System::os_version().unwrap().replace('.', "");
        Self {
            task_mgr: Arc::new(TaskMgr::new()),
            state_mgr: Arc::new(STATE_MGR.clone()),
            cpu_arch,
            os_id,
            os_version,
            home,
            pg_client: OnceLock::new(),
        }
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

    pub fn os_pretty(&self) -> String {
        format!("{}{}", self.os_id, self.os_version)
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

    async fn save_deployment_config(&self, config: &DeploymentConfig, upsert: bool) -> Result<()> {
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

    async fn get_config(&self, cmd: CommandArgs) -> anyhow::Result<DeploymentConfig> {
        match cmd.clone() {
            CommandArgs::Deploy { topology_file }
            | CommandArgs::Launch {
                topology_file,
                skip_deps: _,
            } => {
                let mut config = DeploymentConfig::load(Some(topology_file))?;
                self.resolve_version(&mut config.deployment).await?;
                config.scan_hardware().await?;
                self.save_deployment_config(&config, false).await?;
                info!("CmdExecutor Save DeploymentConfig successfully.");
                Ok(config)
            }
            CommandArgs::Demo {
                product,
                store,
                version,
                skip_deps: _,
                unlimited,
                no_monitor,
                union_wal,
                ext_cass,
                ext_cass_port,
                ext_cass_user,
                ext_cass_pwd,
            } => {
                self.gen_demo_config(
                    product,
                    store,
                    version,
                    unlimited,
                    no_monitor,
                    union_wal,
                    ext_cass,
                    ext_cass_port,
                    ext_cass_user,
                    ext_cass_pwd,
                )
                .await
            }
            CommandArgs::Install { cluster }
            | CommandArgs::Stop {
                cluster,
                force: _,
                all: _,
            }
            | CommandArgs::Start { cluster }
            | CommandArgs::LogService {
                cluster,
                command: _,
            }
            | CommandArgs::Restart { cluster }
            | CommandArgs::UpdateConf {
                cluster,
                restart: _,
            }
            | CommandArgs::Status {
                cluster,
                user: _,
                password: _,
                wait: _,
            }
            | CommandArgs::Monitor {
                command: _,
                cluster,
            }
            | CommandArgs::Inspect { cluster, .. }
            | CommandArgs::Remove { cluster }
            | CommandArgs::Connect { cluster }
            | CommandArgs::Scale {
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
            CommandArgs::RunDeps { topology_file }
            | CommandArgs::Check { topology_file }
            | CommandArgs::Exec {
                command: _,
                topology_file,
            } => Ok(DeploymentConfig::load(Some(topology_file))?),
            CommandArgs::Update {
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
        cmd: CommandArgs,
        deployment_config: Option<DeploymentConfig>,
    ) -> Result<()> {
        match &cmd {
            CommandArgs::List => return self.list_clusters().await,
            CommandArgs::Versions { product, store } => {
                return self.list_versions(product.clone(), store.clone()).await
            }
            CommandArgs::Update { cluster: None, .. } => return self.update().await,
            CommandArgs::Launch { .. }
            | CommandArgs::Demo { .. }
            | CommandArgs::Deploy { .. }
            | CommandArgs::Update {
                cluster: Some(_), ..
            }
            | CommandArgs::UpdateConf { .. } => {
                std::fs::remove_dir_all(upload_dir())?;
                std::fs::create_dir(upload_dir())?;
            }
            _ => {}
        }

        // fetch config from file or database
        let config = match deployment_config {
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
            CommandArgs::Connect { .. } => {
                println!("{}", config.client_conn());
            }
            CommandArgs::Inspect { cluster: _, format } => match format {
                Some(fmt) => match fmt {
                    TopoFormat::Yaml => println!("{}", config.to_yaml()),
                    TopoFormat::Json => println!("{}", config.to_json()),
                },
                None => println!("{:#?}", config),
            },
            CommandArgs::Scale {
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
                let recv_rs_and_print_join = tokio::task::spawn(async move {
                    let not_print_task_rs = option_env!("NOT_PRINT_TASK_RESULT");
                    if not_print_task_rs.is_none() {
                        task_mgr.print_task_result().await;
                    }
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

    async fn finishing(&self, cmd: CommandArgs, config: DeploymentConfig) -> Result<()> {
        // After all tasks finished
        match cmd {
            CommandArgs::Launch { .. } | CommandArgs::Demo { .. } => {
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
            CommandArgs::Remove { cluster } => {
                let n = self.state_mgr.delete_cluster(&cluster).await?;
                info!("cluster state cleared rows={}", n);
            }
            CommandArgs::Update {
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
            .query(&sql, &[&self.cpu_arch, &self.os_pretty()])
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
        let arch = &self.cpu_arch;
        let store = cnf.storage_service.pretty_name();
        if cnf.version.is_some() && cnf.version_str().to_ascii_lowercase() == "latest" {
            // request latest release version ID
            let client = self.pg_client().await?;
            let row = client
                .query_one(
                    "SELECT * FROM tx_release WHERE product=$1 AND arch=$2 AND os=$3 AND store=$4
                         ORDER BY version_major DESC,version_minor DESC,version_build DESC LIMIT 1",
                    &[&product, &arch, &self.os_pretty(), &store],
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
        prefix.push(self.os_pretty());
        let prefix = prefix.as_path().to_str().unwrap();
        if cnf.tx_service.image.is_none() {
            let vers = cnf.version.as_deref().expect("version is missing");
            let img = format!("{prefix}/{store}/{product}-{vers}-{arch}.tar.gz");
            info!("tx service image is set: {img}");
            cnf.tx_service.image = Some(img);
        }
        if let Some(logsrv) = &mut cnf.log_service {
            if logsrv.image.is_none() {
                let vers = cnf.version.as_deref().expect("version is missing");
                let img = format!("{prefix}/logservice/log-service-{vers}-{arch}.tar.gz");
                info!("log service image is set: {img}");
                logsrv.image = Some(img);
            }
        }
        Ok(())
    }

    async fn gen_demo_config(
        &self,
        product: Product,
        store: StorageProvider,
        version: String,
        unlimited: bool,
        no_monitor: bool,
        union_wal: bool,
        ext_cass: Vec<String>,
        ext_cass_port: Option<u16>,
        ext_cass_user: Option<String>,
        ext_cass_pwd: Option<String>,
    ) -> Result<DeploymentConfig> {
        let topology = format!(
            "{}/demo-{product}.yaml",
            self.dir_config().to_string_lossy()
        );
        let mut config = DeploymentConfig::load(Some(topology))?;
        let deploy = &mut config.deployment;
        // set storage
        match store {
            StorageProvider::Cassandra => {
                let host;
                let kind;
                if !ext_cass.is_empty() {
                    host = ext_cass;
                    kind = CassKind::External(CassConnect {
                        port: ext_cass_port,
                        user: ext_cass_user,
                        password: ext_cass_pwd,
                    });
                } else {
                    host = vec!["127.0.0.1".to_owned()];
                    kind = CassKind::Internal(CassDeploy {
                        mirror: Some(CDN.to_owned()),
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
        // set log-service
        if union_wal {
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

    async fn update(&self) -> Result<()> {
        let os = self.os_pretty();
        let arch = &self.cpu_arch;
        let filename = format!("waiter-{os}-{arch}.tar.gz");
        let url = format!("{CDN}/waiter/{filename}");
        info!("Fetching latest package {url}");
        let resp = HTTP_CLIENT.get(&url).send().await?;
        if !resp.status().is_success() {
            bail!("Fetch package failed: {}", resp.status());
        }
        let len = resp
            .content_length()
            .ok_or_else(|| anyhow!("can't know package size"))?;
        let mut cache_path = self.dir_download();
        cache_path.push(filename);
        if cache_path.exists() {
            let local_len = std::fs::metadata(&cache_path)?.len();
            info!("latest package length {len}, local package length {local_len}");
            if len == local_len {
                println!("cluster_mgr is already latest");
                return Ok(());
            }
        }
        // start downloading new package
        let pg_bar = ProgressBar::new(len);
        let mut file = pg_bar.wrap_write(std::fs::File::create(&cache_path)?);
        let mut stream_reader = resp.bytes_stream();
        while let Some(stream_chunk) = stream_reader.next().await {
            let chunk = stream_chunk.map_err(|e| anyhow!("download failed: {e}"))?;
            file.write_all(&chunk)
                .map_err(|e| anyhow!("can't write file: {e}"))?;
        }
        println!(
            "Execute the following command to complete the update:\n tar -xzvf {} -C {} --strip-components 1 --overwrite",
            cache_path.to_string_lossy(), self.dir_home()
        );
        Ok(())
    }
}
