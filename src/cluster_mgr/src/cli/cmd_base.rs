use crate::cli::task::task_base::TaskMgr;
use crate::cli::CommandArgs;
use crate::config::config_base::DeploymentConfig;
use crate::config::storage_service_config::{
    CassConnect, CassDeploy, CassKind, Cassandra, RocksDB,
};
use crate::config::{StorageProvider, CONFIG_PATH_DIR, DOWNLOAD_SRC};
use crate::state::deployment_operation::{DeploymentEntity, DeploymentOperation};
use crate::state::state_base::{QueryCondition, StateOperation};
use crate::state::state_mgr::{StateMgr, DEPLOYMENT_STATE, STATE_MGR};
use crate::StateValue;
use anyhow::{anyhow, bail, Result};
use itertools::Itertools;
use std::collections::HashSet;
use std::env;
use std::sync::Arc;
use tracing::{debug, info, warn};

pub static NOT_PRINT_TASK_RESULT: &str = "NOT_PRINT_TASK_RESULT";

#[derive(Clone)]
pub struct CommandExecutor {
    task_mgr: Arc<TaskMgr>,
    state_mgr: Arc<StateMgr>,
}

impl Default for CommandExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandExecutor {
    pub fn new() -> Self {
        info!("CommandExecutor init.");
        Self {
            task_mgr: Arc::new(TaskMgr::new()),
            state_mgr: Arc::new(STATE_MGR.clone()),
        }
    }

    pub fn task_mgr(&self) -> &Arc<TaskMgr> {
        &self.task_mgr
    }

    pub fn state_mgr(&self) -> &Arc<StateMgr> {
        &self.state_mgr
    }

    async fn save_deployment_config(&self, config: &DeploymentConfig, upsert: bool) -> Result<()> {
        let deployment_operation = self
            .state_mgr
            .get_state_operation::<DeploymentOperation>(DEPLOYMENT_STATE);

        let curr_cluster = &config.deployment.cluster_name;
        let deployment_entity = deployment_operation
            .load(|| -> Option<QueryCondition> {
                Some(QueryCondition {
                    cond_text: "cluster_name = $1".to_string(),
                    bind_values: vec![StateValue::Varchar(curr_cluster.clone())],
                })
            })
            .await?;
        if !deployment_entity.is_empty() {
            if !upsert {
                bail!("cluster {} already exists", curr_cluster);
            }
        }
        let all_hosts = config.get_unique_host_list().join(";");
        let config_string = config.config_to_string();
        debug!(
            "CmdExecutor save DeploymentConfig {} {}",
            config_string, all_hosts
        );
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
            | CommandArgs::Upgrade { topology_file }
            | CommandArgs::Launch {
                topology_file,
                skip_deps: _,
            } => {
                let mut config = DeploymentConfig::load(Some(topology_file))?;
                config.deployment.set_image().await?;
                config.scan_hardware().await?;
                self.save_deployment_config(&config, cmd.as_ref().eq("upgrade"))
                    .await?;
                info!("CmdExecutor Save DeploymentConfig successfully.");
                Ok(config)
            }
            CommandArgs::Demo {
                product,
                store,
                version,
                skip_deps: _,
                unlimited,
                ext_cass,
                ext_cass_port,
                ext_cass_user,
                ext_cass_pwd,
            } => {
                let dir = env::var(CONFIG_PATH_DIR)?;
                let topology = format!("{dir}/demo-{product}.yaml");
                let mut config = DeploymentConfig::load(Some(topology))?;
                let deploy = &mut config.deployment;
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
                            let download_url = format!(
                                "{}/others/apache-cassandra-4.1.3-bin.tar.gz",
                                DOWNLOAD_SRC.as_str()
                            );
                            kind = CassKind::Internal(CassDeploy {
                                download_url,
                                cluster_name: None,
                            });
                        };
                        deploy.storage_service.cassandra = Some(Cassandra { host, kind });
                    }
                    StorageProvider::Dynamo => unimplemented!(),
                    StorageProvider::Rocks => {
                        deploy.storage_service.rocksdb = Some(RocksDB::Local);
                    }
                }
                if let Some(monitor) = &mut deploy.monitor {
                    if store != StorageProvider::Cassandra {
                        monitor.cassandra_collector = None
                    }
                }
                deploy.version.replace(version);
                deploy.set_image().await?;
                // add kv-store name to cluster name suffix
                let name_suffix = format!("-{store}");
                deploy.cluster_name.push_str(&name_suffix);
                // add an unique number (pid) to WAL directory
                let pid = std::process::id().to_string();
                deploy
                    .log_service
                    .as_mut()
                    .unwrap()
                    .nodes
                    .first_mut()
                    .unwrap()
                    .data_dir
                    .first_mut()
                    .unwrap()
                    .push_str(&pid);
                if unlimited {
                    deploy.hardware = None;
                    config.scan_hardware().await?;
                }
                self.save_deployment_config(&config, false).await?;
                Ok(config)
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
            | CommandArgs::Inspect { cluster, yaml: _ }
            | CommandArgs::Remove { cluster }
            | CommandArgs::Connect { cluster }
            | CommandArgs::Scale {
                cluster,
                add_tx_node: _,
                del_tx_node: _,
            } => {
                let config = self
                    .state_mgr
                    .load_deployment_from_state(cluster.as_str())
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
            CommandArgs::List => unreachable!(),
        }
    }

    pub async fn run(
        &'static self,
        cmd: CommandArgs,
        deployment_config: Option<DeploymentConfig>,
    ) -> Result<()> {
        match cmd {
            CommandArgs::List => {
                return self.list_clusters().await;
            }
            _ => {}
        }

        // fetch config from file or database
        let cmd_ref = cmd.as_ref();
        let config = match deployment_config {
            Some(mut config) => {
                config.scan_hardware().await?;
                self.save_deployment_config(&config, cmd_ref.eq("upgrade"))
                    .await?;
                config
            }
            None => self.get_config(cmd.clone()).await?,
        };

        match cmd.clone() {
            CommandArgs::Scale {
                cluster: _,
                add_tx_node,
                del_tx_node,
            } => {
                let add: HashSet<String> = HashSet::from_iter(add_tx_node.into_iter());
                let del: HashSet<String> = HashSet::from_iter(del_tx_node.into_iter());
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
            }
            _ => {}
        }

        let recv_rs_and_print_join = tokio::task::spawn(async move {
            let not_print_task_rs = option_env!("NOT_PRINT_TASK_RESULT");
            if not_print_task_rs.is_none() {
                self.task_mgr.print_task_result().await;
            }
        });
        // Generate and run tasks
        let rs = self.task_mgr.run_tasks(cmd.clone(), config.clone()).await?;
        recv_rs_and_print_join.await?;
        info!(r#"all tasks complete.task_size={}"#, rs.len());

        // After all tasks finished
        match cmd {
            CommandArgs::Launch {
                topology_file: _,
                skip_deps: _,
            }
            | CommandArgs::Demo {
                product: _,
                store: _,
                version: _,
                skip_deps: _,
                unlimited: _,
                ext_cass: _,
                ext_cass_port: _,
                ext_cass_user: _,
                ext_cass_pwd: _,
            } => {
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
            CommandArgs::Inspect { cluster: _, yaml } => {
                if yaml {
                    println!("{}", config.config_to_string())
                } else {
                    println!("{:#?}", config);
                }
            }
            CommandArgs::Connect { cluster: _ } => {
                println!("{}", config.client_conn());
            }
            CommandArgs::Scale {
                cluster,
                add_tx_node,
                del_tx_node,
            } => {
                let mut config = config;
                let tx_hosts = &mut config.deployment.tx_service.host;
                tx_hosts.retain(|h| !del_tx_node.contains(h));
                tx_hosts.extend(add_tx_node);
                self.save_deployment_config(&config, true).await?;
                println!("cluster {cluster} is scaled done!");
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
}
