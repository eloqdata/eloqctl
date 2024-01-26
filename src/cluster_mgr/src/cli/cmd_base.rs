use crate::cli::task::task_base::TaskMgr;
use crate::cli::{CommandArgs, HOME_DIR};
use crate::config::config_base::DeploymentConfig;
use crate::config::CONFIG_PATH_DIR;
use crate::state::deployment_operation::{DeploymentEntity, DeploymentOperation};
use crate::state::state_base::{QueryCondition, StateOperation};
use crate::state::state_mgr::{StateMgr, DEPLOYMENT_STATE, STATE_MGR};
use crate::StateValue;
use anyhow::anyhow;
use std::env;
use std::sync::Arc;
use tracing::{info, warn};

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

    async fn save_deployment_config(
        &self,
        config: &DeploymentConfig,
        upsert: bool,
    ) -> anyhow::Result<()> {
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
        if !deployment_entity.is_empty() && !upsert {
            warn!(
                "current cluster {} already exists. do nothing",
                curr_cluster
            );
            return Ok(());
        }
        let all_hosts = config.get_unique_host_list().join(";");
        let config_string = config.config_to_string();
        info!(
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
            | CommandArgs::Launch { topology_file } => {
                let mut config = DeploymentConfig::load(Some(topology_file)).unwrap();
                let scan_ret = config.scan_hardware().await?;
                if let Some(hw) = config.deployment.hardware.as_mut() {
                    hw.extend(scan_ret);
                } else {
                    config.deployment.hardware = Some(scan_ret);
                }
                self.save_deployment_config(&config, cmd.as_ref().eq("upgrade"))
                    .await?;
                info!("CmdExecutor Save DeploymentConfig successfully.");
                Ok(config)
            }
            CommandArgs::Demo { product } => {
                let topology = format!(
                    "{}/demo-{}.yaml",
                    env::var(CONFIG_PATH_DIR)?,
                    product.to_lowercase()
                );
                let mut config = DeploymentConfig::load(Some(topology)).unwrap();
                config.connection.username = whoami::username();
                config.connection.auth.keypair = Some(format!("{}/ed25519", env::var(HOME_DIR)?));
                config.deployment.install_dir = env::var(HOME_DIR)?;
                self.save_deployment_config(&config, false).await?;
                info!(
                    "CmdExecutor Save DeploymentConfig successfully. username={}",
                    config.connection.username
                );
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
            | CommandArgs::Remove { cluster } => {
                let config = self
                    .state_mgr
                    .load_deployment_from_state(cluster.as_str())
                    .await?
                    .ok_or(anyhow!("cluster {} not exist", cluster))?;
                Ok(config)
            }
            CommandArgs::RunDeps { topology_file }
            | CommandArgs::Exec {
                command: _,
                topology_file,
            } => Ok(DeploymentConfig::load(Some(topology_file))?),
        }
    }

    pub async fn run(
        &'static self,
        cmd: CommandArgs,
        deployment_config: Option<DeploymentConfig>,
    ) -> anyhow::Result<()> {
        let cmd_ref = cmd.as_ref();
        let config = match deployment_config {
            Some(config) => {
                self.save_deployment_config(&config, cmd_ref.eq("upgrade"))
                    .await?;
                config
            }
            None => self.get_config(cmd.clone()).await?,
        };

        let recv_rs_and_print_join = tokio::task::spawn(async move {
            let not_print_task_rs = option_env!("NOT_PRINT_TASK_RESULT");
            if not_print_task_rs.is_none() {
                self.task_mgr.print_task_result().await;
            }
        });
        let rs = self.task_mgr.run_tasks(cmd.clone(), config.clone()).await?;
        recv_rs_and_print_join.await?;
        println!(r#"all tasks complete.task_size={}"#, rs.len());

        match cmd {
            CommandArgs::Launch { topology_file: _ } | CommandArgs::Demo { product: _ } => {
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
            _ => {}
        }
        Ok(())
    }
}
