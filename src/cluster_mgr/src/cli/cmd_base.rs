use crate::cli::task::task_base::TaskMgr;
use crate::cli::CommandArgs;
use crate::config::config_base::DeploymentConfig;
use crate::state::deployment_operation::{DeploymentEntity, DeploymentOperation};
use crate::state::state_base::{QueryCondition, StateOperation};
use crate::state::state_mgr::{StateMgr, DEPLOYMENT_STATE, STATE_MGR};
use crate::StateValue;
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

    async fn save_deployment_config(&self, config: &DeploymentConfig) -> anyhow::Result<()> {
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

    async fn get_config(&self, cmd: CommandArgs) -> anyhow::Result<Option<DeploymentConfig>> {
        match cmd.clone() {
            CommandArgs::Deploy { topology_file } => {
                let config_rs = DeploymentConfig::load(Some(topology_file));
                let config = config_rs.unwrap().clone();
                self.save_deployment_config(&config).await?;
                info!("CmdExecutor Save DeploymentConfig successfully.");
                Ok(Some(config))
            }
            CommandArgs::Install { cluster }
            | CommandArgs::Stop { cluster, force: _ }
            | CommandArgs::Start { cluster }
            | CommandArgs::Restart { cluster }
            | CommandArgs::Status {
                cluster,
                user: _,
                password: _,
            }
            | CommandArgs::Exec {
                command: _,
                cluster,
            }
            | CommandArgs::Monitor {
                command: _,
                cluster,
            } => {
                let config = self
                    .state_mgr
                    .load_deployment_from_state(cluster.as_str()) //load_deployment_from_state(cluster.as_str())
                    .await?
                    .unwrap();
                Ok(Some(config))
            }
            CommandArgs::RunDeps { topology_file } => {
                Ok(Some(DeploymentConfig::load(Some(topology_file))?))
            }
        }
    }

    pub async fn run(
        &'static self,
        cmd: CommandArgs,
        deployment_config: Option<DeploymentConfig>,
    ) -> anyhow::Result<()> {
        let config = match deployment_config {
            Some(config) => {
                self.save_deployment_config(&config).await?;
                config
            }
            None => self.get_config(cmd.clone()).await?.unwrap(),
        };

        let recv_rs_and_print_join = tokio::task::spawn(async move {
            let not_print_task_rs = option_env!("NOT_PRINT_TASK_RESULT");
            if not_print_task_rs.is_none() {
                self.task_mgr.print_task_result().await;
            }
        });
        let rs = self.task_mgr.run_tasks(cmd, config).await?;
        recv_rs_and_print_join.await?;
        println!(r#"all tasks complete.task_size={}"#, rs.len());
        Ok(())
    }
}
