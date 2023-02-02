use crate::cli::cmd_printer::{CmdPrinter, Printable};
use crate::cli::config::DeploymentConfig;
use crate::cli::task::task_base::{CmdErr, TaskExecutionContext, TaskMgr};
use crate::cli::CommandArgs;
use crate::state::deployment_operation::{DeploymentEntity, DeploymentOperation};
use crate::state::state_base::{QueryCondition, StateOperation};
use crate::state::state_mgr::{StateMgr, DEPLOYMENT_STATE, STATE_MGR, TASK_STATUS_STATE};
use crate::state::task_status_operation::{TaskStatusEntity, TaskStatusOperation};
use crate::StateValue;
use anyhow::anyhow;
use owo_colors::OwoColorize;
use std::sync::Arc;
use tracing::{error, info};

#[derive(Clone)]
pub struct CommandExecutor {
    task_mgr: TaskMgr,
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
            task_mgr: TaskMgr::new(),
            state_mgr: Arc::new(STATE_MGR.clone()),
        }
    }

    pub async fn get_deployment_by_cluster(
        &self,
        cluster: &str,
    ) -> anyhow::Result<Option<DeploymentConfig>> {
        let deployment_state = self
            .state_mgr
            .get_state_operation::<DeploymentOperation>(DEPLOYMENT_STATE);
        let deployment_entity_vec = deployment_state
            .load(|| -> Option<QueryCondition> {
                Some(QueryCondition {
                    cond_text: "cluster_name = $1".to_string(),
                    bind_values: vec![StateValue::Varchar(cluster.to_string())],
                })
            })
            .await?;

        if let Some(deployment_entity) = deployment_entity_vec.first() {
            let config_content = &deployment_entity.deployment_config;
            let config = DeploymentConfig::load_from_string(config_content.to_string())?;
            Ok(Some(config))
        } else {
            Ok(None)
        }
    }

    pub async fn get_task_status_by_condition(
        &self,
        cluster_name: String,
        status: Option<i32>,
        command: Option<Vec<String>>,
    ) -> anyhow::Result<Vec<TaskStatusEntity>> {
        let task_state_operation = self
            .state_mgr
            .get_state_operation::<TaskStatusOperation>(TASK_STATUS_STATE);

        let mut bind_index = 1;
        let mut cond_text = format!("cluster_name=${bind_index} ");
        let mut bind_values = vec![StateValue::Varchar(cluster_name.clone())];

        if let Some(task_status) = status {
            bind_index += 1;
            cond_text.push_str(format!(" and task_status=${bind_index}").as_str());
            bind_values.push(StateValue::Integer(task_status))
        }
        if let Some(cmd_vec) = command {
            cond_text.push_str(" and command in (");
            let cmd_len = cmd_vec.len();
            cmd_vec.iter().enumerate().for_each(|(idx, cmd)| {
                bind_index += 1;
                cond_text.push_str(format!("${bind_index}").as_str());
                if idx < cmd_len - 1 {
                    cond_text.push(',');
                }
                bind_values.push(StateValue::Varchar(cmd.to_string()))
            });
            cond_text.push(')');
        }
        let task_status_entity = task_state_operation
            .load(|| -> Option<QueryCondition> {
                Some(QueryCondition {
                    cond_text: cond_text.clone(),
                    bind_values: bind_values.clone(),
                })
            })
            .await?;
        Ok(task_status_entity)
    }

    async fn get_success_tasks(
        &self,
        cmd_str: String,
        cluster: String,
    ) -> anyhow::Result<Option<Vec<TaskStatusEntity>>> {
        let tasks_status = self
            .get_task_status_by_condition(cluster, Some(0), Some(vec![cmd_str]))
            .await?;
        Ok(Some(tasks_status))
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
            error!("current cluster {} already exists.", curr_cluster);
            return Err(anyhow!(CmdErr::ClusterAlreadyExists(
                curr_cluster.to_string()
            )));
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
            | CommandArgs::Status { cluster }
            | CommandArgs::TaskStatus { cluster }
            | CommandArgs::Exec {
                command: _,
                cluster,
            } => {
                let config = self
                    .get_deployment_by_cluster(cluster.as_str())
                    .await?
                    .unwrap();
                Ok(Some(config))
            }
            CommandArgs::RunDeps { topology_file } => {
                Ok(Some(DeploymentConfig::load(Some(topology_file))?))
            }
        }
    }

    async fn simple_cmd_handle(&self, cmd: CommandArgs) -> anyhow::Result<()> {
        if let CommandArgs::TaskStatus {
            cluster: cluster_value,
        } = cmd
        {
            let task_status = self
                .get_task_status_by_condition(cluster_value.to_string(), None, None)
                .await?;
            let cmd_printer = CmdPrinter::new();
            task_status.iter().for_each(|status| {
                let task = status.clone().task;
                cmd_printer.add_row(task, status, |task, status| -> Printable {
                    Printable {
                        task_id: task,
                        cmd: status.clone().command,
                        cmd_status: if status.task_status == 0 {
                            "Success".green().to_string()
                        } else {
                            "Failure".red().to_string()
                        },
                        cmd_output: "".to_string(),
                    }
                })
            });
            cmd_printer.table_print();
        }
        Ok(())
    }

    pub fn task_context(&self, cmd: CommandArgs, config: DeploymentConfig) -> TaskExecutionContext {
        assert!(cmd.is_parallel_cmd());
        let context_rs = self.task_mgr.task_context(cmd, &config, None);
        assert!(context_rs.is_ok());
        context_rs.unwrap()
        //context.list_task_ids()
    }

    pub async fn run(
        &'static self,
        cmd: CommandArgs,
        deployment_config: Option<DeploymentConfig>,
    ) -> anyhow::Result<()> {
        if !cmd.is_parallel_cmd() {
            self.simple_cmd_handle(cmd).await?;
        } else {
            let config = match deployment_config {
                Some(config) => {
                    self.save_deployment_config(&config).await?;
                    config
                }
                None => self.get_config(cmd.clone()).await?.unwrap(),
            };
            let cluster = &config.deployment.cluster_name;
            let success_task_ids = self
                .get_success_tasks(cmd.as_ref().to_string(), cluster.clone())
                .await?;
            let join = tokio::task::spawn(async move {
                self.task_mgr.receive_task_result().await;
            });
            let rs = self
                .task_mgr
                .run_tasks(cmd, config, success_task_ids)
                .await?;
            join.await?;
            println!(r#"all tasks complete.task_size={}"#, rs.len());
        }
        Ok(())
    }
}
