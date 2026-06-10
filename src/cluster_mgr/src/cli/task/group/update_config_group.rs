use crate::cli::task::config_fields::{field_exists, is_cluster_wide_field};
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::Config;
use crate::cli::task::group::{TaskGroup, UpdateConfigTaskGroup};
use crate::cli::task::redis_op_task::{ClusterNodes, RedisOpTask};
use crate::cli::task::rolling_upgrade::{self, steps::UpgradeContext};
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::task::topology_update_task::TopologyUpdateTask;
use crate::cli::task::upload::upload_task_builder::{upload_tasks, UploadTaskBuilderType};
use crate::cli::SubCommand;
use anyhow::{bail, Result};
use indexmap::IndexMap;
use std::collections::HashMap;
use tokio::sync::watch;
use tracing::{info, warn};

#[async_trait::async_trait]
impl TaskGroup for UpdateConfigTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        let Config::Cluster(deploy_config) = config;

        let (need_restart, fields, tx_node_id, password) = match &cmd_arg {
            SubCommand::UpdateConf {
                restart,
                fields,
                tx_node_id,
                password,
                ..
            } => (*restart, fields.clone(), *tx_node_id, password),
            _ => unreachable!("Expected UpdateConf command"),
        };

        // Build config update tasks
        let config_ctx = build_config_update(deploy_config, &fields, tx_node_id, password, config)?;

        // Run config update first
        if !config_ctx.executable.is_empty() {
            rolling_upgrade::run_step_context(config_ctx, config.clone()).await?;
        }

        // Run rolling restart
        if need_restart {
            let ctx = UpgradeContext::new(&cmd_arg, config.clone());
            let steps = rolling_upgrade::steps::build_config_restart_steps(ctx);
            let ru = rolling_upgrade::RollingUpgrade::new(steps, config.clone());
            ru.execute().await?;
        }

        Ok(TaskExecutionContext::dummy())
    }
}

fn build_config_update(
    deploy_config: &crate::config::config_base::DeployConfig,
    fields: &[String],
    tx_node_id: Option<u32>,
    password: &Option<String>,
    config: &Config,
) -> Result<TaskExecutionContext> {
    let mut executable = IndexMap::new();
    let mut barrier = vec![];

    if deploy_config.deployment.tls_enabled() {
        let mut mkdir_targets = vec![deploy_config.install_dir()];
        let tls_dir = deploy_config.deployment.tls_cert_install_dir();
        if !mkdir_targets.contains(&tls_dir) {
            mkdir_targets.push(tls_dir);
        }
        let mkdir_tasks = ExecCustomCommand::build_task_by_host(
            format!("mkdir -p {}", mkdir_targets.join(" ")),
            config,
            deploy_config.get_unique_host_list(),
            Some("mkdir_tls_dirs".to_string()),
        );
        barrier.push(mkdir_tasks.len());
        executable.extend(mkdir_tasks);
    }

    validate_fields(fields, tx_node_id)?;

    if fields.is_empty() && !deploy_config.deployment.tls_enabled() {
        return Ok(TaskExecutionContext::dummy());
    }

    if let Some(node_id) = tx_node_id {
        let mut candidate_nodes =
            deploy_config.get_host_port_list(crate::config::DeploymentPackage::EloqTx);
        let standby_host_ports =
            deploy_config.get_host_port_list(crate::config::DeploymentPackage::EloqStandby);
        candidate_nodes.extend(standby_host_ports);

        let (redis_op_tx, redis_op_rx) = watch::channel(ClusterNodes {
            masters: Vec::new(),
            replicas: Vec::new(),
        });

        let redis_task_id = TaskId {
            cmd: "topology".to_string(),
            task: "get-current-topology".to_string(),
            host: "_local".to_string(),
        };
        executable.insert(
            redis_task_id.clone(),
            TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(
                    RedisOpTask::new(
                        redis_task_id,
                        candidate_nodes,
                        "cluster topology".to_string(),
                        redis_op_tx,
                        deploy_config.redis_password(password.clone()),
                        true,
                    )
                    .with_service_endpoints(deploy_config.connection.service_endpoints.clone()),
                ),
                task_host: TaskHost::Local,
            },
        );
        barrier.push(1);

        if !fields.is_empty() {
            executable.extend(TopologyUpdateTask::for_config_update(
                deploy_config,
                redis_op_rx,
                node_id,
                fields.to_vec(),
            ));
            barrier.push(1);
        }

        let upload_tasks = upload_tasks(UploadTaskBuilderType::TxConf, config);
        barrier.push(upload_tasks.len());
        executable.extend(upload_tasks);
    } else {
        if !fields.is_empty() {
            executable.extend(TopologyUpdateTask::for_all_nodes_config_update(
                deploy_config,
                fields.to_vec(),
            ));
            barrier.push(executable.len());
        }

        let upload_tasks = upload_tasks(UploadTaskBuilderType::TxConf, config);
        barrier.push(upload_tasks.len());
        executable.extend(upload_tasks);
    }

    Ok(TaskExecutionContext {
        task_group: "update-tx-conf".to_string(),
        barrier: if barrier.is_empty() {
            None
        } else {
            Some(barrier)
        },
        executable,
    })
}

fn validate_fields(field_updates: &[String], tx_node_id: Option<u32>) -> Result<()> {
    for field_update in field_updates {
        if let Some((field, _)) = field_update.split_once(':') {
            if !field_exists(field) {
                bail!(
                    "Unknown configuration field '{}'. Run 'eloqctl help config-fields' for a list of valid fields.",
                    field
                );
            }
            if is_cluster_wide_field(field) && tx_node_id.is_some() {
                warn!(
                    "Field '{}' is cluster-wide but a specific node ID was provided. The update will apply to all nodes.",
                    field
                );
            } else if !is_cluster_wide_field(field) && tx_node_id.is_none() {
                info!(
                    "Node-specific field '{}' will be updated on all nodes.",
                    field
                );
            }
        } else {
            bail!(
                "Invalid field update format: '{}'. Expected 'field:value'.",
                field_update
            );
        }
    }
    Ok(())
}
