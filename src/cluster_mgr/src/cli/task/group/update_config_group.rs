use crate::cli::task::config_fields::{field_exists, is_cluster_wide_field};
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::Config;
use crate::cli::task::group::{TaskGroup, UpdateConfigTaskGroup};
use crate::cli::task::monograph_tx_ctl_task::{MonographTxCtlTask, ServerType};
use crate::cli::task::redis_op_task::{ClusterNodes, RedisOpTask};
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::task::task_utils::stop_with_failover;
use crate::cli::task::topology_update_task::TopologyUpdateTask;
use crate::cli::task::upload::upload_task_builder::{upload_tasks, UploadTaskBuilderType};
use crate::cli::SubCommand;
use crate::config::DeploymentPackage;
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
        let deploy_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => {
                return Err(anyhow::anyhow!(
                    "Expected ClusterConfig for UpdateConfigTaskGroup"
                ))
            }
        };

        let cluster_name = &deploy_config.deployment.cluster_name;
        let (need_restart, fields, tx_node_id, password) = match cmd_arg {
            SubCommand::UpdateConf {
                restart,
                fields,
                tx_node_id,
                password,
                ..
            } => (restart, fields, tx_node_id, password),
            _ => unreachable!(),
        };

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

        // Validate the field updates
        validate_fields(&fields, tx_node_id)?;

        // Use the TopologyUpdateTask if tx_node_id is provided
        if let Some(node_id) = tx_node_id {
            // Get all Redis host:port combinations from the config
            let mut candidate_nodes =
                deploy_config.get_host_port_list(DeploymentPackage::MonographTx);
            let standby_host_ports =
                deploy_config.get_host_port_list(DeploymentPackage::MonographStandby);
            candidate_nodes.extend(standby_host_ports);

            // Create an empty ClusterNodes struct for the channel
            let empty_cluster_nodes = ClusterNodes {
                masters: Vec::new(),
                replicas: Vec::new(),
            };

            // Create channel for passing topology data from RedisOpTask to TopologyUpdateTask
            let (redis_op_tx, redis_op_rx) = watch::channel(empty_cluster_nodes);

            // Create a RedisOpTask to get the current cluster topology
            let redis_task_id = TaskId {
                cmd: "topology".to_string(),
                task: "get-current-topology".to_string(),
                host: "_local".to_string(),
            };

            let redis_task = RedisOpTask::new(
                redis_task_id.clone(),
                candidate_nodes,
                "cluster topology".to_string(),
                redis_op_tx,
                deploy_config.redis_password(password.clone()),
                true, // Skip checkpoint
            );

            let redis_task_instance = TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(redis_task),
                task_host: TaskHost::Local,
            };

            // Add RedisOpTask
            executable.insert(redis_task_id, redis_task_instance);
            barrier.push(1);

            // Add TopologyUpdateTask
            if !fields.is_empty() {
                executable.extend(TopologyUpdateTask::for_config_update(
                    deploy_config,
                    redis_op_rx,
                    node_id,
                    fields,
                ));
                barrier.push(1);
            }

            // Add upload task to ensure the updated INI file is copied to target host
            let upload_tasks = upload_tasks(UploadTaskBuilderType::TxConf, config);
            barrier.push(upload_tasks.len());
            executable.extend(upload_tasks);
        } else {
            // Use TopologyUpdateTask to update configuration for all nodes
            if !fields.is_empty() {
                executable.extend(TopologyUpdateTask::for_all_nodes_config_update(
                    deploy_config,
                    fields,
                ));
                barrier.push(executable.len());
            }

            // Then upload the updated INI files to target hosts
            let upload_tasks = upload_tasks(UploadTaskBuilderType::TxConf, config);
            barrier.push(upload_tasks.len());
            executable.extend(upload_tasks);
        }

        if need_restart {
            if deploy_config
                .deployment
                .tx_service
                .standby_host_ports
                .is_some()
            {
                // --- Round 1: failover masters, restart them as standbys ---
                let tx_host_ports =
                    deploy_config.get_host_port_list(DeploymentPackage::MonographTx);
                stop_with_failover(
                    SubCommand::Stop {
                        cluster: cluster_name.clone(),
                        tx: Some(true),
                        log: true,
                        store: false,
                        monitor: false,
                        force: false,
                        all: false,
                        password: None,
                        nodes: tx_host_ports,
                    },
                    deploy_config,
                    &mut barrier,
                    &mut executable,
                )
                .await;

                let start_tx_round1 = MonographTxCtlTask::from_config(
                    SubCommand::Start {
                        cluster: cluster_name.to_string(),
                        nodes: Vec::new(),
                    },
                    deploy_config,
                    ServerType::Tx,
                );
                barrier.push(start_tx_round1.len());
                executable.extend(start_tx_round1);

                // --- Round 2: failover back, restart old standbys ---
                // Inline the failover logic for round 2 with a unique topology task ID
                // to avoid key collision with round 1's topology task.
                let standby_host_ports =
                    deploy_config.get_host_port_list(DeploymentPackage::MonographStandby);
                let skip_checkpoint =
                    crate::cli::task::task_utils::check_whether_to_skip_checkpoint(cluster_name)
                        .await;
                let topo_task_id_2 = TaskId {
                    cmd: "topology".to_string(),
                    task: "check-topology-round2".to_string(),
                    host: "_local".to_string(),
                };
                let (topo_tx_2, failover_rx_2) = watch::channel::<ClusterNodes>(ClusterNodes {
                    masters: Vec::new(),
                    replicas: Vec::new(),
                });
                let stop_nodes_rx_2 = failover_rx_2.clone();
                let topo_task_2 = RedisOpTask::new(
                    topo_task_id_2.clone(),
                    standby_host_ports.clone(),
                    "cluster topology".to_string(),
                    topo_tx_2,
                    deploy_config.redis_password(None),
                    skip_checkpoint,
                );
                executable.insert(
                    topo_task_id_2,
                    TaskInstance {
                        task_input: HashMap::default(),
                        task: Box::new(topo_task_2),
                        task_host: TaskHost::Local,
                    },
                );
                barrier.push(1);

                let mut failover_ids_2 = Vec::new();
                for node_addr in &standby_host_ports {
                    if let Some((host, port_str)) = node_addr.split_once(':') {
                        if let Ok(port) = port_str.parse::<u16>() {
                            let fid = TaskId {
                                cmd: "failover".to_string(),
                                task: format!("failover-check-round2-{}", port_str),
                                host: host.to_string(),
                            };
                            let ftask = crate::cli::task::failover_op_task::FailoverOpTask::new(
                                fid.clone(),
                                host.to_string(),
                                port,
                                String::new(),
                                0u16,
                                failover_rx_2.clone(),
                                deploy_config.redis_password(None),
                            );
                            executable.insert(
                                fid.clone(),
                                TaskInstance {
                                    task_input: HashMap::default(),
                                    task: Box::new(ftask),
                                    task_host: TaskHost::Local,
                                },
                            );
                            failover_ids_2.push(fid);
                        }
                    }
                }
                barrier.push(failover_ids_2.len());

                let stop_nodes_2 = MonographTxCtlTask::from_config_with_channel(
                    SubCommand::Stop {
                        cluster: cluster_name.clone(),
                        tx: Some(true),
                        log: true,
                        store: false,
                        monitor: false,
                        force: false,
                        all: false,
                        password: None,
                        nodes: standby_host_ports,
                    },
                    deploy_config,
                    ServerType::Node,
                    Some(stop_nodes_rx_2),
                )
                .expect("stop nodes round2 error");
                barrier.push(stop_nodes_2.len());
                executable.extend(stop_nodes_2);

                let start_tx_round2 = MonographTxCtlTask::from_config(
                    SubCommand::Start {
                        cluster: cluster_name.to_string(),
                        nodes: Vec::new(),
                    },
                    deploy_config,
                    ServerType::Tx,
                );
                barrier.push(start_tx_round2.len());
                executable.extend(start_tx_round2);

                let start_standby = MonographTxCtlTask::from_config(
                    SubCommand::Start {
                        cluster: cluster_name.to_string(),
                        nodes: Vec::new(),
                    },
                    deploy_config,
                    ServerType::Standby,
                );
                barrier.push(start_standby.len());
                executable.extend(start_standby);
            } else {
                let stop_tx_task = MonographTxCtlTask::from_config(
                    SubCommand::Stop {
                        cluster: cluster_name.clone(),
                        tx: Some(true),
                        log: true,
                        store: false,
                        monitor: false,
                        force: false,
                        all: false,
                        password: None,
                        nodes: Vec::new(),
                    },
                    deploy_config,
                    ServerType::Tx,
                );
                barrier.push(stop_tx_task.len());
                executable.extend(stop_tx_task);
                let start_tx_task = MonographTxCtlTask::from_config(
                    SubCommand::Start {
                        cluster: cluster_name.to_string(),
                        nodes: Vec::new(),
                    },
                    deploy_config,
                    ServerType::Tx,
                );
                barrier.push(start_tx_task.len());
                executable.extend(start_tx_task);
            }

            if deploy_config
                .deployment
                .tx_service
                .voter_host_ports
                .is_some()
            {
                let stop_voter = MonographTxCtlTask::from_config(
                    SubCommand::Stop {
                        cluster: cluster_name.clone(),
                        tx: None,
                        log: false,
                        store: false,
                        monitor: false,
                        force: false,
                        all: false,
                        password: None,
                        nodes: Vec::new(),
                    },
                    deploy_config,
                    ServerType::Voter,
                );
                barrier.push(stop_voter.len());
                executable.extend(stop_voter);

                let start_voter = MonographTxCtlTask::from_config(
                    SubCommand::Start {
                        cluster: cluster_name.to_string(),
                        nodes: Vec::new(),
                    },
                    deploy_config,
                    ServerType::Voter,
                );
                barrier.push(start_voter.len());
                executable.extend(start_voter);
            }
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
}

/// Validates the field updates and ensures they comply with scope rules
fn validate_fields(field_updates: &[String], tx_node_id: Option<u32>) -> Result<()> {
    for field_update in field_updates {
        if let Some((field, _)) = field_update.split_once(':') {
            // Check if field exists in registry
            if !field_exists(field) {
                bail!(
                    "Unknown configuration field '{}'. Run 'eloqctl help config-fields' for a list of valid fields.",
                    field
                );
            }

            // If updating a node-specific field, ensure we have a tx_node_id
            // If updating a cluster-wide field, warn if tx_node_id is provided
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
