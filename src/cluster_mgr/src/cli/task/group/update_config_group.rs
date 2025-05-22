use crate::cli::task::group::Config;
use crate::cli::task::group::{TaskGroup, UpdateConfigTaskGroup};
use crate::cli::task::ini_config_update_task::IniConfigUpdateTask;
use crate::cli::task::monograph_tx_ctl_task::{MonographTxCtlTask, ServerType};
use crate::cli::task::redis_op_task::{ClusterNodes, RedisOpTask};
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::task::task_utils::stop_with_hot_standby;
use crate::cli::task::topology_update_task::TopologyUpdateTask;
use crate::cli::task::upload::upload_task_builder::{upload_tasks, UploadTaskBuilderType};
use crate::cli::SubCommand;
use crate::config::DeploymentPackage;
use indexmap::IndexMap;
use std::collections::HashMap;
use tokio::sync::watch;

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

        // Use the TopologyUpdateTask if both fields and tx_node_id are provided
        if let (Some(field_updates), Some(node_id)) = (&fields, tx_node_id) {
            if !field_updates.is_empty() {
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
                    "cluster nodes".to_string(),
                    redis_op_tx,
                    password.clone(), // Pass the password here
                    true,             // Skip checkpoint
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
                executable.extend(TopologyUpdateTask::for_config_update(
                    &deploy_config,
                    redis_op_rx,
                    node_id,
                    field_updates.clone(),
                ));
                barrier.push(1);
            }
        }
        // Use the IniConfigUpdateTask if fields are specified but no node_id, or use the upload task
        else if let Some(field_updates) = fields {
            if !field_updates.is_empty() {
                executable.extend(IniConfigUpdateTask::new(
                    cluster_name,
                    Some(field_updates),
                    None,
                ));
                barrier.push(executable.len());
            } else {
                executable.extend(upload_tasks(UploadTaskBuilderType::TxConf, &config));
                barrier.push(executable.len());

                // Add a step to parse and store the uploaded INI files
                executable.extend(IniConfigUpdateTask::new(cluster_name, None, None));
                barrier.push(executable.len());
            }
        } else {
            executable.extend(upload_tasks(UploadTaskBuilderType::TxConf, &config));
            barrier.push(executable.len());

            // Add a step to parse and store the uploaded INI files
            executable.extend(IniConfigUpdateTask::new(cluster_name, None, None));
            barrier.push(executable.len());
        }

        if need_restart {
            // stop order: (standby-server -> voter-server ->) tx-server -> log-server -> kv-store
            if deploy_config
                .deployment
                .tx_service
                .standby_host_ports
                .is_some()
            {
                stop_with_hot_standby(
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
                    &deploy_config,
                    &mut barrier,
                    &mut executable,
                );
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
                    &deploy_config,
                    ServerType::Tx,
                );
                barrier.push(stop_tx_task.len());
                executable.extend(stop_tx_task);
            }

            let start_tx_task = MonographTxCtlTask::from_config(
                SubCommand::Start {
                    cluster: cluster_name.to_string(),
                    nodes: Vec::new(),
                },
                &deploy_config,
                ServerType::Tx,
            );
            barrier.push(start_tx_task.len());
            executable.extend(start_tx_task);

            if deploy_config
                .deployment
                .tx_service
                .standby_host_ports
                .is_some()
            {
                let start_standby = MonographTxCtlTask::from_config(
                    SubCommand::Start {
                        cluster: cluster_name.to_string(),
                        nodes: Vec::new(),
                    },
                    &deploy_config,
                    ServerType::Standby,
                );
                barrier.push(start_standby.len());
                executable.extend(start_standby);
            }

            if deploy_config
                .deployment
                .tx_service
                .voter_host_ports
                .is_some()
            {
                let start_voter = MonographTxCtlTask::from_config(
                    SubCommand::Start {
                        cluster: cluster_name.to_string(),
                        nodes: Vec::new(),
                    },
                    &deploy_config,
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
