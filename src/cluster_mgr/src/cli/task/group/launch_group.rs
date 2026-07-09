use indexmap::IndexMap;

use crate::cli::task::eloq_log_ctl_task::EloqLogCtlTask;
use crate::cli::task::eloq_log_probe_task::EloqLogProbeTask;
use crate::cli::task::group::{
    CheckTaskGroup, Config, CtrlDBTaskGroup, DeploymentTaskGroup, InstallDBTaskGroup,
    InstallDepPkgTaskGroup, LaunchTaskGroup, MonitorCtlTaskGroup, TaskGroup,
};
use crate::cli::task::redis_op_task::{ClusterNodes, RedisOpTask};
use crate::cli::task::ssh_check_task::SshCheckTask;
use crate::cli::task::task_base::{
    merge_execution, TaskExecutionContext, TaskHost, TaskId, TaskInstance,
};
use crate::cli::task::topology_display_task::TopologyDisplayTask;
use crate::cli::task::topology_update_task::TopologyUpdateTask;
use crate::cli::{MonitorCommand, SubCommand};
use crate::config::CONFIG_PATH_DIR;
use std::collections::HashMap;
use std::env;
use tokio::sync::watch;
use tracing::{info, warn};

#[async_trait::async_trait]
impl TaskGroup for LaunchTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        let Config::Cluster(cluster_config) = config;

        let mut executable = IndexMap::new();
        let mut barrier = vec![];

        let ssh_check_tasks = SshCheckTask::from_hosts(
            cluster_config,
            config.get_unique_host_list(),
            "ssh-connectivity",
        );
        barrier.push(ssh_check_tasks.len());
        executable.extend(ssh_check_tasks);

        let (skip_deps, topo_file) = match cmd_arg.clone() {
            SubCommand::Launch {
                topology_file,
                skip_deps,
            } => (skip_deps, topology_file),
            SubCommand::Demo {
                product, skip_deps, ..
            } => {
                let topo = format!("{}/demo-{product}.yaml", env::var(CONFIG_PATH_DIR)?);
                (skip_deps, topo)
            }
            _ => {
                unreachable!()
            }
        };

        let dep_tasks = if skip_deps {
            TaskExecutionContext::dummy()
        } else {
            let cmd = SubCommand::RunDeps {
                topology_file: topo_file.clone(),
            };
            InstallDepPkgTaskGroup.tasks(cmd, config).await?
        };

        // Log log service configuration status
        if let Some(log_svc) = &cluster_config.deployment.log_service {
            let log_nodes_count = log_svc.nodes.len();
            let log_hosts: Vec<String> = log_svc
                .nodes
                .iter()
                .map(|n| format!("{}:{}", n.host, n.port))
                .collect();
            info!(
                "Launch: Log service is configured with {} node(s): {:?}",
                log_nodes_count, log_hosts
            );
        } else {
            info!(
                "Launch: Log service is not configured. \
                 Log service will not be started during launch. \
                 To enable log service, add 'log_service' configuration to your deployment YAML."
            );
        }

        // Start log service before bootstrap (InstallDBTaskGroup) for EloqKV.
        // A configured `log_service` means a standalone (launch_sv) log
        // deployment, so it must be started whenever it is present -- even
        // without `storage_service`. (When `log_service` is omitted, EloqKV
        // runs its in-process log instead and there is nothing to start here.)
        let log_service_startup = if cluster_config.deployment.log_service.is_some() {
            let start_cmd = SubCommand::Start {
                cluster: cluster_config.deployment.cluster_name.clone(),
                nodes: Vec::new(),
            };
            let mut log_barrier = vec![];
            let mut log_executable = IndexMap::new();

            let start_log = EloqLogCtlTask::from_config(start_cmd.clone(), cluster_config);
            if start_log.is_empty() {
                warn!(
                    "Launch: Log service is configured but no start tasks were generated. \
                     This may indicate a configuration error."
                );
            } else {
                let start_log_len = start_log.len();
                log_barrier.push(start_log_len);
                log_executable.extend(start_log);
                info!(
                    "Launch: Added {} log service start task(s) before bootstrap",
                    start_log_len
                );
            }

            let probe = EloqLogProbeTask::from_config(cluster_config);
            if !probe.is_empty() {
                let probe_len = probe.len();
                log_barrier.push(probe_len);
                log_executable.extend(probe);
                info!(
                    "Launch: Added {} log service probe task(s) before bootstrap",
                    probe_len
                );
            }

            TaskExecutionContext {
                task_group: "log-service-startup".to_string(),
                barrier: Some(log_barrier),
                executable: log_executable,
            }
        } else {
            TaskExecutionContext::dummy()
        };

        let exe_ctx = vec![
            dep_tasks,
            CheckTaskGroup
                .tasks(
                    SubCommand::Check {
                        topology_file: topo_file.clone(),
                    },
                    config,
                )
                .await?,
            DeploymentTaskGroup
                .tasks(
                    SubCommand::Deploy {
                        topology_file: topo_file.clone(),
                    },
                    config,
                )
                .await?,
            log_service_startup,
            if cluster_config.deployment.storage_service.is_some() {
                InstallDBTaskGroup
                    .tasks(
                        SubCommand::Install {
                            cluster: cluster_config.deployment.cluster_name.clone(),
                        },
                        config,
                    )
                    .await?
            } else {
                TaskExecutionContext::dummy()
            },
            {
                // Create start tasks with skip_log_service=true since log service
                // was already started before bootstrap
                let start_cmd = SubCommand::Start {
                    cluster: cluster_config.deployment.cluster_name.clone(),
                    nodes: Vec::new(),
                };
                let (start_barrier, start_executable) =
                    CtrlDBTaskGroup.start_tasks(start_cmd, cluster_config, true);
                TaskExecutionContext {
                    task_group: "cluster-control-start".to_string(),
                    barrier: Some(start_barrier),
                    executable: start_executable,
                }
            },
            MonitorCtlTaskGroup
                .tasks(
                    SubCommand::Monitor {
                        cluster: Some(cluster_config.deployment.cluster_name.clone()),
                        command: MonitorCommand::Start {
                            cluster: None,
                            components: vec![],
                        },
                    },
                    config,
                )
                .await?,
        ];
        merge_execution(&mut barrier, &mut executable, exe_ctx);

        // Add topology update and display tasks as the final step

        // Create channel for cluster nodes information
        let empty_cluster_nodes = ClusterNodes {
            masters: Vec::new(),
            replicas: Vec::new(),
        };
        let (redis_op_tx, redis_op_rx) = watch::channel(empty_cluster_nodes.clone());

        // Add RedisOpTask to get cluster topology
        let redis_op_task_id = TaskId {
            cmd: "topology".to_string(),
            task: "get-topology".to_string(),
            host: "_local".to_string(),
        };

        let mut topology_nodes =
            cluster_config.get_host_port_list(crate::config::DeploymentPackage::EloqTx);
        topology_nodes.extend(
            cluster_config.get_host_port_list(crate::config::DeploymentPackage::EloqStandby),
        );

        let redis_op_task = RedisOpTask::new(
            redis_op_task_id.clone(),
            topology_nodes,
            "cluster topology".to_string(),
            redis_op_tx.clone(),
            cluster_config.redis_password(None),
            true, // Skip checkpoint
        )
        .with_service_endpoints(cluster_config.connection.service_endpoints.clone());

        let redis_op_instance = TaskInstance {
            task_input: HashMap::default(),
            task: Box::new(redis_op_task),
            task_host: TaskHost::Local,
        };

        barrier.push(1);
        executable.insert(redis_op_task_id, redis_op_instance);

        // Add TopologyUpdateTask using proper constructor
        let topology_update_tasks =
            TopologyUpdateTask::from_redis(cluster_config, redis_op_rx.clone(), None);
        barrier.push(topology_update_tasks.len());
        executable.extend(topology_update_tasks);

        // Add TopologyDisplayTask
        let topology_display_task_id = TaskId {
            cmd: "topology".to_string(),
            task: "display-topology".to_string(),
            host: "_local".to_string(),
        };

        let topology_display_task = TopologyDisplayTask::new(
            topology_display_task_id.clone(),
            cluster_config.deployment.cluster_name.clone(),
        );

        let topology_display_instance = TaskInstance {
            task_input: HashMap::default(),
            task: Box::new(topology_display_task),
            task_host: TaskHost::Local,
        };

        barrier.push(1);
        executable.insert(topology_display_task_id, topology_display_instance);

        Ok(TaskExecutionContext {
            task_group: cmd_arg.as_ref().to_string(),
            barrier: Some(barrier),
            executable,
        })
    }
}
