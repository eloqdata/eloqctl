use indexmap::IndexMap;

use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{
    CheckTaskGroup, Config, CtrlDBTaskGroup, DeploymentTaskGroup, InstallDBTaskGroup,
    InstallDepPkgTaskGroup, LaunchTaskGroup, MonitorCtlTaskGroup, TaskGroup,
};
use crate::cli::task::redis_op_task::{ClusterNodes, RedisOpTask};
use crate::cli::task::task_base::{
    merge_execution, TaskExecutionContext, TaskHost, TaskId, TaskInstance,
};
use crate::cli::task::topology_display_task::TopologyDisplayTask;
use crate::cli::task::topology_update_task::TopologyUpdateTask;
use crate::cli::SubCommand;
use crate::config::{config_template, CONFIG_PATH_DIR, SSH_PYTHON_SCRIPT};
use std::collections::HashMap;
use std::env;
use tokio::sync::watch;

#[async_trait::async_trait]
impl TaskGroup for LaunchTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => {
                return Err(anyhow::anyhow!(
                    "Expected ClusterConfig for LaunchTaskGroup"
                ))
            }
        };

        // Q? launch should check if the cluster is already running in the target nodes?

        let mut executable = IndexMap::new();
        let mut barrier = vec![];

        let ssh_python_bin = config_template(SSH_PYTHON_SCRIPT)?
            .to_string_lossy()
            .into_owned();
        let host_values = config.get_unique_host_list().join(" ");
        // This should execute locally.
        let ssh_python_task = ExecCustomCommand::build_local_task(
            format!("python3 {} {}", ssh_python_bin, host_values),
            config,
            "ssh check",
        );
        barrier.push(ssh_python_task.len());
        executable.extend(ssh_python_task);

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
            CtrlDBTaskGroup
                .tasks(
                    SubCommand::Start {
                        cluster: cluster_config.deployment.cluster_name.clone(),
                        nodes: Vec::new(),
                    },
                    config,
                )
                .await?,
            MonitorCtlTaskGroup
                .tasks(
                    SubCommand::Monitor {
                        cluster: cluster_config.deployment.cluster_name.clone(),
                        command: "start".to_string(),
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

        let redis_op_task = RedisOpTask::new(
            redis_op_task_id.clone(),
            cluster_config.get_host_port_list(crate::config::DeploymentPackage::MonographTx),
            "cluster nodes".to_string(),
            redis_op_tx.clone(),
            None, // No password
            true, // Skip checkpoint
        );

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
