use crate::cli::task::check_tx_cluster_scale_status_task::CheckTxClusterScaleStatusTask;
use crate::cli::task::db_update_task::DbDeploymentUpdateTask;
use crate::cli::task::download_task::DownloadTask;
use crate::cli::task::eloq_tx_ctl_task::{EloqTxCtlTask, ServerType};
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{Config, ScaleTaskGroup};
use crate::cli::task::install_dep_pkg::DepPkgTask;
use crate::cli::task::local_extract_task::LocalExtractTask;
use crate::cli::task::redis_op_task::{ClusterNodes, RedisOpTask};
use crate::cli::task::scale_op_task::{ScaleOpConfig, ScaleOpTask};
use crate::cli::task::ssh_check_task::SshCheckTask;
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::task::task_utils::{ClusterNodesWithConfig, ScaleOperationType};
use crate::cli::task::topology_display_task::TopologyDisplayTask;
use crate::cli::task::topology_update_task::TopologyUpdateTask;
use crate::cli::task::tx_conf_update_task::TxConfUpdateTask;
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, upload_tasks, upload_tasks_with_nodes,
    UploadTaskBuilderType,
};
use crate::cli::SubCommand;
use crate::config::config_base::{UploadFile, ELOQ_FILE_KEY};
use crate::config::{DeploymentPackage, DownloadUrl};
use anyhow::{anyhow, bail};
use async_trait::async_trait;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;
use tokio::sync::watch;
use tracing::{info, warn};

#[async_trait]
impl super::TaskGroup for ScaleTaskGroup {
    async fn tasks(
        &self,
        cmd: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        // Validate command is Scale
        if !matches!(cmd, SubCommand::Scale { .. }) {
            return Err(anyhow!("Expected Scale command"));
        }

        // Get deployment config
        let deploy_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => return Err(anyhow!("Expected Cluster config for Scale command")),
        };

        let mut barrier = Vec::new();
        let mut executable = IndexMap::new();

        let (
            mut add_nodes,
            mut remove_nodes,
            mut is_candidate,
            cluster_name,
            ng_id,
            password,
            version,
        ) = if let SubCommand::Scale {
            add_nodes: add,
            remove_nodes: remove,
            is_candidate,
            cluster,
            ng_id,
            password,
            version,
            ..
        } = &cmd
        {
            (
                add.clone(),
                remove.clone(),
                is_candidate.clone(),
                cluster.clone(),
                *ng_id,
                password.clone(),
                version.clone(),
            )
        } else {
            return Err(anyhow!("Invalid command for scale task group"));
        };
        let redis_password = deploy_config.redis_password(password);

        let mut existing_config_nodes = deploy_config.get_host_port_list(DeploymentPackage::EloqTx);
        existing_config_nodes
            .extend(deploy_config.get_host_port_list(DeploymentPackage::EloqStandby));
        existing_config_nodes
            .extend(deploy_config.get_host_port_list(DeploymentPackage::EloqVoter));
        if !add_nodes.is_empty() {
            let original_add_nodes = add_nodes.clone();
            if let Some(candidate_flags) = is_candidate.clone() {
                let mut filtered_nodes = Vec::new();
                let mut filtered_flags = Vec::new();
                for (node, flag) in original_add_nodes.iter().zip(candidate_flags.iter()) {
                    if existing_config_nodes.contains(node) {
                        info!("Scale add no-op for existing node {node}");
                    } else {
                        filtered_nodes.push(node.clone());
                        filtered_flags.push(*flag);
                    }
                }
                add_nodes = filtered_nodes;
                is_candidate = Some(filtered_flags);
            } else {
                add_nodes.retain(|node| {
                    let exists = existing_config_nodes.contains(node);
                    if exists {
                        info!("Scale add no-op for existing node {node}");
                    }
                    !exists
                });
            }
            if add_nodes.is_empty() {
                println!("All requested nodes already exist; scale add is a no-op.");
                return Ok(TaskExecutionContext::dummy());
            }
        }

        if !remove_nodes.is_empty() {
            remove_nodes.retain(|node| {
                let exists = existing_config_nodes.contains(node);
                if !exists {
                    info!("Scale remove no-op for absent node {node}");
                }
                exists
            });
            if remove_nodes.is_empty() {
                println!("All requested nodes are already absent; scale remove is a no-op.");
                return Ok(TaskExecutionContext::dummy());
            }
        }

        if version.is_some() && add_nodes.is_empty() {
            return Err(anyhow!(
                "--version requires --add-nodes with at least one node"
            ));
        }

        if version.is_some() && !remove_nodes.is_empty() {
            return Err(anyhow!("--version cannot be combined with --remove-nodes"));
        }

        if add_nodes.is_empty() && remove_nodes.is_empty() {
            return Err(anyhow!(
                "Must specify either --add-nodes or --remove-nodes with at least one node"
            ));
        }

        if !add_nodes.is_empty() && !remove_nodes.is_empty() {
            return Err(anyhow!(
                "Cannot specify both --add-nodes and --remove-nodes in the same command"
            ));
        }

        if !add_nodes.is_empty() && is_candidate.is_none() {
            return Err(anyhow!("--is-candidate must be provided when adding nodes"));
        }

        if !add_nodes.is_empty() && ng_id.is_none() {
            return Err(anyhow!("--ng-id must be provided when adding nodes"));
        }

        let empty_cluster_nodes = ClusterNodes {
            masters: Vec::new(),
            replicas: Vec::new(),
        };

        let empty_cluster_nodes_with_config = ClusterNodesWithConfig {
            nodes: empty_cluster_nodes.clone(),
            cluster_config: None,
        };

        let effective_tx_image_url = deploy_config
            .tx_image_override
            .as_ref()
            .cloned()
            .unwrap_or_else(|| deploy_config.deployment.tx_image().to_string());
        let effective_version = deploy_config
            .tx_version_override
            .as_ref()
            .or(deploy_config.deployment.version.as_ref())
            .cloned();

        if let Some(ver) = effective_version.as_ref() {
            info!("Using Eloq version {} for new nodes", ver);
        }

        // Channel for getting candidate nodes, RedisOpTask to ScaleOpTask
        let (redis_op_tx, redis_op_rx) = watch::channel(empty_cluster_nodes.clone());

        // Channel for getting cluster config information, ScaleOpTask to TxConfUpdateTask
        let (scale_op_tx, scale_op_rx) = watch::channel(empty_cluster_nodes_with_config.clone());

        // Get all Redis host:port combinations from the config
        let mut candidate_nodes_before_scale =
            deploy_config.get_host_port_list(DeploymentPackage::EloqTx);
        let standby_host_ports = deploy_config.get_host_port_list(DeploymentPackage::EloqStandby);
        candidate_nodes_before_scale.extend(standby_host_ports);

        let voter_host_ports = deploy_config.get_host_port_list(DeploymentPackage::EloqVoter);
        let mut all_nodes_before_scale = candidate_nodes_before_scale.clone();
        all_nodes_before_scale.extend(voter_host_ports);

        // Determine event_id
        let scale_event_id = uuid::Uuid::new_v4().to_string();

        if !add_nodes.is_empty() {
            let existing_hosts = config.get_unique_host_list();
            let newly_added_hosts = add_nodes
                .clone()
                .iter()
                .map(|host_port| host_port.split(':').next().unwrap_or("").to_string())
                .filter(|host| !host.is_empty())
                .filter(|host| !existing_hosts.contains(host))
                .dedup()
                .collect::<Vec<String>>();

            if !newly_added_hosts.is_empty() {
                let mut all_hosts = config.get_unique_host_list();
                all_hosts.extend(newly_added_hosts.clone());
                let ssh_check_tasks =
                    SshCheckTask::from_hosts(deploy_config, all_hosts, "ssh-connectivity");
                barrier.push(ssh_check_tasks.len());
                executable.extend(ssh_check_tasks);
            }
            info!("Scaling cluster by adding nodes: {:?}", add_nodes);
        } else {
            info!("Scaling cluster by removing nodes: {:?}", remove_nodes);
        }

        let (operation_type, nodes_list, is_candidate) = if !add_nodes.is_empty() {
            (
                ScaleOperationType::AddNodes,
                add_nodes.clone(),
                is_candidate.clone(),
            )
        } else {
            (ScaleOperationType::RemoveNodes, remove_nodes, None)
        };

        let candidate_nodes_after_scale = match operation_type {
            ScaleOperationType::AddNodes => {
                let mut after = candidate_nodes_before_scale.clone();
                // Only add nodes marked as candidate
                if let Some(cands) = &is_candidate {
                    for (node, &flag) in nodes_list.iter().zip(cands.iter()) {
                        if flag {
                            after.push(node.clone());
                        }
                    }
                }
                after
            }
            ScaleOperationType::RemoveNodes => candidate_nodes_before_scale
                .clone()
                .into_iter()
                .filter(|node| !nodes_list.contains(node))
                .collect::<Vec<String>>(),
        };

        // Topology query — runs before any changes
        let redis_op_task_id = TaskId {
            cmd: "topology".to_string(),
            task: "topology".to_string(),
            host: "_local".to_string(),
        };

        let redis_op_task = RedisOpTask::new(
            redis_op_task_id.clone(),
            candidate_nodes_before_scale.clone(),
            "cluster topology".to_string(),
            redis_op_tx.clone(),
            redis_password.clone(),
            true,
        )
        .with_service_endpoints(deploy_config.connection.service_endpoints.clone());

        let redis_op_instance = TaskInstance {
            task_input: HashMap::default(),
            task: Box::new(redis_op_task),
            task_host: TaskHost::Local,
        };

        barrier.push(1);
        executable.insert(redis_op_task_id, redis_op_instance);

        // ── System deps for new hosts (AddNodes only) ──
        if let ScaleOperationType::AddNodes = operation_type {
            let existing_hosts = config.get_unique_host_list();
            let new_hosts: Vec<String> = nodes_list
                .iter()
                .map(|hp| hp.split(':').next().unwrap_or("").to_string())
                .filter(|h| !h.is_empty() && !existing_hosts.contains(h))
                .dedup()
                .collect();
            if !new_hosts.is_empty() {
                let mut dep_tasks = DepPkgTask::from_config(deploy_config)?;
                dep_tasks.retain(|tid, _| new_hosts.contains(&tid.host));
                if !dep_tasks.is_empty() {
                    let len = dep_tasks.len();
                    barrier.push(len);
                    executable.extend(dep_tasks);
                    info!("Added dep install tasks for {} new hosts", new_hosts.len());
                }
            }
        }

        let all_hosts_before_scale: Vec<String> = all_nodes_before_scale
            .clone()
            .iter()
            .filter_map(|node| node.split(':').next().map(|h| h.to_string()))
            .collect();

        // include candidate and non-candidate
        let new_hosts_to_upload_tarball = nodes_list
            .clone()
            .iter()
            .filter_map(|node| node.split(':').next().map(|h| h.to_string()))
            .filter(|host| !all_hosts_before_scale.contains(host))
            .unique()
            .collect_vec();

        // Derive the full host:port strings for new nodes
        let new_nodes_to_upload_tarball: Vec<String> = nodes_list
            .clone()
            .into_iter()
            .filter(|node| {
                let host = node.split(':').next().unwrap_or("");
                new_hosts_to_upload_tarball.contains(&host.to_string())
            })
            .collect();

        if let ScaleOperationType::AddNodes = operation_type {
            // Prepare new nodes before mutating cluster topology. The node-specific
            // config still waits until after the scale RPC because it needs the
            // cluster_config returned by the server.
            info!("Creating directories for newly added nodes");
            for host_port in nodes_list.iter() {
                let parts: Vec<&str> = host_port.split(':').collect();
                if parts.len() != 2 {
                    warn!("Invalid node format: {}, expected host:port", host_port);
                    continue;
                }

                let host = parts[0].to_string();
                let port = parts[1];

                let mkdir_remote_dir = ExecCustomCommand::build_task_by_host(
                    format!(
                        "mkdir -p {}/data/port-{}/tx_service",
                        deploy_config.deployment.tx_srv_home(),
                        port
                    ),
                    config,
                    vec![host.clone()],
                    Some(format!("mkdir-{}", port)),
                );

                barrier.push(mkdir_remote_dir.len());
                executable.extend(mkdir_remote_dir);
            }

            if deploy_config.deployment.tls_enabled() {
                let tls_dir = deploy_config.deployment.tls_cert_install_dir();
                let tls_hosts: Vec<String> = nodes_list
                    .iter()
                    .map(|hp| hp.split(':').next().unwrap_or("").to_string())
                    .filter(|h| !h.is_empty())
                    .collect();
                let tls_mkdir = ExecCustomCommand::build_task_by_host(
                    format!("mkdir -p {}", tls_dir),
                    config,
                    tls_hosts,
                    Some("mkdir-tls-dirs".to_string()),
                );
                if !tls_mkdir.is_empty() {
                    barrier.push(tls_mkdir.len());
                    executable.extend(tls_mkdir);
                    info!("Added TLS cert directory creation tasks");
                }
            }

            if !new_nodes_to_upload_tarball.is_empty() {
                let tx_download_url = DownloadUrl::from_url_str(&effective_tx_image_url)?;
                let tx_home = deploy_config.deployment.product().home().to_string();

                let download_tasks = DownloadTask::instances(DownloadTask::from_urls(vec![
                    effective_tx_image_url.clone(),
                ]));
                if !download_tasks.is_empty() {
                    barrier.push(download_tasks.len());
                    executable.extend(download_tasks);
                }

                let extract_tasks = LocalExtractTask::from_urls(vec![(
                    ELOQ_FILE_KEY.to_string(),
                    tx_download_url.clone(),
                    tx_home.clone(),
                )]);
                if !extract_tasks.is_empty() {
                    barrier.push(extract_tasks.len());
                    executable.extend(extract_tasks);
                }

                let staged_dir = LocalExtractTask::staged_dir_for(&tx_download_url, &tx_home);
                if !staged_dir.exists() {
                    bail!(
                        "TX service staged directory not found: {}",
                        staged_dir.display()
                    );
                }

                info!(
                    "Preparing to sync TX service directory '{}' to {} new hosts",
                    staged_dir.display(),
                    new_nodes_to_upload_tarball.len()
                );

                for host in &new_hosts_to_upload_tarball {
                    let source_host = get_source_host(None);
                    let upload_file = UploadFile {
                        source: staged_dir.to_string_lossy().to_string(),
                        dest: format!("{}/{}", deploy_config.install_dir(), tx_home),
                        extension: "dir".to_string(),
                        host: host.clone(),
                        copy_dir: true,
                        delete_remote: true,
                    };

                    let (id, instance) = build_task_instance(
                        source_host,
                        upload_file,
                        config,
                        "deploy",
                        "tx_service_upload",
                    );
                    barrier.push(1);
                    executable.insert(id, instance);
                    info!("Added TX service sync task for host {}", host);
                }
            }
        }

        // ── gRPC scale operation (add_peers / remove_node) ──
        let scale_task_id = TaskId {
            cmd: "scale".to_string(),
            task: "execute-scale".to_string(),
            host: "_local".to_string(),
        };
        let scale_config = ScaleOpConfig {
            operation_type: operation_type.clone(),
            nodes_list: nodes_list.clone(),
            is_candidate: is_candidate.clone(),
            cluster_name: cluster_name.clone(),
            ng_id,
        };
        let scale_task = ScaleOpTask::new(
            scale_task_id.clone(),
            scale_event_id.clone(),
            scale_config,
            redis_op_rx.clone(),
            scale_op_tx.clone(),
        )
        .with_service_endpoints(deploy_config.connection.service_endpoints.clone());
        let scale_instance = TaskInstance {
            task_input: HashMap::default(),
            task: Box::new(scale_task),
            task_host: TaskHost::Local,
        };
        barrier.push(1);
        executable.insert(scale_task_id, scale_instance);

        // ── Common steps for both add and remove ──

        // Update configuration files with the new topology in case the log node changed
        let conf_update_task_id = TaskId {
            cmd: "config-update".to_string(),
            task: "update-tx-configs".to_string(),
            host: "_local".to_string(),
        };

        let conf_update_task = TxConfUpdateTask::new(
            conf_update_task_id.clone(),
            deploy_config.clone(),
            operation_type.clone(),
            nodes_list.clone(),
            cluster_name.clone(),
            scale_op_rx.clone(),
        );

        let conf_update_instance = TaskInstance {
            task_input: HashMap::default(),
            task: Box::new(conf_update_task),
            task_host: TaskHost::Local,
        };

        barrier.push(1);
        executable.insert(conf_update_task_id, conf_update_instance);

        // Upload configuration files(cluster_config) to nodes
        let upload_task_id = TaskId {
            cmd: "upload".to_string(),
            task: "upload-configs".to_string(),
            host: "_local".to_string(),
        };

        let upload_tasks_map = upload_tasks_with_nodes(
            UploadTaskBuilderType::ScaleTxConf,
            config,
            &operation_type,
            &nodes_list,
            &is_candidate,
            scale_op_rx.clone(),
        );

        for (id, instance) in upload_tasks_map {
            let combined_id = TaskId {
                cmd: upload_task_id.cmd.clone(),
                task: format!("{}-{}", upload_task_id.task, id.task),
                host: id.host.clone(),
            };

            barrier.push(1);
            executable.insert(combined_id, instance);
        }

        // Channel for getting candidate nodes after scaling, RedisOpTask to CheckTxClusterScaleStatusTask
        let (redis_op_scaled_tx, redis_op_scaled_rx) = watch::channel(empty_cluster_nodes.clone());

        if let ScaleOperationType::RemoveNodes = operation_type {
            // scale may not finish yet, use topology from RedisOpTask may cause overlapping slots

            // Check if the remove rpc is finished, only then we can stop the tx service.
            let validate_config_task_id = TaskId {
                cmd: "validate".to_string(),
                task: "check-cluster-scale-status".to_string(),
                host: "_local".to_string(),
            };

            let validate_config_task = CheckTxClusterScaleStatusTask::new(
                validate_config_task_id.clone(),
                scale_event_id.clone(),
                true, // poll until finished
                None, // no scale_status_tx needed here
                None, // no redis_op_rx needed here
                Some(candidate_nodes_after_scale.clone()),
            )
            .with_service_endpoints(deploy_config.connection.service_endpoints.clone());

            let validate_config_instance = TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(validate_config_task),
                task_host: TaskHost::Local,
            };

            barrier.push(1);
            executable.insert(validate_config_task_id, validate_config_instance);
        }

        // Start or stop TX service nodes
        let ctl_task_id = TaskId {
            cmd: match operation_type {
                ScaleOperationType::AddNodes => "start".to_string(),
                ScaleOperationType::RemoveNodes => "stop".to_string(),
            },
            task: "tx-service-nodes".to_string(),
            host: "_local".to_string(),
        };

        let tx_ctl_tasks = match operation_type {
            ScaleOperationType::AddNodes => {
                let start_cmd = SubCommand::Start {
                    cluster: cluster_name.clone(),
                    nodes: nodes_list.clone(),
                };

                info!("Starting EloqTxCtlTask with start_cmd: {:?}", start_cmd);

                // This old config is not used in `start --nodes` command
                EloqTxCtlTask::from_config(start_cmd, deploy_config, ServerType::Node)
            }
            ScaleOperationType::RemoveNodes => {
                let stop_cmd = SubCommand::Stop {
                    cluster: cluster_name.clone(),
                    tx: Some(true),
                    log: false,
                    store: false,
                    monitor: false,
                    force: true,
                    all: false,
                    password: redis_password.clone(),
                    nodes: nodes_list.clone(),
                };

                info!("Starting EloqTxCtlTask with stop_cmd: {:?}", stop_cmd);

                // Use from_config_with_channel for stop operations with nodes
                match EloqTxCtlTask::from_config_with_channel(
                    stop_cmd,
                    deploy_config,
                    ServerType::Node,
                    Some(redis_op_rx.clone()),
                ) {
                    Ok(tasks) => tasks,
                    Err(err) => {
                        warn!("Failed to create stop tasks for nodes: {}", err);
                        indexmap::IndexMap::new()
                    }
                }
            }
        };
        for (id, instance) in tx_ctl_tasks {
            let combined_id = TaskId {
                cmd: ctl_task_id.cmd.clone(),
                task: format!("{}-{}", ctl_task_id.task, id.task),
                host: id.host.clone(),
            };

            barrier.push(1);
            executable.insert(combined_id, instance);
        }

        if let ScaleOperationType::AddNodes = operation_type {
            // Verify final cluster topology after scaling operation
            let verify_task_id = TaskId {
                cmd: "topology".to_string(),
                task: "post-scale-topology".to_string(),
                host: "_local".to_string(),
            };
            let verify_task = RedisOpTask::new(
                verify_task_id.clone(),
                candidate_nodes_after_scale.clone(),
                "cluster topology".to_string(),
                redis_op_scaled_tx.clone(),
                redis_password.clone(),
                true, // Skip checkpoint
            )
            .with_service_endpoints(deploy_config.connection.service_endpoints.clone());

            let verify_instance = TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(verify_task),
                task_host: TaskHost::Local,
            };

            barrier.push(1);
            executable.insert(verify_task_id, verify_instance);

            // Check if the add rpc is finished, have to check after we start the tx service in new nodes.
            let validate_config_task_id = TaskId {
                cmd: "validate".to_string(),
                task: "check-cluster-scale-status".to_string(),
                host: "_local".to_string(),
            };

            let validate_config_task = CheckTxClusterScaleStatusTask::new(
                validate_config_task_id.clone(),
                scale_event_id.clone(),
                true,                             // poll until finished
                None,                             // no scale_status_tx needed here
                Some(redis_op_scaled_rx.clone()), // Add redis_op_rx
                None,
            )
            .with_service_endpoints(deploy_config.connection.service_endpoints.clone());

            let validate_config_instance = TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(validate_config_task),
                task_host: TaskHost::Local,
            };

            barrier.push(1);
            executable.insert(validate_config_task_id, validate_config_instance);
        }

        // Update saved eloqctl topology with new cluster configuration
        let db_update_task_id = TaskId {
            cmd: "topology-save".to_string(),
            task: "save-cluster-topology".to_string(),
            host: "_local".to_string(),
        };

        let db_update_task = DbDeploymentUpdateTask::new(
            db_update_task_id.clone(),
            deploy_config.clone(),
            operation_type.clone(),
            nodes_list.clone(),
            cluster_name.clone(),
            scale_op_rx.clone(),
        );

        let db_update_instance = TaskInstance {
            task_input: HashMap::default(),
            task: Box::new(db_update_task),
            task_host: TaskHost::Local,
        };

        barrier.push(1);
        executable.insert(db_update_task_id, db_update_instance);

        // ── Regenerate and upload monitor configs (Prometheus targets) ──
        if deploy_config.deployment.monitor.is_some() {
            let mon_conf_tasks = upload_tasks(UploadTaskBuilderType::MonitorConf, config);
            if !mon_conf_tasks.is_empty() {
                let len = mon_conf_tasks.len();
                barrier.push(len);
                executable.extend(mon_conf_tasks);
            }
        }

        // Add topology update and display tasks after everything else is complete

        // Create new channel for getting final cluster topology
        let empty_cluster_nodes = ClusterNodes {
            masters: Vec::new(),
            replicas: Vec::new(),
        };
        let (final_topology_tx, final_topology_rx) = watch::channel(empty_cluster_nodes.clone());

        // Add RedisOpTask to get final cluster topology
        let final_topology_task_id = TaskId {
            cmd: "topology".to_string(),
            task: "get-final-topology".to_string(),
            host: "_local".to_string(),
        };

        let final_topology_task = RedisOpTask::new(
            final_topology_task_id.clone(),
            candidate_nodes_after_scale,
            "cluster topology".to_string(),
            final_topology_tx.clone(),
            redis_password.clone(),
            true, // Skip checkpoint
        )
        .with_service_endpoints(deploy_config.connection.service_endpoints.clone());

        let final_topology_instance = TaskInstance {
            task_input: HashMap::default(),
            task: Box::new(final_topology_task),
            task_host: TaskHost::Local,
        };

        barrier.push(1);
        executable.insert(final_topology_task_id, final_topology_instance);

        // Add TopologyUpdateTask using proper constructor
        let topology_update_tasks = match operation_type {
            ScaleOperationType::AddNodes => {
                // For add operation, no nodes are being removed
                TopologyUpdateTask::from_redis(deploy_config, final_topology_rx.clone(), None)
            }
            ScaleOperationType::RemoveNodes => {
                // For remove operation, pass the list of nodes being removed
                TopologyUpdateTask::from_redis(
                    deploy_config,
                    final_topology_rx.clone(),
                    Some(nodes_list.clone()),
                )
            }
        };
        barrier.push(topology_update_tasks.len());
        executable.extend(topology_update_tasks);

        // Add TopologyDisplayTask
        let topology_display_task_id = TaskId {
            cmd: "topology".to_string(),
            task: "display-topology".to_string(),
            host: "_local".to_string(),
        };

        let topology_display_task =
            TopologyDisplayTask::new(topology_display_task_id.clone(), cluster_name.clone());

        let topology_display_instance = TaskInstance {
            task_input: HashMap::default(),
            task: Box::new(topology_display_task),
            task_host: TaskHost::Local,
        };

        barrier.push(1);
        executable.insert(topology_display_task_id, topology_display_instance);

        if let ScaleOperationType::RemoveNodes = operation_type {
            info!("Creating cleanup tasks for removed TX node directories and files");

            // Group removed nodes by host for efficient cleanup
            let nodes_by_host: HashMap<String, Vec<u16>> = nodes_list
                .iter()
                .filter_map(|node_str| {
                    if let Some((host, port_str)) = node_str.split_once(':') {
                        if let Ok(port) = port_str.parse::<u16>() {
                            return Some((host.to_string(), port));
                        }
                    }
                    None
                })
                .into_group_map();

            let install_dir = deploy_config.install_dir();
            let tx_srv_home = deploy_config.deployment.tx_srv_home();

            for (host, ports) in nodes_by_host {
                // Check if this is the last node on this host
                let host_ports = all_nodes_before_scale
                    .iter()
                    .filter_map(|node| {
                        if let Some((h, _)) = node.split_once(':') {
                            if h == host {
                                return Some(node.clone());
                            }
                        }
                        None
                    })
                    .collect::<Vec<_>>();

                let is_last_node = host_ports.len() <= ports.len();

                if is_last_node {
                    // If this is the last node on the host, remove the entire installation directory
                    info!("Removing entire installation directory on host {}", host);
                    let clean_cmd = format!("rm -rf {}/EloqKV", install_dir);
                    let clean_tasks = ExecCustomCommand::build_task_by_host(
                        clean_cmd,
                        config,
                        vec![host.clone()],
                        Some(format!("clean_tx_all_{}", host)),
                    );

                    barrier.push(clean_tasks.len());
                    executable.extend(clean_tasks);
                } else {
                    // Otherwise, only remove directories specific to the removed ports
                    let clean_cmd = ports
                        .iter()
                        .map(|port| {
                            format!(
                                "rm -rf {}/rocksdb-{port} {}/data/port-{port} {}/logs/node-{port} {}/EloqKv-node-{port}.ini",
                                install_dir, tx_srv_home, tx_srv_home, install_dir
                            )
                        })
                        .join(" && ");

                    info!(
                        "Removing node-specific directories on host {} for ports {:?}",
                        host, ports
                    );
                    let clean_tasks = ExecCustomCommand::build_task_by_host(
                        clean_cmd,
                        config,
                        vec![host.clone()],
                        Some(format!("clean_tx_nodes_{}", host)),
                    );

                    barrier.push(clean_tasks.len());
                    executable.extend(clean_tasks);
                }
            }
        }

        info!("Scale task group configured with sequential tasks for scaling operation");

        Ok(TaskExecutionContext {
            task_group: "scale".to_string(),
            barrier: Some(barrier),
            executable,
        })
    }
}
