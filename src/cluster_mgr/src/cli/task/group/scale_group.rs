use crate::cli::task::check_tx_cluster_scale_status_task::CheckTxClusterScaleStatusTask;
use crate::cli::task::db_update_task::DbDeploymentUpdateTask;
use crate::cli::task::download_task::DownloadTask;
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{Config, ScaleTaskGroup};
use crate::cli::task::install_dep_pkg::DepPkgTask;
use crate::cli::task::monograph_tx_ctl_task::{MonographTxCtlTask, ServerType};
use crate::cli::task::redis_op_task::{ClusterNodes, RedisOpTask};
use crate::cli::task::scale_op_task::{ScaleOpConfig, ScaleOpTask};
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::task::task_utils::{ClusterNodesWithConfig, ScaleOperationType};
use crate::cli::task::topology_display_task::TopologyDisplayTask;
use crate::cli::task::topology_update_task::TopologyUpdateTask;
use crate::cli::task::tx_conf_update_task::TxConfUpdateTask;
use crate::cli::task::unpack_file_task::UnpackFileTask;
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, upload_tasks_with_nodes, UploadTaskBuilderType,
};
use crate::cli::{download_dir, SubCommand};
use crate::config::config_base::UploadFile;
use crate::config::deployment::Product;
use crate::config::DeploymentPackage;
use crate::config::{config_template, SSH_PYTHON_SCRIPT};
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

        let (add_nodes, remove_nodes, is_candidate, cluster_name, ng_id, password, version) =
            if let SubCommand::Scale {
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
        let tx_image_filename = effective_tx_image_url
            .split('/')
            .next_back()
            .unwrap_or("")
            .to_string();
        let effective_version = deploy_config
            .tx_version_override
            .as_ref()
            .or(deploy_config.deployment.version.as_ref())
            .cloned();

        if let Some(ver) = effective_version.as_ref() {
            info!("Using Monograph version {} for new nodes", ver);
        }

        // Channel for getting candidate nodes, RedisOpTask to ScaleOpTask
        let (redis_op_tx, redis_op_rx) = watch::channel(empty_cluster_nodes.clone());

        // Channel for getting cluster config information, ScaleOpTask to TxConfUpdateTask
        let (scale_op_tx, scale_op_rx) = watch::channel(empty_cluster_nodes_with_config.clone());

        // Get all Redis host:port combinations from the config
        let mut candidate_nodes_before_scale =
            deploy_config.get_host_port_list(DeploymentPackage::MonographTx);
        let standby_host_ports =
            deploy_config.get_host_port_list(DeploymentPackage::MonographStandby);
        candidate_nodes_before_scale.extend(standby_host_ports);

        let voter_host_ports = deploy_config.get_host_port_list(DeploymentPackage::MonographVoter);
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
                let ssh_python_bin = config_template(SSH_PYTHON_SCRIPT)?
                    .to_string_lossy()
                    .into_owned();
                let mut all_hosts = config.get_unique_host_list();
                all_hosts.extend(newly_added_hosts.clone());
                let host_values = all_hosts.join(" ");
                let ssh_python_task = ExecCustomCommand::build_local_task(
                    format!("python3 {} {}", ssh_python_bin, host_values),
                    config,
                    "ssh setup for new nodes",
                );
                barrier.push(ssh_python_task.len());
                executable.extend(ssh_python_task);
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

        if version.is_some() {
            let download_tasks = DownloadTask::instances(DownloadTask::from_urls(vec![
                effective_tx_image_url.clone(),
            ]));
            if !download_tasks.is_empty() {
                barrier.push(download_tasks.len());
                executable.extend(download_tasks);
            }
        }

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
        );

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

        // ── Common steps for both add and remove ──

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

        // Create directories for each node in nodes_list
        info!("Creating directories for each newly added host:port in nodes_list");

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

        // ── TLS cert directory creation ──
        if deploy_config.deployment.tls_enabled() {
            let tls_dir = deploy_config.deployment.tls_cert_install_dir();
            let tls_hosts: Vec<String> = match operation_type {
                ScaleOperationType::AddNodes => nodes_list
                    .iter()
                    .map(|hp| hp.split(':').next().unwrap_or("").to_string())
                    .filter(|h| !h.is_empty())
                    .collect(),
                ScaleOperationType::RemoveNodes => nodes_list
                    .iter()
                    .map(|hp| hp.split(':').next().unwrap_or("").to_string())
                    .filter(|h| !h.is_empty())
                    .collect(),
            };
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

        // Upload the tx-service tarball to new hosts
        if let ScaleOperationType::AddNodes = operation_type {
            if !new_nodes_to_upload_tarball.is_empty() {
                if tx_image_filename.is_empty() {
                    bail!(
                        "Unable to determine TX service image filename from {}",
                        effective_tx_image_url
                    );
                }

                info!(
                    "Preparing to upload TX service tarball '{}' to {} new hosts",
                    tx_image_filename,
                    new_nodes_to_upload_tarball.len()
                );

                let download_dir = download_dir();
                let product_dir = match deploy_config.deployment.product() {
                    Product::EloqSQL => "eloqsql",
                    Product::EloqKV => "eloqkv",
                };
                let store = deploy_config
                    .deployment
                    .storage_service
                    .as_ref()
                    .map_or("rocksdb".to_string(), |s| s.pretty_name());
                let tarball_path = download_dir
                    .join(product_dir)
                    .join(&store)
                    .join(&tx_image_filename);

                if tarball_path.exists() {
                    // Upload TX service tarball to new hosts
                    for host in &new_hosts_to_upload_tarball {
                        let source_host = get_source_host(None);
                        let upload_file = UploadFile {
                            source: tarball_path.to_string_lossy().to_string(),
                            dest: deploy_config.install_dir(),
                            extension: "gz".to_string(),
                            host: host.clone(),
                            copy_dir: false,
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
                        info!("Added upload task for TX service tarball to host {}", host);
                    }

                    // Unpack TX service tarballs on new hosts
                    let mut temp_config = deploy_config.clone();
                    let mut temp_tx_hosts = Vec::new();

                    // Extract just the needed hosts for the unpack
                    for node in &new_nodes_to_upload_tarball {
                        temp_tx_hosts.push(node.clone());
                    }

                    // Check the operation type and set appropriate host lists
                    if operation_type == ScaleOperationType::AddNodes {
                        // For add operation, set only the new nodes
                        temp_config.deployment.tx_service.tx_host_ports = temp_tx_hosts;
                        temp_config.deployment.tx_service.standby_host_ports = None;
                        temp_config.deployment.tx_service.voter_host_ports = None;
                        temp_config.deployment.log_service = None;
                    }

                    temp_config.deployment.tx_service.image = Some(effective_tx_image_url.clone());
                    temp_config.tx_image_override = Some(effective_tx_image_url.clone());

                    // Generate unpack tasks using the public unpack_eloqservers method
                    let tx_unpack_tasks = UnpackFileTask::unpack_eloqservers(&temp_config);

                    // Add the tasks to executable
                    for (task_id, instance) in tx_unpack_tasks {
                        info!("Added unpack task for TX service on host {}", task_id.host);
                        barrier.push(1);
                        executable.insert(task_id, instance);
                    }
                } else {
                    bail!(
                        "TX service tarball not found at expected path: {}",
                        tarball_path.display()
                    );
                }
            }
        }

        // ── gRPC scale operation (add_peers / remove_node) ──
        // Must run AFTER all preparation (SSH, deps, upload, unpack, config) is done
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
        );
        let scale_instance = TaskInstance {
            task_input: HashMap::default(),
            task: Box::new(scale_task),
            task_host: TaskHost::Local,
        };
        barrier.push(1);
        executable.insert(scale_task_id, scale_instance);

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
                None,
            );

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

                info!(
                    "Starting MonographTxCtlTask with start_cmd: {:?}",
                    start_cmd
                );

                // This old config is not used in `start --nodes` command
                MonographTxCtlTask::from_config(start_cmd, deploy_config, ServerType::Node)
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

                info!("Starting MonographTxCtlTask with stop_cmd: {:?}", stop_cmd);

                // Use from_config_with_channel for stop operations with nodes
                match MonographTxCtlTask::from_config_with_channel(
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
            );

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
            );

            let validate_config_instance = TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(validate_config_task),
                task_host: TaskHost::Local,
            };

            barrier.push(1);
            executable.insert(validate_config_task_id, validate_config_instance);
        }

        // Update eloqctl database with new cluster configuration
        let db_update_task_id = TaskId {
            cmd: "db-update".to_string(),
            task: "update-deployment-db".to_string(),
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
        );

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
