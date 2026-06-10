use crate::cli::task::db_update_log_task::DbDeploymentUpdateLogTask;
use crate::cli::task::download_task::DownloadTask;
use crate::cli::task::eloq_log_ctl_task::{EloqLogCtlTask, LogCtlCmd};
use crate::cli::task::eloq_log_probe_task::EloqLogProbeTask;
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{Config, ScaleLogTaskGroup, TaskGroup};
use crate::cli::task::local_extract_task::LocalExtractTask;
use crate::cli::task::redis_op_task::{ClusterNodes, RedisOpTask};
use crate::cli::task::scale_log_cleanup_task::ScaleLogCleanupTask;
use crate::cli::task::scale_log_op_task::ScaleLogOpTask;
use crate::cli::task::ssh_check_task::SshCheckTask;
use crate::cli::task::task_base::{
    TaskArgValue, TaskExecutionContext, TaskHost, TaskId, TaskInstance,
};
use crate::cli::task::task_utils::ScaleOperationType;
use crate::cli::task::topology_display_task::TopologyDisplayTask;
use crate::cli::task::topology_update_task::TopologyUpdateTask;
use crate::cli::task::upload::upload_task_builder::{build_task_instance, get_source_host};
use crate::cli::SubCommand;
use crate::config::config_base::{UploadFile, ELOQ_LOG_FILE_KEY, LOG_SERVICE_HOME};
use crate::config::log_service::{LogProcessKey, LogServiceNode};
use crate::config::DownloadUrl;
use anyhow::anyhow;
use async_trait::async_trait;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;
use tokio::sync::watch;
use tracing::info;
use uuid::Uuid;

#[async_trait]
impl TaskGroup for ScaleLogTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        if let SubCommand::ScaleLog {
            cluster: _cluster,
            add_nodes,
            remove_nodes,
            log_ng_id,
        } = cmd_arg.clone()
        {
            // Extract DeployConfig
            let Config::Cluster(c) = config;
            let deploy_cfg = c.clone();

            if add_nodes.is_empty() && remove_nodes.is_empty() {
                return Err(anyhow!(
                    "Either --add-nodes or --remove-nodes must be provided with at least one log node"
                ));
            }

            if !add_nodes.is_empty() && !remove_nodes.is_empty() {
                return Err(anyhow!(
                    "Cannot specify both --add-nodes and --remove-nodes in the same command"
                ));
            }

            // Determine operation type and nodes list
            let (operation_type, mut scale_node_list) = if !add_nodes.is_empty() {
                info!("Scaling log cluster by adding nodes: {:?}", add_nodes);
                (ScaleOperationType::AddNodes, add_nodes)
            } else {
                info!("Scaling log cluster by removing nodes: {:?}", remove_nodes);
                (ScaleOperationType::RemoveNodes, remove_nodes)
            };

            // Validate log_ng_id based on operation type
            match operation_type {
                ScaleOperationType::AddNodes => {
                    if log_ng_id.is_none() {
                        return Err(anyhow!("--log-ng-id is required when adding log nodes"));
                    }
                }
                ScaleOperationType::RemoveNodes => {
                    if log_ng_id.is_some() {
                        return Err(anyhow!(
                            "--log-ng-id should not be provided when removing log nodes"
                        ));
                    }
                }
            }

            let event_id = Uuid::new_v4().to_string();
            info!("Generated event ID for scale-log operation: {}", event_id);

            // Create a task execution context with multiple tasks in sequence
            let mut executable = IndexMap::new();
            let mut barrier = Vec::new();

            //  Get the current log service configuration
            let mut temp_log_service = match &deploy_cfg.deployment.log_service {
                Some(service) => service.clone(),
                None => return Err(anyhow!("Log service configuration not found")),
            };

            match operation_type {
                ScaleOperationType::AddNodes => {
                    scale_node_list.retain(|node| {
                        if let Some((host, port_str)) = node.split_once(':') {
                            if let Ok(port) = port_str.parse::<u16>() {
                                let exists = temp_log_service
                                    .nodes
                                    .iter()
                                    .any(|existing| existing.host == host && existing.port == port);
                                if exists {
                                    info!("Scale log add no-op for existing node {node}");
                                }
                                return !exists;
                            }
                        }
                        true
                    });
                    if scale_node_list.is_empty() {
                        println!("All requested log nodes already exist; scalelog add is a no-op.");
                        return Ok(TaskExecutionContext::dummy());
                    }
                }
                ScaleOperationType::RemoveNodes => {
                    scale_node_list.retain(|node| {
                        if let Some((host, port_str)) = node.split_once(':') {
                            if let Ok(port) = port_str.parse::<u16>() {
                                let exists = temp_log_service
                                    .nodes
                                    .iter()
                                    .any(|existing| existing.host == host && existing.port == port);
                                if !exists {
                                    info!("Scale log remove no-op for absent node {node}");
                                }
                                return exists;
                            }
                        }
                        false
                    });
                    if scale_node_list.is_empty() {
                        println!("All requested log nodes are already absent; scalelog remove is a no-op.");
                        return Ok(TaskExecutionContext::dummy());
                    }
                }
            }

            let mut temp_config = deploy_cfg.clone();

            match operation_type {
                ScaleOperationType::AddNodes => {
                    let existing_hosts = config.get_unique_host_list();

                    // Add SSH setup for newly added log nodes
                    let newly_added_hosts = scale_node_list
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
                            SshCheckTask::from_hosts(&deploy_cfg, all_hosts, "ssh-connectivity");
                        barrier.push(ssh_check_tasks.len());
                        executable.extend(ssh_check_tasks);
                    }

                    // Update log service configuration with new nodes (for add operation)
                    info!("Updating log service configuration with new nodes");

                    // Get the current number of nodes to determine node_id for new nodes
                    let mut current_node_count = temp_log_service.nodes.len();

                    // Add new nodes to the log service configuration
                    for node in &scale_node_list {
                        if let Some((host, port_str)) = node.split_once(':') {
                            if let Ok(port) = port_str.parse::<u16>() {
                                let storage_path =
                                    format!("{}/wal_eloqkv/{}", deploy_cfg.install_dir(), port);

                                // Create a new LogServiceNode for the node
                                let new_member = LogServiceNode {
                                    host: host.to_string(),
                                    data_dir: vec![storage_path],
                                    port,
                                };

                                // Add to the log service
                                temp_log_service.nodes.push(new_member);
                                temp_log_service.replica += 1;
                                current_node_count += 1;

                                info!(
                                    "Added new log node configuration: {}:{} with node_id {}",
                                    host, port, current_node_count
                                );
                            }
                        }
                    }

                    info!(
                        "Current node count: {}, log_group_replica_num: {}",
                        current_node_count,
                        temp_log_service.log_replica()
                    );

                    // Create a modified deployment config with the updated log service
                    temp_config.deployment.log_service = Some(temp_log_service.clone());

                    // Step 3: Generate start scripts for all log nodes
                    info!("Generating start scripts for all log nodes");
                    let log_scripts_paths = temp_config.gen_log_start_script()?;
                    info!("Generated log scripts: {:?}", log_scripts_paths);

                    // Extract all host names with ports from the temp_log_service (includes both existing and new nodes)
                    let all_log_nodes: Vec<String> = temp_log_service
                        .nodes
                        .iter()
                        .map(|node| format!("{}:{}", node.host, node.port))
                        .collect();

                    info!(
                        "All log nodes after configuration update: {:?}",
                        all_log_nodes
                    );

                    // Create a comprehensive list for uploading bash scripts to all nodes in the cluster
                    let node_list_to_upload_bash = all_log_nodes.clone();

                    // Extract host names from add_nodes - keep this for binary uploads
                    let hosts_to_upload_log_binary = scale_node_list
                        .iter()
                        .filter_map(|node| node.split(':').next().map(|h| h.to_string()))
                        .unique()
                        .collect_vec();

                    // Step 4: Create mkdir task for new nodes
                    info!("Creating mkdir task for new nodes");
                    let mkdir_remote_dir = ExecCustomCommand::build_task_by_host(
                        format!("mkdir -p {}", deploy_cfg.install_dir()),
                        config,
                        hosts_to_upload_log_binary.clone(),
                        Some("mkdir".to_string()),
                    );

                    // Add mkdir task to the executable
                    barrier.push(mkdir_remote_dir.len());
                    executable.extend(mkdir_remote_dir);

                    if !hosts_to_upload_log_binary.is_empty() {
                        let log_image = deploy_cfg
                            .deployment
                            .log_image()
                            .ok_or_else(|| anyhow!("Log service image not configured"))?;
                        let log_download_url = DownloadUrl::from_url_str(log_image)?;

                        let download_tasks =
                            DownloadTask::instances(DownloadTask::from_urls(vec![
                                log_image.to_string()
                            ]));
                        if !download_tasks.is_empty() {
                            barrier.push(download_tasks.len());
                            executable.extend(download_tasks);
                        }

                        let extract_tasks = LocalExtractTask::from_urls(vec![(
                            ELOQ_LOG_FILE_KEY.to_string(),
                            log_download_url.clone(),
                            LOG_SERVICE_HOME.to_string(),
                        )]);
                        if !extract_tasks.is_empty() {
                            barrier.push(extract_tasks.len());
                            executable.extend(extract_tasks);
                        }

                        let staged_dir =
                            LocalExtractTask::staged_dir_for(&log_download_url, LOG_SERVICE_HOME);
                        if !staged_dir.exists() {
                            return Err(anyhow!(
                                "Log service staged directory not found: {}",
                                staged_dir.display()
                            ));
                        }

                        for host in &hosts_to_upload_log_binary {
                            let upload_file = UploadFile {
                                source: staged_dir.to_string_lossy().to_string(),
                                dest: format!("{}/{}", deploy_cfg.install_dir(), LOG_SERVICE_HOME),
                                extension: "dir".to_string(),
                                host: host.to_string(),
                                copy_dir: true,
                                delete_remote: true,
                            };

                            let source_host = get_source_host(None);
                            let (id, instance) = build_task_instance(
                                source_host,
                                upload_file,
                                config,
                                "deploy",
                                "deploy_eloq_log_dir",
                            );

                            barrier.push(1);
                            executable.insert(id, instance);
                        }
                    }

                    // Step 5: Create upload tasks for the newly added log nodes scripts
                    info!("Setting up upload tasks for log start scripts");

                    // Upload log start bash file for all nodes
                    for host in node_list_to_upload_bash
                        .iter()
                        .filter_map(|node| node.split_once(':').map(|(host, _)| host.to_string()))
                        .unique()
                    {
                        if let Some(script_list) = &log_scripts_paths {
                            for path in script_list {
                                if !path.to_string_lossy().contains(&host) {
                                    continue;
                                }
                                info!("Adding script to upload: {}", path.display());
                                let upload_file = UploadFile {
                                    source: path.to_string_lossy().to_string(),
                                    dest: deploy_cfg.install_dir(),
                                    extension: "bash".to_string(),
                                    host: host.clone(),
                                    copy_dir: false,
                                    delete_remote: false,
                                };

                                let task_name = path
                                    .file_name()
                                    .and_then(|name| name.to_str())
                                    .map(|name| format!("deploy_{}", name.replace('.', "_")))
                                    .unwrap_or_else(|| {
                                        format!("deploy_log_start_bash_to_{}", host)
                                    });
                                let source_host = get_source_host(None);
                                let (id, instance) = build_task_instance(
                                    source_host,
                                    upload_file,
                                    config,
                                    "deploy",
                                    &task_name,
                                );

                                barrier.push(1);
                                executable.insert(id, instance);
                            }
                        }
                    }

                    // Step 7: Use EloqLogCtlTask to start the new log nodes
                    info!("Setting up tasks to start new log nodes");

                    // Create start command for all new nodes using EloqLogCtlTask
                    let mut log_cmd_by_key = HashMap::new();

                    for node in &scale_node_list {
                        if let Some((host, port_str)) = node.split_once(':') {
                            if let Ok(port) = port_str.parse::<u16>() {
                                let process_key = LogProcessKey {
                                    host: host.to_string(),
                                    port,
                                };

                                let start_cmd = format!(
                                    "/bin/bash {}/start_tx_log_{}.bash",
                                    deploy_cfg.install_dir(),
                                    port
                                );

                                log_cmd_by_key.insert(process_key, LogCtlCmd::Start(start_cmd));
                                info!("Added start command for log node {}:{}", host, port);
                            }
                        }
                    }

                    // Create EloqLogCtlTask for each host
                    log_cmd_by_key
                        .iter()
                        .into_group_map_by(|(process_key, _cmd)| process_key.host.clone())
                        .into_iter()
                        .for_each(|(host, key_cmd_pair)| {
                            let task_host = TaskHost::remote(&deploy_cfg.connection, &host);

                            let task_id = TaskId {
                                cmd: "eloq_log_start".to_string(),
                                task: "start".to_string(),
                                host: host.clone(),
                            };

                            let log_cmd = key_cmd_pair
                                .iter()
                                .map(|(key, cmd)| ((*key).clone(), (*cmd).clone()))
                                .collect::<HashMap<LogProcessKey, LogCtlCmd>>();

                            let task =
                                EloqLogCtlTask::new(temp_config.clone(), task_id.clone(), log_cmd);

                            let instance = TaskInstance {
                                task_input: HashMap::from([(
                                    "cluster_cmd".to_string(),
                                    crate::cli::task::task_base::TaskArgValue::Str(
                                        "start".to_string(),
                                    ),
                                )]),
                                task: Box::new(task),
                                task_host,
                            };

                            barrier.push(1);
                            executable.insert(task_id, instance);
                        });

                    // Step 8: Send RPC to add nodes to the cluster
                    let scale_task_id = TaskId {
                        cmd: "scalelog".to_string(),
                        task: match operation_type {
                            ScaleOperationType::AddNodes => "execute-scale-log-add",
                            ScaleOperationType::RemoveNodes => "execute-scale-log-remove",
                        }
                        .to_string(),
                        host: "_local".to_string(),
                    };

                    // Create channel for passing scale operation result to cleanup task
                    let (scale_result_tx, scale_result_rx) = watch::channel(false); // false = failed by default

                    info!(
                        "Setting up RPC task to {} log nodes {} cluster",
                        match operation_type {
                            ScaleOperationType::AddNodes => "add",
                            ScaleOperationType::RemoveNodes => "remove",
                        },
                        match operation_type {
                            ScaleOperationType::AddNodes => "to",
                            ScaleOperationType::RemoveNodes => "from",
                        }
                    );

                    let log_task = ScaleLogOpTask::new(
                        scale_task_id.clone(),
                        event_id.clone(),
                        scale_node_list.clone(),
                        log_ng_id,
                        deploy_cfg.clone(),
                        operation_type.clone(),
                        scale_result_tx,
                    );

                    let scale_instance = TaskInstance {
                        task_input: HashMap::new(),
                        task: Box::new(log_task),
                        task_host: TaskHost::Local,
                    };
                    barrier.push(1);
                    executable.insert(scale_task_id, scale_instance);

                    // Step 8.5: Add cleanup task that will be executed if the scale operation fails
                    let cleanup_task_id = TaskId {
                        cmd: "scalelog".to_string(),
                        task: "cleanup-on-failure".to_string(),
                        host: "_local".to_string(),
                    };

                    let cleanup_task = ScaleLogCleanupTask::new(
                        cleanup_task_id.clone(),
                        scale_node_list.clone(),
                        deploy_cfg.clone(),
                        scale_result_rx,
                    );

                    let cleanup_instance = TaskInstance {
                        task_input: HashMap::new(),
                        task: Box::new(cleanup_task),
                        task_host: TaskHost::Local,
                    };
                    barrier.push(1);
                    executable.insert(cleanup_task_id, cleanup_instance);
                }
                ScaleOperationType::RemoveNodes => {
                    // Step 2: Send RPC to remove nodes from the cluster (before stopping nodes)
                    // need to send rpc before stop the nodes to allow potential leader transfer
                    let scale_task_id = TaskId {
                        cmd: "scalelog".to_string(),
                        task: match operation_type {
                            ScaleOperationType::AddNodes => "execute-scale-log-add",
                            ScaleOperationType::RemoveNodes => "execute-scale-log-remove",
                        }
                        .to_string(),
                        host: "_local".to_string(),
                    };

                    // Create a dummy channel for RemoveNodes (not used for cleanup)
                    let (dummy_tx, _dummy_rx) = watch::channel(false);

                    info!(
                        "Setting up RPC task to {} log nodes {} cluster",
                        match operation_type {
                            ScaleOperationType::AddNodes => "add",
                            ScaleOperationType::RemoveNodes => "remove",
                        },
                        match operation_type {
                            ScaleOperationType::AddNodes => "to",
                            ScaleOperationType::RemoveNodes => "from",
                        }
                    );

                    let log_task = ScaleLogOpTask::new(
                        scale_task_id.clone(),
                        event_id.clone(),
                        scale_node_list.clone(),
                        log_ng_id,
                        deploy_cfg.clone(),
                        operation_type.clone(),
                        dummy_tx,
                    );

                    let scale_instance = TaskInstance {
                        task_input: HashMap::new(),
                        task: Box::new(log_task),
                        task_host: TaskHost::Local,
                    };
                    barrier.push(1);
                    executable.insert(scale_task_id, scale_instance);

                    // Step 3: Use EloqLogCtlTask to stop the nodes that will be removed
                    info!("Setting up tasks to stop log nodes that will be removed");

                    // Create stop commands for all nodes to be removed
                    let mut log_cmd_by_key = HashMap::new();

                    for node in &scale_node_list {
                        if let Some((host, port_str)) = node.split_once(':') {
                            if let Ok(port) = port_str.parse::<u16>() {
                                let process_key = LogProcessKey {
                                    host: host.to_string(),
                                    port,
                                };

                                let stop_cmd = format!("ps -e -o pid,cmd --no-headers -u {} | grep '{}/bin/launch_sv' | grep 'wal_eloqkv/{}' | grep -v grep | awk '{{print $1}}' | xargs -r kill",
                                    deploy_cfg.connection.username,
                                    deploy_cfg.deployment.log_srv_home(),
                                    port
                                );

                                log_cmd_by_key.insert(process_key, LogCtlCmd::Stop(stop_cmd));
                                info!("Added stop command for log node {}:{}", host, port);
                            }
                        }
                    }

                    // Create EloqLogCtlTask for each host
                    log_cmd_by_key
                        .iter()
                        .into_group_map_by(|(process_key, _cmd)| process_key.host.clone())
                        .into_iter()
                        .for_each(|(host, key_cmd_pair)| {
                            let task_host = TaskHost::remote(&deploy_cfg.connection, &host);

                            let task_id = TaskId {
                                cmd: "eloq_log_stop".to_string(),
                                task: "stop".to_string(),
                                host: host.clone(),
                            };

                            let log_cmd = key_cmd_pair
                                .iter()
                                .map(|(key, cmd)| ((*key).clone(), (*cmd).clone()))
                                .collect::<HashMap<LogProcessKey, LogCtlCmd>>();

                            // Stop cmd needs the old configuration
                            let task =
                                EloqLogCtlTask::new(temp_config.clone(), task_id.clone(), log_cmd);

                            let instance = TaskInstance {
                                task_input: HashMap::from([(
                                    "cluster_cmd".to_string(),
                                    TaskArgValue::Str("stop".to_string()),
                                )]),
                                task: Box::new(task),
                                task_host,
                            };

                            barrier.push(1);
                            executable.insert(task_id, instance);
                        });

                    // Step 3.5: Delete the start scripts for removed nodes
                    info!("Setting up tasks to delete start scripts for removed nodes");

                    // Group removed nodes by host to execute delete commands efficiently
                    let nodes_by_host: HashMap<String, Vec<u16>> = scale_node_list
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

                    // Create delete commands for each host
                    for (host, ports) in &nodes_by_host {
                        // Create bash command to delete all start scripts for the removed ports
                        let files_to_delete = ports
                            .iter()
                            .map(|port| format!("start_tx_log_{}.bash", port))
                            .collect::<Vec<_>>()
                            .join(" ");

                        let delete_cmd = format!(
                            "cd {} && rm -f {}",
                            deploy_cfg.install_dir(),
                            files_to_delete
                        );

                        info!(
                            "Adding command to delete scripts on {}: {}",
                            host, delete_cmd
                        );

                        // Create the delete task using ExecCustomCommand
                        let delete_tasks = ExecCustomCommand::build_task_by_host(
                            delete_cmd,
                            config,
                            vec![host.clone()],
                            Some(format!("delete_removed_log_scripts_{}", host)),
                        );

                        barrier.push(delete_tasks.len());
                        executable.extend(delete_tasks);
                    }

                    // Add tasks to remove log data directories
                    info!("Setting up tasks to remove log data directories for removed nodes");
                    let install_dir = deploy_cfg.install_dir();

                    // Get all log nodes to determine if we're removing the last node on a host
                    let all_log_nodes: Vec<(String, u16)> =
                        if let Some(log_service) = &deploy_cfg.deployment.log_service {
                            log_service
                                .nodes
                                .iter()
                                .map(|node| (node.host.clone(), node.port))
                                .collect()
                        } else {
                            Vec::new()
                        };

                    for (host, ports) in &nodes_by_host {
                        // Check if these are the last log nodes on this host
                        let remaining_nodes_on_host = all_log_nodes
                            .iter()
                            .filter(|(h, p)| h == host && !ports.contains(p))
                            .count();

                        let is_last_node = remaining_nodes_on_host == 0;

                        // Build the cleanup command
                        let cleanup_cmd = if is_last_node {
                            // If this is the last log node on the host, remove all log-related directories
                            format!(
                                "rm -rf {}/wal_eloqkv {}/{}",
                                install_dir, install_dir, LOG_SERVICE_HOME
                            )
                        } else {
                            // Otherwise, only remove directories specific to the removed ports
                            ports
                                .iter()
                                .map(|port| format!("rm -rf {}/wal_eloqkv/{}", install_dir, port))
                                .join(" && ")
                        };

                        info!(
                            "Adding cleanup command for log nodes on host {}: {}",
                            host, cleanup_cmd
                        );

                        // Create the cleanup task using ExecCustomCommand
                        let cleanup_tasks = ExecCustomCommand::build_task_by_host(
                            cleanup_cmd,
                            config,
                            vec![host.clone()],
                            Some(format!("cleanup_log_data_{}", host)),
                        );

                        barrier.push(cleanup_tasks.len());
                        executable.extend(cleanup_tasks);
                    }

                    // Step 4: Update log service configuration to remove nodes
                    info!("Updating log service configuration to remove nodes");

                    // Create a new vector to store nodes that should be kept
                    let mut nodes_to_keep = Vec::new();
                    let mut removed_count = 0;

                    // Filter out nodes specified for removal
                    for node in &temp_log_service.nodes {
                        let node_addr = format!("{}:{}", node.host, node.port);
                        if scale_node_list.contains(&node_addr) {
                            info!("Removing log node: {}", node_addr);
                            temp_log_service.replica -= 1;
                            removed_count += 1;
                        } else {
                            nodes_to_keep.push(node.clone());
                        }
                    }

                    if removed_count == 0 {
                        return Err(anyhow!("None of the specified nodes were found in the log service configuration"));
                    }

                    if nodes_to_keep.len() < 3 {
                        return Err(anyhow!("Cannot remove nodes if it would result in fewer than 3 nodes remaining in the log service"));
                    }

                    // Update the log_service with the filtered nodes
                    temp_log_service.nodes = nodes_to_keep;

                    info!(
                        "Updated log service configuration: removed {} nodes, {} nodes remaining, replica count: {}",
                        removed_count,
                        temp_log_service.nodes.len(),
                        temp_log_service.log_replica()
                    );

                    // Update the modified configuration
                    temp_config.deployment.log_service = Some(temp_log_service.clone());

                    // Step 5: Generate start scripts for remaining log nodes after removal
                    info!("Generating start scripts for remaining log nodes");
                    let log_scripts_paths = temp_config.gen_log_start_script()?;
                    info!("Generated log scripts: {:?}", log_scripts_paths);

                    // Extract all host:port combinations from the updated temp_log_service (only remaining nodes)
                    let all_log_nodes: Vec<String> = temp_log_service
                        .nodes
                        .iter()
                        .map(|node| format!("{}:{}", node.host, node.port))
                        .collect();

                    info!(
                        "Remaining log nodes after configuration update: {:?}",
                        all_log_nodes
                    );

                    // Create a comprehensive list for uploading bash scripts to all remaining nodes in the cluster
                    let node_list_to_upload_bash = all_log_nodes.clone();

                    // Step 6: Upload updated start scripts to all remaining nodes
                    info!("Setting up upload tasks for updated log start scripts");

                    // Upload log start bash file for all remaining nodes
                    for host in node_list_to_upload_bash
                        .iter()
                        .filter_map(|node| node.split_once(':').map(|(host, _)| host.to_string()))
                        .unique()
                    {
                        if let Some(script_list) = &log_scripts_paths {
                            for path in script_list {
                                if !path.to_string_lossy().contains(&host) {
                                    continue;
                                }
                                info!("Adding script to upload: {}", path.display());
                                let upload_file = UploadFile {
                                    source: path.to_string_lossy().to_string(),
                                    dest: deploy_cfg.install_dir(),
                                    extension: "bash".to_string(),
                                    host: host.clone(),
                                    copy_dir: false,
                                    delete_remote: false,
                                };

                                let task_name = path
                                    .file_name()
                                    .and_then(|name| name.to_str())
                                    .map(|name| format!("deploy_{}", name.replace('.', "_")))
                                    .unwrap_or_else(|| {
                                        format!("deploy_log_start_bash_to_{}", host)
                                    });
                                let source_host = get_source_host(None);
                                let (id, instance) = build_task_instance(
                                    source_host,
                                    upload_file,
                                    config,
                                    "deploy",
                                    &task_name,
                                );

                                barrier.push(1);
                                executable.insert(id, instance);
                            }
                        }
                    }
                }
            }

            // Step 9: Insert a task to probe that all log nodes are started
            info!("Setting up task to probe log nodes readiness");
            let probe_tasks = EloqLogProbeTask::from_config(&temp_config);
            for (task_id, instance) in probe_tasks {
                barrier.push(1);
                executable.insert(task_id, instance);
            }

            // Step 10: Add task to update the database with the updated log configuration
            let db_update_task_id = TaskId {
                cmd: "scalelog".to_string(),
                task: "update-database".to_string(),
                host: "_local".to_string(),
            };

            info!("Setting up task to update deployment configuration in database");

            let db_update_task = DbDeploymentUpdateLogTask::new(
                db_update_task_id.clone(),
                temp_config.clone(),
                deploy_cfg.deployment.cluster_name.clone(),
            );

            let db_update_instance = TaskInstance {
                task_input: HashMap::new(),
                task: Box::new(db_update_task),
                task_host: TaskHost::Local,
            };
            barrier.push(1);
            executable.insert(db_update_task_id, db_update_instance);

            // Add topology update and display tasks as final steps

            // Create new channel for getting final cluster topology
            let empty_cluster_nodes = ClusterNodes {
                masters: Vec::new(),
                replicas: Vec::new(),
            };
            let (final_topology_tx, final_topology_rx) =
                watch::channel(empty_cluster_nodes.clone());

            // Add RedisOpTask to get final cluster topology for tx nodes
            // We need to use tx nodes for TopologyUpdateTask as it requires tx nodes topology
            let final_topology_task_id = TaskId {
                cmd: "topology".to_string(),
                task: "get-final-topology".to_string(),
                host: "_local".to_string(),
            };

            // Get all tx nodes from deployment config
            let tx_nodes = temp_config.get_host_port_list(crate::config::DeploymentPackage::EloqTx);

            let final_topology_task = RedisOpTask::new(
                final_topology_task_id.clone(),
                tx_nodes,
                "cluster topology".to_string(),
                final_topology_tx.clone(),
                temp_config.redis_password(None),
                true, // Skip checkpoint
            )
            .with_service_endpoints(temp_config.connection.service_endpoints.clone());

            let final_topology_instance = TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(final_topology_task),
                task_host: TaskHost::Local,
            };

            barrier.push(1);
            executable.insert(final_topology_task_id, final_topology_instance);

            // Add TopologyUpdateTask using proper constructor
            // This will update both TX and LOG topology in the database
            let topology_update_tasks = match operation_type {
                ScaleOperationType::AddNodes => {
                    // For add operation, no nodes are being removed
                    TopologyUpdateTask::from_redis(&temp_config, final_topology_rx.clone(), None)
                }
                ScaleOperationType::RemoveNodes => {
                    // For remove operation, pass the list of nodes being removed
                    TopologyUpdateTask::from_redis(
                        &temp_config,
                        final_topology_rx.clone(),
                        Some(scale_node_list.clone()),
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

            let topology_display_task = TopologyDisplayTask::new(
                topology_display_task_id.clone(),
                temp_config.deployment.cluster_name.clone(),
            );

            let topology_display_instance = TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(topology_display_task),
                task_host: TaskHost::Local,
            };

            barrier.push(1);
            executable.insert(topology_display_task_id, topology_display_instance);

            return Ok(TaskExecutionContext {
                task_group: "scalelog".to_string(),
                barrier: Some(barrier),
                executable,
            });
        }
        Err(anyhow!("ScaleLogTaskGroup received wrong subcommand"))
    }
}
