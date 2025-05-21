use crate::cli::task::check_tx_cluster_scale_status_task::CheckTxClusterScaleStatusTask;
use crate::cli::task::db_update_task::DbDeploymentUpdateTask;
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{Config, ScaleTaskGroup};
use crate::cli::task::monograph_tx_ctl_task::{MonographTxCtlTask, ServerType};
use crate::cli::task::redis_op_task::{ClusterNodes, RedisOpTask};
use crate::cli::task::scale_op_task::ScaleOpTask;
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::task::task_utils::{ClusterNodesWithConfig, ScaleOperationType};
use crate::cli::task::topology_display_task::TopologyDisplayTask;
use crate::cli::task::topology_update_task::TopologyUpdateTask;
use crate::cli::task::tx_conf_update_task::TxConfUpdateTask;
use crate::cli::task::unpack_file_task::UnpackFileTask;
use crate::cli::task::update_scale_status_task::DbScaleOpUpdateTask;
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, upload_tasks_with_nodes, UploadTaskBuilderType,
};
use crate::cli::{download_dir, SubCommand};
use crate::config::config_base::{DeployConfig, UploadFile};
use crate::config::deployment::Product;
use crate::config::DeploymentPackage;
use crate::config::{config_template, SSH_PYTHON_SCRIPT};
use crate::state::scale_operation::ScaleOperation;
use crate::state::state_base::QueryCondition;
use crate::state::state_base::StateOperation;
use crate::state::state_mgr::{SCALE_STATE, STATE_MGR};
use crate::StateValue;
use anyhow::{anyhow, bail};
use async_trait::async_trait;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;
use tokio::sync::watch;
use tracing::{info, warn};
use uuid::Uuid;

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

        let (add_nodes, remove_nodes, is_candidate, cluster_name, ng_id, resume, password) =
            if let SubCommand::Scale {
                add_nodes: add,
                remove_nodes: remove,
                is_candidate,
                cluster,
                ng_id,
                resume,
                password,
                ..
            } = &cmd
            {
                (
                    add.clone(),
                    remove.clone(),
                    is_candidate.clone(),
                    cluster.clone(),
                    *ng_id,
                    resume.clone(),
                    password.clone(),
                )
            } else {
                return Err(anyhow!("Invalid command for scale task group"));
            };

        if let Some(resume_id) = &resume {
            if !add_nodes.is_empty()
                || !remove_nodes.is_empty()
                || is_candidate.is_some()
                || ng_id.is_some()
            {
                return Err(anyhow!("When using --resume, no other flags (--add-nodes, --remove-nodes, --is-candidate, --ng-id) should be provided"));
            }
            info!("Resuming scale operation with event_id: {}", resume_id);
        } else {
            // If not resuming, validate normal operation flags
            if add_nodes.is_empty() && remove_nodes.is_empty() {
                return Err(anyhow!("Must specify either --add-nodes or --remove-nodes with at least one node; or use --resume with an event_id"));
            }

            if !add_nodes.is_empty() && !remove_nodes.is_empty() {
                return Err(anyhow!(
                    "Cannot specify both --add-nodes and --remove-nodes in the same command"
                ));
            }

            // For add operations, is_candidate must be specified
            if !add_nodes.is_empty() && is_candidate.is_none() {
                return Err(anyhow!("--is-candidate must be provided when adding nodes"));
            }

            // For add operations, ng_id must be specified
            if !add_nodes.is_empty() && ng_id.is_none() {
                return Err(anyhow!("--ng-id must be provided when adding nodes"));
            }
        }

        let empty_cluster_nodes = ClusterNodes {
            masters: Vec::new(),
            replicas: Vec::new(),
        };

        let empty_cluster_nodes_with_config = ClusterNodesWithConfig {
            nodes: empty_cluster_nodes.clone(),
            cluster_config: None,
        };

        // Channel for getting candidate nodes, RedisOpTask to ScaleOpTask
        let (redis_op_tx, redis_op_rx) = watch::channel(empty_cluster_nodes.clone());

        // Channel for getting cluster config information, ScaleOpTask to TxConfUpdateTask
        let (scale_op_tx, scale_op_rx) = watch::channel(empty_cluster_nodes_with_config.clone());

        // Channel for scale status result, CheckTxClusterScaleStatusTask to UpdateScaleStatusTask
        let (scale_status_tx, scale_status_rx) = watch::channel(-1); // Default -1 (undefined)

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
        let (scale_event_id, operation_type, nodes_list, is_candidate) =
            if let Some(event_id) = &resume {
                info!(
                    "Retrieving details for resumed operation with event_id: {}",
                    event_id
                );

                // Retrieve the operation details from the database
                let scale_op = STATE_MGR.get_state_operation::<ScaleOperation>(SCALE_STATE);

                // We need to run this synchronously since we're in a non-async context
                let op_result = scale_op
                    .load(move || -> Option<QueryCondition> {
                        Some(QueryCondition {
                            cond_text: "event_id = $1".to_string(),
                            bind_values: vec![StateValue::Varchar(event_id.clone())],
                        })
                    })
                    .await?;

                if op_result.is_empty() {
                    return Err(anyhow!(
                        "No scale operation found with event_id: {}",
                        event_id
                    ));
                }

                let operation = &op_result[0];

                // Convert the stored operation details back to the format needed for processing
                let op_type = match operation.operation_type {
                    0 => ScaleOperationType::AddNodes,
                    1 => ScaleOperationType::RemoveNodes,
                    _ => {
                        return Err(anyhow!(
                            "Unknown operation type: {}",
                            operation.operation_type
                        ))
                    }
                };

                let nodes = operation
                    .nodes_list
                    .split(',')
                    .map(String::from)
                    .collect::<Vec<String>>();

                // Convert is_candidate from text (CSV) back to Vec<bool>
                let is_cand = if let Some(is_candidate_str) = &operation.is_candidate {
                    // Only relevant for AddNodes operations
                    if op_type == ScaleOperationType::AddNodes {
                        let result = is_candidate_str
                            .split(',')
                            .map(|flag| flag.trim().parse::<bool>().unwrap_or(false))
                            .collect::<Vec<bool>>();
                        Some(result)
                    } else {
                        None
                    }
                } else {
                    None
                };

                info!(
                    "Resumed operation details - type: {:?}, nodes: {:?}, is_candidate: {:?}",
                    op_type, nodes, is_cand
                );

                (event_id.clone(), op_type, nodes, is_cand)
            } else {
                // Generate a new event_id for the scale operation
                let new_event_id = Uuid::new_v4().to_string();

                if !add_nodes.is_empty() {
                    // Check if any nodes already exist in the configuration when adding nodes
                    for node in &add_nodes {
                        if candidate_nodes_before_scale.contains(node) {
                            return Err(anyhow!("Node {} already exists in configuration", node));
                        }
                    }

                    let existing_hosts = config.get_unique_host_list();

                    // Add SSH setup for newly added log nodes
                    let newly_added_hosts = add_nodes
                        .clone()
                        .iter()
                        .map(|host_port| host_port.split(':').next().unwrap_or("").to_string())
                        .filter(|host| !host.is_empty())
                        .filter(|host| !existing_hosts.contains(host))
                        .dedup()
                        .collect::<Vec<String>>();

                    if !newly_added_hosts.is_empty() {
                        // Add SSH setup for new nodes with Python SSH script
                        let ssh_python_bin = config_template(SSH_PYTHON_SCRIPT)?
                            .to_string_lossy()
                            .into_owned();

                        // Merge existing hosts with new hosts
                        let mut all_hosts = config.get_unique_host_list();
                        all_hosts.extend(newly_added_hosts.clone());
                        // Join the hostnames with spaces
                        let host_values = all_hosts.join(" ");

                        // Create SSH setup task for added nodes with --new-nodes flag
                        let ssh_python_task = ExecCustomCommand::build_local_task(
                            format!("python3 {} {}", ssh_python_bin, host_values),
                            config,
                            "ssh setup for new nodes",
                        );

                        barrier.push(ssh_python_task.len());
                        executable.extend(ssh_python_task);
                    }

                    info!("Scaling cluster by adding nodes: {:?}", add_nodes);
                    (
                        new_event_id,
                        ScaleOperationType::AddNodes,
                        add_nodes.clone(),
                        is_candidate.clone(),
                    )
                } else {
                    info!("Scaling cluster by removing nodes: {:?}", remove_nodes);
                    (
                        new_event_id,
                        ScaleOperationType::RemoveNodes,
                        remove_nodes,
                        None,
                    )
                }
            };

        // Derive candidate_nodes_after_scale based on actual operation
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

        let candidate_nodes_that_must_be_valid_in_resume = match operation_type {
            ScaleOperationType::AddNodes => candidate_nodes_before_scale.clone(),
            ScaleOperationType::RemoveNodes => candidate_nodes_before_scale
                .clone()
                .into_iter()
                .filter(|node| !nodes_list.contains(node)) // keep nodes that are not in nodes_list
                .collect::<Vec<String>>(),
        };

        // The beginning of adding tasks
        if let Some(event_id) = &resume {
            let redis_op_task_id = TaskId {
                cmd: "topology".to_string(),
                task: "check-resume-topology".to_string(),
                host: "_local".to_string(),
            };

            info!(
                "Setting up topology task with redis_host_ports: {:?}",
                candidate_nodes_that_must_be_valid_in_resume
            );

            let redis_op_task = RedisOpTask::new(
                redis_op_task_id.clone(),
                candidate_nodes_that_must_be_valid_in_resume.clone(),
                "cluster nodes".to_string(),
                redis_op_tx.clone(),
                password.clone(),
                true, // Skip checkpoint
            );

            let redis_op_instance = TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(redis_op_task),
                task_host: TaskHost::Local,
            };

            barrier.push(1);
            executable.insert(redis_op_task_id, redis_op_instance);

            // Add a task to check the cluster scale status via RPC before proceeding
            let check_status_task_id = TaskId {
                cmd: "validate".to_string(),
                task: "check-resume-status".to_string(),
                host: "_local".to_string(),
            };

            info!(
                "Setting up check-resume-status task with endpoints: {:?}",
                candidate_nodes_that_must_be_valid_in_resume
            );

            let check_status_task = CheckTxClusterScaleStatusTask::new(
                check_status_task_id.clone(),
                event_id.clone(),
                true,
                Some(scale_status_tx.clone()),
                Some(redis_op_rx.clone()),
                None,
            );

            let check_status_instance = TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(check_status_task),
                task_host: TaskHost::Local,
            };

            barrier.push(1);
            executable.insert(check_status_task_id, check_status_instance);

            // If CheckTxClusterScaleStatusTask fails, mark scale operation as failed then abort the operation
            let handle_status_task_id = TaskId {
                cmd: "status-handler".to_string(),
                task: "handle-scale-status".to_string(),
                host: "_local".to_string(),
            };

            // Check if scale_status_rx is NOT_STARTED, if so, mark scale operation as failed then abort the operation(abort logic in DbScaleOpUpdateTask::execute)
            let handle_status_task = DbScaleOpUpdateTask::new_with_status_channel(
                handle_status_task_id.clone(),
                event_id.clone(),
                operation_type.clone() as i32,
                nodes_list.clone(),
                is_candidate.clone(),
                scale_status_rx.clone(),
                cluster_name.clone(),
            );

            let handle_status_instance = TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(handle_status_task),
                task_host: TaskHost::Local,
            };

            barrier.push(1);
            executable.insert(handle_status_task_id, handle_status_instance);
        } else {
            // not resume
            let redis_op_task_id = TaskId {
                cmd: "topology".to_string(),
                task: "topology".to_string(),
                host: "_local".to_string(),
            };

            info!(
                "Setting up topology task with redis_host_ports: {:?}",
                candidate_nodes_before_scale
            );

            let redis_op_task = RedisOpTask::new(
                redis_op_task_id.clone(),
                candidate_nodes_before_scale.clone(),
                "cluster nodes".to_string(),
                redis_op_tx.clone(),
                password.clone(),
                true, // Skip checkpoint
            );

            let redis_op_instance = TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(redis_op_task),
                task_host: TaskHost::Local,
            };

            barrier.push(1);
            executable.insert(redis_op_task_id, redis_op_instance);

            // Add a check for unfinished operations AND log stage 0 if check passes
            let scale_op = STATE_MGR.get_state_operation::<ScaleOperation>(SCALE_STATE);

            // Check for unfinished operations
            let unfinished_ops = scale_op
                .load(move || -> Option<QueryCondition> {
                    Some(QueryCondition {
                        cond_text: "stage = $1".to_string(),
                        bind_values: vec![StateValue::Integer(0)],
                    })
                })
                .await?;

            // If any unfinished operations exist, abort immediately
            if !unfinished_ops.is_empty() {
                assert!(unfinished_ops.len() == 1);
                let unfinished_op = &unfinished_ops[0];
                bail!(
                    "Found unfinished scale operation with event_id: {}. Please resolve or delete this operation before starting a new one, or resume it with --resume: eloqctl scale {} --resume {}",
                    unfinished_op.event_id,
                    cluster_name,
                    unfinished_op.event_id
                );
            }

            // If no unfinished operations, create the task to log stage 0
            let check_unfinished_ops_task_id = TaskId {
                cmd: "scale-status".to_string(),
                task: "log-stage-0".to_string(),
                host: "_local".to_string(),
            };

            let check_unfinished_ops_task = DbScaleOpUpdateTask::new(
                check_unfinished_ops_task_id.clone(),
                scale_event_id.clone(),
                operation_type.clone() as i32,
                nodes_list.clone(),
                is_candidate.clone(),
                0, // Stage 0: scale started
                cluster_name.clone(),
            );

            // Create the task input map
            let task_input = HashMap::default();
            let check_unfinished_ops_instance = TaskInstance {
                task_input,
                task: Box::new(check_unfinished_ops_task),
                task_host: TaskHost::Local,
            };

            barrier.push(1);
            executable.insert(check_unfinished_ops_task_id, check_unfinished_ops_instance);

            // Execute the actual scaling operation (add/remove nodes)
            let scale_task_id = TaskId {
                cmd: "scale".to_string(),
                task: "execute-scale".to_string(),
                host: "_local".to_string(),
            };

            // Take the RedisOpTask result from redis_op_rx(old cluster config) and send the ScaleOpTask result to scale_op_tx(new cluster config)
            let scale_task = ScaleOpTask::new(
                scale_task_id.clone(),
                scale_event_id.clone(),
                operation_type.clone(),
                nodes_list.clone(),
                is_candidate.clone(),
                cluster_name.clone(),
                ng_id,
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
        }

        // Common steps for both new and resumed scale operations

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

        // Create directories only for unique newly added hosts
        info!("Creating directories for unique newly added hosts");

        for host in &new_hosts_to_upload_tarball {
            let mkdir_remote_dir = ExecCustomCommand::build_task_by_host(
                format!(
                    "mkdir -p {}/data/tx_service",
                    deploy_config.deployment.tx_srv_home(),
                ),
                config,
                vec![host.clone()],
                Some(format!("mkdir-{}", host)),
            );

            barrier.push(mkdir_remote_dir.len());
            executable.extend(mkdir_remote_dir);
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

        // // We need to upload to all hosts that will be in the final cluster configuration
        // // For AddNodes: all existing hosts + newly added hosts
        // // For RemoveNodes: all existing hosts except the ones being removed
        // let upload_target_nodes = match operation_type {
        //     ScaleOperationType::AddNodes => {
        //         // For add operations, we need all hosts (existing + new)
        //         let mut all_hosts = candidate_nodes_before_scale.clone();

        //         // Add the new nodes (both candidate and non-candidate)
        //         all_hosts.extend(nodes_list.clone());

        //         info!(
        //             "Uploading configuration to all existing and newly added hosts: {} total nodes",
        //             all_hosts.len()
        //         );
        //         all_hosts
        //     }
        //     ScaleOperationType::RemoveNodes => {
        //         // For remove operations, we need all hosts except those being removed
        //         let nodes_to_remove: std::collections::HashSet<String> =
        //             nodes_list.iter().cloned().collect();

        //         let remaining_hosts: Vec<String> = all_nodes_before_scale
        //             .iter()
        //             .filter(|node| !nodes_to_remove.contains(*node))
        //             .cloned()
        //             .collect();

        //         info!(
        //             "Uploading configuration to remaining hosts after removal: {} total nodes",
        //             remaining_hosts.len()
        //         );
        //         remaining_hosts
        //     }
        // };

        let upload_tasks_map = upload_tasks_with_nodes(
            UploadTaskBuilderType::ScaleTxConf,
            config,
            &operation_type,
            &nodes_list,
            // &upload_target_nodes,
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
                if let Some(tx_image) = deploy_config.deployment.tx_image().split('/').last() {
                    info!(
                        "Preparing to upload TX service tarball to {} new hosts",
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
                    let tarball_path = download_dir.join(product_dir).join(store).join(tx_image);

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
                Some(candidate_nodes_that_must_be_valid_in_resume.clone()),
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
                    password: password.clone(),
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
                "cluster nodes".to_string(),
                redis_op_scaled_tx.clone(),
                password.clone(),
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

        // Update eloqctl database with new scale status
        let update_stage1_task_id = TaskId {
            cmd: "scale-status".to_string(),
            task: "update-stage-1".to_string(),
            host: "_local".to_string(),
        };

        let update_stage1_task = DbScaleOpUpdateTask::new(
            update_stage1_task_id.clone(),
            scale_event_id.clone(),
            operation_type.clone() as i32,
            nodes_list.clone(),
            is_candidate,
            1,
            cluster_name.clone(),
        );
        let update_stage1_instance = TaskInstance {
            task_input: HashMap::default(),
            task: Box::new(update_stage1_task),
            task_host: TaskHost::Local,
        };
        barrier.push(1);
        executable.insert(update_stage1_task_id, update_stage1_instance);

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

        // Get all nodes that should exist after scale operation
        let final_nodes = match operation_type {
            ScaleOperationType::AddNodes => {
                // For add operation, use candidate_nodes_after_scale
                candidate_nodes_after_scale.clone()
            }
            ScaleOperationType::RemoveNodes => {
                // For remove operation, use candidate_nodes_after_scale
                candidate_nodes_after_scale.clone()
            }
        };

        let final_topology_task = RedisOpTask::new(
            final_topology_task_id.clone(),
            final_nodes,
            "cluster nodes".to_string(),
            final_topology_tx.clone(),
            password.clone(),
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

        // Implementation of TODO: Remove directories and files for removed nodes
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
                                "rm -rf {}/rocksdb-{port} {}/data/tx_service/{port} {}/logs/node-{port} {}/EloqKv-node-{port}.ini",
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
