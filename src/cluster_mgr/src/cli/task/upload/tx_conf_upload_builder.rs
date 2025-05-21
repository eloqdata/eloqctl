use crate::cli::task::group::Config;
use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::task_utils::{ClusterNodesWithConfig, ScaleOperationType};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, UploadTaskBuilder,
};
use crate::cli::{create_upload_cluster_dir, upload_dir};
use crate::config::config_base::{UploadFile, SCALED_CLUSTER_CONFIG};
use crate::config::DeploymentPackage;
use indexmap::IndexMap;
use itertools::Itertools;
use regex::Regex;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::sync::watch;
use tracing::{info, warn};

pub struct TxConfUpload;

impl UploadTaskBuilder for TxConfUpload {
    fn build(&self, config: &Config) -> IndexMap<TaskId, TaskInstance> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => panic!("Expected ClusterConfig for TxConfUpload"),
        };

        info!("Checking for host-specific configuration files in ~/.eloqctl/upload/{}/ for update-conf command", 
              cluster_config.deployment.cluster_name);

        let remote_dest = cluster_config.deployment.tx_srv_home();
        let mut all_hosts = cluster_config.get_host_list(DeploymentPackage::MonographTx);
        all_hosts.extend(cluster_config.get_host_list(DeploymentPackage::MonographStandby));
        all_hosts.extend(cluster_config.get_host_list(DeploymentPackage::MonographVoter));

        let cluster_name = &cluster_config.deployment.cluster_name;
        let mut upload_cnf_files = Vec::new();

        // Check for existing host-specific configuration files first
        for host in &all_hosts {
            let host_dir = format!("{}/{}", cluster_name, host);
            let host_path = create_upload_cluster_dir(&host_dir);

            // Check if directory exists and has .ini files
            if host_path.exists() {
                let host_files: Vec<PathBuf> = fs::read_dir(&host_path)
                    .map(|entries| {
                        entries
                            .filter_map(Result::ok)
                            .filter(|entry| {
                                let path = entry.path();
                                path.is_file() && path.extension().map_or(false, |ext| ext == "ini")
                            })
                            .map(|entry| entry.path())
                            .collect()
                    })
                    .unwrap_or_default();

                if !host_files.is_empty() {
                    info!(
                        "Found existing configuration files for host {}: {:?}",
                        host, host_files
                    );

                    // Use existing host-specific configuration files
                    for file_path in host_files {
                        let file_name = file_path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or_default();

                        // Check if filename matches the pattern and extract port
                        let renamed_file = if let Some(captures) =
                            Regex::new(r"EloqKv-(tx|candidate|voter)-(\d+)\.ini")
                                .ok()
                                .and_then(|re| re.captures(file_name))
                        {
                            let port = captures.get(2).map_or("", |m| m.as_str());
                            format!("EloqKv-node-{}.ini", port)
                        } else {
                            file_name.to_string()
                        };

                        upload_cnf_files.push(UploadFile {
                            source: file_path.to_string_lossy().to_string(),
                            dest: format!("{}/{}", remote_dest, renamed_file),
                            extension: "ini".to_string(),
                            host: host.to_string(),
                            copy_dir: false,
                        });
                    }
                    continue;
                } else {
                    warn!(
                        "No .ini files found in directory for host {}: {}",
                        host,
                        host_path.display()
                    );
                }
            } else {
                warn!(
                    "Directory for host {} does not exist: {}",
                    host,
                    host_path.display()
                );
            }

            // If no host-specific files exist, fall back to the template generation
            // This code will only execute for hosts that don't have custom configs
            warn!("Generating new configuration for host: {}", host);
        }

        // If no host-specific files were found, generate all configs from templates
        if upload_cnf_files.is_empty() {
            info!("No custom host configurations found, generating from templates");
            let all_conf_path = cluster_config
                .gen_all_monograph_configs()
                .expect("Failed generate my_HOST.ini")
                .iter()
                .map(|path_buf| path_buf.to_str().unwrap().to_string())
                .collect_vec();

            upload_cnf_files = all_hosts
                .iter()
                .flat_map(|host| {
                    all_conf_path
                        .iter()
                        .filter(|path| path.contains(host.as_str()))
                        .map(|path| UploadFile {
                            source: path.to_string(),
                            dest: remote_dest.clone(),
                            extension: "ini".to_string(),
                            host: host.to_string(),
                            copy_dir: false,
                        })
                })
                .collect_vec();
        }

        let source_host = get_source_host(None);
        upload_cnf_files
            .iter()
            .map(|upload_file| {
                let host = upload_file.host.clone();
                let source_path = upload_file.source.clone();

                let file_stem_str = Path::new(&source_path)
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .unwrap_or("unknown");

                build_task_instance(
                    source_host.clone(),
                    upload_file.clone(),
                    config,
                    "config-update",
                    format!("upload-ini-{host}-{file_stem_str}").as_str(),
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>()
    }
}

impl TxConfUpload {
    /// Build upload tasks with nodes that are being added or removed
    pub fn build_with_nodes(
        &self,
        config: &Config,
        operation_type: &ScaleOperationType,
        nodes_list: &Vec<String>,
        is_candidate: &Option<Vec<bool>>,
    ) -> IndexMap<TaskId, TaskInstance> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => panic!("Expected ClusterConfig for TxConfUpload"),
        };

        info!(
            "Building upload tasks for scale operation {:?} with nodes: {:?}",
            operation_type, nodes_list
        );

        let remote_dest = cluster_config.deployment.tx_srv_home();
        let mut all_hosts = cluster_config.get_host_list(DeploymentPackage::MonographTx);
        all_hosts.extend(cluster_config.get_host_list(DeploymentPackage::MonographStandby));
        all_hosts.extend(cluster_config.get_host_list(DeploymentPackage::MonographVoter));

        let cluster_name = &cluster_config.deployment.cluster_name;
        let mut upload_cnf_files = Vec::new();

        // Create a set of nodes to be added/removed for efficient lookup
        let nodes_hosts: std::collections::HashSet<String> = nodes_list
            .iter()
            .filter_map(|node| {
                let parts: Vec<&str> = node.split(':').collect();
                if parts.len() == 2 {
                    Some(parts[0].to_string())
                } else {
                    None
                }
            })
            .collect();

        // For each host, determine if we should include it in the upload tasks
        for host in &all_hosts {
            // For AddNodes, include all hosts (both existing and new)
            // For RemoveNodes, skip hosts that are being removed
            let should_skip = match operation_type {
                ScaleOperationType::RemoveNodes => nodes_hosts.contains(host),
                ScaleOperationType::AddNodes => false,
            };

            if should_skip {
                info!("Skipping host {} as it is being removed", host);
                continue;
            }

            let host_dir = format!("{}/{}", cluster_name, host);
            let host_path = create_upload_cluster_dir(&host_dir);

            // Check if directory exists and has .ini files
            if host_path.exists() {
                let host_files: Vec<PathBuf> = fs::read_dir(&host_path)
                    .map(|entries| {
                        entries
                            .filter_map(Result::ok)
                            .filter(|entry| {
                                let path = entry.path();
                                path.is_file() && path.extension().map_or(false, |ext| ext == "ini")
                            })
                            .map(|entry| entry.path())
                            .collect()
                    })
                    .unwrap_or_default();

                if !host_files.is_empty() {
                    info!(
                        "Found existing configuration files for host {}: {:?}",
                        host, host_files
                    );

                    // Use existing host-specific configuration files
                    for file_path in host_files {
                        let file_name = file_path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or_default();

                        // Check if filename matches the pattern and extract port
                        let renamed_file = if let Some(captures) =
                            Regex::new(r"EloqKv-(tx|candidate|voter)-(\d+)\.ini")
                                .ok()
                                .and_then(|re| re.captures(file_name))
                        {
                            let port = captures.get(2).map_or("", |m| m.as_str());
                            format!("EloqKv-node-{}.ini", port)
                        } else {
                            file_name.to_string()
                        };

                        upload_cnf_files.push(UploadFile {
                            source: file_path.to_string_lossy().to_string(),
                            dest: format!("{}/{}", remote_dest, renamed_file),
                            extension: "ini".to_string(),
                            host: host.to_string(),
                            copy_dir: false,
                        });
                    }
                    continue;
                }
            }

            // For hosts with no existing files, we'll generate them in the next step
            warn!(
                "No configuration files found for host {}, will generate",
                host
            );
        }

        // If there are any hosts without configuration files, generate them
        let hosts_with_configs: std::collections::HashSet<String> =
            upload_cnf_files.iter().map(|f| f.host.clone()).collect();

        let hosts_needing_configs: Vec<String> = all_hosts
            .iter()
            .filter(|host| {
                !hosts_with_configs.contains(*host)
                    && match operation_type {
                        ScaleOperationType::RemoveNodes => !nodes_hosts.contains(*host),
                        ScaleOperationType::AddNodes => true,
                    }
            })
            .cloned()
            .collect();

        if !hosts_needing_configs.is_empty() {
            info!(
                "Generating configs for hosts without existing files: {:?}",
                hosts_needing_configs
            );

            let all_conf_path = cluster_config
                .gen_all_monograph_configs()
                .expect("Failed to generate configs")
                .iter()
                .map(|path_buf| path_buf.to_str().unwrap().to_string())
                .collect_vec();

            for host in hosts_needing_configs {
                for path in &all_conf_path {
                    if path.contains(&host) {
                        // Get the file name and check if it matches the pattern
                        let file_name = Path::new(path)
                            .file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or_default();

                        // Check if filename matches the pattern and extract port
                        let renamed_file = if let Some(captures) =
                            Regex::new(r"EloqKv-(tx|candidate|voter)-(\d+)\.ini")
                                .ok()
                                .and_then(|re| re.captures(file_name))
                        {
                            let port = captures.get(2).map_or("", |m| m.as_str());
                            format!("EloqKv-node-{}.ini", port)
                        } else {
                            file_name.to_string()
                        };

                        upload_cnf_files.push(UploadFile {
                            source: path.to_string(),
                            dest: format!("{}/{}", remote_dest, renamed_file),
                            extension: "ini".to_string(),
                            host: host.clone(),
                            copy_dir: false,
                        });
                    }
                }
            }
        }

        // Create upload tasks for each file
        let source_host = get_source_host(None);
        let mut result = upload_cnf_files
            .iter()
            .map(|upload_file| {
                let host = upload_file.host.clone();
                let source_path = upload_file.source.clone();

                let file_stem_str = Path::new(&source_path)
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .unwrap_or("unknown");

                let task_name = match operation_type {
                    ScaleOperationType::AddNodes => {
                        format!("upload-ini-add-{host}-{file_stem_str}")
                    }
                    ScaleOperationType::RemoveNodes => {
                        format!("upload-ini-remove-{host}-{file_stem_str}")
                    }
                };

                build_task_instance(
                    source_host.clone(),
                    upload_file.clone(),
                    config,
                    "config-update",
                    task_name.as_str(),
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>();

        // For AddNodes operation, also add upload tasks for the newly added nodes
        if let ScaleOperationType::AddNodes = operation_type {
            let upload_base = crate::cli::upload_dir()
                .join(cluster_name)
                .to_string_lossy()
                .to_string();

            // Validate that the nodes_list and is_candidate list have the same size
            if let Some(candidate_list) = is_candidate {
                if nodes_list.len() != candidate_list.len() {
                    panic!(
                        "Mismatch between nodes list size ({}) and candidate status list size ({})",
                        nodes_list.len(),
                        candidate_list.len()
                    );
                }
            } else {
                panic!("Candidate status list is required for node addition");
            }

            for (i, node) in nodes_list.iter().enumerate() {
                let parts: Vec<&str> = node.split(':').collect();
                if parts.len() != 2 {
                    continue;
                }
                let host = parts[0].to_string();
                let port = parts[1];

                // Get the candidate status for this node from the is_candidate parameter
                let node_is_candidate = match is_candidate {
                    Some(candidate_list) => candidate_list[i],
                    _ => unreachable!(),
                };

                // Choose the right config file type based on is_candidate
                let config_type = if node_is_candidate {
                    "candidate"
                } else {
                    "voter"
                };

                let source_path = format!("{}/{}/EloqKv-node-{}.ini", upload_base, host, port);
                info!(
                    "Using configuration file: {} for node {}:{} (is_candidate: {})",
                    source_path, host, port, node_is_candidate
                );

                // Rename to the new format
                let dest_filename = format!("EloqKv-node-{}.ini", port);

                let upload_file = UploadFile {
                    source: source_path,
                    dest: format!("{}/{}", remote_dest, dest_filename),
                    extension: "ini".to_string(),
                    host: host.clone(),
                    copy_dir: false,
                };

                let (id, instance) = build_task_instance(
                    source_host.clone(),
                    upload_file,
                    config,
                    "upload",
                    format!("upload-{}-node-in-{}-{}", config_type, host, port).as_str(),
                );

                result.insert(id, instance);
            }
        }

        result
    }

    /// Build upload tasks for the cluster configuration
    pub fn build_cluster_config_upload_tasks(
        config: &Config,
        operation_type: &ScaleOperationType,
        nodes_list: &Vec<String>,
        scale_op_rx: &watch::Receiver<ClusterNodesWithConfig>,
    ) -> IndexMap<TaskId, TaskInstance> {
        // Only create upload tasks for AddNodes operations
        if let ScaleOperationType::AddNodes = operation_type {
            // Check if we have any cluster config in the receiver (mainly for validation)
            let nodes_with_config = scale_op_rx.borrow().clone();
            if nodes_with_config.cluster_config.is_none() {
                info!("No cluster configuration available in the receiver yet");
            }

            let deploy_config = match config {
                Config::Cluster(cfg) => cfg,
                _ => {
                    warn!("Expected ClusterConfig for cluster config upload");
                    return IndexMap::new();
                }
            };

            // Check if nodes_list is empty
            if nodes_list.is_empty() {
                info!("No target hosts found for uploading cluster configuration");
                return IndexMap::new();
            }

            // Use relative path instead of placeholder as we know the file location now
            let config_file_path = upload_dir()
                .join(deploy_config.deployment.cluster_name.clone())
                .join(SCALED_CLUSTER_CONFIG);

            // Create upload tasks for each target host
            let mut result = IndexMap::new();
            let source_host = get_source_host(None);

            // Extract unique hosts from the nodes_list
            let unique_new_hosts: Vec<String> = nodes_list
                .iter()
                .filter_map(|node| node.split(':').next().map(|h| h.to_string()))
                .unique()
                .collect();

            info!(
                "Uploading cluster config to {} unique hosts",
                unique_new_hosts.len()
            );

            for host in unique_new_hosts {
                let upload_file = UploadFile {
                    source: config_file_path.to_string_lossy().to_string(),
                    dest: format!(
                        "{}/data/tx_service/{}",
                        deploy_config.deployment.tx_srv_home(),
                        SCALED_CLUSTER_CONFIG
                    ),
                    extension: "".to_string(), // No extension for this file
                    host: host.clone(),
                    copy_dir: false,
                };

                let (task_id, task_instance) = build_task_instance(
                    source_host.clone(),
                    upload_file,
                    config,
                    "upload-cluster-config",
                    &format!("cluster-config-{}", host),
                );

                result.insert(task_id, task_instance);
                info!("Added upload task for cluster config to host {}", host);
            }

            info!(
                "Created {} upload tasks for cluster configuration",
                result.len()
            );
            return result;
        }

        // Return empty map for non-AddNodes operations
        info!(
            "No cluster config to upload for operation {:?}",
            operation_type
        );
        IndexMap::new()
    }
}
