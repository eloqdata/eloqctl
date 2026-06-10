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
use once_cell::sync::Lazy;
use regex::Regex;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::sync::watch;
use tracing::{info, warn};

pub struct TxConfUpload;

static TX_INI_RENAME_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"EloqKv-(tx|candidate|voter)-(\d+)\.ini").unwrap());

fn renamed_node_ini(file_name: &str) -> String {
    if let Some(captures) = TX_INI_RENAME_RE.captures(file_name) {
        let port = captures.get(2).map_or("", |m| m.as_str());
        format!("EloqKv-node-{port}.ini")
    } else {
        file_name.to_string()
    }
}

fn task_file_id(source_path: &str) -> String {
    Path::new(source_path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown")
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

impl UploadTaskBuilder for TxConfUpload {
    fn build(&self, config: &Config) -> IndexMap<TaskId, TaskInstance> {
        let Config::Cluster(cluster_config) = config;

        info!("Checking for host-specific configuration files in ~/.eloqctl/upload/{}/ for update-conf command", 
              cluster_config.deployment.cluster_name);

        let remote_dest = cluster_config.deployment.tx_srv_home();
        let mut all_hosts = cluster_config.get_host_list(DeploymentPackage::EloqTx);
        all_hosts.extend(cluster_config.get_host_list(DeploymentPackage::EloqStandby));
        all_hosts.extend(cluster_config.get_host_list(DeploymentPackage::EloqVoter));

        let cluster_name = &cluster_config.deployment.cluster_name;
        let mut upload_cnf_files = Vec::new();
        // Collect DSS ini uploads separately so TX generation decision ignores them
        let mut dss_upload_files: Vec<UploadFile> = Vec::new();
        let mut found_any_tx_ini = false;

        // Check for existing host-specific configuration files first
        for host in &all_hosts {
            let host_dir = format!("{}/{}", cluster_name, host);
            let host_path = create_upload_cluster_dir(&host_dir);

            // Check if directory exists and has relevant .ini files (EloqKv-* only)
            if host_path.exists() {
                let host_files: Vec<PathBuf> = fs::read_dir(&host_path)
                    .map(|entries| {
                        entries
                            .filter_map(Result::ok)
                            .filter(|entry| {
                                let path = entry.path();
                                path.is_file()
                                    && path.extension().is_some_and(|ext| ext == "ini")
                                    && path
                                        .file_name()
                                        .and_then(|n| n.to_str())
                                        .map(|name| name.starts_with("EloqKv-"))
                                        .unwrap_or(false)
                            })
                            .map(|entry| entry.path())
                            .collect()
                    })
                    .unwrap_or_default();

                if !host_files.is_empty() {
                    found_any_tx_ini = true;
                    info!(
                        "Found existing configuration files for host {}: {:?}",
                        host, host_files
                    );

                    // Use existing host-specific configuration files (tx ini only)
                    for file_path in host_files {
                        let file_name = file_path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or_default();

                        // Check if filename matches the pattern and extract port
                        let renamed_file = renamed_node_ini(file_name);

                        upload_cnf_files.push(UploadFile {
                            source: file_path.to_string_lossy().to_string(),
                            dest: format!("{}/{}", remote_dest, renamed_file),
                            extension: "ini".to_string(),
                            host: host.to_string(),
                            copy_dir: false,
                            delete_remote: false,
                        });
                    }

                    // Also collect DSS ini if present for this host (does not affect TX generation logic)
                    let dss_files: Vec<PathBuf> = fs::read_dir(&host_path)
                        .map(|entries| {
                            entries
                                .filter_map(Result::ok)
                                .filter(|entry| {
                                    let path = entry.path();
                                    path.is_file()
                                        && path.extension().is_some_and(|ext| ext == "ini")
                                        && path
                                            .file_name()
                                            .and_then(|n| n.to_str())
                                            .map(|name| name.starts_with("EloqDss-"))
                                            .unwrap_or(false)
                                })
                                .map(|entry| entry.path())
                                .collect()
                        })
                        .unwrap_or_default();
                    for file_path in dss_files {
                        let file_name = file_path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or_default()
                            .to_string();
                        dss_upload_files.push(UploadFile {
                            source: file_path.to_string_lossy().to_string(),
                            dest: format!("{}/{}", remote_dest, file_name),
                            extension: "ini".to_string(),
                            host: host.to_string(),
                            copy_dir: false,
                            delete_remote: false,
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

            // If no host-specific tx files exist, fall back to the template generation
            // This code will only execute for hosts that don't have custom configs
            warn!("Generating new configuration for host: {}", host);

            // Still collect DSS ini if present
            if host_path.exists() {
                let dss_files: Vec<PathBuf> = fs::read_dir(&host_path)
                    .map(|entries| {
                        entries
                            .filter_map(Result::ok)
                            .filter(|entry| {
                                let path = entry.path();
                                path.is_file()
                                    && path.extension().is_some_and(|ext| ext == "ini")
                                    && path
                                        .file_name()
                                        .and_then(|n| n.to_str())
                                        .map(|name| name.starts_with("EloqDss-"))
                                        .unwrap_or(false)
                            })
                            .map(|entry| entry.path())
                            .collect()
                    })
                    .unwrap_or_default();
                for file_path in dss_files {
                    let file_name = file_path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or_default()
                        .to_string();
                    dss_upload_files.push(UploadFile {
                        source: file_path.to_string_lossy().to_string(),
                        dest: format!("{}/{}", remote_dest, file_name),
                        extension: "ini".to_string(),
                        host: host.to_string(),
                        copy_dir: false,
                        delete_remote: false,
                    });
                }
            }
        }

        // If no host-specific TX files were found, generate all TX configs from templates
        if !found_any_tx_ini {
            info!("No custom host configurations found, generating from templates");
            let all_conf_path = cluster_config
                .gen_all_eloq_configs()
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
                            delete_remote: false,
                        })
                })
                .collect_vec();
        }

        // Append any collected DSS uploads
        upload_cnf_files.extend(dss_upload_files);
        upload_cnf_files.extend(Self::collect_tls_upload_files(cluster_config, &all_hosts));

        let source_host = get_source_host(None);
        upload_cnf_files
            .iter()
            .map(|upload_file| {
                let host = upload_file.host.clone();
                let source_path = upload_file.source.clone();
                let file_id = task_file_id(&source_path);

                build_task_instance(
                    source_host.clone(),
                    upload_file.clone(),
                    config,
                    "config-update",
                    format!("upload-ini-{host}-{file_id}").as_str(),
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>()
    }
}

impl TxConfUpload {
    fn collect_tls_upload_files(
        cluster_config: &crate::config::config_base::DeployConfig,
        all_hosts: &[String],
    ) -> Vec<UploadFile> {
        if !cluster_config.deployment.tls_enabled() {
            return Vec::new();
        }

        cluster_config
            .deployment
            .ensure_tls_certs_for_all_kv_nodes()
            .expect("failed to auto-generate TLS certificates");

        let cert_dest_dir = cluster_config.deployment.tls_cert_install_dir();
        all_hosts
            .iter()
            .flat_map(|host| {
                let host_dir = format!("{}/{}", cluster_config.deployment.cluster_name, host);
                let host_path = create_upload_cluster_dir(&host_dir);
                fs::read_dir(&host_path)
                    .map(|entries| {
                        entries
                            .filter_map(Result::ok)
                            .filter_map(|entry| {
                                let path = entry.path();
                                if !path.is_file() {
                                    return None;
                                }
                                let file_name =
                                    path.file_name().and_then(|n| n.to_str())?.to_string();
                                let is_tls_file = file_name.starts_with("eloqkv-tls-")
                                    && (file_name.ends_with(".crt") || file_name.ends_with(".key"));
                                if !is_tls_file {
                                    return None;
                                }
                                Some(UploadFile {
                                    source: path.to_string_lossy().to_string(),
                                    dest: format!("{}/{}", cert_dest_dir, file_name),
                                    extension: "tls".to_string(),
                                    host: host.to_string(),
                                    copy_dir: false,
                                    delete_remote: false,
                                })
                            })
                            .collect::<Vec<UploadFile>>()
                    })
                    .unwrap_or_else(|_| {
                        warn!(
                            "Failed to read host TLS cert dir: {}",
                            host_path.to_string_lossy()
                        );
                        Vec::new()
                    })
            })
            .collect_vec()
    }

    /// Build upload tasks with nodes that are being added or removed
    pub fn build_with_nodes(
        &self,
        config: &Config,
        operation_type: &ScaleOperationType,
        nodes_list: &Vec<String>,
        is_candidate: &Option<Vec<bool>>,
    ) -> IndexMap<TaskId, TaskInstance> {
        let Config::Cluster(cluster_config) = config;

        info!(
            "Building upload tasks for scale operation {:?} with nodes: {:?}",
            operation_type, nodes_list
        );

        let remote_dest = cluster_config.deployment.tx_srv_home();
        let mut all_hosts = cluster_config.get_host_list(DeploymentPackage::EloqTx);
        all_hosts.extend(cluster_config.get_host_list(DeploymentPackage::EloqStandby));
        all_hosts.extend(cluster_config.get_host_list(DeploymentPackage::EloqVoter));

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

            // Check if directory exists and has relevant .ini files (EloqKv-* only)
            if host_path.exists() {
                let host_files: Vec<PathBuf> = fs::read_dir(&host_path)
                    .map(|entries| {
                        entries
                            .filter_map(Result::ok)
                            .filter(|entry| {
                                let path = entry.path();
                                path.is_file()
                                    && path.extension().is_some_and(|ext| ext == "ini")
                                    && path
                                        .file_name()
                                        .and_then(|n| n.to_str())
                                        .map(|name| name.starts_with("EloqKv-"))
                                        .unwrap_or(false)
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
                        let renamed_file = renamed_node_ini(file_name);

                        upload_cnf_files.push(UploadFile {
                            source: file_path.to_string_lossy().to_string(),
                            dest: format!("{}/{}", remote_dest, renamed_file),
                            extension: "ini".to_string(),
                            host: host.to_string(),
                            copy_dir: false,
                            delete_remote: false,
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
                .gen_all_eloq_configs()
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
                        let renamed_file = renamed_node_ini(file_name);

                        upload_cnf_files.push(UploadFile {
                            source: path.to_string(),
                            dest: format!("{}/{}", remote_dest, renamed_file),
                            extension: "ini".to_string(),
                            host: host.clone(),
                            copy_dir: false,
                            delete_remote: false,
                        });
                    }
                }
            }
        }

        upload_cnf_files.extend(Self::collect_tls_upload_files(cluster_config, &all_hosts));

        // Create upload tasks for each file
        let source_host = get_source_host(None);
        let mut result = upload_cnf_files
            .iter()
            .map(|upload_file| {
                let host = upload_file.host.clone();
                let source_path = upload_file.source.clone();
                let file_id = task_file_id(&source_path);

                let task_name = match operation_type {
                    ScaleOperationType::AddNodes => {
                        format!("upload-ini-add-{host}-{file_id}")
                    }
                    ScaleOperationType::RemoveNodes => {
                        format!("upload-ini-remove-{host}-{file_id}")
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
                    None => unreachable!(),
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
                    delete_remote: false,
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

            let Config::Cluster(deploy_config) = config;

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

            for host_port in nodes_list {
                let parts: Vec<&str> = host_port.split(':').collect();
                if parts.len() != 2 {
                    warn!("Invalid node format: {}, expected host:port", host_port);
                    continue;
                }

                let host = parts[0].to_string();
                let port = parts[1];

                let upload_file = UploadFile {
                    source: config_file_path.to_string_lossy().to_string(),
                    dest: format!(
                        "{}/data/port-{}/tx_service/{}",
                        deploy_config.deployment.tx_srv_home(),
                        port,
                        SCALED_CLUSTER_CONFIG
                    ),
                    extension: "".to_string(), // No extension for this file
                    host: host.clone(),
                    copy_dir: false,
                    delete_remote: false,
                };

                let (task_id, task_instance) = build_task_instance(
                    source_host.clone(),
                    upload_file,
                    config,
                    "upload-cluster-config",
                    &format!("cluster-config-{}-{}", host, port),
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

#[cfg(test)]
mod tests {
    use super::task_file_id;

    #[test]
    fn task_file_id_distinguishes_tls_crt_and_key() {
        let crt = "/tmp/eloqkv-tls-eloqkv-dev-sg-002_ai-transsion_com-6379.crt";
        let key = "/tmp/eloqkv-tls-eloqkv-dev-sg-002_ai-transsion_com-6379.key";
        assert_ne!(task_file_id(crt), task_file_id(key));
    }

    #[test]
    fn task_file_id_sanitizes_special_chars() {
        let p = "/tmp/a.b-c_d@e.key";
        assert_eq!(task_file_id(p), "a_b-c_d_e_key");
    }
}
