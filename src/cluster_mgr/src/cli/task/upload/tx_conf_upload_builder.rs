use crate::cli::task::group::Config;
use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, UploadTaskBuilder,
};
use crate::cli::{create_upload_cluster_dir, upload_dir};
use crate::config::config_base::UploadFile;
use crate::config::DeploymentPackage;
use indexmap::IndexMap;
use itertools::Itertools;
use std::fs;
use std::path::{Path, PathBuf};
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
        // TODO(ZX) support log-server, kv-store and monitor-server config update
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
                        upload_cnf_files.push(UploadFile {
                            source: file_path.to_string_lossy().to_string(),
                            dest: remote_dest.clone(),
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
