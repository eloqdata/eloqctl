use crate::cli::task::group::Config;
use crate::cli::task::local_extract_task::LocalExtractTask;
use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, list_files_by_host, UploadTaskBuilder,
};
use crate::cli::{create_upload_cluster_dir, upload_dir};
use crate::config::config_base::{
    DeployConfig, UploadFile, ELOQ_FILE_KEY, ELOQ_LOG_FILE_KEY, GRAFANA_FILE_KEY,
    NODE_EXPORTER_FILE_KEY, PROMETHEUS_FILE_KEY,
};
use crate::config::deployment::Deployment;
use crate::config::storage_service_config::RocksDB;
use crate::config::{
    config_template, DeploymentPackage, DownloadUrl, ELOQDSS_TEMPLATE_INI, ELOQKV_TEMPLATE_INI,
};
use indexmap::IndexMap;
use itertools::Itertools;
use std::fs;
use std::path::Path;

pub struct EloqUploadBuilder;

// decide which file needs to be uploaded to which host
impl EloqUploadBuilder {
    fn eloq_tar_upload_file(&self, config: &DeployConfig) -> Vec<UploadFile> {
        let tx_hosts = config.get_host_list(DeploymentPackage::EloqTx);
        let standby_hosts = config.get_host_list(DeploymentPackage::EloqStandby);
        let voter_hosts = config.get_host_list(DeploymentPackage::EloqVoter);
        let log_hosts = config.get_host_list(DeploymentPackage::EloqLog);
        let storage_hosts = config.get_host_list(DeploymentPackage::Storage);
        let install_dir = config.install_dir();
        config
            .deployment
            .all_download_links()
            .unwrap()
            .iter()
            .map(|(file_key, download_url)| {
                let dest_hosts = match file_key.as_str() {
                    ELOQ_FILE_KEY => [
                        &tx_hosts.clone()[..],
                        &standby_hosts.clone()[..],
                        &voter_hosts.clone()[..],
                    ]
                    .concat(),
                    NODE_EXPORTER_FILE_KEY => [
                        &tx_hosts.clone()[..],
                        &standby_hosts.clone()[..],
                        &voter_hosts.clone()[..],
                        &log_hosts.clone()[..],
                        &storage_hosts.clone()[..],
                    ]
                    .concat(),
                    ELOQ_LOG_FILE_KEY => log_hosts.clone(),
                    PROMETHEUS_FILE_KEY => config.get_host_list(DeploymentPackage::Prometheus),
                    GRAFANA_FILE_KEY => config.get_host_list(DeploymentPackage::Grafana),
                    _ => unreachable!(),
                };
                (dest_hosts, download_url, file_key.clone())
            })
            .filter(|(hosts, _url, _file_key)| !hosts.is_empty())
            .flat_map(|(hosts, url, file_key)| {
                hosts
                    .iter()
                    .map(|host| {
                        let remote_home = Self::remote_home_for_key(config, &file_key);
                        let source = LocalExtractTask::staged_dir_for(url, &remote_home)
                            .to_string_lossy()
                            .to_string();
                        UploadFile {
                            source,
                            dest: format!("{}/{}", install_dir, remote_home),
                            extension: "dir".to_string(),
                            host: host.to_string(),
                            copy_dir: true,
                            delete_remote: true,
                        }
                    })
                    .collect_vec()
            })
            .collect_vec()
    }

    fn remote_home_for_key(config: &DeployConfig, file_key: &str) -> String {
        match file_key {
            ELOQ_FILE_KEY => config.deployment.product().home().to_string(),
            ELOQ_LOG_FILE_KEY => "LogServer".to_string(),
            PROMETHEUS_FILE_KEY => "prometheus".to_string(),
            GRAFANA_FILE_KEY => "grafana".to_string(),
            NODE_EXPORTER_FILE_KEY => "node_exporter".to_string(),
            _ => unreachable!(),
        }
    }

    fn build_eloq_misc_upload_file(&self, config: &DeployConfig) -> Vec<UploadFile> {
        let mut all_files_path = Vec::new();
        let log_start_path_opt = config.gen_log_start_script().unwrap();
        if let Some(log_start_path) = log_start_path_opt {
            all_files_path.extend(log_start_path);
        }

        let all_db_host = config.get_host_as_map();
        let tx_hosts = all_db_host.get(&DeploymentPackage::EloqTx).unwrap();
        let standby_hosts = all_db_host.get(&DeploymentPackage::EloqStandby).unwrap();
        let voter_hosts = all_db_host.get(&DeploymentPackage::EloqVoter).unwrap();

        let mut all_hosts_cloned: Vec<String> = tx_hosts
            .iter()
            .chain(standby_hosts.iter())
            .chain(voter_hosts.iter())
            .cloned()
            .collect();

        if let Some(log_host) = all_db_host.get(&DeploymentPackage::EloqLog) {
            all_hosts_cloned.extend(log_host.clone());
        }
        let dest_file = config.install_dir();

        // Include DSS ini files if generated
        if let Some(storage) = &config.deployment.storage_service {
            if matches!(storage.rocksdb, Some(RocksDB::EloqDssRocksdb(_))) {
                // For each tx host, scan its upload dir for EloqDss-*.ini
                for host in &all_hosts_cloned {
                    let host_dir = create_upload_cluster_dir(&format!(
                        "{}/{}",
                        config.deployment.cluster_name, host
                    ));
                    if let Ok(entries) = std::fs::read_dir(&host_dir) {
                        for entry in entries.flatten() {
                            let path = entry.path();
                            if path.is_file() {
                                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                    if name.starts_with("EloqDss-") && name.ends_with(".ini") {
                                        all_files_path.push(path.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // For DataStoreService Remote mode: only include ini files if not external
            if let Some(dss) = &storage.eloqdss {
                if dss.is_remote_mode() && !dss.is_external() {
                    // For each tx host, scan its upload dir for EloqDss-*.ini
                    for host in &all_hosts_cloned {
                        let host_dir = create_upload_cluster_dir(&format!(
                            "{}/{}",
                            config.deployment.cluster_name, host
                        ));
                        if let Ok(entries) = std::fs::read_dir(&host_dir) {
                            for entry in entries.flatten() {
                                let path = entry.path();
                                if path.is_file() {
                                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                        if name.starts_with("EloqDss-") && name.ends_with(".ini") {
                                            all_files_path.push(path.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let _ = all_files_path;

        all_hosts_cloned
            .into_iter()
            .flat_map(|host| {
                let dest_file = dest_file.clone();
                list_files_by_host(&host, &config.deployment)
                    .into_iter()
                    .map(move |source| {
                        let extension = Path::new(&source)
                            .extension()
                            .and_then(|ext| ext.to_str())
                            .unwrap_or("upload")
                            .to_string();
                        UploadFile {
                            source,
                            dest: dest_file.clone(),
                            extension,
                            host: host.clone(),
                            copy_dir: false,
                            delete_remote: false,
                        }
                    })
                    .collect_vec()
            })
            .unique()
            .collect_vec()
    }
}

impl UploadTaskBuilder for EloqUploadBuilder {
    /// Upload installation package, EloqDB configuration file,
    /// EloqDB install script, install config to remote host.
    fn build(&self, config: &Config) -> IndexMap<TaskId, TaskInstance> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => panic!("Expected ClusterConfig for TxConfUpload"),
        };

        // copy EloqKv.ini from ~/.eloqctl/config to ~/.eloqctl/upload/{cluster_name}
        let config_template_source =
            config_template(ELOQKV_TEMPLATE_INI).expect("get config template error");
        let config_template_dest = upload_dir()
            .join(cluster_config.deployment.cluster_name.clone())
            .join(ELOQKV_TEMPLATE_INI);
        create_upload_cluster_dir(&cluster_config.deployment.cluster_name);
        fs::copy(&config_template_source, &config_template_dest)
            .expect("copy config template error");

        // If DSS is enabled, ensure EloqDss.ini template is present under upload/{cluster}
        // and then generate per-host DSS ini files from template and values.
        if let Some(storage) = &cluster_config.deployment.storage_service {
            // Collect DSS hosts from both EloqDssRocksdb and DataStoreService Remote mode
            let mut dss_hosts = Vec::new();

            // Get hosts from EloqDssRocksdb
            if let Some(RocksDB::EloqDssRocksdb(dss_cfg)) = &storage.rocksdb {
                dss_hosts.extend(dss_cfg.peer_host_ports.clone());
            }

            // Get hosts from DataStoreService Remote mode (only if not external)
            if let Some(dss) = &storage.eloqdss {
                if dss.is_remote_mode() && !dss.is_external() {
                    if let Some(peer_ports) = dss.peer_host_ports.clone() {
                        dss_hosts.extend(peer_ports);
                    }
                }
            }

            if !dss_hosts.is_empty() {
                let dss_template_src =
                    config_template(ELOQDSS_TEMPLATE_INI).expect("get DSS template error");
                let dss_template_dest = upload_dir()
                    .join(cluster_config.deployment.cluster_name.clone())
                    .join(ELOQDSS_TEMPLATE_INI);
                // Best-effort copy; ignore error if already exists
                let _ = fs::copy(&dss_template_src, &dss_template_dest);

                for hp in dss_hosts {
                    let parts: Vec<&str> = hp.split(':').collect();
                    if parts.len() != 2 {
                        continue;
                    }
                    let host = parts[0].to_string();
                    let port = parts[1].to_string();
                    // Build DSS ini content using deployment helper
                    let ini = cluster_config
                        .deployment
                        .build_dss_config(host.clone(), port.clone())
                        .expect("build dss ini failed");
                    // Write into ~/.eloqctl/upload/{cluster}/{host}/EloqDss-<port>.ini
                    let dir = format!("{}/{}", cluster_config.deployment.cluster_name, host);
                    let dest_path =
                        create_upload_cluster_dir(&dir).join(format!("EloqDss-{}.ini", port));
                    let _ = ini.write(dest_path.as_path());
                }
            }
        }

        let mut upload_files = self.build_eloq_misc_upload_file(cluster_config);
        let upload_tar_files = self.eloq_tar_upload_file(cluster_config);

        upload_files.extend(upload_tar_files);

        let final_files = EloqUpload::upload_group_by_dest(upload_files);
        let source_host = get_source_host(None);
        final_files
            .iter()
            .map(|upload_file| {
                let file_name = Path::new(&upload_file.source)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("upload");
                let task_name = format!("deploy_eloq_all_{}", file_name.replace('.', "_"));
                build_task_instance(
                    source_host.clone(),
                    upload_file.clone(),
                    config,
                    "deploy",
                    task_name.as_str(),
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>()
    }
}

pub struct EloqUpload;

impl EloqUpload {
    fn upload_group_by_dest(upload_files: Vec<UploadFile>) -> Vec<UploadFile> {
        upload_files.into_iter().unique().collect_vec()
    }

    pub fn eloq_image_upload(config: &Deployment) -> Vec<UploadFile> {
        let img = config.tx_image();
        let url = DownloadUrl::from_url_str(img).unwrap();
        let img_src = LocalExtractTask::staged_dir_for(&url, config.product().home())
            .to_string_lossy()
            .to_string();
        let install_dir = format!("{}/{}", config.install_dir(), config.product().home());
        let mut uploads = config
            .tx_service
            .tx_host_ports
            .iter()
            .map(|host_port| {
                let host = host_port.split(':').next().unwrap(); // Extract the host part before the colon
                UploadFile {
                    source: img_src.clone(),
                    dest: install_dir.clone(),
                    extension: "dir".to_string(),
                    host: host.to_string(),
                    copy_dir: true,
                    delete_remote: false,
                }
            })
            .collect_vec();

        if let Some(standby_host_ports) = &config.tx_service.standby_host_ports {
            let ups = standby_host_ports
                .iter()
                .map(|host_port| {
                    let host = host_port.split(':').next().unwrap();
                    UploadFile {
                        source: img_src.clone(),
                        dest: install_dir.clone(),
                        extension: "dir".to_string(),
                        host: host.to_string(),
                        copy_dir: true,
                        delete_remote: false,
                    }
                })
                .collect_vec();
            uploads.extend(ups);
        }

        if let Some(voter_host_ports) = &config.tx_service.voter_host_ports {
            let ups = voter_host_ports
                .iter()
                .map(|host_port| {
                    let host = host_port.split(':').next().unwrap();
                    UploadFile {
                        source: img_src.clone(),
                        dest: install_dir.clone(),
                        extension: "dir".to_string(),
                        host: host.to_string(),
                        copy_dir: true,
                        delete_remote: false,
                    }
                })
                .collect_vec();
            uploads.extend(ups);
        }

        if let Some(srv) = &config.log_service {
            let img = srv.image.as_ref().unwrap();
            let url = DownloadUrl::from_url_str(img).unwrap();
            let img_src = LocalExtractTask::staged_dir_for(&url, "LogServer")
                .to_string_lossy()
                .to_string();
            let log_install_dir = format!("{}/LogServer", config.install_dir());
            let ups = srv
                .log_host_unique()
                .iter()
                .map(|host| UploadFile {
                    source: img_src.clone(),
                    dest: log_install_dir.clone(),
                    extension: "dir".to_string(),
                    host: host.to_string(),
                    copy_dir: true,
                    delete_remote: false,
                })
                .collect_vec();
            uploads.extend(ups);
            uploads = Self::upload_group_by_dest(uploads);
        }
        uploads
    }
    pub fn build_tasks(
        config: &Config,
        cmd: &str,
        task: &str,
        uploads: Vec<UploadFile>,
    ) -> IndexMap<TaskId, TaskInstance> {
        let src_host = get_source_host(None);
        uploads
            .into_iter()
            .map(|upload_file| {
                build_task_instance(src_host.clone(), upload_file, config, cmd, task)
            })
            .collect::<IndexMap<TaskId, TaskInstance>>()
    }
}
