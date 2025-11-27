use crate::cli::task::group::Config;
use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, list_files_by_host, UploadTaskBuilder,
};
use crate::cli::{create_upload_cluster_dir, upload_dir};
use crate::config::config_base::{
    DeployConfig, UploadFile, CASSANDRA_COLLECTOR_AGENT_FILE_KEY, CASSANDRA_FILE_KEY,
    GRAFANA_FILE_KEY, MONOGRAPH_FILE_KEY, MONOGRAPH_LOG_FILE_KEY, MYSQL_EXPORTER_FILE_KEY,
    NODE_EXPORTER_FILE_KEY, PROMETHEUS_FILE_KEY,
};
use crate::config::deployment::{Deployment, Product};
use crate::config::storage_service_config::CassKind;
use crate::config::storage_service_config::RocksDB;
use crate::config::{
    config_template, DeploymentPackage, DownloadUrl, ELOQDSS_TEMPLATE_INI, ELOQKV_TEMPLATE_INI,
};
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;
use std::fs;

pub struct MonographUploadBuilder;

// decide which file needs to be uploaded to which host
impl MonographUploadBuilder {
    fn monograph_tar_upload_file(&self, config: &DeployConfig) -> Vec<UploadFile> {
        let tx_hosts = config.get_host_list(DeploymentPackage::MonographTx);
        let standby_hosts = config.get_host_list(DeploymentPackage::MonographStandby);
        let voter_hosts = config.get_host_list(DeploymentPackage::MonographVoter);
        let log_hosts = config.get_host_list(DeploymentPackage::MonographLog);
        let storage_hosts = config.get_host_list(DeploymentPackage::Storage);
        let codis_hosts = config.get_host_list(DeploymentPackage::Codis);
        let install_dir = config.install_dir();
        config
            .deployment
            .all_download_links()
            .unwrap()
            .iter()
            .map(|(file_key, download_url)| {
                let dest_hosts = match file_key.as_str() {
                    MONOGRAPH_FILE_KEY | MYSQL_EXPORTER_FILE_KEY => [
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
                        &codis_hosts.clone()[..],
                    ]
                    .concat(),
                    MONOGRAPH_LOG_FILE_KEY => log_hosts.clone(),
                    CASSANDRA_FILE_KEY | CASSANDRA_COLLECTOR_AGENT_FILE_KEY => {
                        storage_hosts.clone()
                    }
                    PROMETHEUS_FILE_KEY => config.get_host_list(DeploymentPackage::Prometheus),
                    GRAFANA_FILE_KEY => config.get_host_list(DeploymentPackage::Grafana),
                    "codis" => codis_hosts.clone(),
                    _ => unreachable!(),
                };
                (dest_hosts, download_url)
            })
            .filter(|(hosts, _url)| !hosts.is_empty())
            .flat_map(|(hosts, url)| {
                hosts
                    .iter()
                    .map(|host| {
                        let source = format!("{}/{}", url.cache_dir().unwrap(), url.file_name());
                        UploadFile {
                            source,
                            dest: install_dir.clone(),
                            // fixme later
                            extension: "gz".to_string(),
                            host: host.to_string(),
                            copy_dir: false,
                        }
                    })
                    .collect_vec()
            })
            .collect_vec()
    }

    fn build_monograph_misc_upload_file(&self, config: &DeployConfig) -> Vec<UploadFile> {
        let mut all_files_path = vec![config.gen_bootstrap_db_script().unwrap()];
        let log_start_path_opt = config.gen_log_start_script().unwrap();
        if let Some(log_start_path) = log_start_path_opt {
            all_files_path.extend(log_start_path);
        }

        if config.product() == Product::EloqSQL {
            let all_mysql_exporter_conf = config.gen_all_mysql_exporter_config().unwrap();
            if let Some(mysql_exporter_conf) = all_mysql_exporter_conf {
                all_files_path.extend(mysql_exporter_conf);
            }
        }

        let all_db_host = config.get_host_as_map();
        let tx_hosts = all_db_host.get(&DeploymentPackage::MonographTx).unwrap();
        let standby_hosts = all_db_host
            .get(&DeploymentPackage::MonographStandby)
            .unwrap();
        let voter_hosts = all_db_host.get(&DeploymentPackage::MonographVoter).unwrap();

        let mut all_hosts_cloned: Vec<String> = tx_hosts
            .iter()
            .chain(standby_hosts.iter())
            .chain(voter_hosts.iter())
            .cloned()
            .collect();

        if let Some(log_host) = all_db_host.get(&DeploymentPackage::MonographLog) {
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
        }

        all_hosts_cloned
            .into_iter()
            .map(|host| {
                let source_files = list_files_by_host(&host, &config.deployment).join(" ");
                UploadFile {
                    source: source_files,
                    dest: dest_file.clone(),
                    extension: "ini".to_string(),
                    host,
                    copy_dir: false,
                }
            })
            .unique_by(|upload_file| upload_file.source.clone())
            .collect_vec()
    }
}

impl UploadTaskBuilder for MonographUploadBuilder {
    /// Upload installation package, MonographDB configuration file,
    /// MonographDB install script, install config to remote host.
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

            // Get hosts from DataStoreService Remote mode
            if let Some(dss) = &storage.eloqdss {
                if dss.is_remote_mode() {
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

        let mut upload_files = self.build_monograph_misc_upload_file(cluster_config);
        let upload_tar_files = self.monograph_tar_upload_file(cluster_config);

        upload_files.extend(upload_tar_files);

        let final_files = EloqUpload::upload_group_by_dest(upload_files);
        let source_host = get_source_host(None);
        final_files
            .iter()
            .map(|upload_file| {
                let extension = &upload_file.extension;
                let task_name = format!("deploy_monograph_all_{extension}");
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
        // Group the upload files by (host, dest)
        let grouped: HashMap<(String, String), Vec<UploadFile>> = upload_files
            .into_iter()
            .into_group_map_by(|upload| (upload.host.clone(), upload.dest.clone()));

        // Transform each group into a single UploadFile with aggregated fields
        grouped
            .into_iter()
            .map(|((host, dest), group)| {
                // Initialize vectors to collect sources and extensions
                let mut sources = Vec::with_capacity(group.len());
                let mut extensions = Vec::with_capacity(group.len());

                // Iterate once through the group to collect sources and extensions
                for upload in group {
                    sources.push(upload.source);
                    extensions.push(upload.extension);
                }

                // Join the collected sources and extensions
                let aggregated_source = sources.join(" ");
                let aggregated_extension = extensions.join(",");

                // Create a new UploadFile with aggregated data
                UploadFile {
                    source: aggregated_source,
                    dest,
                    extension: aggregated_extension,
                    host,
                    copy_dir: false,
                }
            })
            .collect()
    }

    pub fn eloq_image_upload(config: &Deployment) -> Vec<UploadFile> {
        let install_dir = config.install_dir();
        let img = config.tx_image();
        let url = DownloadUrl::from_url_str(img).unwrap();
        let img_src = format!("{}/{}", url.cache_dir().unwrap(), url.file_name());
        let mut uploads = config
            .tx_service
            .tx_host_ports
            .iter()
            .map(|host_port| {
                let host = host_port.split(':').next().unwrap(); // Extract the host part before the colon
                UploadFile {
                    source: img_src.clone(),
                    dest: install_dir.clone(),
                    extension: "gz".to_string(),
                    host: host.to_string(),
                    copy_dir: false,
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
                        extension: "gz".to_string(),
                        host: host.to_string(),
                        copy_dir: false,
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
                        extension: "gz".to_string(),
                        host: host.to_string(),
                        copy_dir: false,
                    }
                })
                .collect_vec();
            uploads.extend(ups);
        }

        if let Some(srv) = &config.log_service {
            let img = srv.image.as_ref().unwrap();
            let url = DownloadUrl::from_url_str(img).unwrap();
            let img_src = format!("{}/{}", url.cache_dir().unwrap(), url.file_name());
            let ups = srv
                .log_host_unique()
                .iter()
                .map(|host| UploadFile {
                    source: img_src.clone(),
                    dest: install_dir.clone(),
                    extension: "gz".to_string(),
                    host: host.to_string(),
                    copy_dir: false,
                })
                .collect_vec();
            uploads.extend(ups);
            uploads = Self::upload_group_by_dest(uploads);
        }
        uploads
    }

    pub fn cassandra_image_upload(config: &Deployment) -> Vec<UploadFile> {
        let install_dir = config.install_dir();
        if let Some(storage) = &config.storage_service {
            if let Some(cass) = &storage.cassandra {
                if let CassKind::Internal(cdp) = &cass.kind {
                    let url = DownloadUrl::from_url_str(&cdp.image_url()).unwrap();
                    let img_src = format!("{}/{}", url.cache_dir().unwrap(), url.file_name());
                    return cass
                        .host
                        .iter()
                        .map(|host| UploadFile {
                            source: img_src.clone(),
                            dest: install_dir.clone(),
                            extension: "gz".to_string(),
                            host: host.to_string(),
                            copy_dir: false,
                        })
                        .collect_vec();
                }
            }
        }
        vec![]
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
