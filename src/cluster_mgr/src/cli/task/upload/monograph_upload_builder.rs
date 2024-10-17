use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, list_files_by_host, UploadTaskBuilder,
};
use crate::config::config_base::{
    DeployConfig, UploadFile, CASSANDRA_COLLECTOR_AGENT_FILE_KEY, CASSANDRA_FILE_KEY,
    GRAFANA_FILE_KEY, MONOGRAPH_FILE_KEY, MONOGRAPH_LOG_FILE_KEY, MYSQL_EXPORTER_FILE_KEY,
    NODE_EXPORTER_FILE_KEY, PROMETHEUS_FILE_KEY,
};
use crate::config::deployment::{Deployment, Product};
use crate::config::storage_service_config::CassKind;
use crate::config::{DeploymentPackage, DownloadUrl};
use indexmap::IndexMap;
use itertools::Itertools;

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
        let mut all_files_path = vec![
            // config.gen_tx_start_script().unwrap(),
            config.gen_bootstrap_db_script().unwrap(),
        ];
        // all_files_path.extend(config.gen_all_monograph_configs().unwrap());
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
        all_hosts_cloned
            .into_iter()
            .map(|host| {
                let source_files = list_files_by_host(&host, &config.deployment).join(" ");
                UploadFile {
                    source: source_files,
                    dest: dest_file.clone(),
                    extension: "bash,cnf".to_string(),
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
    fn build(&self, config: &DeployConfig) -> IndexMap<TaskId, TaskInstance> {
        let mut upload_files = self.build_monograph_misc_upload_file(config);
        let upload_tar_files = self.monograph_tar_upload_file(config);

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
        upload_files
            .into_iter()
            .into_group_map_by(|upload_file| (upload_file.host.clone(), upload_file.dest.clone()))
            .into_iter()
            .map(|((host, dest), upload_files)| {
                let source = upload_files
                    .into_iter()
                    .map(|upload| upload.source.clone())
                    .join(" ");
                UploadFile {
                    source,
                    dest,
                    extension: "bash,cnf,gz".to_string(),
                    host,
                    copy_dir: false,
                }
            })
            .collect_vec()
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
        if let Some(cass) = &config.storage_service.cassandra {
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
        vec![]
    }

    pub fn build_tasks(
        config: &DeployConfig,
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
