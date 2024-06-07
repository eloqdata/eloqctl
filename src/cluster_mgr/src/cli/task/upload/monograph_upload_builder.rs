use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, list_files_by_host, UploadTaskBuilder,
};
use crate::config::config_base::{
    DeploymentConfig, UploadFile, CASSANDRA_COLLECTOR_AGENT_FILE_KEY, CASSANDRA_FILE_KEY,
    GRAFANA_FILE_KEY, MONOGRAPH_FILE_KEY, MONOGRAPH_LOG_FILE_KEY, MYSQL_EXPORTER_FILE_KEY,
    NODE_EXPORTER_FILE_KEY, PROMETHEUS_FILE_KEY,
};
use crate::config::deployment::Product;
use crate::config::DeploymentPackage;
use indexmap::IndexMap;
use itertools::Itertools;

pub struct MonographUploadBuilder;

impl MonographUploadBuilder {
    fn monograph_tar_upload_file(&self, config: &DeploymentConfig) -> Vec<UploadFile> {
        let tx_hosts = config.get_host_list(DeploymentPackage::MonographTx);
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
                    MONOGRAPH_FILE_KEY | MYSQL_EXPORTER_FILE_KEY => tx_hosts.clone(),
                    NODE_EXPORTER_FILE_KEY => [
                        &tx_hosts.clone()[..],
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

    fn build_monograph_misc_upload_file(&self, config: &DeploymentConfig) -> Vec<UploadFile> {
        let mut all_files_path = vec![
            // config.gen_tx_start_script().unwrap(),
            config.gen_bootstrap_db_script().unwrap(),
        ];
        all_files_path.extend(config.gen_all_monograph_configs().unwrap());
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

        let mut tx_hosts_cloned = tx_hosts.clone();
        if let Some(log_host) = all_db_host.get(&DeploymentPackage::MonographLog) {
            tx_hosts_cloned.extend(log_host.clone());
        }
        let dest_file = config.install_dir();
        tx_hosts_cloned
            .into_iter()
            .map(|host| {
                let source_files = list_files_by_host(&host, config.product()).join(" ");
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

    fn upload_group_by_dest(&self, upload_files: Vec<UploadFile>) -> Vec<UploadFile> {
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
}

impl UploadTaskBuilder for MonographUploadBuilder {
    /// Upload installation package, MonographDB configuration file,
    /// MonographDB install script, install config to remote host.
    fn build(&self, config: &DeploymentConfig) -> IndexMap<TaskId, TaskInstance> {
        let mut upload_files = self.build_monograph_misc_upload_file(config);
        let upload_tar_files = self.monograph_tar_upload_file(config);

        upload_files.extend(upload_tar_files);

        let final_files = self.upload_group_by_dest(upload_files);
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
