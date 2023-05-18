use crate::cli::download_dir;
use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, create_temp_dir, UploadTaskBuilder,
};
use crate::config::config_base::{
    DeploymentConfig, UploadFile, CASSANDRA_FILE_KEY, MONOGRAPH_FILE_KEY, MONOGRAPH_LOG_FILE_KEY,
};
use crate::config::DeploymentPackage;
use indexmap::IndexMap;
use itertools::Itertools;
use std::ffi::OsStr;
use std::path::PathBuf;

pub struct MonographUploadBuilder;

impl MonographUploadBuilder {
    fn monograph_tar_upload_file(&self, config: &DeploymentConfig) -> Vec<UploadFile> {
        let deployment_ref = &config.deployment;
        let all_db_hosts = config.get_host_as_map();
        let download_dir_path = download_dir();
        let download_dir = download_dir_path.to_str().unwrap();
        let monograph_download_links = deployment_ref.monograph_download_links().unwrap();
        let install_dir = config.install_dir();
        monograph_download_links
            .iter()
            .map(|(file_key, download_url)| {
                let dest_hosts = match file_key.as_str() {
                    MONOGRAPH_FILE_KEY => all_db_hosts.get(&DeploymentPackage::MonographTx),
                    MONOGRAPH_LOG_FILE_KEY => all_db_hosts.get(&DeploymentPackage::MonographLog),
                    CASSANDRA_FILE_KEY => all_db_hosts.get(&DeploymentPackage::Storage),
                    _ => unreachable!(),
                };
                (download_url, dest_hosts)
            })
            .filter(|(_url, hosts)| hosts.is_some())
            .flat_map(|(url, host_opt)| {
                let hosts = host_opt.unwrap();
                let path_buf = PathBuf::from(url.get_url());
                let path = path_buf.as_path();
                let extension = path.extension().and_then(OsStr::to_str).unwrap();
                hosts
                    .iter()
                    .map(|host| UploadFile {
                        source: format!("{download_dir}/*.{extension}"),
                        dest: install_dir.clone(),
                        extension: extension.to_string(),
                        host: host.to_string(),
                        copy_dir: false,
                    })
                    .collect_vec()
            })
            .collect_vec()
    }

    fn build_monograph_misc_upload_file(&self, config: &DeploymentConfig) -> Vec<UploadFile> {
        let deployment = &config.deployment;
        let log_start_path_opt = config.gen_log_start_script().unwrap();

        let mut all_files_path = vec![
            config.gen_tx_start_script().unwrap(),
            config.gen_bootstrap_db_script().unwrap(),
        ];
        let my_cnf_path = deployment.gen_all_monograph_configs().unwrap();
        all_files_path.extend(my_cnf_path.into_iter());
        if let Some(log_start_path) = log_start_path_opt {
            all_files_path.extend(log_start_path.into_iter());
        }

        let all_mysql_exporter_conf = config.gen_all_mysql_exporter_config().unwrap();
        if let Some(mysql_exporter_conf) = all_mysql_exporter_conf {
            all_files_path.extend(mysql_exporter_conf.into_iter());
        }

        let conf_tmp_dir = create_temp_dir("monograph_conf_misc", "/tmp").unwrap();
        let conf_tmp_path = conf_tmp_dir.as_path();
        all_files_path.iter().for_each(move |file| {
            println!("Upload task curr config file= {file:#?}");
            let file_name = file.file_name().unwrap().to_str().unwrap();
            let dest_file = conf_tmp_path.join(file_name);
            let copy_rs = std::fs::copy(file, dest_file.as_path());
            if let Err(copy_err) = copy_rs {
                panic!("Failed Upload db_config files error cause by {copy_err}")
            }
        });

        let all_db_host = config.get_host_as_map();
        let tx_hosts = all_db_host.get(&DeploymentPackage::MonographTx).unwrap();

        let mut tx_hosts_cloned = tx_hosts.clone();
        if let Some(log_host) = all_db_host.get(&DeploymentPackage::MonographLog) {
            tx_hosts_cloned.extend(log_host.clone().into_iter());
        }

        let tmp_files_string = conf_tmp_dir.to_str().unwrap();
        let dest_file = config.install_dir();

        tx_hosts_cloned
            .iter()
            .map(|host| UploadFile {
                source: format!("{tmp_files_string}/*.*"),
                dest: dest_file.clone(),
                extension: "bash,cnf".to_string(),
                host: host.to_string(),
                copy_dir: false,
            })
            .collect_vec()
    }
}

impl UploadTaskBuilder for MonographUploadBuilder {
    /// Upload installation package, MonographDB configuration file (my.cnf),
    /// MonographDB install script, install config to remote host.
    fn build(&self, config: &DeploymentConfig) -> IndexMap<TaskId, TaskInstance> {
        let mut upload_files = self.build_monograph_misc_upload_file(config);
        let upload_tar_files = self.monograph_tar_upload_file(config);
        upload_files.extend(upload_tar_files.into_iter());
        upload_files
            .iter()
            .map(|upload_file| {
                let extension = &upload_file.extension;
                let task_name = format!("deploy_monograph_{extension}");
                build_task_instance(upload_file.clone(), config, "deploy", task_name.as_str())
            })
            .collect::<IndexMap<TaskId, TaskInstance>>()
    }
}
