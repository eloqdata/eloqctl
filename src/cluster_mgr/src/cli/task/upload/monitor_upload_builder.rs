use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, create_temp_dir, UploadTaskBuilder,
};
use crate::config::config_base::{DeploymentConfig, UploadFile};
use crate::config::monitor::{
    Monitor, GRAFANA_CONFIG_DIR, GRAFANA_DATASOURCE_CONFIG_DIR, PROMETHEUS_CONFIG_DIR,
};
use crate::config::DeploymentPackage;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Clone)]
struct ConfigAndHostPair {
    path: Vec<PathBuf>,
    hosts: Vec<String>,
}

pub struct MonitorInfraConfUploadBuilder;

impl MonitorInfraConfUploadBuilder {
    fn monitor_upload_files(
        &self,
        all_monitor_config: HashMap<String, ConfigAndHostPair>,
    ) -> Vec<UploadFile> {
        all_monitor_config
            .iter()
            .flat_map(|(dest_dir, path_and_hosts)| {
                let tmp_prefix = if dest_dir.ends_with(PROMETHEUS_CONFIG_DIR) {
                    "monograph_prometheus"
                } else if dest_dir.ends_with(GRAFANA_CONFIG_DIR) {
                    "monograph_grafana"
                } else if dest_dir.ends_with(GRAFANA_DATASOURCE_CONFIG_DIR) {
                    "monograph_grafana_ds"
                } else {
                    "monograph_monitor_files"
                };
                println!("tmp_fs={tmp_prefix},dest_dir={dest_dir}");
                let monitor_conf_tmp_dir = create_temp_dir(tmp_prefix, "/tmp").unwrap();
                let path_vec = &path_and_hosts.path;
                let hosts = &path_and_hosts.hosts;

                path_vec.iter().for_each(|path| {
                    let source_file = path.file_name().unwrap().to_str().unwrap();
                    let dest_file = monitor_conf_tmp_dir.join(source_file);
                    std::fs::copy(path, dest_file.as_path()).unwrap();
                });
                let source_file = monitor_conf_tmp_dir.to_str().unwrap();
                hosts
                    .iter()
                    .map(|host| UploadFile {
                        source: format!("{source_file}/*.*"),
                        dest: dest_dir.clone(),
                        extension: tmp_prefix.to_string(),
                        host: host.to_string(),
                        copy_dir: false,
                    })
                    .collect_vec()
            })
            .collect_vec()
    }

    fn gen_monitor_config(
        &self,
        monitor: &Monitor,
        config: &DeploymentConfig,
    ) -> HashMap<String, ConfigAndHostPair> {
        let all_host = config.get_host_as_map();
        let install_dir = config.install_dir();
        let monograph_tx_hosts_ref = all_host.get(&DeploymentPackage::MonographTx).unwrap();
        let cass_config_host_ref = all_host.get(&DeploymentPackage::Storage).unwrap();
        let monograph_tx_hosts = monograph_tx_hosts_ref.clone();

        let create_user_script = monitor.gen_monitor_user_sql_file().unwrap(); //install_dir
        let grafana_ds_conf_path = monitor.gen_grafana_datasource_config().unwrap(); //grafana datasource
        let grafana_conf_path = monitor.gen_grafana_config().unwrap(); //grafana
        let mcac_config = monitor
            .gen_mcac_file_sd_config(cass_config_host_ref.clone())
            .unwrap(); // prometheus
        let prometheus_conf = monitor.gen_prometheus_config(monograph_tx_hosts).unwrap(); //prometheus config
        let prometheus_files = if let Some(mcac) = mcac_config {
            vec![prometheus_conf, mcac]
        } else {
            vec![prometheus_conf]
        };
        // key is dest dir, value is source path
        HashMap::from([
            (
                format!("{install_dir}/{PROMETHEUS_CONFIG_DIR}"),
                ConfigAndHostPair {
                    path: prometheus_files,
                    hosts: all_host
                        .get(&DeploymentPackage::Prometheus)
                        .unwrap()
                        .clone(),
                },
            ),
            (
                format!("{install_dir}/{GRAFANA_CONFIG_DIR}"),
                ConfigAndHostPair {
                    path: vec![grafana_conf_path],
                    hosts: all_host.get(&DeploymentPackage::Grafana).unwrap().clone(),
                },
            ),
            (
                format!("{install_dir}/{GRAFANA_DATASOURCE_CONFIG_DIR}"),
                ConfigAndHostPair {
                    path: vec![grafana_ds_conf_path],
                    hosts: all_host.get(&DeploymentPackage::Grafana).unwrap().clone(),
                },
            ),
            (
                install_dir,
                ConfigAndHostPair {
                    path: vec![create_user_script],
                    hosts: all_host
                        .get(&DeploymentPackage::MonographTx)
                        .unwrap()
                        .clone(),
                },
            ),
        ])
    }
}

impl UploadTaskBuilder for MonitorInfraConfUploadBuilder {
    fn build(&self, config: &DeploymentConfig) -> IndexMap<TaskId, TaskInstance> {
        let monitor_opt = config.deployment.monitor.as_ref();

        if let Some(monitor) = monitor_opt {
            let all_monitor_config = self.gen_monitor_config(monitor, config);
            let all_upload_files = self.monitor_upload_files(all_monitor_config);
            all_upload_files
                .iter()
                .map(|upload_file| {
                    build_task_instance(
                        upload_file.clone(),
                        config,
                        "deploy",
                        upload_file.extension.as_str(),
                    )
                })
                .collect::<IndexMap<TaskId, TaskInstance>>()
        } else {
            IndexMap::new()
        }
    }
}
