use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, UploadTaskBuilder,
};
use crate::config::config_base::{DeploymentConfig, UploadFile};
use crate::config::monitor::{
    Monitor, GRAFANA_CONFIG_DIR, GRAFANA_DASHBOARD_CONFIG_DIR, GRAFANA_DATASOURCE_CONFIG_DIR,
    PROMETHEUS_CONFIG_DIR,
};
use crate::config::DeploymentPackage;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct MonitorInfraConfUploadBuilder;

impl MonitorInfraConfUploadBuilder {
    fn dashboard_upload_files(&self, config: &DeploymentConfig) -> Option<UploadFile> {
        let files = config.load_monitor_dashboard(None);
        if files.is_empty() {
            None
        } else {
            let host = config.get_host_list(DeploymentPackage::Grafana);
            assert_eq!(1, host.len());
            let dest_host = host.first().unwrap();
            let dashboard_files = files.iter().join(" ");
            let install_dir = config.install_dir();
            Some(UploadFile {
                source: dashboard_files,
                dest: format!("{install_dir}/{GRAFANA_DASHBOARD_CONFIG_DIR}"),
                extension: "json".to_string(),
                host: dest_host.to_string(),
                copy_dir: false,
            })
        }
    }

    fn path_string(path: PathBuf) -> String {
        let path_str = path.to_str().unwrap();
        path_str.to_string()
    }

    fn monitor_host(
        host_list: &HashMap<DeploymentPackage, Vec<String>>,
        pkg: DeploymentPackage,
    ) -> String {
        let curr_host = host_list.get(&pkg);
        assert!(curr_host.is_some());
        let monitor_host = curr_host.unwrap();
        monitor_host.first().unwrap().to_string()
    }

    fn monitor_config_upload_files(
        &self,
        monitor: &Monitor,
        config: &DeploymentConfig,
    ) -> Vec<UploadFile> {
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
        let prometheus_conf = monitor
            .gen_prometheus_config(monograph_tx_hosts.clone())
            .unwrap(); //prometheus config
        let prometheus_conf_files = if let Some(mcac) = mcac_config {
            vec![prometheus_conf, mcac]
        } else {
            vec![prometheus_conf]
        };

        let prometheus_conf_source_files = prometheus_conf_files
            .iter()
            .map(|prome_conf| MonitorInfraConfUploadBuilder::path_string(prome_conf.clone()))
            .join(" ");

        let mut prometheus_grafana_conf = vec![
            UploadFile {
                source: MonitorInfraConfUploadBuilder::path_string(grafana_conf_path),
                dest: format!("{install_dir}/{GRAFANA_CONFIG_DIR}"),
                extension: "grafana.cnf".to_string(),
                host: MonitorInfraConfUploadBuilder::monitor_host(
                    &all_host,
                    DeploymentPackage::Grafana,
                ),
                copy_dir: false,
            },
            UploadFile {
                source: MonitorInfraConfUploadBuilder::path_string(grafana_ds_conf_path),
                dest: format!("{install_dir}/{GRAFANA_DATASOURCE_CONFIG_DIR}"),
                extension: "grafana_ds".to_string(),
                host: MonitorInfraConfUploadBuilder::monitor_host(
                    &all_host,
                    DeploymentPackage::Grafana,
                ),
                copy_dir: false,
            },
            UploadFile {
                source: prometheus_conf_source_files,
                dest: format!("{install_dir}/{PROMETHEUS_CONFIG_DIR}"),
                extension: "prometheus,mcac".to_string(),
                host: MonitorInfraConfUploadBuilder::monitor_host(
                    &all_host,
                    DeploymentPackage::Prometheus,
                ),
                copy_dir: false,
            },
        ];

        let upload_create_user_files = monograph_tx_hosts
            .iter()
            .map(|host| UploadFile {
                source: MonitorInfraConfUploadBuilder::path_string(create_user_script.clone()),
                dest: install_dir.clone(),
                extension: "sql".to_string(),
                host: host.to_string(),
                copy_dir: false,
            })
            .collect_vec();

        prometheus_grafana_conf.extend(upload_create_user_files.into_iter());
        prometheus_grafana_conf
    }
}

impl UploadTaskBuilder for MonitorInfraConfUploadBuilder {
    fn build(&self, config: &DeploymentConfig) -> IndexMap<TaskId, TaskInstance> {
        let monitor_opt = config.deployment.monitor.as_ref();
        let source_host = get_source_host(None);
        if let Some(monitor) = monitor_opt {
            let mut all_upload_files = self.monitor_config_upload_files(monitor, config);
            if let Some(upload_dashboard_file) = self.dashboard_upload_files(config) {
                all_upload_files.push(upload_dashboard_file);
            }
            // println!("MonitorInfraConfUploadBuilder all configs={all_upload_files:#?}");
            all_upload_files
                .iter()
                .map(|upload_file| {
                    let extension = upload_file.extension.clone();
                    build_task_instance(
                        source_host.clone(),
                        upload_file.clone(),
                        config,
                        "deploy",
                        format!("upload_monitor_cnf_{extension}").as_str(),
                    )
                })
                .collect::<IndexMap<TaskId, TaskInstance>>()
        } else {
            IndexMap::new()
        }
    }
}
