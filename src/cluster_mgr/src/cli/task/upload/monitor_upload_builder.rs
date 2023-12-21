use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, UploadTaskBuilder,
};
use crate::config::config_base::{DeploymentConfig, UploadFile};
use crate::config::monitor::{
    Monitor, GRAFANA_CONFIG_DIR, GRAFANA_DASHBOARD_CONFIG_DIR, GRAFANA_DATASOURCE_CONFIG_DIR,
    MONOGRAPH_TX_JOB_NAME, MYSQL_EXPORTER_JOB_NAME, NODE_EXPORTER_JOB_NAME, PROMETHEUS_CONFIG_DIR,
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
        let install_dir = config.install_dir();
        let dashboard_conf_path_string = format!("{install_dir}/{GRAFANA_DASHBOARD_CONFIG_DIR}");
        let monitor_opt = config.deployment.monitor.as_ref();
        assert!(monitor_opt.is_some());
        let monitor = monitor_opt.unwrap();
        let dashboard_path = monitor.gen_grafana_dashboard_config(dashboard_conf_path_string);
        assert!(dashboard_path.is_ok());
        let path_binding = dashboard_path.unwrap();
        let local_config_path_string = path_binding.to_str().unwrap().to_string();

        if files.is_empty() {
            None
        } else {
            let host = config.get_host_list(DeploymentPackage::Grafana);
            assert_eq!(1, host.len());
            let dest_host = host.first().unwrap();
            let mut dashboard_files = files.iter().join(" ");
            dashboard_files.push(' ');
            dashboard_files.push_str(local_config_path_string.as_str());
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
        let log_hosts = all_host.get(&DeploymentPackage::MonographLog).unwrap();
        let prometheus_conf = monitor
            .gen_prometheus_config(HashMap::from([
                (
                    MONOGRAPH_TX_JOB_NAME.to_string(),
                    monograph_tx_hosts.clone(),
                ),
                (
                    MYSQL_EXPORTER_JOB_NAME.to_string(),
                    monograph_tx_hosts.clone(),
                ),
                (
                    NODE_EXPORTER_JOB_NAME.to_string(),
                    [
                        &monograph_tx_hosts[..],
                        &log_hosts.clone()[..],
                        &cass_config_host_ref[..],
                    ]
                    .concat(),
                ),
            ]))
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

        prometheus_grafana_conf.extend(upload_create_user_files);
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
