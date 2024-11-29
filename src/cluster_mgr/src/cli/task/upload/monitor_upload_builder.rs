use crate::cli::task::group::Config;
use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, UploadTaskBuilder,
};
use crate::config::config_base::{DeployConfig, UploadFile};
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
    fn dashboard_upload_files(&self, config: &DeployConfig) -> Option<UploadFile> {
        let files = config.load_monitor_dashboard();
        let install_dir = config.install_dir();
        let dashboard_conf_path_string = format!("{install_dir}/{GRAFANA_DASHBOARD_CONFIG_DIR}");
        let monitor_opt = config.deployment.monitor.as_ref();
        assert!(monitor_opt.is_some());
        let monitor = monitor_opt.unwrap();
        let dashboard_path = monitor.gen_grafana_dashboard_config(dashboard_conf_path_string);
        assert!(dashboard_path.is_ok());
        let path_binding = dashboard_path.unwrap();
        let local_config_path_string = path_binding.to_str().unwrap().to_string();

        if files.is_empty() || monitor.grafana.is_none() {
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
        config: &DeployConfig,
    ) -> Vec<UploadFile> {
        // Return an empty vector if both Prometheus and Grafana are not configured
        if monitor.prometheus.is_none() && monitor.grafana.is_none() {
            return vec![];
        }

        let mut upload_files = Vec::new();
        let all_host = config.get_host_as_map();
        let install_dir = config.install_dir();
        let monograph_tx_hosts_ref = all_host.get(&DeploymentPackage::MonographTx).unwrap();
        let monograph_tx_hosts = monograph_tx_hosts_ref.clone();

        // Generate the create_user_script
        let create_user_script = monitor.gen_monitor_user_sql_file().unwrap();

        // Prepare upload files for the create_user_script
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

        // Add the create_user_script files to the upload list
        upload_files.extend(upload_create_user_files);

        // Handle Prometheus configuration separately
        if monitor.prometheus.is_some() {
            let cass_config_host_ref = all_host.get(&DeploymentPackage::Storage).unwrap();
            let cass_config_hosts = cass_config_host_ref.clone();

            let mcac_config = monitor
                .gen_mcac_file_sd_config(cass_config_hosts.clone())
                .unwrap(); // Prometheus MCAC config

            let log_hosts = all_host.get(&DeploymentPackage::MonographLog).unwrap();
            let mut jobs = HashMap::from([
                (
                    MONOGRAPH_TX_JOB_NAME.to_string(),
                    monograph_tx_hosts.clone(),
                ),
                (
                    NODE_EXPORTER_JOB_NAME.to_string(),
                    [
                        &monograph_tx_hosts[..],
                        &log_hosts.clone()[..],
                        &cass_config_hosts[..],
                    ]
                    .concat(),
                ),
            ]);

            if monitor.mysql_exporter.is_some() {
                jobs.insert(
                    MYSQL_EXPORTER_JOB_NAME.to_owned(),
                    monograph_tx_hosts.clone(),
                );
            }

            let prometheus_conf = monitor.gen_prometheus_config(jobs).unwrap(); // Prometheus config
            let prometheus_conf_files = if let Some(mcac) = mcac_config {
                vec![prometheus_conf, mcac]
            } else {
                vec![prometheus_conf]
            };

            let prometheus_conf_source_files = prometheus_conf_files
                .iter()
                .map(|prome_conf| MonitorInfraConfUploadBuilder::path_string(prome_conf.clone()))
                .join(" ");

            // Prepare the Prometheus UploadFile
            let prometheus_upload_file = UploadFile {
                source: prometheus_conf_source_files,
                dest: format!("{install_dir}/{PROMETHEUS_CONFIG_DIR}"),
                extension: "prometheus,mcac".to_string(),
                host: MonitorInfraConfUploadBuilder::monitor_host(
                    &all_host,
                    DeploymentPackage::Prometheus,
                ),
                copy_dir: false,
            };

            // Add the Prometheus configuration file to the upload list
            upload_files.push(prometheus_upload_file);
        }

        // Handle Grafana configuration separately
        if monitor.grafana.is_some() {
            let grafana_ds_conf_path = monitor.gen_grafana_datasource_config().unwrap(); // Grafana datasource
            let grafana_conf_path = monitor.gen_grafana_config().unwrap(); // Grafana config

            // Prepare the Grafana configuration UploadFile
            let grafana_conf_upload_file = UploadFile {
                source: MonitorInfraConfUploadBuilder::path_string(grafana_conf_path),
                dest: format!("{install_dir}/{GRAFANA_CONFIG_DIR}"),
                extension: "grafana.cnf".to_string(),
                host: MonitorInfraConfUploadBuilder::monitor_host(
                    &all_host,
                    DeploymentPackage::Grafana,
                ),
                copy_dir: false,
            };

            // Prepare the Grafana datasource UploadFile
            let grafana_ds_upload_file = UploadFile {
                source: MonitorInfraConfUploadBuilder::path_string(grafana_ds_conf_path),
                dest: format!("{install_dir}/{GRAFANA_DATASOURCE_CONFIG_DIR}"),
                extension: "grafana_ds".to_string(),
                host: MonitorInfraConfUploadBuilder::monitor_host(
                    &all_host,
                    DeploymentPackage::Grafana,
                ),
                copy_dir: false,
            };

            // Add the Grafana files to the upload list
            upload_files.push(grafana_conf_upload_file);
            upload_files.push(grafana_ds_upload_file);
        }

        // Return the list of UploadFile instances
        upload_files
    }
}

impl UploadTaskBuilder for MonitorInfraConfUploadBuilder {
    fn build(&self, config: &Config) -> IndexMap<TaskId, TaskInstance> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => panic!("Expected ClusterConfig for MonitorInfraConfUploadBuilder"),
        };

        let monitor_opt = cluster_config.deployment.monitor.as_ref();
        let source_host = get_source_host(None);
        if let Some(monitor) = monitor_opt {
            let mut all_upload_files = self.monitor_config_upload_files(monitor, cluster_config);
            if monitor.grafana.is_some() {
                if let Some(upload_dashboard_file) = self.dashboard_upload_files(cluster_config) {
                    all_upload_files.push(upload_dashboard_file);
                }
            }
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
