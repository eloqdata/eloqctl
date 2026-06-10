use crate::cli::task::group::Config;
use crate::cli::task::task_base::{TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{
    build_task_instance, get_source_host, UploadTaskBuilder,
};
use crate::cli::upload_dir;
use crate::config::config_base::{DeployConfig, UploadFile};
use crate::config::monitor::{
    Monitor, ALERTMANAGER_CONFIG_DIR, ALERTMANAGER_WEBHOOK_ADAPTER_TEMPLATE_DIR,
    GRAFANA_CONFIG_DIR, GRAFANA_DASHBOARD_CONFIG_DIR, GRAFANA_DATASOURCE_CONFIG_DIR,
    MONITOR_JOB_NAME, NODE_EXPORTER_JOB_NAME, PROMETHEUS_CONFIG_DIR,
};
use crate::config::{DeploymentPackage, ALERT_RULES_TEMPLATE, MONITOR_DIR};
use anyhow::anyhow;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

pub struct MonitorInfraConfUploadBuilder;

impl MonitorInfraConfUploadBuilder {
    fn stage_named_dir(
        &self,
        config: &DeployConfig,
        dir_name: &str,
        files: &[PathBuf],
    ) -> anyhow::Result<PathBuf> {
        let staged_dir = upload_dir()
            .join(&config.deployment.cluster_name)
            .join(MONITOR_DIR)
            .join(dir_name);
        if staged_dir.exists() {
            fs::remove_dir_all(&staged_dir)?;
        }
        fs::create_dir_all(&staged_dir)?;

        for source_path in files {
            let file_name = source_path
                .file_name()
                .ok_or_else(|| anyhow!("staged source path has no file name"))?;
            fs::copy(source_path, staged_dir.join(file_name))?;
        }

        Ok(staged_dir)
    }

    fn stage_dashboard_upload_dir(
        &self,
        config: &DeployConfig,
        dashboard_files: &[String],
        dashboard_config_path: &Path,
    ) -> anyhow::Result<PathBuf> {
        let staged_dir = upload_dir()
            .join(&config.deployment.cluster_name)
            .join(MONITOR_DIR)
            .join("grafana_dashboards");
        if staged_dir.exists() {
            fs::remove_dir_all(&staged_dir)?;
        }
        fs::create_dir_all(&staged_dir)?;

        for source in dashboard_files {
            let source_path = PathBuf::from(source);
            let file_name = source_path
                .file_name()
                .ok_or_else(|| anyhow!("dashboard source path has no file name: {source}"))?;
            fs::copy(&source_path, staged_dir.join(file_name))?;
        }

        let dashboard_config_name = dashboard_config_path
            .file_name()
            .ok_or_else(|| anyhow!("dashboard config path has no file name"))?;
        fs::copy(
            dashboard_config_path,
            staged_dir.join(dashboard_config_name),
        )?;

        Ok(staged_dir)
    }

    fn dashboard_upload_files(&self, config: &DeployConfig) -> Option<UploadFile> {
        let files = config.load_monitor_dashboard();
        let install_dir = config.install_dir();
        let dashboard_conf_path_string = format!("{install_dir}/{GRAFANA_DASHBOARD_CONFIG_DIR}");
        let monitor_opt = config.deployment.monitor.as_ref();
        assert!(monitor_opt.is_some());
        let monitor = monitor_opt.unwrap();
        let dashboard_path = monitor.gen_grafana_dashboard_config(
            &config.deployment.cluster_name,
            dashboard_conf_path_string,
        );
        assert!(dashboard_path.is_ok());
        let path_binding = dashboard_path.unwrap();
        if files.is_empty() || monitor.grafana.is_none() {
            None
        } else {
            let host = config.get_host_list(DeploymentPackage::Grafana);
            assert_eq!(1, host.len());
            let dest_host = host.first().unwrap();
            let staged_dashboard_dir = self
                .stage_dashboard_upload_dir(config, &files, path_binding.as_path())
                .expect("stage grafana dashboard upload dir");
            let install_dir = config.install_dir();
            Some(UploadFile {
                source: staged_dashboard_dir.to_string_lossy().to_string(),
                dest: format!("{install_dir}/{GRAFANA_DASHBOARD_CONFIG_DIR}"),
                extension: "dashboard_dir".to_string(),
                host: dest_host.to_string(),
                copy_dir: true,
                delete_remote: false,
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
        if monitor.prometheus.is_none()
            && monitor.alertmanager.is_none()
            && monitor.grafana.is_none()
        {
            return vec![];
        }

        let mut upload_files = Vec::new();
        let all_host = config.get_host_as_map();
        let install_dir = config.install_dir();
        let eloq_tx_hosts = all_host.get(&DeploymentPackage::EloqTx).unwrap().clone();
        let eloq_standby_hosts = all_host
            .get(&DeploymentPackage::EloqStandby)
            .unwrap()
            .clone();

        let merged_tx_standby_hosts =
            config.merge_and_deduplicate(eloq_tx_hosts, eloq_standby_hosts);

        // Handle Prometheus configuration separately
        if monitor.prometheus.is_some() {
            let log_hosts = all_host.get(&DeploymentPackage::EloqLog).unwrap();
            let jobs = HashMap::from([
                (
                    MONITOR_JOB_NAME.to_string(),
                    merged_tx_standby_hosts.clone(),
                ),
                (
                    NODE_EXPORTER_JOB_NAME.to_string(),
                    [&merged_tx_standby_hosts[..], &log_hosts.clone()[..]].concat(),
                ),
            ]);

            let prometheus_conf = monitor
                .gen_prometheus_config(&config.deployment.cluster_name, jobs)
                .unwrap(); // Prometheus config

            // Upload alert.rules to prometheus config directory
            let alert_rules_source = upload_dir()
                .join(&config.deployment.cluster_name)
                .join(MONITOR_DIR)
                .join(ALERT_RULES_TEMPLATE);

            // Prepare the alert.rules UploadFile
            let alert_rules_upload_file = UploadFile {
                source: alert_rules_source.to_string_lossy().to_string(),
                dest: format!("{install_dir}/{PROMETHEUS_CONFIG_DIR}"),
                extension: "rules".to_string(),
                host: MonitorInfraConfUploadBuilder::monitor_host(
                    &all_host,
                    DeploymentPackage::Prometheus,
                ),
                copy_dir: false,
                delete_remote: false,
            };

            // Add the alert.rules file to the upload list
            upload_files.push(alert_rules_upload_file);

            // Prepare the Prometheus UploadFile
            let prometheus_upload_file = UploadFile {
                source: MonitorInfraConfUploadBuilder::path_string(prometheus_conf),
                dest: format!("{install_dir}/{PROMETHEUS_CONFIG_DIR}"),
                extension: "prometheus".to_string(),
                host: MonitorInfraConfUploadBuilder::monitor_host(
                    &all_host,
                    DeploymentPackage::Prometheus,
                ),
                copy_dir: false,
                delete_remote: false,
            };

            // Add the Prometheus configuration file to the upload list
            upload_files.push(prometheus_upload_file);
        }

        if monitor.alertmanager.is_some() {
            let alertmanager_conf = monitor
                .gen_alertmanager_config(&config.deployment.cluster_name)
                .unwrap();
            let alertmanager_webhook_template = monitor
                .gen_alertmanager_webhook_adapter_template(&config.deployment.cluster_name)
                .unwrap();
            let staged_template_dir = self
                .stage_named_dir(
                    config,
                    "alertmanager_webhook_adapter_templates",
                    &[alertmanager_webhook_template],
                )
                .unwrap();
            let alertmanager_upload_file = UploadFile {
                source: MonitorInfraConfUploadBuilder::path_string(alertmanager_conf),
                dest: format!("{install_dir}/{ALERTMANAGER_CONFIG_DIR}"),
                extension: "alertmanager".to_string(),
                host: MonitorInfraConfUploadBuilder::monitor_host(
                    &all_host,
                    DeploymentPackage::Alertmanager,
                ),
                copy_dir: false,
                delete_remote: false,
            };
            upload_files.push(alertmanager_upload_file);
            let alertmanager_webhook_template_upload_file = UploadFile {
                source: staged_template_dir.to_string_lossy().to_string(),
                dest: format!("{install_dir}/{ALERTMANAGER_WEBHOOK_ADAPTER_TEMPLATE_DIR}"),
                extension: "alertmanager-webhook-adapter.tmpl".to_string(),
                host: MonitorInfraConfUploadBuilder::monitor_host(
                    &all_host,
                    DeploymentPackage::Alertmanager,
                ),
                copy_dir: true,
                delete_remote: false,
            };
            upload_files.push(alertmanager_webhook_template_upload_file);
        }

        // Handle Grafana configuration separately
        if monitor.grafana.is_some() {
            let grafana_ds_conf_path = monitor
                .gen_grafana_datasource_config(&config.deployment.cluster_name)
                .unwrap(); // Grafana datasource
            let grafana_conf_path = monitor
                .gen_grafana_config(&config.deployment.cluster_name)
                .unwrap(); // Grafana config

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
                delete_remote: false,
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
                delete_remote: false,
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
        Self::build_for_cmd(config, "deploy")
    }
}

impl MonitorInfraConfUploadBuilder {
    pub fn build_for_cmd(config: &Config, cmd: &str) -> IndexMap<TaskId, TaskInstance> {
        let builder = MonitorInfraConfUploadBuilder;
        let Config::Cluster(cluster_config) = config;

        let monitor_opt = cluster_config.deployment.monitor.as_ref();
        let source_host = get_source_host(None);
        if let Some(monitor) = monitor_opt {
            // Generate alert.rules from template with custom thresholds
            if monitor.prometheus.is_some() {
                monitor
                    .gen_alert_rules(&cluster_config.deployment.cluster_name)
                    .expect("generate alert rules error");
            }

            let mut all_upload_files = builder.monitor_config_upload_files(monitor, cluster_config);
            if monitor.grafana.is_some() {
                if let Some(upload_dashboard_file) = builder.dashboard_upload_files(cluster_config)
                {
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
                        cmd,
                        format!("upload_monitor_cnf_{extension}").as_str(),
                    )
                })
                .collect::<IndexMap<TaskId, TaskInstance>>()
        } else {
            IndexMap::new()
        }
    }
}
