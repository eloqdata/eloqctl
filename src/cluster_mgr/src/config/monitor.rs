use crate::cli::upload_dir;
use crate::config::config_base::{GRAFANA_FILE_KEY, NODE_EXPORTER_FILE_KEY, PROMETHEUS_FILE_KEY};
use crate::config::{
    config_template, load_yaml_config_template, DownloadUrl, GRAFANA_CONFIG_FILE,
    GRAFANA_CONFIG_TEMPLATE, GRAFANA_DASHBOARDS_CONFIG_TEMPLATE, GRAFANA_PROMETHEUS_DS_FILE,
    MONITOR_DIR, PROMETHEUS_CONFIG_FILE, PROMETHEUS_CONFIG_TEMPLATE,
};
use crate::download_urls;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::path::PathBuf;

pub const GRAFANA_CONFIG_DIR: &str = "grafana/conf";
pub const GRAFANA_DATASOURCE_CONFIG_DIR: &str = "grafana/conf/provisioning/datasources";
pub const GRAFANA_DASHBOARD_CONFIG_DIR: &str = "grafana/conf/provisioning/dashboards";
pub const PROMETHEUS_CONFIG_DIR: &str = "prometheus";

pub const NODE_EXPORTER_JOB_NAME: &str = "eloq-node";

pub const MONITOR_JOB_NAME: &str = "eloq-monitor";

#[macro_export]
macro_rules! monitor_component_config_dir {
    ($component:expr) => {{
        match $component.to_lowercase().as_str() {
            "prometheus" => "prometheus".to_string(),
            "grafana" => "grafana/conf/provisioning/datasources".to_string(),
            _ => unreachable!(),
        }
    }};
}

#[macro_export]
macro_rules! monitor_components {
    ($component_name:ident) => {
        #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
        pub struct $component_name {
            pub download_url: String,
            pub port: u16,
            pub host: String,
        }
    };
}

monitor_components!(Grafana);

#[serde_with::skip_serializing_none]
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Prometheus {
    pub download_url: String,
    pub port: u16,
    pub host: String,
    pub retention_time: Option<String>,
    pub retention_size: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_write_urls: Option<Vec<String>>,
}

impl Prometheus {
    pub fn retention_flags(&self) -> String {
        let mut flags = Vec::new();
        if let Some(retention_time) = &self.retention_time {
            flags.push(format!(
                r#"--storage.tsdb.retention.time="{retention_time}""#
            ));
        }
        if let Some(retention_size) = &self.retention_size {
            flags.push(format!(
                r#"--storage.tsdb.retention.size="{retention_size}""#
            ));
        }
        flags.join(" ")
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct EloqMetrics {
    pub path: Option<String>,
    pub port: Option<u16>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Exporter {
    pub url: String,
    pub port: u16,
}

#[serde_with::skip_serializing_none]
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct AlertThresholds {
    #[serde(default = "default_memory_warning_pct")]
    pub memory_warning_pct: u8,
    #[serde(default = "default_memory_critical_pct")]
    pub memory_critical_pct: u8,
    #[serde(default = "default_memory_emergency_pct")]
    pub memory_emergency_pct: u8,
    #[serde(default = "default_cache_hit_ratio_pct")]
    pub cache_hit_ratio_pct: u8,
    #[serde(default = "default_fragmentation_ratio_pct")]
    pub fragmentation_ratio_pct: u8,
    #[serde(default = "default_max_connections_pct")]
    pub max_connections_pct: u8,
    #[serde(default = "default_max_standby_lag_pct")]
    pub max_standby_lag_pct: u8,
    #[serde(default = "default_leader_change_max_per_10m")]
    pub leader_change_max_per_10m: u8,
}

fn default_memory_warning_pct() -> u8 {
    70
}
fn default_memory_critical_pct() -> u8 {
    80
}
fn default_memory_emergency_pct() -> u8 {
    90
}
fn default_cache_hit_ratio_pct() -> u8 {
    95
}
fn default_fragmentation_ratio_pct() -> u8 {
    30
}
fn default_max_connections_pct() -> u8 {
    70
}
fn default_max_standby_lag_pct() -> u8 {
    70
}
fn default_leader_change_max_per_10m() -> u8 {
    2
}

impl Default for AlertThresholds {
    fn default() -> Self {
        Self {
            memory_warning_pct: default_memory_warning_pct(),
            memory_critical_pct: default_memory_critical_pct(),
            memory_emergency_pct: default_memory_emergency_pct(),
            cache_hit_ratio_pct: default_cache_hit_ratio_pct(),
            fragmentation_ratio_pct: default_fragmentation_ratio_pct(),
            max_connections_pct: default_max_connections_pct(),
            max_standby_lag_pct: default_max_standby_lag_pct(),
            leader_change_max_per_10m: default_leader_change_max_per_10m(),
        }
    }
}

#[serde_with::skip_serializing_none]
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Monitor {
    pub data_dir: Option<String>,
    pub prometheus: Option<Prometheus>,
    pub grafana: Option<Grafana>,
    pub node_exporter: Option<Exporter>,
    pub eloq_metrics: Option<EloqMetrics>,
    #[serde(default)]
    pub alert_thresholds: Option<AlertThresholds>,
}

impl Monitor {
    pub fn download_links(&self) -> anyhow::Result<Vec<DownloadUrl>> {
        let download_links = self.download_links_as_map()?;
        Ok(download_links.into_values().collect_vec())
    }

    pub fn download_links_as_map(&self) -> anyhow::Result<HashMap<String, DownloadUrl>> {
        let mut links = HashMap::new();
        if let Some(prom) = &self.prometheus {
            download_urls!(links, {PROMETHEUS_FILE_KEY, &prom.download_url});
        }
        if let Some(graf) = &self.grafana {
            download_urls!(links, {GRAFANA_FILE_KEY, &graf.download_url});
        }
        if let Some(noex) = &self.node_exporter {
            download_urls!(links, {NODE_EXPORTER_FILE_KEY, &noex.url});
        }
        Ok(links)
    }

    pub fn gen_grafana_dashboard_config(
        &self,
        cluster_name: &str,
        path: String,
    ) -> anyhow::Result<PathBuf> {
        if self.grafana.is_some() {
            let dashboard_conf = load_yaml_config_template(GRAFANA_DASHBOARDS_CONFIG_TEMPLATE);
            assert!(dashboard_conf.is_ok());
            let mut dashboard = dashboard_conf.unwrap();
            let mut providers = dashboard.get("providers").unwrap().clone();
            providers[0]["options"]["path"] = Value::String(path);
            dashboard.insert("providers".to_string(), providers);
            let dashboard_path = upload_dir()
                .join(cluster_name)
                .join(MONITOR_DIR)
                .join(GRAFANA_DASHBOARDS_CONFIG_TEMPLATE);
            if let Some(parent) = dashboard_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let dashboard_rs = File::create(dashboard_path.as_path())?;
            serde_yaml::to_writer(dashboard_rs, &dashboard)?;
            Ok(dashboard_path)
        } else {
            panic!("graf config not found");
        }
    }

    pub fn gen_grafana_config(&self, cluster_name: &str) -> anyhow::Result<PathBuf> {
        if let Some(graf) = &self.grafana {
            let grafana_http_port = graf.port;
            let grafana_config_path = config_template(GRAFANA_CONFIG_TEMPLATE)?;
            let mut grafana_ini = ini::Ini::load_from_file(grafana_config_path)
                .expect("can not load grafana config template");
            grafana_ini.set_to(
                Some("server"),
                "http_port".to_string(),
                grafana_http_port.to_string(),
            );
            let grafana_default_ini = upload_dir()
                .join(cluster_name)
                .join(MONITOR_DIR)
                .join(GRAFANA_CONFIG_FILE);
            if let Some(parent) = grafana_default_ini.parent() {
                fs::create_dir_all(parent)?;
            }
            grafana_ini.write_to_file(grafana_default_ini.as_path())?;
            Ok(grafana_default_ini)
        } else {
            panic!("graf config not found");
        }
    }

    fn build_prometheus_target_value(
        job_name: String,
        metrics_url: Option<String>,
        urls: Vec<String>,
    ) -> Value {
        let target_job = if let Some(metrics_path) = metrics_url {
            format!(
                r#"
           job_name: '{job_name}'
           metrics_path: {metrics_path}
           scrape_interval: 3s
           static_configs:
             - targets:
        "#
            )
        } else {
            format!(
                r#"
           job_name: '{job_name}'
           scrape_interval: 3s
           static_configs:
             - targets:
        "#
            )
        };

        let mut target_job_yaml_value = serde_yaml::from_str::<Value>(target_job.as_str())
            .unwrap()
            .as_mapping()
            .unwrap()
            .clone();
        let mut static_configs_sequence = target_job_yaml_value
            .get("static_configs")
            .unwrap()
            .as_sequence()
            .unwrap()
            .clone();
        let mut targets = static_configs_sequence
            .first()
            .unwrap()
            .as_mapping()
            .unwrap()
            .clone();
        let yaml_url_sequence = urls
            .iter()
            .map(|url| Value::String(url.to_string()))
            .collect_vec();
        targets.insert(
            Value::String("targets".to_string()),
            Value::Sequence(yaml_url_sequence),
        );
        static_configs_sequence.clear();
        static_configs_sequence.insert(0, Value::Mapping(targets));
        target_job_yaml_value.insert(
            Value::String("static_configs".to_string()),
            Value::Sequence(static_configs_sequence.clone()),
        );
        Value::Mapping(target_job_yaml_value)
    }

    pub fn gen_alert_rules(&self, cluster_name: &str) -> anyhow::Result<PathBuf> {
        use crate::config::ALERT_RULES_TEMPLATE;
        let threshold = self.alert_thresholds.as_ref().cloned().unwrap_or_default();
        let template_path = config_template(ALERT_RULES_TEMPLATE)?;
        let mut template = std::fs::read_to_string(&template_path)?;

        template = template.replace(
            "${MEMORY_WARNING_PCT}",
            &threshold.memory_warning_pct.to_string(),
        );
        template = template.replace(
            "${MEMORY_CRITICAL_PCT}",
            &threshold.memory_critical_pct.to_string(),
        );
        template = template.replace(
            "${MEMORY_EMERGENCY_PCT}",
            &threshold.memory_emergency_pct.to_string(),
        );
        template = template.replace(
            "${CACHE_HIT_RATIO_PCT}",
            &threshold.cache_hit_ratio_pct.to_string(),
        );
        template = template.replace(
            "${FRAGMENTATION_RATIO_PCT}",
            &threshold.fragmentation_ratio_pct.to_string(),
        );
        template = template.replace(
            "${MAX_CONNECTIONS_PCT}",
            &threshold.max_connections_pct.to_string(),
        );
        template = template.replace(
            "${MAX_STANDBY_LAG_PCT}",
            &threshold.max_standby_lag_pct.to_string(),
        );
        template = template.replace(
            "${LEADER_CHANGE_MAX_PER_10M}",
            &threshold.leader_change_max_per_10m.to_string(),
        );
        template = template.replace(
            "${MAX_CONNECTIONS_PCT_DECIMAL}",
            &format!("{:.1}", threshold.max_connections_pct as f64 / 100.0),
        );
        template = template.replace(
            "${MAX_STANDBY_LAG_PCT_DECIMAL}",
            &format!("{:.1}", threshold.max_standby_lag_pct as f64 / 100.0),
        );

        let output_dir = upload_dir().join(cluster_name).join(MONITOR_DIR);
        fs::create_dir_all(&output_dir)?;
        let output_path = output_dir.join(ALERT_RULES_TEMPLATE);
        fs::write(&output_path, template)?;
        Ok(output_path)
    }

    // node_exporter, prometheus
    pub fn gen_prometheus_config(
        &self,
        cluster_name: &str,
        job_hosts: HashMap<String, Vec<String>>,
    ) -> anyhow::Result<PathBuf> {
        let node_exporter_port = self.node_exporter.as_ref().unwrap().port;
        let eloq_metrics_opt = self.eloq_metrics.as_ref();

        let mut scrape_configs: Vec<Value> = vec![];
        job_hosts.iter().for_each(|(job_name, hosts)| {
            let mut target_hosts: Vec<String> = vec![];
            let mut url = None;
            hosts.iter().for_each(|host| match job_name.as_str() {
                MONITOR_JOB_NAME => {
                    if let Some(eloq_metrics) = eloq_metrics_opt {
                        if let Some(port) = eloq_metrics.port {
                            target_hosts.push(format!("{host}:{port}"));
                            url = eloq_metrics.path.clone();
                        }
                    }
                }
                NODE_EXPORTER_JOB_NAME => {
                    target_hosts.push(format!("{host}:{node_exporter_port}"));
                }
                _ => unreachable!(),
            });
            let scrape_job_value =
                Monitor::build_prometheus_target_value(job_name.to_string(), url, target_hosts);
            scrape_configs.push(scrape_job_value);
        });

        let prometheus_host = &self.prometheus.as_ref().unwrap().host;
        let prometheus_port = self.prometheus.as_ref().unwrap().port;
        let prometheus_job_value = Monitor::build_prometheus_target_value(
            "prometheus".to_string(),
            None,
            vec![format!("{prometheus_host}:{prometheus_port}")],
        );
        scrape_configs.push(prometheus_job_value);
        let mut prometheus_config_map = load_yaml_config_template(PROMETHEUS_CONFIG_TEMPLATE)?;
        prometheus_config_map.insert(
            "scrape_configs".to_string(),
            Value::Sequence(scrape_configs),
        );

        if let Some(remote_write_urls) = &self.prometheus.as_ref().unwrap().remote_write_urls {
            let remote_write_entries: Vec<Value> = remote_write_urls
                .iter()
                .map(|url| {
                    let mut entry = serde_yaml::Mapping::new();
                    entry.insert(Value::String("url".to_string()), Value::String(url.clone()));
                    Value::Mapping(entry)
                })
                .collect();
            prometheus_config_map.insert(
                "remote_write".to_string(),
                Value::Sequence(remote_write_entries),
            );
        }

        let prometheus_config_file = upload_dir()
            .join(cluster_name)
            .join(MONITOR_DIR)
            .join(PROMETHEUS_CONFIG_FILE);
        if let Some(parent) = prometheus_config_file.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = File::create(&prometheus_config_file)?;
        serde_yaml::to_writer(file, &prometheus_config_map)?;
        Ok(prometheus_config_file)
    }

    pub fn gen_grafana_datasource_config(&self, cluster_name: &str) -> anyhow::Result<PathBuf> {
        let prometheus = &self.prometheus;
        let prometheus_url = format!(
            "http://{}:{}",
            prometheus.as_ref().unwrap().host,
            prometheus.as_ref().unwrap().port
        );
        let datasource_config_yaml = format!(
            r#"
        apiVersion: 1
        datasources:
           - name: Prometheus
             type: prometheus
             access: proxy
             url: {prometheus_url}
        "#
        );
        let prometheus_datasource: Value =
            serde_yaml::from_str(datasource_config_yaml.as_str()).unwrap();
        let grafana_ds_config = upload_dir()
            .join(cluster_name)
            .join(MONITOR_DIR)
            .join(GRAFANA_PROMETHEUS_DS_FILE);
        if let Some(parent) = grafana_ds_config.parent() {
            fs::create_dir_all(parent)?;
        }
        let prometheus_datasource_file = File::create(grafana_ds_config.as_path()).unwrap();
        serde_yaml::to_writer(prometheus_datasource_file, &prometheus_datasource)?;
        Ok(grafana_ds_config)
    }
}
