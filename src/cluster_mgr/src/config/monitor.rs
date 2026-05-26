use crate::cli::upload_dir;
use crate::config::config_base::{
    ALERTMANAGER_FILE_KEY, GRAFANA_FILE_KEY, NODE_EXPORTER_FILE_KEY, PROMETHEUSALERT_FILE_KEY,
    PROMETHEUS_FILE_KEY,
};
use crate::config::{
    config_template, load_yaml_config_template, DownloadUrl, ALERTMANAGER_CONFIG_FILE,
    ALERTMANAGER_CONFIG_TEMPLATE, ALERTMANAGER_WEBHOOK_ADAPTER_FEISHU_TEMPLATE,
    GRAFANA_CONFIG_FILE, GRAFANA_CONFIG_TEMPLATE_V13, GRAFANA_CONFIG_TEMPLATE_V9,
    GRAFANA_DASHBOARDS_CONFIG_TEMPLATE, GRAFANA_PROMETHEUS_DS_FILE, MONITOR_DIR,
    PROMETHEUS_CONFIG_FILE, PROMETHEUS_CONFIG_TEMPLATE,
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
pub const ALERTMANAGER_CONFIG_DIR: &str = "alertmanager";
pub const ALERTMANAGER_WEBHOOK_ADAPTER_DIR: &str = "alertmanager-webhook-adapter";
pub const ALERTMANAGER_WEBHOOK_ADAPTER_BINARY: &str = "alertmanager-webhook-adapter";
pub const ALERTMANAGER_WEBHOOK_ADAPTER_TEMPLATE_DIR: &str =
    "alertmanager-webhook-adapter/templates";
pub const ALERTMANAGER_WEBHOOK_ADAPTER_PORT: u16 = 18080;
pub const ALERTMANAGER_WEBHOOK_ADAPTER_DEFAULT_URL: &str =
    "https://github.com/bougou/alertmanager-webhook-adapter/releases/download/v1.1.11/alertmanager-webhook-adapter-v1.1.11-linux-amd64";

pub const NODE_EXPORTER_JOB_NAME: &str = "eloq-node";

pub const MONITOR_JOB_NAME: &str = "eloq-monitor";

#[macro_export]
macro_rules! monitor_component_config_dir {
    ($component:expr) => {{
        match $component.to_lowercase().as_str() {
            "prometheus" => "prometheus".to_string(),
            "alertmanager" => "alertmanager".to_string(),
            "grafana" => "grafana/conf/provisioning/datasources".to_string(),
            "prometheusalert" => "alertmanager-webhook-adapter".to_string(),
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
pub struct Alertmanager {
    pub download_url: String,
    pub port: u16,
    pub host: String,
    #[serde(default)]
    pub feishu_robot_urls: Option<Vec<String>>,
    #[serde(default = "default_alertmanager_webhook_adapter_download_url")]
    pub webhook_adapter_download_url: String,
    #[serde(default = "default_alertmanager_webhook_adapter_port")]
    pub webhook_adapter_port: u16,
}

fn default_alertmanager_webhook_adapter_download_url() -> String {
    ALERTMANAGER_WEBHOOK_ADAPTER_DEFAULT_URL.to_string()
}

fn default_alertmanager_webhook_adapter_port() -> u16 {
    ALERTMANAGER_WEBHOOK_ADAPTER_PORT
}

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alertmanager_targets: Option<Vec<String>>,
    #[serde(default)]
    pub alert_thresholds: Option<AlertThresholds>,
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
    pub alertmanager: Option<Alertmanager>,
    pub grafana: Option<Grafana>,
    pub node_exporter: Option<Exporter>,
    pub eloq_metrics: Option<EloqMetrics>,
}

impl Monitor {
    fn grafana_is_modern(download_url: &str) -> bool {
        Self::grafana_major_version(download_url).unwrap_or(9) >= 13
    }

    fn grafana_major_version(download_url: &str) -> Option<u32> {
        let filename = download_url
            .rsplit('/')
            .next()
            .unwrap_or(download_url)
            .trim_end_matches(".tar.gz");
        let mut digit_start = None;
        for (idx, ch) in filename.char_indices() {
            if ch.is_ascii_digit() {
                digit_start = Some(idx);
                break;
            }
        }
        let start = digit_start?;
        let version_tail = &filename[start..];
        let major = version_tail.split('.').next()?;
        major.parse().ok()
    }

    pub fn download_links(&self) -> anyhow::Result<Vec<DownloadUrl>> {
        let download_links = self.download_links_as_map()?;
        Ok(download_links.into_values().collect_vec())
    }

    pub fn download_links_as_map(&self) -> anyhow::Result<HashMap<String, DownloadUrl>> {
        let mut links = HashMap::new();
        if let Some(prom) = &self.prometheus {
            download_urls!(links, {PROMETHEUS_FILE_KEY, &prom.download_url});
        }
        if let Some(alertmanager) = &self.alertmanager {
            download_urls!(links, {ALERTMANAGER_FILE_KEY, &alertmanager.download_url});
            download_urls!(
                links,
                {
                    PROMETHEUSALERT_FILE_KEY,
                    &alertmanager.webhook_adapter_download_url
                }
            );
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
            let template = if Self::grafana_is_modern(&graf.download_url) {
                GRAFANA_CONFIG_TEMPLATE_V13
            } else {
                GRAFANA_CONFIG_TEMPLATE_V9
            };
            let grafana_config_path = config_template(template)?;
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
        let threshold = self
            .prometheus
            .as_ref()
            .and_then(|p| p.alert_thresholds.clone())
            .unwrap_or_default();
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
        template = template.replace("${CLUSTER_NAME}", cluster_name);

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

        if let Some(alertmanager_targets) = &self.prometheus.as_ref().and_then(|p| {
            p.alertmanager_targets.as_ref().cloned().or_else(|| {
                self.alertmanager
                    .as_ref()
                    .map(|am| vec![format!("{}:{}", am.host, am.port)])
            })
        }) {
            let am_static_targets: Vec<Value> = alertmanager_targets
                .iter()
                .map(|t| Value::String(t.clone()))
                .collect();
            let mut am_static_config = serde_yaml::Mapping::new();
            am_static_config.insert(
                Value::String("targets".to_string()),
                Value::Sequence(am_static_targets),
            );
            let mut am_target_entry = serde_yaml::Mapping::new();
            am_target_entry.insert(
                Value::String("static_configs".to_string()),
                Value::Sequence(vec![Value::Mapping(am_static_config)]),
            );
            let mut alerting_section = serde_yaml::Mapping::new();
            alerting_section.insert(
                Value::String("alertmanagers".to_string()),
                Value::Sequence(vec![Value::Mapping(am_target_entry)]),
            );
            prometheus_config_map.insert("alerting".to_string(), Value::Mapping(alerting_section));
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

    pub fn gen_alertmanager_config(&self, cluster_name: &str) -> anyhow::Result<PathBuf> {
        let mut alertmanager_config_map = load_yaml_config_template(ALERTMANAGER_CONFIG_TEMPLATE)?;
        let receiver_name = if self
            .alertmanager
            .as_ref()
            .and_then(|alertmanager| alertmanager.feishu_robot_urls.as_ref())
            .is_some_and(|urls| !urls.is_empty())
        {
            "alertmanager_webhook_adapter"
        } else {
            "null"
        };

        if let Some(route) = alertmanager_config_map
            .get_mut("route")
            .and_then(|v| v.as_mapping_mut())
        {
            route.insert(
                Value::String("receiver".to_string()),
                Value::String(receiver_name.to_string()),
            );
        }

        let mut receivers = vec![{
            let mut receiver = serde_yaml::Mapping::new();
            receiver.insert(
                Value::String("name".to_string()),
                Value::String("null".to_string()),
            );
            Value::Mapping(receiver)
        }];

        if let Some(alertmanager) = &self.alertmanager {
            let webhook_entries = alertmanager
                .feishu_robot_urls
                .clone()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|url| Self::extract_feishu_robot_token(&url))
                .map(|token| {
                    let mut webhook_entry = serde_yaml::Mapping::new();
                    webhook_entry.insert(
                        Value::String("url".to_string()),
                        Value::String(format!(
                            "http://{}:{}/webhook/send?channel_type=feishu&token={token}",
                            alertmanager.host, alertmanager.webhook_adapter_port
                        )),
                    );
                    Value::Mapping(webhook_entry)
                })
                .collect_vec();
            if !webhook_entries.is_empty() {
                let mut receiver = serde_yaml::Mapping::new();
                receiver.insert(
                    Value::String("name".to_string()),
                    Value::String("alertmanager_webhook_adapter".to_string()),
                );
                receiver.insert(
                    Value::String("webhook_configs".to_string()),
                    Value::Sequence(webhook_entries),
                );
                receivers.push(Value::Mapping(receiver));
            }
        }

        alertmanager_config_map.insert("receivers".to_string(), Value::Sequence(receivers));

        let alertmanager_config_file = upload_dir()
            .join(cluster_name)
            .join(MONITOR_DIR)
            .join(ALERTMANAGER_CONFIG_FILE);
        if let Some(parent) = alertmanager_config_file.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = File::create(&alertmanager_config_file)?;
        serde_yaml::to_writer(file, &alertmanager_config_map)?;
        Ok(alertmanager_config_file)
    }

    pub fn gen_alertmanager_webhook_adapter_template(
        &self,
        cluster_name: &str,
    ) -> anyhow::Result<PathBuf> {
        if self.alertmanager.is_none() {
            anyhow::bail!("alertmanager config not found");
        }
        let template_path = config_template(ALERTMANAGER_WEBHOOK_ADAPTER_FEISHU_TEMPLATE)?;
        let target_path = upload_dir()
            .join(cluster_name)
            .join(MONITOR_DIR)
            .join(ALERTMANAGER_WEBHOOK_ADAPTER_FEISHU_TEMPLATE);
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(template_path, &target_path)?;
        Ok(target_path)
    }

    fn extract_feishu_robot_token(url: &str) -> Option<String> {
        let trimmed = url.trim().trim_end_matches('/');
        trimmed
            .rsplit("/hook/")
            .next()
            .filter(|token| !token.is_empty() && *token != trimmed)
            .map(|token| token.to_string())
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

#[cfg(test)]
mod tests {
    use super::{Alertmanager, Grafana, Monitor, Prometheus};
    use crate::config::{CONFIG_PATH_DIR, UPLOAD_PATH_DIR};
    use serde_yaml::Value;
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_upload_dir(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cluster-mgr-{name}-{suffix}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn set_template_dirs(upload_dir: &PathBuf) {
        std::env::set_var(
            CONFIG_PATH_DIR,
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("config")
                .to_string_lossy()
                .to_string(),
        );
        std::env::set_var(UPLOAD_PATH_DIR, upload_dir.to_string_lossy().to_string());
    }

    #[test]
    fn grafana_version_detection_supports_legacy_and_modern_names() {
        assert_eq!(
            Monitor::grafana_major_version(
                "https://dl.grafana.com/oss/release/grafana-9.3.6.linux-amd64.tar.gz"
            ),
            Some(9)
        );
        assert_eq!(
            Monitor::grafana_major_version(
                "https://dl.grafana.com/grafana/release/13.0.1+security-01/grafana_13.0.1+security-01_25720641773_linux_amd64.tar.gz"
            ),
            Some(13)
        );
    }

    #[test]
    fn grafana_legacy_config_keeps_full_template() {
        let upload_dir = test_upload_dir("grafana-legacy");
        set_template_dirs(&upload_dir);
        let monitor = Monitor {
            data_dir: None,
            prometheus: None,
            alertmanager: None,
            grafana: Some(Grafana {
                download_url: "https://dl.grafana.com/oss/release/grafana-9.3.6.linux-amd64.tar.gz"
                    .to_string(),
                port: 3301,
                host: "127.0.0.1".to_string(),
            }),
            node_exporter: None,
            eloq_metrics: None,
        };

        let path = monitor.gen_grafana_config("legacy").unwrap();
        let content = fs::read_to_string(path).unwrap();
        assert!(content.contains("[security]"));
        assert!(content.contains("http_port=3301"));
    }

    #[test]
    fn grafana_modern_config_only_overrides_required_fields() {
        let upload_dir = test_upload_dir("grafana-modern");
        set_template_dirs(&upload_dir);
        let monitor = Monitor {
            data_dir: None,
            prometheus: None,
            alertmanager: None,
            grafana: Some(Grafana {
                download_url:
                    "https://dl.grafana.com/grafana/release/13.0.1+security-01/grafana_13.0.1+security-01_25720641773_linux_amd64.tar.gz"
                        .to_string(),
                port: 3301,
                host: "127.0.0.1".to_string(),
            }),
            node_exporter: None,
            eloq_metrics: None,
        };

        let path = monitor.gen_grafana_config("modern").unwrap();
        let content = fs::read_to_string(path).unwrap();
        assert!(content.contains("[server]"));
        assert!(content.contains("http_port=3301"));
        assert!(!content.contains("[security]"));
    }

    #[test]
    fn prometheus_config_auto_wires_alertmanager_target() {
        let upload_dir = test_upload_dir("prometheus-alertmanager");
        set_template_dirs(&upload_dir);
        let monitor = Monitor {
            data_dir: None,
            prometheus: Some(Prometheus {
                download_url: "https://example.com/prometheus.tar.gz".to_string(),
                port: 9500,
                host: "127.0.0.1".to_string(),
                retention_time: None,
                retention_size: None,
                remote_write_urls: None,
                alertmanager_targets: None,
                alert_thresholds: None,
            }),
            alertmanager: Some(Alertmanager {
                download_url: "https://example.com/alertmanager.tar.gz".to_string(),
                port: 9093,
                host: "127.0.0.1".to_string(),
                feishu_robot_urls: None,
                webhook_adapter_download_url: super::ALERTMANAGER_WEBHOOK_ADAPTER_DEFAULT_URL
                    .to_string(),
                webhook_adapter_port: super::ALERTMANAGER_WEBHOOK_ADAPTER_PORT,
            }),
            grafana: None,
            node_exporter: Some(super::Exporter {
                url: "https://example.com/node_exporter.tar.gz".to_string(),
                port: 9200,
            }),
            eloq_metrics: None,
        };

        let path = monitor
            .gen_prometheus_config("prom", HashMap::<String, Vec<String>>::new())
            .unwrap();
        let content = fs::read_to_string(path).unwrap();
        assert!(content.contains("127.0.0.1:9093"));
    }

    #[test]
    fn alertmanager_extracts_feishu_robot_token() {
        let upload_dir = test_upload_dir("alertmanager-feishu-token");
        set_template_dirs(&upload_dir);
        assert_eq!(
            Monitor::extract_feishu_robot_token("https://open.feishu.cn/open-apis/bot/v2/hook/abc"),
            Some("abc".to_string())
        );
        assert_eq!(Monitor::extract_feishu_robot_token("invalid"), None);
    }

    #[test]
    fn alertmanager_config_targets_webhook_adapter() {
        let upload_dir = test_upload_dir("alertmanager-config");
        set_template_dirs(&upload_dir);
        let monitor = Monitor {
            data_dir: None,
            prometheus: None,
            alertmanager: Some(Alertmanager {
                download_url: "https://example.com/alertmanager.tar.gz".to_string(),
                port: 9093,
                host: "127.0.0.1".to_string(),
                feishu_robot_urls: Some(vec![
                    "https://open.feishu.cn/open-apis/bot/v2/hook/abc".to_string()
                ]),
                webhook_adapter_download_url: super::ALERTMANAGER_WEBHOOK_ADAPTER_DEFAULT_URL
                    .to_string(),
                webhook_adapter_port: 18080,
            }),
            grafana: None,
            node_exporter: None,
            eloq_metrics: None,
        };

        let path = monitor.gen_alertmanager_config("alertmanager").unwrap();
        let content = fs::read_to_string(path).unwrap();
        let yaml: HashMap<String, Value> = serde_yaml::from_str(&content).unwrap();
        assert!(content.contains("/webhook/send?channel_type=feishu&token=abc"));
        assert_eq!(
            yaml.get("route")
                .and_then(Value::as_mapping)
                .and_then(|m| m.get(Value::String("receiver".to_string())))
                .and_then(Value::as_str),
            Some("alertmanager_webhook_adapter")
        );
    }

    #[test]
    fn alertmanager_webhook_adapter_template_is_staged() {
        let upload_dir = test_upload_dir("alertmanager-template");
        set_template_dirs(&upload_dir);
        let monitor = Monitor {
            data_dir: None,
            prometheus: None,
            alertmanager: Some(Alertmanager {
                download_url: "https://example.com/alertmanager.tar.gz".to_string(),
                port: 9093,
                host: "127.0.0.1".to_string(),
                feishu_robot_urls: Some(vec![
                    "https://open.feishu.cn/open-apis/bot/v2/hook/abc".to_string()
                ]),
                webhook_adapter_download_url: super::ALERTMANAGER_WEBHOOK_ADAPTER_DEFAULT_URL
                    .to_string(),
                webhook_adapter_port: 18080,
            }),
            grafana: None,
            node_exporter: None,
            eloq_metrics: None,
        };

        let path = monitor
            .gen_alertmanager_webhook_adapter_template("alertmanager")
            .unwrap();
        let content = fs::read_to_string(path).unwrap();
        assert!(content.contains("EloqKV"));
        assert!(content.contains("**实例详情**"));
        assert!(content.contains("severity.tag"));
    }

    #[test]
    fn alert_rules_use_cluster_ops_and_embed_cluster_name() {
        let upload_dir = test_upload_dir("alert-rules-cluster-ops");
        set_template_dirs(&upload_dir);
        let monitor = Monitor {
            data_dir: None,
            prometheus: Some(Prometheus {
                download_url: "https://example.com/prometheus.tar.gz".to_string(),
                port: 9500,
                host: "127.0.0.1".to_string(),
                retention_time: None,
                retention_size: None,
                remote_write_urls: None,
                alertmanager_targets: None,
                alert_thresholds: None,
            }),
            alertmanager: None,
            grafana: None,
            node_exporter: None,
            eloq_metrics: None,
        };

        let path = monitor.gen_alert_rules("test-e2e").unwrap();
        let content = fs::read_to_string(path).unwrap();

        assert!(content.contains("alert: eloqkv_cluster_ops_too_low"));
        assert!(content.contains("sum(rate(redis_command_aggregated_total[1m])) < 100000"));
        assert!(content.contains("EloqKV cluster test-e2e OPS has been below 100000 for 1 minute"));
        assert!(!content.contains("eloqkv_instance_ops_too_low"));
        assert!(!content.contains("sum by (instance) (rate(redis_command_aggregated_total[1m]))"));
        assert!(!content.contains("${CLUSTER_NAME}"));
    }
}
