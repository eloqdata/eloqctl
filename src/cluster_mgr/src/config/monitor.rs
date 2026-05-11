use crate::cli::upload_dir;
use crate::config::config_base::{
    GRAFANA_FILE_KEY, MYSQL_EXPORTER_FILE_KEY, NODE_EXPORTER_FILE_KEY, PROMETHEUS_FILE_KEY,
};
use crate::config::{
    config_template, load_yaml_config_template, DownloadUrl, CREATE_MONITOR_USER_SQL_FILE,
    GRAFANA_CONFIG_FILE, GRAFANA_CONFIG_TEMPLATE, GRAFANA_DASHBOARDS_CONFIG_TEMPLATE,
    GRAFANA_PROMETHEUS_DS_FILE, MONITOR_DIR, MYSQL_EXPORTER_CLIENT_CONFIG, PROMETHEUS_CONFIG_FILE,
    PROMETHEUS_CONFIG_TEMPLATE,
};
use crate::download_urls;
use configparser::ini::Ini;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::path::PathBuf;

pub const MONO_MONITOR_USER: &str = "mono_monitor";

pub const GRAFANA_CONFIG_DIR: &str = "grafana/conf";
pub const GRAFANA_DATASOURCE_CONFIG_DIR: &str = "grafana/conf/provisioning/datasources";
pub const GRAFANA_DASHBOARD_CONFIG_DIR: &str = "grafana/conf/provisioning/dashboards";
pub const PROMETHEUS_CONFIG_DIR: &str = "prometheus";

pub const MYSQL_EXPORTER_JOB_NAME: &str = "monograph-myslqd";
pub const NODE_EXPORTER_JOB_NAME: &str = "monograph-node";

pub const MONITOR_JOB_NAME: &str = "eloq-monitor";

#[macro_export]
macro_rules! monitor_component_config_dir {
    ($component:expr) => {{
        match $component.to_lowercase().as_str() {
            "prometheus" => "prometheus".to_string(),
            "grafana" => "grafana/conf/provisioning/datasources".to_string(),
            "mysql_exporter" => "mysqld_exporter".to_string(),
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
pub struct MonographMetrics {
    pub path: Option<String>,
    pub port: Option<u16>,
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
pub struct Monitor {
    pub data_dir: Option<String>,
    pub prometheus: Option<Prometheus>,
    pub grafana: Option<Grafana>,
    pub node_exporter: Option<Exporter>,
    pub mysql_exporter: Option<Exporter>,
    pub monograph_metrics: Option<MonographMetrics>,
    pub eloq_metrics: Option<EloqMetrics>,
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
        if let Some(myex) = &self.mysql_exporter {
            download_urls!(links, {MYSQL_EXPORTER_FILE_KEY, &myex.url});
        }
        Ok(links)
    }

    pub fn gen_monitor_user_sql_file(&self, cluster_name: &str) -> anyhow::Result<PathBuf> {
        let create_monitor_user = config_template(CREATE_MONITOR_USER_SQL_FILE)?;
        let sql_file_template = fs::read_to_string(create_monitor_user)?;
        let create_sql_script = sql_file_template.replace("_MONITOR_", MONO_MONITOR_USER);
        let script_path = upload_dir()
            .join(cluster_name)
            .join(MONITOR_DIR)
            .join(CREATE_MONITOR_USER_SQL_FILE);
        if let Some(parent) = script_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(script_path.clone(), create_sql_script)
            .expect("unable write create_monitor_user.sql");
        Ok(script_path)
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

    pub fn gen_mysql_exporter_connect_config(
        &self,
        cluster_name: &str,
        host: String,
        mysql_port: u16,
    ) -> anyhow::Result<PathBuf> {
        let mysql_exporter_conf_template = config_template(MYSQL_EXPORTER_CLIENT_CONFIG)?;
        let mut mysql_exporter_conf = Ini::new();
        mysql_exporter_conf
            .load(mysql_exporter_conf_template)
            .expect("unable load mysql_exporter connect config");
        mysql_exporter_conf.set("client", "user", Some(MONO_MONITOR_USER.to_string()));
        mysql_exporter_conf.set("client", "password", Some(MONO_MONITOR_USER.to_string()));
        mysql_exporter_conf.set("client", "host", Some(host.clone()));
        mysql_exporter_conf.set("client", "port", Some(mysql_port.to_string()));

        let final_exporter_path = upload_dir()
            .join(cluster_name)
            .join(MONITOR_DIR)
            .join(format!("mysql_exporter_{host}.cnf"));

        if let Some(parent) = final_exporter_path.parent() {
            fs::create_dir_all(parent)?;
        }

        mysql_exporter_conf.write(final_exporter_path.as_path())?;
        Ok(final_exporter_path)
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

    // node_exporter, mysql_exporter, prometheus
    pub fn gen_prometheus_config(
        &self,
        cluster_name: &str,
        job_hosts: HashMap<String, Vec<String>>,
    ) -> anyhow::Result<PathBuf> {
        let node_exporter_port = self.node_exporter.as_ref().unwrap().port;
        let monograph_metrics_opt = self.monograph_metrics.as_ref();
        let eloq_metrics_opt = self.eloq_metrics.as_ref();

        let mut scrape_configs: Vec<Value> = vec![];
        job_hosts.iter().for_each(|(job_name, hosts)| {
            let mut target_hosts: Vec<String> = vec![];
            let mut url = None;
            hosts.iter().for_each(|host| match job_name.as_str() {
                MONITOR_JOB_NAME => {
                    // Try monograph_metrics first
                    if let Some(monograph_metrics) = monograph_metrics_opt {
                        if let Some(port) = monograph_metrics.port {
                            target_hosts.push(format!("{host}:{port}"));
                            url = monograph_metrics.path.clone();
                        }
                    }
                    // If monograph_metrics not available, try eloq_metrics
                    else if let Some(eloq_metrics) = eloq_metrics_opt {
                        if let Some(port) = eloq_metrics.port {
                            target_hosts.push(format!("{host}:{port}"));
                            url = eloq_metrics.path.clone();
                        }
                    }
                }
                NODE_EXPORTER_JOB_NAME => {
                    target_hosts.push(format!("{host}:{node_exporter_port}"));
                }
                MYSQL_EXPORTER_JOB_NAME => {
                    let port = self.mysql_exporter.as_ref().unwrap().port;
                    target_hosts.push(format!("{host}:{port}"));
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
