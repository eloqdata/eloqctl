use crate::cli::{upload_dir, upload_host_dir};
use crate::config::config_base::{
    CASSANDRA_COLLECTOR_AGENT_FILE_KEY, GRAFANA_FILE_KEY, MONOGRAPH_TX_SERVICE_DIR,
    MYSQL_EXPORTER_FILE_KEY, NODE_EXPORTER_FILE_KEY, PROMETHEUS_FILE_KEY,
};
use crate::config::{
    config_template, load_yaml_config_template, DownloadUrl, CASS_MCAC_CONF_FILE,
    CREATE_MONITOR_USER_SQL_FILE, GRAFANA_CONFIG_FILE, GRAFANA_CONFIG_TEMPLATE,
    GRAFANA_DASHBOARDS_CONFIG_TEMPLATE, GRAFANA_PROMETHEUS_DS_FILE,
    MCAC_PROMETHEUS_CONFIG_TEMPLATE, MYSQL_EXPORTER_CLIENT_CONFIG, PROMETHEUS_CONFIG_FILE,
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
pub const MONOGRAPH_TX_JOB_NAME: &str = "monograph-tx";

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

monitor_components!(Prometheus);
monitor_components!(Grafana);

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct CassandraCollector {
    pub mcac_agent: String,
    pub mcac_port: u16,
}

impl CassandraCollector {
    pub fn update_http_port_cmd(&self, install_dir: String) -> String {
        let mcac_config_file_path =
            format!("{install_dir}/datastax-mcac-agent/config/collectd.conf.tmpl");
        let http_port = self.mcac_port;
        format!(r#"sed -i -e 's/9103/{http_port}/g' {mcac_config_file_path}"#)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct MonographMetrics {
    pub path: String,
    pub port: u16,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Exporter {
    pub url: String,
    pub port: u16,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Monitor {
    pub data_dir: Option<String>,
    pub prometheus: Prometheus,
    pub grafana: Grafana,
    pub node_exporter: Exporter,
    pub mysql_exporter: Option<Exporter>,
    pub cassandra_collector: Option<CassandraCollector>,
    pub monograph_metrics: Option<MonographMetrics>,
}

impl Monitor {
    pub fn download_links(&self) -> anyhow::Result<Vec<DownloadUrl>> {
        let download_links = self.download_links_as_map()?;
        Ok(download_links.into_values().collect_vec())
    }

    pub fn download_links_as_map(&self) -> anyhow::Result<HashMap<String, DownloadUrl>> {
        let mut links = HashMap::new();
        download_urls!(links,
            {PROMETHEUS_FILE_KEY, self.prometheus.download_url},
            {GRAFANA_FILE_KEY, self.grafana.download_url},
            {NODE_EXPORTER_FILE_KEY, self.node_exporter.url},
        );
        if let Some(myex) = &self.mysql_exporter {
            download_urls!(links, {MYSQL_EXPORTER_FILE_KEY, myex.url});
        }
        if let Some(mcac) = &self.cassandra_collector {
            download_urls!(links, {CASSANDRA_COLLECTOR_AGENT_FILE_KEY, mcac.mcac_agent});
        }
        Ok(links)
    }

    pub fn flush_privileges_for_create_user(&self, install_dir: String, port: u16) -> String {
        let bin = format!("{install_dir}/{MONOGRAPH_TX_SERVICE_DIR}/install/bin/mysql");
        format!("{bin} -S /tmp/mysql{port}.sock -e  'FLUSH PRIVILEGES'")
    }

    pub fn create_monitor_user_cmd(&self, install_dir: String, port: u16) -> String {
        let bin = format!("{install_dir}/{MONOGRAPH_TX_SERVICE_DIR}/install/bin/mysql");
        let script_path = format!("{install_dir}/{CREATE_MONITOR_USER_SQL_FILE}");
        format!("{bin} -S /tmp/mysql{port}.sock < {script_path}")
    }

    pub fn gen_monitor_user_sql_file(&self) -> anyhow::Result<PathBuf> {
        let create_monitor_user = config_template(CREATE_MONITOR_USER_SQL_FILE)?;
        let sql_file_template = fs::read_to_string(create_monitor_user)?;
        let create_sql_script = sql_file_template.replace("_MONITOR_", MONO_MONITOR_USER);
        let script_path = upload_dir().join(CREATE_MONITOR_USER_SQL_FILE);
        fs::write(script_path.clone(), create_sql_script)
            .expect("unable write create_monitor_user.sql");
        Ok(script_path)
    }

    pub fn gen_grafana_dashboard_config(&self, path: String) -> anyhow::Result<PathBuf> {
        let dashboard_conf = load_yaml_config_template(GRAFANA_DASHBOARDS_CONFIG_TEMPLATE);
        assert!(dashboard_conf.is_ok());
        let mut dashboard = dashboard_conf.unwrap();
        let mut providers = dashboard.get("providers").unwrap().clone();
        providers[0]["options"]["path"] = Value::String(path);
        dashboard.insert("providers".to_string(), providers);
        let dashboard_path = upload_dir().join(GRAFANA_DASHBOARDS_CONFIG_TEMPLATE);
        let dashboard_rs = File::create(dashboard_path.as_path())?;
        serde_yaml::to_writer(dashboard_rs, &dashboard)?;
        Ok(dashboard_path)
    }

    pub fn gen_grafana_config(&self) -> anyhow::Result<PathBuf> {
        let grafana_http_port = self.grafana.port;
        let grafana_config_path = config_template(GRAFANA_CONFIG_TEMPLATE)?;
        let mut grafana_ini = ini::Ini::load_from_file(grafana_config_path)
            .expect("can not local grafana config template");
        grafana_ini.set_to(
            Some("server"),
            "http_port".to_string(),
            grafana_http_port.to_string(),
        );
        let grafana_default_ini = upload_dir().join(GRAFANA_CONFIG_FILE);
        grafana_ini.write_to_file(grafana_default_ini.as_path())?;
        Ok(grafana_default_ini)
    }

    pub fn gen_mysql_exporter_connect_config(
        &self,
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

        let final_exporter_path = upload_host_dir(&host).join(format!("mysql_exporter_{host}.cnf"));
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

    // node_exporter, mysql_exporter,prometheus,cassandra_metrics(optional)
    pub fn gen_prometheus_config(
        &self,
        job_hosts: HashMap<String, Vec<String>>,
    ) -> anyhow::Result<PathBuf> {
        let node_exporter_port = self.node_exporter.port;
        let monograph_metrics_opt = self.monograph_metrics.as_ref();

        let mut scrape_configs: Vec<Value> = vec![];
        job_hosts.iter().for_each(|(job_name, hosts)| {
            let mut target_hosts: Vec<String> = vec![];
            let mut url = None;
            hosts.iter().for_each(|host| match job_name.as_str() {
                MONOGRAPH_TX_JOB_NAME => {
                    if let Some(monograph_metrics) = monograph_metrics_opt {
                        let port = &monograph_metrics.port;
                        target_hosts.push(format!("{host}:{port}"));
                        url = Some("/mono_metrics".to_string())
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
        if self.cassandra_collector.is_some() {
            let mcac_prometheus_config =
                load_yaml_config_template(MCAC_PROMETHEUS_CONFIG_TEMPLATE)?;
            let mcac_scrape_value = mcac_prometheus_config
                .get("scrape_configs")
                .unwrap()
                .clone();
            let mcac_scrape_job = mcac_scrape_value.get(0).unwrap().clone();
            scrape_configs.push(mcac_scrape_job);
        }

        // if !monograph_targets.is_empty() {
        //     let monograph_scrap_job_value = Monitor::build_prometheus_target_value(
        //         "monograph-service".to_string(),
        //         Some("/mono_metrics".to_string()),
        //         monograph_targets,
        //     );
        //     scrape_configs.push(monograph_scrap_job_value);
        // }

        let prometheus_host = &self.prometheus.host;
        let prometheus_port = self.prometheus.port;
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
        let prometheus_config_path = upload_dir().join(PROMETHEUS_CONFIG_FILE);
        let prometheus_config_file = File::create(prometheus_config_path.as_path()).unwrap();
        serde_yaml::to_writer(prometheus_config_file, &prometheus_config_map)?;
        Ok(prometheus_config_path)
    }

    pub fn gen_mcac_file_sd_config(
        &self,
        cassandra_host: Vec<String>,
    ) -> anyhow::Result<Option<PathBuf>> {
        if let Some(cassandra_collector) = self.cassandra_collector.as_ref() {
            let mcac_port = cassandra_collector.mcac_port;
            let cassandra_target = cassandra_host
                .iter()
                .map(|host| format!("{host}:{mcac_port}"))
                .collect_vec()
                .join(",");
            let mcac_json = serde_json::json!([
                {
                    "targets": [cassandra_target],"labels": {}
                }
            ]);
            let mcac_json_path = upload_dir().join(CASS_MCAC_CONF_FILE);
            let mcac_json_file = File::create(mcac_json_path.as_path()).unwrap();
            serde_json::to_writer(mcac_json_file, &mcac_json)?;
            Ok(Some(mcac_json_path))
        } else {
            Ok(None)
        }
    }

    pub fn gen_grafana_datasource_config(&self) -> anyhow::Result<PathBuf> {
        let prometheus = &self.prometheus;
        let prometheus_url = format!("http://{}:{}", prometheus.host, prometheus.port);
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
        let prometheus_datasource_path = upload_dir().join(GRAFANA_PROMETHEUS_DS_FILE);
        let prometheus_datasource_file =
            File::create(prometheus_datasource_path.as_path()).unwrap();
        serde_yaml::to_writer(prometheus_datasource_file, &prometheus_datasource)?;
        Ok(prometheus_datasource_path)
    }
}
