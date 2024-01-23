use crate::cli::download_dir;
use crate::config::config_base::CASSANDRA_COLLECTOR_AGENT_FILE_KEY;
use crate::config::monitor::Monitor;
use crate::config::ConfigErr::GenCassandraConfigErr;
use crate::config::{
    config_template, load_yaml_config_template, StorageProvider, CASSANDRA_CONF_TEMPLATE,
    CASSANDRA_ENV_TEMPLATE, CASSANDRA_JVM_SERVER_CONF,
};
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct StorageService {
    pub cassandra: Option<Cassandra>,
    pub dynamodb: Option<Dynamodb>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Cassandra {
    pub host: Vec<String>,
    pub download_url: String,
    pub storage_cluster: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Dynamodb {
    pub access_key_id: String,
    pub secret_key: String,
    pub region: String,
    pub endpoint: String,
}

impl StorageService {
    pub fn provider(&self) -> Option<StorageProvider> {
        if self.cassandra.is_some() {
            Some(StorageProvider::Cassandra)
        } else if self.dynamodb.is_some() {
            Some(StorageProvider::DynamoDB)
        } else {
            None
        }
    }
    pub fn gen_cassandra_env(
        &self,
        install_dir: String,
        monitor_opt: Option<Monitor>,
    ) -> anyhow::Result<bool> {
        if let Some(monitor) = monitor_opt {
            if monitor.cassandra_collector.is_some() {
                // let install_dir = self.install_dir();
                let mcac_root =
                    format!("MCAC_ROOT={install_dir}/{CASSANDRA_COLLECTOR_AGENT_FILE_KEY}\n",);
                let append_jvm_opts =
                    r#"JVM_OPTS="$JVM_OPTS -javaagent:${MCAC_ROOT}/lib/datastax-mcac-agent.jar""#;

                let cass_env_file_path = config_template(CASSANDRA_ENV_TEMPLATE)?;
                let final_cass_env = download_dir().join("cassandra-env.sh");
                fs::copy(cass_env_file_path, final_cass_env.clone())?;
                let mut cass_env_file = File::options()
                    .write(true)
                    .append(true)
                    .open(final_cass_env)?;
                cass_env_file.write_all(mcac_root.as_bytes())?;
                cass_env_file.write_all(append_jvm_opts.as_bytes())?;
                cass_env_file.flush()?;
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }
    // key is cassandra host, value is cassandra.yaml config
    pub fn gen_cassandra_config(
        &self,
        install_dir: String,
        cluster_name: String,
        monitor: Option<Monitor>,
    ) -> anyhow::Result<HashMap<String, Vec<PathBuf>>> {
        if self.cassandra.is_none() {
            return Err(anyhow!(GenCassandraConfigErr("Dynamodb".to_string())));
        }
        let has_cassandra_monitor = self.gen_cassandra_env(install_dir, monitor)?;
        let cass_env_sh = if has_cassandra_monitor {
            Some(download_dir().join("cassandra-env.sh"))
        } else {
            None
        };
        let jvm_server_options_path = config_template(CASSANDRA_JVM_SERVER_CONF)?;
        let cass = self.cassandra.as_ref().unwrap();
        // cassandra.yaml config object
        let mut cass_conf_map = load_yaml_config_template(CASSANDRA_CONF_TEMPLATE)?;
        let cassandra_hosts = cass.clone().host;
        let storage_cluster = if cass.storage_cluster.is_none() {
            format!("{cluster_name}_cass_cluster")
        } else {
            cass.clone().storage_cluster.unwrap()
        };

        cass_conf_map.insert("cluster_name".to_string(), Value::String(storage_cluster));
        let seeds = cassandra_hosts.join(",");
        let seed_values = format!(
            r#"
           - class_name: org.apache.cassandra.locator.SimpleSeedProvider
             parameters:
             - seeds: {seeds}"#,
        );
        let seed_yaml_value: Value = serde_yaml::from_str(seed_values.as_str())?;
        cass_conf_map.insert(String::from("seed_provider"), seed_yaml_value);
        let cass_config_vec = cassandra_hosts
            .iter()
            .map(|host| {
                let host_value = Value::String(host.to_string());
                cass_conf_map.insert(String::from("listen_address"), host_value.clone());
                cass_conf_map.insert(
                    String::from("rpc_address"),
                    Value::String("0.0.0.0".to_string()),
                );
                cass_conf_map.insert(String::from("broadcast_rpc_address"), host_value.clone());
                cass_conf_map.insert(String::from("broadcast_address"), host_value);
                let config_path = download_dir().join(format!("cassandra_{host}.yaml"));
                let new_config_file = File::create(config_path.as_path()).unwrap();
                let gen_config_write = serde_yaml::to_writer(new_config_file, &cass_conf_map);
                assert!(gen_config_write.is_ok());
                let mut config_path_vec = vec![config_path, jvm_server_options_path.clone()];
                if let Some(env_sh) = &cass_env_sh {
                    config_path_vec.push(env_sh.clone());
                }
                (host.to_string(), config_path_vec)
            })
            .collect::<HashMap<String, Vec<PathBuf>>>();

        Ok(cass_config_vec)
    }
}
