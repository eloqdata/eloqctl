use crate::config::common::{Cassandra, Common};
use ini::{Ini, Properties};
use once_cell::sync::OnceCell;
use std::collections::HashMap;

pub mod common;

#[macro_export]
macro_rules! extract_config_value {
    ($config_obj_key:expr, $config_obj:ident, $input_config_path:expr) => {{
        use $crate::config::load_config;
        let mut input_path = match $input_config_path {
            Some(val) => val,
            _ => "".to_string(),
        };
        if input_path.is_empty() {
            input_path = std::env::var("MONO_WATER_CONF_DIR").unwrap_or_else(|_| {
                panic!("Maybe it's a bug.The path to the configuration file must exist")
            });
        }
        let config_mapping = load_config(input_path.as_str());
        let config_obj = config_mapping.get($config_obj_key).unwrap();
        let rtn_value = match config_obj {
            ConfigObject::$config_obj(data) => data,
            _ => unreachable!(),
        };
        rtn_value
    }};
}

#[macro_export]
macro_rules! load_toml_config {
    ($config_path:expr, $config_type:ty) => {{
        use std::path::Path;
        let config_binary = std::fs::read(Path::new($config_path))
            .unwrap_or_else(|_| panic!("can't read common.toml from path = {}", $config_path));
        let obj: $config_type = toml::from_slice(&config_binary).unwrap();
        obj
    }};
}

pub enum ConfigObject {
    Common(Box<Common>),
    Storage(Cassandra),
    MySQL(Properties),
}

pub fn load_config(config_dir: &str) -> &'static HashMap<String, ConfigObject> {
    static INSTANCE: OnceCell<HashMap<String, ConfigObject>> = OnceCell::new();
    INSTANCE.get_or_init(|| {
        let mut config_mapping = HashMap::new();

        let common_object = load_toml_config!(
            format!("{}/{}", config_dir, "/common.toml").as_str(),
            Common
        );
        if common_object.monograph.storage.eq("cassandra") {
            let cassandra = load_toml_config!(
                format!("{}/{}", config_dir, "/cassandra.toml").as_str(),
                Cassandra
            );
            config_mapping.insert("cassandra".to_string(), ConfigObject::Storage(cassandra));
        }

        config_mapping.insert(
            "common".to_string(),
            ConfigObject::Common(Box::new(common_object)),
        );
        let properties =
            load_mysql_config(format!("{}/{}", config_dir, "/mysql/mysql_template.cnf").as_str());
        config_mapping.insert("mysql".to_string(), ConfigObject::MySQL(properties));
        config_mapping
    })
}

fn load_mysql_config(mysql_config_path: &str) -> Properties {
    let my_cnf = Ini::load_from_file(mysql_config_path).unwrap();
    my_cnf.section(Some("mariadb")).unwrap().clone()
}

#[cfg(test)]
mod tests {
    use crate::config::common::Common;
    use crate::config::{load_mysql_config, ConfigObject};

    pub fn config_file(file: &str) -> String {
        let mut base_path = env!("CARGO_MANIFEST_DIR").to_owned();
        base_path.push_str(file);
        base_path
    }

    #[test]
    pub fn test_load_common_config() {
        let common_path = config_file("/config/common.toml");
        let common = load_toml_config!(common_path.as_str(), Common);
        println!("Common {:?}", common);
    }

    #[test]
    pub fn test_load_mysql_config() {
        let mysql_config_path = config_file("/config/mysql/mysql_template.cnf");
        load_mysql_config(mysql_config_path.as_str());
    }

    #[test]
    pub fn test_extract_config_value() {
        let common_path = config_file("/config");
        let common_config = extract_config_value!("common", Common, Some(common_path.clone()));
        println!("{:?} -> {:?}", common_path, common_config);
    }
}
