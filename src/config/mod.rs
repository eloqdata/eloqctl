use crate::config::common::{Cassandra, Common};
use crate::{extract_config_value, load_toml_config};
use ini::{Ini, Properties};
use once_cell::sync::{Lazy, OnceCell};
use std::collections::HashMap;

pub mod common;
pub mod config_macro;

pub static MONOGRAPH_WATER_CONFIG_DIR: &str = "MONO_WATER_CONF_DIR";
pub static MONOGRAPH_WORKSPACE_DIR: &str = "MONOGRAPH_WORKSPACE_DIR";

pub static WORKSPACE_LAYOUT: Lazy<HashMap<String, String>> = Lazy::new(|| {
    let mut workspace_layout_map = HashMap::new();
    workspace_layout_map.insert("data".to_string(), "/monograph/datafarm".to_string());
    workspace_layout_map.insert("etc".to_string(), "/monograph/etc".to_string());
    workspace_layout_map.insert("install".to_string(), "/monograph/install".to_string());
    workspace_layout_map.insert("source".to_string(), "/monograph/source".to_string());
    workspace_layout_map.insert(
        "third_party".to_string(),
        "/monograph/third_party".to_string(),
    );
    workspace_layout_map
});

pub enum ConfigObject {
    Common(Box<Common>),
    Storage(Cassandra),
    MySQL(Properties),
}

pub fn workspace_sub_dir(path: Option<String>) -> HashMap<String, String> {
    let workspace = extract_config_value!("common", Common, path)
        .clone()
        .workspace;
    WORKSPACE_LAYOUT
        .iter()
        .map(|entry| {
            (
                entry.0.clone(),
                format!("{}/{}", workspace, entry.1.clone()),
            )
        })
        .collect::<HashMap<String, String>>()
}

pub fn load_config(config_dir: &str) -> &'static HashMap<String, ConfigObject> {
    static INSTANCE: OnceCell<HashMap<String, ConfigObject>> = OnceCell::new();
    INSTANCE.get_or_init(|| {
        let mut config_mapping = HashMap::new();

        let common_object = load_toml_config!(
            format!("{}/{}", config_dir, "/common.toml").as_str(),
            Common
        );
        // set workspace dir
        std::env::set_var(MONOGRAPH_WORKSPACE_DIR, common_object.clone().workspace);
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
    use crate::config::load_mysql_config;
    use crate::{build_script, extract_config_value, load_toml_config};

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

    #[test]
    pub fn test_build_script() {
        let config_path = config_file("/config");
        let protobuf_build_cmd = build_script!(download, Some(config_path.clone()), protobuf);
        let thirty_party_git_build =
            build_script!(git, Some(config_path), brpc, braft, catch2, aws);
        println!("{:?}", protobuf_build_cmd);
        println!("{:?}", thirty_party_git_build);
        assert_eq!(protobuf_build_cmd.cmd_vec.len(), 1);
        assert_eq!(thirty_party_git_build.cmd_vec.len(), 4);
    }
}
