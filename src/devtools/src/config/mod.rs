use crate::config::common::{Cassandra, Common};
use configparser::ini::Ini;
use once_cell::sync::{Lazy, OnceCell};
use std::collections::HashMap;

pub mod common;
#[macro_use]
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
    MySQL(Ini),
}

pub fn workspace_sub_dir(path: Option<String>) -> HashMap<String, String> {
    let workspace = if let Some(config_path) = path {
        extract_config_value!("common", Common, config_path)
            .clone()
            .workspace
    } else {
        extract_config_value!("common", Common, "".to_string())
            .clone()
            .workspace
    };
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
        let mut mysql_ini = Ini::new();
        mysql_ini
            .load(format!("{}/{}", config_dir, "/mysql/mysql_template.cnf").as_str())
            .unwrap();
        config_mapping.insert("mysql".to_string(), ConfigObject::MySQL(mysql_ini));
        config_mapping
    })
}

#[cfg(test)]
mod tests {
    use crate::config::common::Common;
    use crate::config::workspace_sub_dir;

    pub fn config_file(file: &str) -> String {
        let mut base_path = env!("CARGO_MANIFEST_DIR").to_owned();
        base_path.push_str(file);
        base_path
    }

    #[test]
    pub fn test_load_common_config() {
        let common_path = config_file("/config/common.toml");
        let common = load_toml_config!(common_path.as_str(), Common);
        println!("Common {common:?}")
    }

    #[test]
    pub fn test_extract_config_value() {
        let common_path = config_file("/config");
        let common_config = extract_config_value!("common", Common, common_path.clone());
        println!("{common_path:?} -> {common_config:?}");
    }

    #[test]
    pub fn test_build_script() {
        let config_path = config_file("/config");
        let protobuf_build_cmd = build_script!(download, config_path.clone(), protobuf);
        let thirty_party_git_build = build_script!(git, config_path, brpc, braft, catch2, aws);
        println!("{protobuf_build_cmd:?}");
        println!("{thirty_party_git_build:?}");
        assert_eq!(protobuf_build_cmd.cmd_vec.len(), 1);
        assert_eq!(thirty_party_git_build.cmd_vec.len(), 4);
    }

    #[test]
    pub fn test_gen_multi_mysql_config() {
        let config_path = config_file("/config");
        let mut mysql_cnf = extract_config_value!("mysql", MySQL, config_path.clone()).clone();

        let workspace_sub_dir = workspace_sub_dir(Some(config_path));
        let data_dir = workspace_sub_dir.get("data").unwrap().clone();
        //let std::fs::remove_file(dest_path);
        mysql_cnf.set("mariadb", "datadir", Some(data_dir.clone()));
        let datadir_value = mysql_cnf.get("mariadb", "datadir");
        assert!(datadir_value.is_some());
        assert_eq!(datadir_value.unwrap(), data_dir);
    }
}
