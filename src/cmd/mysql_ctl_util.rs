use crate::config::workspace_sub_dir;
use std::path::Path;

pub(crate) fn connect_mysqld_from_cli(cnf: String) -> Vec<String> {
    let sub_dirs = workspace_sub_dir(None);
    let install_dir = sub_dirs.get("install").unwrap().clone();
    vec![
        format!("{}/bin/mysql", install_dir),
        "-u".to_string(),
        "root".to_string(),
        "-p".to_string(),
        "mysql".to_string(),
        "--password=''".to_string(),
        "-S".to_string(),
        cnf,
    ]
}

pub(crate) fn list_mysql_cnf(config: Option<String>) -> Vec<String> {
    let sub_dirs = workspace_sub_dir(config);
    let etc_config = sub_dirs.get("etc").unwrap();
    let dir_entry_set = std::fs::read_dir(Path::new(etc_config));
    let mut my_cnf_vec = Vec::new();
    for dir_entry in dir_entry_set.unwrap() {
        let entry = dir_entry.unwrap();
        let file_path = entry.path();
        if !file_path.is_file() {
            continue;
        }
        let extension = file_path.extension();
        if let Some(extension_name) = extension {
            let file_name_str = extension_name.to_str().unwrap();
            if file_name_str.eq("cnf") {
                let file_name = file_path.file_name().unwrap().to_str().unwrap();
                my_cnf_vec.push(file_name.to_string());
            }
        }
    }
    my_cnf_vec
}
