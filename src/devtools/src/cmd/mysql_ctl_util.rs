use crate::cmd::base::CmdDef;
use crate::cmd::cmd_utils::get_platform_info;
use crate::config::workspace_sub_dir;
use anyhow::anyhow;
use std::path::Path;

pub(crate) fn connect_mysqld_from_cli(
    user: String,
    pwd: Option<String>,
    cnf: String,
) -> Vec<String> {
    let sub_dirs = workspace_sub_dir(None);
    let install_dir = sub_dirs.get("install").unwrap().clone();
    let sock_file = pick_up_mysql_sock(cnf);
    let set_pwd = if let Some(password) = pwd {
        format!("--password='{}'", password)
    } else {
        "--password=''".to_string()
    };
    vec![
        format!("{}/bin/mysql", install_dir),
        "-u".to_string(),
        user,
        "-p".to_string(),
        "mysql".to_string(),
        set_pwd,
        "-S".to_string(),
        sock_file.unwrap(),
    ]
}

pub(crate) fn get_mysql_prepare_cmd(
    user: String,
    pwd: Option<String>,
    config: Option<String>,
    cmd_args: Vec<String>,
) -> CmdDef {
    let user_info = get_platform_info(None).clone().user;
    if !user_info.has_sudo && !user_info.is_root {
        println!("Creating a user requires Sudo privileges. Please switch users to execute it.");
        return CmdDef::default();
    }
    let mysql_conf = list_mysql_cnf(config);
    if mysql_conf.is_empty() {
        println!("not found mysql config in $MONOGRAPH_WORKSPACE_DIR/etc");
        CmdDef::default()
    } else {
        let conn_mysql =
            connect_mysqld_from_cli(user, pwd, mysql_conf.first().unwrap().to_string());
        CmdDef {
            name: "/usr/bin/sudo".to_string(),
            args: Some(vec![&conn_mysql[..], &cmd_args[..]].concat()),
            show_progress_type: None,
            payload: None,
        }
    }
}

pub(crate) fn pick_up_mysql_sock(config_file: String) -> anyhow::Result<String> {
    let mut mysql_ini = configparser::ini::Ini::new();
    let load_config_rs = mysql_ini.load(config_file.as_str());
    if load_config_rs.is_err() {
        println!(
            "load config_file error. Please check whether the {}exists",
            config_file
        );
        Err(anyhow!(load_config_rs.err().unwrap()))
    } else {
        Ok(mysql_ini.get("mariadb", "socket").unwrap())
    }
}

pub(crate) fn list_mysql_cnf(config: Option<String>) -> Vec<String> {
    let sub_dirs = workspace_sub_dir(config);
    let etc_config = sub_dirs.get("etc").unwrap();
    let dir_entry_set = std::fs::read_dir(Path::new(etc_config));
    if dir_entry_set.is_err() {
        println!(
            "etc directory does not exist. Perhaps you can execute the setup_workspace command"
        );
        return vec![];
    }
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
                my_cnf_vec.push(file_path.as_path().to_str().unwrap().to_string());
            }
        }
    }
    my_cnf_vec
}
