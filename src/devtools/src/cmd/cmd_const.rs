use once_cell::sync::Lazy;
use std::collections::HashMap;
use sysinfo::{SystemExt, UserExt};

pub static SYSTEM_DEPS: &[&str; 2] = &["darwin", "ubuntu"];
pub static MONO_WATER_CONF: &str = "MONO_WATER_CONF_DIR";
pub static PROTOBUF_TAR_FILE_NAME: &str = "protobuf-bin.tar.gz";
pub static CASSANDRA_TAR_FILE_NAME: &str = "cassandra-bin.tar.gz";

pub static SUPPORT_CMD_LIST: Lazy<Vec<&'static str>> = Lazy::new(|| {
    vec![
        "check_deps",
        "install_deps",
        "setup_workspace",
        "ln_source",
        "gen_mysql_cnf",
        "build_all",
        "build_monograph",
        "playground",
        "init_db",
        "stop",
        "start",
    ]
});

pub static SYSTEM_INFO: Lazy<HashMap<&'static str, String>> = Lazy::new(|| {
    let mut sys_info_map = HashMap::new();
    let system = sysinfo::System::new_all();
    let os_type = system.name().unwrap().to_lowercase();
    sys_info_map.insert("os_type", os_type.clone());
    sys_info_map.insert("os_version", system.os_version().unwrap());
    let is_linux = os_type.eq("darwin");
    unsafe {
        let (uid, euid) = (libc::getuid(), libc::geteuid());
        let current_super_user = system
            .users()
            .iter()
            .filter(|user| user.id().to_string().eq(&uid.to_string()))
            .map(|user| user.name())
            .collect::<Vec<_>>();
        assert!(!current_super_user.is_empty());
        sys_info_map.insert("uid", uid.to_string());
        sys_info_map.insert("euid", euid.to_string());
        // is_root, has_sudo
        let mut running_as = (false, false);
        match (uid, euid) {
            (0, 0) => running_as.0 = true,
            (_, 0) => running_as.1 = true,
            (_, _) => {}
        };
        sys_info_map.insert("is_root", running_as.0.to_string());
        if is_linux {
            sys_info_map.insert("has_sudo", (!current_super_user.is_empty()).to_string());
        } else {
            //TODO check others OS_TYPE. currently if MacOS set default value.
            sys_info_map.insert("has_sudo", true.to_string());
        }
        sys_info_map.insert(
            "current_user",
            current_super_user.first().unwrap().to_string(),
        );
    }
    sys_info_map
});
