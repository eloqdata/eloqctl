use crate::cmd::base::{Cmd, CmdDesc, CmdStatus};
use crate::cmd::cmd_utils::{cmd_process, curr_platform};
use lazy_static::lazy_static;
use std::collections::HashMap;

// Build and runtime dependencies. For now, it only supports Linux and macOS
lazy_static! {
    pub static ref DEPS: HashMap<&'static str, Vec<&'static str>> = {
        let mut dep_mapping = HashMap::new();
        dep_mapping.insert(
            "macos",
            vec![
                "git",
                "cmake",
                "ninja",
                "libuv",
                "glog",
                "openssl@1.1",
                "gnu-getopt",
                "coreutils",
                "gflags",
                "leveldb",
                "gperftools",
                "bison",
            ],
        );
        dep_mapping.insert(
            "linux",
            vec![
                "git",
                "g++",
                "make",
                "libssl-dev",
                "libgflags-dev",
                "libgoogle-glog-dev",
                "libprotobuf-dev",
                "libprotoc-dev",
                "protobuf-compiler",
                "libleveldb-dev",
                "libsnappy-dev",
            ],
        );
        dep_mapping
    };
}
/// Check if a monograph instance is started
/// and if the installation compilation environment matches the requirements.
#[derive(Clone, Debug)]
pub struct CheckEnv;

impl Cmd for CheckEnv {
    fn cmd_desc(&self) -> CmdDesc {
        let os_type = curr_platform().os_type;
        let mut args = vec!["list"];
        match os_type.as_str() {
            "macos" => args.extend(DEPS.get("macos").unwrap()),
            _ => panic!("not support cmd"),
        };
        CmdDesc {
            name: "brew".to_string(),
            args: Some(args.iter().map(|arg| arg.to_string()).collect()),
            show_progress_type: Some("pipe".to_string()),
        }
    }

    fn set_up(&self) -> CmdStatus {
        match curr_platform().os_type.as_str() {
            "macos" => cmd_process(
                "command".to_string(),
                Some(vec!["-v".to_string(), "brew".to_string()]),
                |stdout: &str| {
                    println!("brew location {}", stdout);
                },
            ),
            _ => CmdStatus::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cmd::base::{Cmd, CmdContext, CmdDesc};
    use crate::cmd::check_env::CheckEnv;
    use crate::cmd::cmd_utils::{curr_platform, default_log_handler};

    pub fn init_check_env() -> (CheckEnv, CmdDesc) {
        let check_env_cmd_desc = CmdDesc {
            name: "brew".to_string(),
            args: Some(vec![
                "list".to_string(),
                "brew".to_string(),
                "cmake".to_string(),
            ]),
            show_progress_type: Some("pipe".to_string()),
        };
        (CheckEnv {}, check_env_cmd_desc)
    }

    #[test]
    pub fn test_check_env_set_up() {
        let check_env_and_cmd = init_check_env();
        let setup_cmd_status = check_env_and_cmd.0.set_up();
        println!("{}", setup_cmd_status);
    }

    #[test]
    pub fn test_check_env() {
        let platform = curr_platform();
        if platform.os_type.eq("macos") {
            let check_env_and_cmd = init_check_env();
            let mut log = default_log_handler().unwrap();
            let mut mac_os_check_env_context = CmdContext::new(check_env_and_cmd.1, &mut log);
            let cmd_status = check_env_and_cmd.0.exec(&mut mac_os_check_env_context);
            println!("cmd_status = {:?}", cmd_status);
            assert!(!cmd_status.success);
        }
    }
}
