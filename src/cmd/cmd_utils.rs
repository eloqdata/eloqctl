use crate::cmd::base::{CmdDef, CmdStatus, PipeDef, Platform, UserInfo};
use crate::cmd::cmd_const::{SUPPORT_CMD_LIST, SYSTEM_DEPS, SYSTEM_INFO};
use crate::config::{workspace_sub_dir, MONOGRAPH_WATER_CONFIG_DIR};
use crate::extract_config_value;
use anyhow::anyhow;
use indicatif::{ProgressBar, ProgressStyle};
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::env;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use sysinfo::{ProcessExt, SystemExt};

pub fn get_platform_info(config_path: Option<String>) -> &'static Platform {
    static INSTANCE: OnceCell<Platform> = OnceCell::new();
    INSTANCE.get_or_init(|| {
        let sys_deps = load_deps(config_path);
        let os_type = SYSTEM_INFO.get("os_type").unwrap();
        Platform {
            os_type: os_type.clone(),
            arch: env::consts::ARCH.to_string(),
            family: env::consts::FAMILY.to_string(),
            deps: sys_deps.get(os_type).unwrap().clone(),
            user: UserInfo {
                is_root: bool::from_str(SYSTEM_INFO.get("is_root").unwrap()).unwrap(),
                has_sudo: bool::from_str(SYSTEM_INFO.get("has_sudo").unwrap()).unwrap(),
                user_name: SYSTEM_INFO.get("current_user").unwrap().to_string(),
            },
        }
    })
}

pub fn default_log_handler() -> anyhow::Result<File> {
    let log = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(create_log_path_and_get());
    if log.is_ok() {
        Ok(log.ok().unwrap())
    } else {
        Err(anyhow::Error::from(log.err().unwrap()))
    }
}

pub fn get_process_bar(progress_bar_type: &str, cmd: &str) -> ProgressBar {
    match progress_bar_type {
        "pipe" => pipe_progress_bar(cmd.to_string()),
        "elapsed" => elapsed_progress_bar(None, None),
        _ => unreachable!(),
    }
}

pub fn pipe_progress_bar(cmd_str: String) -> ProgressBar {
    let cmd_pb = ProgressBar::new_spinner();
    cmd_pb.set_style(
        ProgressStyle::default_bar()
            .template(&format!("{{spinner:.dim.bold}} {}: {{wide_msg}}", cmd_str))
            .unwrap()
            .progress_chars("##-"),
    );
    cmd_pb
}

pub fn elapsed_progress_bar(len: Option<u64>, customer_msg: Option<String>) -> ProgressBar {
    let total_size = if let Some(size) = len { size } else { 0_u64 };
    let cmd_pb = ProgressBar::new(total_size);
    let sty = if let Some(msg) = customer_msg {
        format!(
            "{{spinner:.green}} {:15}: [{{elapsed_precise}}] [{{wide_bar:.green/white}}] {{bytes}}/{{total_bytes}} ({{eta}})", msg)
    } else {
        "{spinner:.green} [{elapsed_precise}] [{wide_bar:.green/white}] {bytes}/{total_bytes} ({eta})"
            .to_string()
    };
    cmd_pb.set_style(
        ProgressStyle::default_spinner()
            .template(sty.as_str())
            .unwrap()
            .progress_chars("#>-"),
    );
    cmd_pb
}

pub fn cmd_process<F, T>(cmd_desc: CmdDef, mut stdout_f: F) -> CmdStatus<T>
    where
        F: FnMut(&str),
        T: Clone + Debug,
{
    let mut cmd = std::process::Command::new(cmd_desc.name.as_str());
    if let Some(cmd_args) = cmd_desc.args.clone() {
        let real_args = cmd_args.iter().map(|c| c.as_str()).collect::<Vec<_>>();
        cmd.args(real_args);
    }

    let pipe_rs = os_pipe::pipe();
    if let Ok((reader, writer)) = pipe_rs {
        let writer_clone = writer.try_clone().unwrap();
        let mut child = cmd.stdout(writer).stderr(writer_clone).spawn().unwrap();
        drop(cmd);

        let buffer_reader = std::io::BufReader::new(reader);
        for line_rs in buffer_reader.lines() {
            let line = line_rs.unwrap();
            let stripped_line = line.trim();
            if !stripped_line.is_empty() {
                stdout_f(stripped_line);
            }
        }
        let child_exist_status = child.wait();
        if let Ok(exitstatus) = child_exist_status {
            println!("{} success={}", cmd_desc, exitstatus.success());
            CmdStatus {
                success: exitstatus.success(),
                output: None,
                data: None,
            }
        } else {
            CmdStatus {
                success: false,
                output: None, //Some(stderr_output),
                data: None,
            }
        }
    } else {
        CmdStatus {
            success: false,
            output: Some(format!(
                "os_pipe::pipe() error. cause by {}",
                pipe_rs.err().unwrap()
            )),
            data: None,
        }
    }
}

pub fn all_support_cmd_string() -> String {
    SUPPORT_CMD_LIST
        .iter()
        .map(|cmd_str| format!("\t{}", cmd_str))
        .collect::<Vec<String>>()
        .join("\n")
}

pub fn create_log_path_and_get() -> String {
    let curr_path = if let Ok(log_path) = env::var("MONO_WAITER_LOG") {
        log_path
    } else {
        "./.monograph_waiter/logs".to_string()
    };
    let path_buf = Path::new(&curr_path);
    let rs = std::fs::create_dir_all(path_buf.as_os_str().to_str().unwrap());
    if let Err(err) = rs {
        println!("Create Log root error path={} err={:?}", curr_path, err);
    }
    curr_path + "/monograph_waiter.log"
}

pub fn cmd_status_ok<T>(input_status: &[(CmdDef, CmdStatus<T>)]) -> bool
    where
        T: Clone + Debug,
{
    input_status
        .iter()
        .filter(|(_, status)| !status.success)
        .count()
        == 0
}

pub fn extract_tar_cmd(file_name: String) -> CmdDef {
    let third_party = workspace_sub_dir(None).get("third_party").unwrap().clone();
    CmdDef {
        name: "tar".to_string(),
        args: Some(vec![
            "-zxvf".to_string(),
            format!("{}/{}", third_party, file_name),
            "-C".to_string(),
            third_party,
        ]),
        show_progress_type: Some("pipe".to_string()),
        payload: None,
    }
}

pub fn mk_data_dir_cmd(count: usize) -> PipeDef {
    let sub_dir = workspace_sub_dir(None);
    let data_dir = sub_dir.get("data").unwrap();
    let mut mkdir_cmd_vec = Vec::default();
    for i in 1..=count {
        mkdir_cmd_vec.push(CmdDef {
            name: "mkdir".to_string(),
            args: Some(vec!["-p".to_string(), format!("{}/data_{}", data_dir, i)]),
            show_progress_type: None,
            payload: None,
        });
    }
    PipeDef {
        cmd_vec: mkdir_cmd_vec,
    }
}

pub fn copy_data_dir_cmd(source_dir: String, dest_dir: Vec<String>) -> PipeDef {
    let sub_dir = workspace_sub_dir(None);
    let data_dir = sub_dir.get("data").unwrap();
    let mut cp_cmd = vec![];
    for dest in dest_dir {
        let absolute_dest_dir = format!("{}/{}", data_dir, dest);
        let cp_cmd_vec = vec!["mysql", "sys", "test", "performance_schema"]
            .iter()
            .map(|schema| {
                let source = format!("{}/{}/{}", data_dir, source_dir, schema);
                CmdDef {
                    name: "cp".to_string(),
                    args: Some(vec!["-r".to_string(), source, absolute_dest_dir.clone()]),
                    show_progress_type: None,
                    payload: None,
                }
            })
            .collect::<Vec<_>>();
        cp_cmd.extend(cp_cmd_vec);
    }
    PipeDef { cmd_vec: cp_cmd }
}

pub fn load_deps(deps_path: Option<String>) -> HashMap<String, Vec<String>> {
    let sys_dep_path = if let Some(path) = deps_path {
        format!("{}/deps", path)
    } else {
        format!(
            "{}/deps",
            std::env::var(MONOGRAPH_WATER_CONFIG_DIR).unwrap()
        )
    };
    let read_dir_rs = std::fs::read_dir(Path::new(sys_dep_path.as_str()));
    if read_dir_rs.is_err() {
        panic!(
            "Load system deps failure. Please check if the path = {}",
            sys_dep_path
        );
    }
    let read_dir = read_dir_rs.unwrap();
    read_dir
        .into_iter()
        .map(|dir_entry_rs| dir_entry_rs.unwrap())
        .filter(|dir_entry_rs| {
            let path = dir_entry_rs.path();
            path.is_file()
        })
        .filter(|file_entry| {
            let file_name_os_str = file_entry.file_name();
            let file_name_str = file_name_os_str.to_str().unwrap();
            SYSTEM_DEPS.contains(&file_name_str)
        })
        .map(|file_entry| {
            let path = file_entry.path();
            let file_name = path.file_name().unwrap();
            let file_name_str = file_name.to_str().unwrap().to_string();
            let file = std::fs::File::open(path).unwrap();
            let file_reader = BufReader::new(file);
            let dep_list = file_reader
                .lines()
                .filter_map(Result::ok)
                .collect::<Vec<_>>();

            (file_name_str, dep_list)
        })
        .collect::<HashMap<String, Vec<String>>>()
}

pub fn storage_service_running() -> bool {
    let sys = sysinfo::System::new_all();
    // for now monograph use cassandra
    let process_vec = sys.processes_by_name("java");
    let has_process = process_vec
        .into_iter()
        .filter(|process| {
            let process_cmd_args = process.cmd();
            process_cmd_args
                .iter()
                .filter(|cmd| !cmd.is_empty() && cmd.contains("cassandra"))
                .count()
                > 0
        })
        .count()
        > 0;
    println!("list java process {}", has_process);
    if !has_process {
        false
    } else {
        let cassandra_bin = env::var("CASSANDRA_BIN_DIR");
        if cassandra_bin.is_err() {
            println!("not found ENV CASSANDRA_BIN_DIR");
            false
        } else {
            let node_tools = CmdDef {
                name: "bash".to_string(),
                args: Some(vec![
                    "-c".to_string(),
                    format!("{}/nodetool status", cassandra_bin.unwrap()),
                ]),
                show_progress_type: None,
                payload: None,
            };
            println!("Check cassandra status {}", node_tools);
            let mut has_un_status = false;
            let mut node_status_outpout = String::new();
            let status: CmdStatus<()> = cmd_process(node_tools, |stdout| {
                node_status_outpout.push_str(format!("{}\n", stdout).as_str());
                has_un_status = stdout.starts_with("UN");
                println!("{}", stdout);
            });
            let mut has_host_name = false;
            if has_un_status {
                for line in node_status_outpout.lines() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    if !line.contains(char::is_whitespace) {
                        continue;
                    }
                    if line.starts_with("UN") {
                        let split_rs = line.split_whitespace().collect::<Vec<_>>();
                        has_host_name = split_rs.len() == 8;
                        println!("{:?}", split_rs);
                    }
                }
            }
            status.success && has_host_name
        }
    }
}

pub fn wait_storage_status_running(interval: Duration) -> bool {
    let mut retry_count = 0;
    loop {
        if retry_count > 500 {
            break;
        }
        if storage_service_running() {
            // dirty hack
            std::thread::sleep(Duration::from_secs(5));
            return true;
        } else {
            std::thread::sleep(interval);
            retry_count += 1
        }
    }
    storage_service_running()
}

pub fn start_storage_service_cmd(third_party_dir: Option<String>) -> CmdDef {
    let set_env = set_storage_env_cmd(third_party_dir);
    if set_env.is_err() {
        panic!(
            "set CASSANDRA_BIN_DIR Err. please check if \
        [$MONOGRAPH_WORKSPACE_DIR/thirty_party/cassandra_XXX] exists"
        );
    }
    let bin_dir = env::var("CASSANDRA_BIN_DIR").unwrap();
    let user_info = get_platform_info(None).clone().user;
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let in_ms = since_the_epoch.as_millis();
    let start_cmd = if user_info.has_sudo || user_info.is_root {
        println!("It is not recommended to start with the root user");
        format!(
            "{}/cassandra -f -R > {}/cassandra_start_{}.log 2>&1 &",
            bin_dir,
            format_args!("{}/..", bin_dir),
            in_ms
        )
    } else {
        format!(
            "{}/cassandra -f > {}/cassandra_start_{}.log 2>&1 &",
            bin_dir,
            format_args!("{}/..", bin_dir),
            in_ms
        )
    };
    CmdDef {
        name: "bash".to_string(),
        args: Some(vec!["-c".to_string(), start_cmd]),
        show_progress_type: None,
        payload: None,
    }
}

pub fn set_storage_env_cmd(dir: Option<String>) -> anyhow::Result<String> {
    let third_path = if let Some(third_party_dir) = dir {
        third_party_dir
    } else {
        let sub_dirs = workspace_sub_dir(None);
        sub_dirs.get("third_party").unwrap().to_string()
    };
    let red_dir = Path::new(third_path.as_str()).read_dir().unwrap();
    for dir_entry in red_dir {
        if dir_entry.is_err() {
            return Err(anyhow::Error::from(dir_entry.err().unwrap()));
        }
        let dir = dir_entry.unwrap();
        let path = dir.path();
        if path.is_dir() {
            let file_name = path.file_name().unwrap().to_str().unwrap();
            if file_name.contains("cassandra") {
                return Ok(format!("{}/bin", path.to_str().unwrap()));
            }
        }
    }
    Err(anyhow!("not found storage from {}", third_path))
}

pub fn workspace_is_empty(config: Option<String>) -> bool {
    let path_string = if let Some(config_path) = config {
        config_path
    } else {
        "".to_string()
    };
    let common = extract_config_value!("common", Common, path_string).clone();
    let workspace = common.workspace;
    let path = Path::new(workspace.as_str());
    if !path.exists() {
        true
    } else {
        std::fs::read_dir(path).unwrap().count() == 0
    }
}
