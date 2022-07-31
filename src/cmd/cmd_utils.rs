use crate::cmd::base::{CmdStatus, Platform, SUPPORT_CMD_LIST};
use crate::output_handle;
use indicatif::{ProgressBar, ProgressStyle};
use std::env;
use std::fs::{File, OpenOptions};
use std::io::BufRead;
use std::path::Path;

pub fn curr_platform() -> Platform {
    Platform {
        os_type: env::consts::OS.to_string(),
        arch: env::consts::ARCH.to_string(),
        family: env::consts::FAMILY.to_string(),
    }
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
        "elapsed" => elapsed_progress_bar(),
        _ => unreachable!(),
    }
}

pub fn pipe_progress_bar(cmd_str: String) -> ProgressBar {
    let cmd_pb = ProgressBar::new_spinner();
    cmd_pb.enable_steady_tick(200);
    cmd_pb.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("/|\\- ")
            .template(&format!("{{spinner:.dim.bold}} {}: {{wide_msg}}", cmd_str)),
    );
    cmd_pb
}

pub fn elapsed_progress_bar() -> ProgressBar {
    let cmd_pb = ProgressBar::new(0_u64);
    cmd_pb.set_style(ProgressStyle::default_bar().template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.green/white}] {bytes}/{total_bytes} ({eta})")
        .progress_chars("#>-"));
    cmd_pb
}

pub fn cmd_process<F>(cmd_name: String, args: Option<Vec<String>>, mut stdout_f: F) -> CmdStatus
where
    F: FnMut(&str),
{
    let mut cmd = std::process::Command::new(cmd_name.as_str());
    if let Some(cmd_args) = args {
        for arg in cmd_args {
            cmd.arg(arg.as_str());
        }
    }
    let mut p = cmd
        .stderr(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    output_handle!(p.stdout.take().unwrap(), stdout_f, false);
    let stderr_output_vec = output_handle!(
        p.stderr.take().unwrap(),
        |stderr: &str| {
            println!("❗{}", stderr);
        },
        true
    );
    let stderr_output = stderr_output_vec
        .iter()
        .map(|stderr_str| stderr_str.clone() + "\n")
        .collect::<String>();
    let exists_rs = p.wait();
    println!("ExistsRs={:?}", exists_rs);
    if let Ok(exitstatus) = exists_rs {
        CmdStatus {
            success: exitstatus.success(),
            output: Some(stderr_output),
            status_file: None,
        }
    } else {
        CmdStatus {
            success: false,
            output: Some(stderr_output),
            status_file: None,
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
