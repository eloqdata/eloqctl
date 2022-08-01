use crate::cmd::check_env::CheckEnv;
use crate::cmd::cmd_utils::{cmd_process, get_process_bar};
use async_trait::async_trait;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::path::PathBuf;
use thiserror::Error;

pub static MONO_WATER_CONF: &str = "MONO_WATER_CONF_DIR";

lazy_static! {
    pub static ref SUPPORT_CMD_LIST: Vec<&'static str> = vec![
        "check",
        "setup_workspace",
        "playground",
        "stop_all",
        "start_all"
    ];
    pub static ref CMD_DESC_MAP: HashMap<&'static str, CmdDesc> = {
        let mut cmd_desc_mapping = HashMap::new();
        cmd_desc_mapping.insert("check", CheckEnv {}.cmd_desc());
        cmd_desc_mapping
    };
}

#[macro_export]
macro_rules! output_handle {
    ($cmd_output:expr, $output_by_line:expr, $has_output_post:expr) => {{
        let mut output_vec: Vec<String> = Vec::default();
        let buffer_reader = std::io::BufReader::new($cmd_output);
        for line in buffer_reader.lines() {
            let line = line.unwrap();
            let stripped_line = line.trim();
            if !stripped_line.is_empty() {
                $output_by_line(stripped_line);
            }
            if $has_output_post {
                output_vec.push(stripped_line.to_string() + "\n");
            }
        }
        output_vec
    }};
}

#[derive(Error, Debug)]
pub enum CmdErrorCode {
    #[error("For now only support Linux and MacOS. current OS is {0}")]
    UnSupportOS(String),
}

#[derive(Clone, Debug)]
pub struct CmdDesc {
    pub name: String,
    pub args: Option<Vec<String>>,
    pub show_progress_type: Option<String>,
    pub payload: Option<HashMap<String, String>>,
}

impl Default for CmdDesc {
    fn default() -> Self {
        Self {
            name: "".to_string(),
            args: None,
            show_progress_type: None,
            payload: None,
        }
    }
}

#[async_trait]
pub trait Cmd: 'static + Send {
    /// Command unique identifier
    fn cmd_desc(&self) -> CmdDesc {
        CmdDesc::default()
    }
    /// The action is executed before the command is executed. For example, modifying configuration files,
    /// setting environment variables, etc., is not required to implement
    fn set_up(&self) -> CmdStatus {
        CmdStatus::default()
    }
    /// Execute the OS command in a synchronized way, e.g.: brew list leveldb
    fn exec(&self, context: &mut CmdContext<impl Write>) -> CmdStatus {
        println!("Trait Cmd Exec= {:?}", self.cmd_desc());
        context.record_context()
    }
    /// Actions executed after the command finishes running,
    /// such as cleaning up specific resources, are not required to be implemented.
    fn tear_down(&self) -> CmdStatus {
        CmdStatus::default()
    }

    async fn exec_async(&self) -> CmdStatus {
        CmdStatus::default()
    }

    fn run_flow(&self, context: &mut CmdContext<impl Write>) -> CmdStatus {
        let mut cmd_status = self.set_up();
        cmd_status = if !cmd_status.success {
            cmd_status
        } else {
            cmd_status = self.exec(context);
            if !cmd_status.success {
                cmd_status
            } else {
                self.tear_down()
            }
        };
        cmd_status
    }
}

#[derive(Clone, Debug)]
pub struct Platform {
    pub os_type: String,
    pub arch: String,
    pub family: String,
}

#[derive(Clone, Debug)]
pub struct CmdStatus {
    pub(crate) success: bool,
    pub(crate) output: Option<String>,
    pub(crate) status_file: Option<PathBuf>,
}

impl Display for CmdStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let prefix = if self.success {
            "✅ success"
        } else {
            "❗error"
        };
        let output_string = if let Some(output) = self.output.clone() {
            output
        } else {
            "".to_string()
        };
        write!(f, "{}", format_args!("{} {}", prefix, output_string))
    }
}

impl Default for CmdStatus {
    fn default() -> Self {
        Self {
            success: true,
            output: None,
            status_file: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CmdContext<Log>
where
    Log: Write,
{
    cmd: CmdDesc,
    log: Log,
}

impl<Log> CmdContext<Log>
where
    Log: Write,
{
    pub fn new(cmd_desc: CmdDesc, log: Log) -> Self {
        Self { cmd: cmd_desc, log }
    }

    pub fn get_cmd_desc(&self) -> CmdDesc {
        self.cmd.clone()
    }

    pub fn record_context(&mut self) -> CmdStatus {
        let cmd_status = if let Some(progress_type) = self.cmd.clone().show_progress_type {
            let pb = get_process_bar(progress_type.as_str(), self.cmd.name.as_str());
            cmd_process(
                self.cmd.clone().name,
                self.cmd.clone().args,
                |output_by_line: &str| {
                    pb.set_message(output_by_line.to_owned());
                },
            )
        } else {
            cmd_process(
                self.cmd.clone().name,
                self.cmd.clone().args,
                |output_by_line: &str| {
                    println!("{}", output_by_line);
                },
            )
        };
        let write_status_to_log =
            writeln!(self.log, "Command={:?}, Status={}", self.cmd, cmd_status);
        if let Err(write_log_err) = write_status_to_log {
            println!(
                "write {:?} status to log error={:?}",
                self.cmd, write_log_err
            );
        }
        cmd_status
    }
}
