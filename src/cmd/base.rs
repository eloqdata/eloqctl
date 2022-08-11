use crate::cmd::cmd_utils::{cmd_process, get_process_bar};
use async_trait::async_trait;
use indicatif::ProgressBar;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::io::Write;
use thiserror::Error;

#[derive(Debug, Clone)]
pub enum CmdEnum {
    CmdExec(CmdDef),
    PipeExec(PipeDef),
}

#[derive(Error, Debug)]
pub enum CmdErrorCode {
    #[error("For now only support Ubuntu and MacOS. current OS is {0}")]
    UnSupportOS(String),
}

#[derive(Clone, Debug)]
pub struct CmdDef {
    pub name: String,
    pub args: Option<Vec<String>>,
    pub show_progress_type: Option<String>,
    pub payload: Option<HashMap<String, String>>,
}

impl CmdDef {
    pub fn is_empty(&self) -> bool {
        self.name.is_empty()
    }
}

#[derive(Clone, Debug, Default)]
pub struct PipeDef {
    pub cmd_vec: Vec<CmdDef>,
}

impl Display for CmdDef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let args = if let Some(arg_vec) = self.args.clone() {
            arg_vec.join(" ")
        } else {
            "None".to_string()
        };

        write!(
            f,
            "{}",
            format_args!(
                "{} {} {} payload={:#?}",
                self.name,
                args,
                self.show_progress_type
                    .clone()
                    .unwrap_or_else(|| "".to_string()),
                self.payload
            )
        )
    }
}

impl CmdDef {
    pub fn cmd_string(&self) -> String {
        let args_string = if let Some(cmd_args) = &self.args {
            cmd_args.join(" ")
        } else {
            "".to_string()
        };
        format!("{} {}", self.name, args_string)
    }
}

impl Default for CmdDef {
    fn default() -> Self {
        Self {
            name: "".to_string(),
            args: None,
            show_progress_type: None,
            payload: None,
        }
    }
}

pub trait CmdV2: 'static + Send {
    type Executable: Default;
    type StatsData;
    /// Description of executable command, can be [`CmdDef`]  or [`PipeDef`].
    /// for example : command -v brew.
    fn definition(&self) -> Self::Executable;
    /// Execute the command and log it, e.g.: brew list leveldb
    fn exec(
        &self,
        context: &mut CmdContext<impl Write>,
    ) -> Vec<(CmdDef, CmdStatus<Self::StatsData>)>
    where
        Self::StatsData: Clone + Debug;
}

#[async_trait]
pub trait AsyncCmd: 'static + Send + CmdV2 {
    type AsyncExistStatus;
    async fn async_exec(&self) -> Self::AsyncExistStatus;
}

#[derive(Clone, Debug)]
pub struct UserInfo {
    pub is_root: bool,
    pub has_sudo: bool,
    pub user_name: String,
}

#[derive(Clone, Debug)]
pub struct Platform {
    pub os_type: String,
    pub arch: String,
    pub family: String,
    pub deps: Vec<String>,
    pub user: UserInfo,
}

// #[allow(dead_code)]
// #[derive(Clone, Debug)]
// pub struct CmdStatus {
//     pub(crate) success: bool,
//     pub(crate) output: Option<String>,
//     pub(crate) data: Option<HashMap<String, String>>,
// }

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct CmdStatus<T>
where
    T: Clone + Debug,
{
    pub success: bool,
    pub output: Option<String>,
    pub data: Option<T>,
}

impl Default for CmdStatus<()> {
    fn default() -> Self {
        CmdStatus {
            success: false,
            output: None,
            data: None,
        }
    }
}

impl Display for CmdStatus<()> {
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

impl Default for CmdStatus<String> {
    fn default() -> Self {
        Self {
            success: true,
            output: None,
            data: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CmdContext<Log>
where
    Log: Write,
{
    logger: Log,
}

pub fn default_stdout_process(stdout: &str, progress_bar: Option<ProgressBar>) {
    if let Some(pb) = progress_bar {
        pb.set_message(stdout.to_owned());
    } else {
        println!("{}", stdout);
    }
}

impl<Log> CmdContext<Log>
where
    Log: Write,
{
    pub fn new(log: Log) -> Self {
        Self { logger: log }
    }

    pub fn cmd_run<F, S>(&mut self, cmd: CmdDef, mut cmd_stdout: F) -> CmdStatus<S>
    where
        F: FnMut(&str, Option<ProgressBar>),
        S: Clone + Debug,
    {
        let mut runtime_log = String::default();
        let cmd_status = if let Some(progress_type) = cmd.clone().show_progress_type {
            let pb = get_process_bar(progress_type.as_str(), cmd.name.as_str());
            cmd_process(cmd.clone(), |output_by_line: &str| {
                runtime_log.push_str(output_by_line);
                cmd_stdout(output_by_line, Some(pb.clone()));
            })
        } else {
            cmd_process(cmd.clone(), |output_by_line: &str| {
                runtime_log.push_str(output_by_line);
                cmd_stdout(output_by_line, None);
            })
        };
        let write_status_to_log = writeln!(
            self.logger,
            "command={}, \n{}\n,status={:?}",
            cmd, runtime_log, cmd_status
        );
        if let Err(write_log_err) = write_status_to_log {
            println!("write {:?} status to log error={:?}", cmd, write_log_err);
        }
        cmd_status
    }

    pub fn logging(&mut self, log: String) {
        let _rs = writeln!(self.logger, "{}", log);
    }

    pub fn run_and_record_context(&mut self, cmd: CmdEnum) -> Vec<(CmdDef, CmdStatus<()>)> {
        match cmd {
            CmdEnum::CmdExec(cmd_def) => {
                let cmd_status = self.cmd_run(cmd_def.clone(), |stdout, pb| {
                    default_stdout_process(stdout, pb)
                });
                vec![(cmd_def, cmd_status)]
            }
            CmdEnum::PipeExec(pipe_def) => {
                let mut cmd_status_rs: Vec<(CmdDef, CmdStatus<()>)> = Vec::new();
                for cmd_def in pipe_def.cmd_vec {
                    let cmd_status = self.cmd_run(cmd_def.clone(), |stdout, pb| {
                        default_stdout_process(stdout, pb)
                    });
                    cmd_status_rs.push((cmd_def.clone(), cmd_status));
                }
                cmd_status_rs
            }
        }
    }
}
