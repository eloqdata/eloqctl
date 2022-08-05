use crate::cmd;
use crate::cmd::cmd_utils::{cmd_process, get_process_bar};
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::io::Write;
use thiserror::Error;

#[derive(Debug, Clone)]
pub enum CmdEnum {
    CmdExec(CmdDef),
    PipeExec(PipeDef),
}

#[macro_export]
macro_rules! sync_cmd_impl {
    ($cmd_impl:ident, $cmd_obj:ident, $cmd_enum:ident, $cmd_build_closure:expr) => {
        #[derive(Clone, Debug)]
        pub struct $cmd_impl;

        impl Default for $cmd_impl {
            fn default() -> Self {
                $cmd_impl {}
            }
        }

        impl CmdV2 for $cmd_impl {
            type Executable = $cmd_obj;

            fn definition(&self) -> $cmd_obj {
                $cmd_build_closure()
            }

            fn exec(&self, context: &mut CmdContext<impl Write>) -> Vec<(CmdDef, CmdStatus)> {
                context.record_context(CmdEnum::$cmd_enum(self.definition()))
            }
        }
    };
}

sync_cmd_impl!(CheckDeps, PipeDef, PipeExec, || {
    cmd::cmd_utils::check_deps_as_pipe()
});

sync_cmd_impl!(MkdirWorkspace, CmdDef, CmdExec, || {
    use crate::config::{MONOGRAPH_WORKSPACE_DIR, WORKSPACE_LAYOUT};
    let workspace_dir = std::env::var(MONOGRAPH_WORKSPACE_DIR).unwrap();
    let workspace_layout = WORKSPACE_LAYOUT
        .iter()
        .map(|entry| format!("{}/{}", workspace_dir, entry.1))
        .collect::<Vec<_>>();
    let mut cmd_args = vec!["-p".to_string()];
    cmd_args.extend(workspace_layout);

    CmdDef {
        name: "mkdir".to_string(),
        args: Some(cmd_args),
        show_progress_type: None,
        payload: None,
    }
});

sync_cmd_impl!(LinkMonographSource, CmdDef, CmdExec, || {
    CmdDef {
        name: "bash".to_string(),
        args: Some(vec![
            "-c".to_string(),
            r#"
    #!/bin/bash
    source_dir=${MONOGRAPH_WORKSPACE_DIR}/source
    monograph_dir=${source_dir}/monograph
    mariadb_dir=${source_dir}/mariadb
    echo ${source_dir} ${monograph_dir} ${mariadb_dir}
    cd $mariadb_dir
    echo "MariaDB git submodule init"
    git_submodel_init="git submodule init"
    eval ${git_submodel_init}
    echo "Link Monograph Source"
    ln -s ${monograph_dir} ${mariadb_dir}/storage/monograph
    ln -s ${source_dir}/log_service ${source_dir}/tx_service/log_service
    ln -s ${source_dir}/cass ${monograph_dir}/cass
    ln -s ${source_dir}/tx_service ${monograph_dir}/tx_service
"#
            .to_string(),
        ]),
        show_progress_type: None,
        payload: None,
    }
});

#[derive(Error, Debug)]
pub enum CmdErrorCode {
    #[error("For now only support Linux and MacOS. current OS is {0}")]
    UnSupportOS(String),
}

#[derive(Clone, Debug)]
pub struct CmdDef {
    pub name: String,
    pub args: Option<Vec<String>>,
    pub show_progress_type: Option<String>,
    pub payload: Option<HashMap<String, String>>,
}

#[derive(Clone, Debug)]
pub struct PipeDef {
    pub cmd_vec: Vec<CmdDef>,
}

impl Default for PipeDef {
    fn default() -> Self {
        Self { cmd_vec: vec![] }
    }
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
                self.show_progress_type.clone().unwrap_or("".to_string()),
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
    /// Description of executable command, can be [`CmdDef`]  or [`Pipe`].
    /// for example : command -v brew.
    fn definition(&self) -> Self::Executable;
    /// Execute the command and log it, e.g.: brew list leveldb
    fn exec(&self, context: &mut CmdContext<impl Write>) -> Vec<(CmdDef, CmdStatus)>;
}

pub fn cmd_status_ok(input_status: &Vec<(CmdDef, CmdStatus)>) -> bool {
    input_status
        .iter()
        .filter(|(_, status)| !status.success)
        .count()
        == 0
}

#[async_trait]
pub trait AsyncCmd: 'static + Send + CmdV2 {
    type AsyncExistStatus;
    async fn async_exec(&self) -> Self::AsyncExistStatus;
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
        }
    }
}

#[derive(Clone, Debug)]
pub struct CmdContext<Log>
where
    Log: Write,
{
    log: Log,
}

impl<Log> CmdContext<Log>
where
    Log: Write,
{
    pub fn new(log: Log) -> Self {
        Self { log }
    }

    pub fn cmd_run(&mut self, cmd: CmdDef) -> CmdStatus {
        let mut runtime_log = String::default();
        let cmd_status = if let Some(progress_type) = cmd.clone().show_progress_type {
            let pb = get_process_bar(progress_type.as_str(), cmd.name.as_str());
            cmd_process(cmd.clone(), |output_by_line: &str| {
                runtime_log.push_str(format!("{}\n", output_by_line.clone()).as_str());
                pb.set_message(output_by_line.to_owned());
            })
        } else {
            cmd_process(cmd.clone(), |output_by_line: &str| {
                runtime_log.push_str(format!("{}\n", output_by_line.clone()).as_str());
                println!("{}", output_by_line);
            })
        };
        let write_status_to_log = writeln!(
            self.log,
            "command={}, \n{}\n,status={}",
            cmd, runtime_log, cmd_status
        );
        if let Err(write_log_err) = write_status_to_log {
            println!("write {:?} status to log error={:?}", cmd, write_log_err);
        }
        cmd_status
    }

    pub fn record_context(&mut self, cmd: CmdEnum) -> Vec<(CmdDef, CmdStatus)> {
        match cmd {
            CmdEnum::CmdExec(cmd_def) => {
                vec![(cmd_def.clone(), self.cmd_run(cmd_def))]
            }
            CmdEnum::PipeExec(pipe_def) => {
                let mut cmd_status_rs = Vec::new();
                for cmd_def in pipe_def.cmd_vec {
                    cmd_status_rs.push((cmd_def.clone(), self.cmd_run(cmd_def)));
                }
                cmd_status_rs
            }
        }
    }
}
