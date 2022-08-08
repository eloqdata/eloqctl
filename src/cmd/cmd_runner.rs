use crate::cmd::base::{CmdContext, CmdDef, CmdStatus, CmdV2};
use crate::cmd::cmd_macro::*;
use crate::cmd::cmd_utils::cmd_status_ok;
use crate::cmd::gen_mysql_cnf::GenMySQLConf;
use crate::cmd::install_deps::InstallDeps;
use crate::cmd::setup_workspace::SetupWorkspace;
use std::collections::HashMap;
use std::fs::File;

#[macro_export]
macro_rules! cmd_exec {
    ($self:ident, $cmd_str:expr, $cmd_impl:ident) => {{
        let context = $self.cmd_context_mapping.get($cmd_str);
        $cmd_impl {}.exec(&mut context.unwrap().clone())
    }};
}

#[macro_export]
macro_rules! cmd_context_mapping {
    ($log:expr $(,$cmd_string:expr)*) => {{
        let mut cmd_context_mapping = HashMap::new();
        $(
          cmd_context_mapping.insert($cmd_string, CmdContext::new($log));
        )*
        cmd_context_mapping
    }}
}

pub struct CmdRunner<'s> {
    cmd_context_mapping: HashMap<&'s str, CmdContext<&'s File>>,
}

impl<'s> CmdRunner<'s> {
    pub fn new(log: &'s File) -> Self {
        Self {
            cmd_context_mapping: cmd_context_mapping!(
                log,
                "check_deps",
                "install_deps",
                "setup_workspace",
                "ln_source",
                "gen_mysql_cnf",
                "build"
            ),
        }
    }

    pub async fn run(&self, cmd: String) -> Vec<(CmdDef, CmdStatus)> {
        let cmd_str = cmd.as_str();
        match cmd_str {
            "check_deps" => {
                cmd_exec!(self, cmd_str, CheckDeps)
            }
            "install_deps" => {
                cmd_exec!(self, cmd_str, InstallDeps)
            }
            "setup_workspace" => {
                let context = self.cmd_context_mapping.get(cmd_str);
                SetupWorkspace {}.exec(&mut context.unwrap().clone()).await
            }
            "ln_source" => {
                cmd_exec!(self, cmd_str, LinkMonographSource)
            }
            "gen_mysql_cnf" => {
                cmd_exec!(self, cmd_str, GenMySQLConf)
                // let context = self.cmd_context_mapping.get(cmd_str);
                // GenMySQLConf {}.exec(&mut context.unwrap().clone())
            }
            "build" => {
                let mut protobuf_build = cmd_exec!(self, cmd_str, ProtobufBuild);
                if !cmd_status_ok(&protobuf_build) {
                    protobuf_build
                } else {
                    let git_repo_build = cmd_exec!(self, cmd_str, GitRepoBuild);
                    protobuf_build.extend(git_repo_build);
                    protobuf_build
                }
            }
            _ => {
                unreachable!()
            }
        }
    }
}
