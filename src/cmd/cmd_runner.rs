use crate::cmd::base::{CmdContext, CmdDef, CmdStatus, CmdV2};
use crate::cmd::cmd_macro::CheckDeps;
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

pub struct CmdRunner<'s> {
    cmd_context_mapping: HashMap<&'s str, CmdContext<&'s File>>,
}

impl<'s> CmdRunner<'s> {
    pub fn new(log: &'s File) -> Self {
        let mut cmd_context_mapping = HashMap::new();
        cmd_context_mapping.insert("check_deps", CmdContext::new(log));
        cmd_context_mapping.insert("install_deps", CmdContext::new(log));
        cmd_context_mapping.insert("setup_workspace", CmdContext::new(log));
        Self {
            cmd_context_mapping,
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
            _ => {
                unreachable!()
            }
        }
    }
}
