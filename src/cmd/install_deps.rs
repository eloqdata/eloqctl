use crate::cmd::base::{CmdContext, CmdDef, CmdEnum, CmdStatus, CmdV2, PipeDef};
use crate::cmd::cmd_macro::CheckDeps;
use crate::cmd::cmd_utils::install_deps;
use std::io::Write;

pub struct InstallDeps;

impl InstallDeps {
    pub fn exec(&self, context: &mut CmdContext<impl Write>) -> Vec<(CmdDef, CmdStatus)> {
        let check_deps = CheckDeps {};
        let check_dep_rs = check_deps.exec(context);
        let mut install_dep_pipe = Vec::new();
        for (cmd, status) in &check_dep_rs {
            if !status.success {
                let args = cmd.clone().args.unwrap();
                let dep_name = args.get(1).unwrap();
                let install_dep = install_deps(dep_name.to_string());
                println!("Install Dep {}", install_dep);
                install_dep_pipe.push(install_dep);
            }
        }
        if install_dep_pipe.is_empty() {
            println!("Success, All dependencies are installed.");
            vec![]
        } else {
            context.record_context(CmdEnum::PipeExec(PipeDef {
                cmd_vec: install_dep_pipe,
            }))
        }
    }
}
