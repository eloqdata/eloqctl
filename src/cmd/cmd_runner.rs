use crate::cmd::base::{CmdContext, CmdStatus};
use std::collections::HashMap;
use std::fs::File;

pub struct CmdRunner<'s> {
    cmd_context_mapping: HashMap<String, CmdContext<&'s File>>,
}

impl<'s> CmdRunner<'s> {
    pub fn new(log: &'s File) -> Self {
        todo!()
        // let mut cmd_processor: HashMap<String, CmdContext<&'s File>> = HashMap::new();
        // for entry in CMD_DESC_MAP.iter() {
        //     cmd_processor.insert(entry.0.to_string(), CmdContext::new(entry.1.clone(), log));
        // }
        // Self {
        //     cmd_context_mapping: cmd_processor,
        // }
    }

    pub async fn run(&self, cmd: String) -> CmdStatus {
        CmdStatus::default()
        // let cmd_as_str = cmd.as_str();
        // let cmd_context = self.cmd_context_mapping.get(cmd_as_str);
        // match cmd_as_str {
        //     "check" => exec_flow(ListDeps {}, &mut cmd_context.unwrap().clone()),
        //     "install_deps" => exec_flow(InstallDeps {}, &mut cmd_context.unwrap().clone()),
        //     "setup_workspace" => {
        //         let build_workspace = SetupWorkspace {};
        //         let mut cmd_status = build_workspace.set_up();
        //         if cmd_status.success {
        //             cmd_status = build_workspace.exec_async().await;
        //         }
        //         cmd_status
        //     }
        //     _ => {
        //         unreachable!()
        //     }
        // }
    }
}
