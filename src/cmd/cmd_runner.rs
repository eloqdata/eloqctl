use crate::cmd::base::{Cmd, CmdContext, CmdStatus, CMD_DESC_MAP};
use crate::cmd::check_env::CheckEnv;
use std::collections::HashMap;
use std::fs::File;

pub struct CmdRunner<'s> {
    cmd_context_mapping: HashMap<String, CmdContext<&'s File>>,
}

impl<'s> CmdRunner<'s> {
    pub fn new(log: &'s File) -> Self {
        let mut cmd_processor: HashMap<String, CmdContext<&'s File>> = HashMap::new();
        for entry in CMD_DESC_MAP.iter() {
            cmd_processor.insert(entry.0.to_string(), CmdContext::new(entry.1.clone(), log));
        }
        Self {
            cmd_context_mapping: cmd_processor,
        }
    }

    pub fn run(&self, cmd: String) -> CmdStatus {
        let cmd_as_str = cmd.as_str();
        let cmd_context = self.cmd_context_mapping.get(cmd_as_str).unwrap();
        match cmd_as_str {
            "check" => CheckEnv {}.run_flow(&mut cmd_context.clone()),
            _ => {
                unreachable!()
            }
        }
    }
}
