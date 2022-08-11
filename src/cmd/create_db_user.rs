use crate::cmd::base::{CmdContext, CmdDef, CmdStatus, CmdV2};
use std::io::Write;

pub struct CreateDBUser;

impl CmdV2 for CreateDBUser {
    type Executable = CmdDef;
    type StatsData = bool;

    fn definition(&self) -> CmdDef {
        todo!()
    }

    fn exec(&self, context: &mut CmdContext<impl Write>) -> Vec<(CmdDef, CmdStatus<bool>)> {
        todo!()
    }
}
