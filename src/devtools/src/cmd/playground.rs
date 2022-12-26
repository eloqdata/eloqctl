use crate::cmd::base::{CmdContext, CmdDef, CmdStatus, CmdV2};
use crate::cmd::check_user_exists::CheckDBUser;
use crate::cmd::cmd_macro::CreateDBUser;
use crate::cmd::cmd_utils::cmd_status_ok;
use crate::cmd::ctl_mysql_process::{CtlMySQLProcess, MySQLOpCode};
use crate::cmd::exec_init_sql_script::ExecSqlScript;
use std::io::Write;

/// If needed, start the MonographDB service,
/// create the user and the create the tables, and insert.
pub struct Playground;

impl CmdV2 for Playground {
    type Executable = CmdDef;
    type StatsData = ();

    fn definition(&self) -> CmdDef {
        CmdDef {
            name: "playground".to_string(),
            args: None,
            show_progress_type: None,
            payload: None,
        }
    }

    fn exec(&self, context: &mut CmdContext<impl Write>) -> Vec<(CmdDef, CmdStatus<()>)> {
        let start_mysql = CtlMySQLProcess {
            op_code: MySQLOpCode::Start,
        };
        let start_service_if_need = start_mysql.exec(context);
        if !cmd_status_ok(&start_service_if_need) {
            return start_service_if_need;
        }
        let check_db_user: Vec<(CmdDef, CmdStatus<bool>)> = CheckDBUser {}.exec(context);
        if !cmd_status_ok(&check_db_user) {
            println!("Check database user [mono] failed.");
            return vec![(self.definition(), CmdStatus::default())];
        }
        let (_, db_user_exists) = check_db_user.first().unwrap().clone();
        println!("User momo exists = {:?}", db_user_exists.data);
        if !db_user_exists.data.unwrap() {
            let create_user_rs = CreateDBUser {}.exec(context);
            if !cmd_status_ok(&create_user_rs) {
                println!("Create database user failed. ");
                return vec![(self.definition(), CmdStatus::default())];
            }
            println!("create mono success.");
        } else {
            let exec_sql_script = ExecSqlScript {};
            let run_sql_status = exec_sql_script.exec(context);
            if !cmd_status_ok(&run_sql_status) {
                println!("exec script failed");
                return vec![(self.definition(), CmdStatus::default())];
            }
        }
        vec![(
            self.definition(),
            CmdStatus {
                success: true,
                output: None,
                data: None,
            },
        )]
    }
}
