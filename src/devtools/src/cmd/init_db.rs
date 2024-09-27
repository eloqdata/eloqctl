use crate::cmd::base::{CmdContext, CmdDef, CmdStatus, CmdV2};
use crate::cmd::cmd_macro::{CopySchemData, InitMySQLInstance, MkDataDir, StoragePrepare};
use crate::cmd::cmd_utils::{cmd_status_ok, wait_storage_status_running};
use std::io::Write;
use std::time::Duration;

pub struct InitDB;

impl InitDB {
    pub fn exec(&self, context: &mut CmdContext<impl Write>) -> Vec<(CmdDef, CmdStatus<()>)> {
        let set_storage_env_status = StoragePrepare {}.exec(context);
        if !cmd_status_ok(&set_storage_env_status) {
            return set_storage_env_status;
        }
        println!("storage prepare success");
        let is_un_status = wait_storage_status_running(Duration::from_millis(500));
        if !is_un_status {
            println!("cassandra is still unavailable. pleas check");
            return vec![(
                CmdDef {
                    name: "init_db".to_string(),
                    args: None,
                    show_progress_type: None,
                    payload: None,
                },
                CmdStatus::default(),
            )];
        }
        let init_db_instance = InitMySQLInstance {};
        let db_instance_status = init_db_instance.exec(context);
        if !cmd_status_ok(&db_instance_status) {
            return db_instance_status;
        }
        println!("MySQL DB Initialize success");
        let mk_data_dir_status = MkDataDir {}.exec(context);
        if !cmd_status_ok(&mk_data_dir_status) {
            return mk_data_dir_status;
        }
        println!("mk data dir success");
        let copy_schema_data = CopySchemData {}.exec(context);
        if !cmd_status_ok(&copy_schema_data) {
            return copy_schema_data;
        }
        println!("copy data dir success");
        [
            &set_storage_env_status[..],
            &db_instance_status[..],
            &mk_data_dir_status[..],
            &copy_schema_data[..],
        ]
        .concat()
    }
}
