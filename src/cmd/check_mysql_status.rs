use crate::cmd::base::{CmdContext, CmdDef, CmdStatus, CmdV2};
use crate::config::workspace_sub_dir;
use std::fmt::Debug;
use std::io::Write;
use sysinfo::{PidExt, ProcessExt, SystemExt};

#[derive(Clone, Debug)]
pub struct CheckMysqlStatus;

#[derive(Clone, Default, Debug)]
pub struct MySQLProcess {
    pub(crate) pid: u32,
    pub(crate) cmd: Vec<String>,
    pub(crate) work_dir: String,
}

impl MySQLProcess {
    pub fn extract_cmd_arg(&self, arg_name: &str) -> Vec<String> {
        let sub_dirs = workspace_sub_dir(None).clone();
        let etc_dir = sub_dirs.get("etc").unwrap();
        self.cmd
            .iter()
            .filter(|arg| (*arg).clone().contains(arg_name))
            .filter(|arg| arg.contains(etc_dir))
            .map(|arg| arg.clone())
            .collect::<Vec<String>>()
    }

    pub fn is_monograph_instance(&self) -> bool {
        self.extract_cmd_arg("defaults-file").is_empty()
    }

    pub fn config_file(&self) -> Option<String> {
        let default_files = self.extract_cmd_arg("defaults-file");
        if default_files.is_empty() {
            None
        } else {
            Some(
                default_files
                    .first()
                    .unwrap()
                    .replace("--defaults-file=", ""),
            )
        }
    }
}

impl CmdV2 for CheckMysqlStatus {
    type Executable = CmdDef;
    type StatsData = MySQLProcess;

    fn definition(&self) -> CmdDef {
        CmdDef {
            name: "check_mysql_status".to_string(),
            args: None,
            show_progress_type: None,
            payload: None,
        }
    }

    fn exec(&self, context: &mut CmdContext<impl Write>) -> Vec<(CmdDef, CmdStatus<MySQLProcess>)> {
        let sys = sysinfo::System::new_all();
        let process_list = sys.processes_by_name("mysqld"); //.processes_by_name("mysqld");
        let mut mysql_process_vec = Vec::new();
        for process in process_list {
            let pid = process.pid();
            let mut process_info = MySQLProcess::default();
            process_info.pid = pid.as_u32();
            process_info.cmd = process.cmd().to_vec();
            process_info.work_dir = process.cwd().to_str().unwrap().clone().to_string();
            context.logging(format!(
                "mysqld pid={:?}/cmd={:?}/cwd={:?}\n",
                pid,
                process.cmd(),
                process.cwd()
            ));
            mysql_process_vec.push(process_info);
        }
        println!("found mysql process={:#?}", mysql_process_vec);
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
