use crate::cmd::base::{CmdContext, CmdDef, CmdStatus, CmdV2};
use crate::cmd::mysql_ctl_util::list_mysql_cnf;
use std::fmt::Debug;
use std::io::Write;
use std::path::Path;
use sysinfo::{PidExt, ProcessExt, ProcessStatus, SystemExt};

#[derive(Clone, Debug)]
pub struct CheckMysqlStatus;

#[derive(Clone, Default, Debug)]
pub struct MySQLProcess {
    pub(crate) pid: u32,
    pub(crate) cmd: Vec<String>,
}

impl MySQLProcess {
    fn extract_cmd_arg(&self, arg_name: &str) -> Vec<String> {
        self.cmd
            .iter()
            .filter(|arg| (*arg).clone().contains(arg_name))
            .cloned()
            .collect::<Vec<String>>()
    }

    pub fn is_monograph_instance(&self, monograph_conf_list: &[String]) -> bool {
        println!("MonographDB config lis = {:?}", monograph_conf_list);
        let exec_cmd = self
            .cmd
            .iter()
            .filter(|arg| (*arg).contains("--defaults-file="))
            .cloned()
            .collect::<Vec<_>>();
        if exec_cmd.is_empty() {
            false
        } else {
            for arg in exec_cmd {
                if !arg.contains("--defaults-file=") {
                    continue;
                }
                let config_file = arg.replace("--defaults-file=", "");
                let file_name = Path::new(config_file.as_str())
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();
                println!("current mysql process config file {}", file_name);
                if monograph_conf_list.contains(&file_name) {
                    return true;
                }
            }
            false
        }
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
    type StatsData = Vec<MySQLProcess>;

    fn definition(&self) -> CmdDef {
        CmdDef {
            name: "check_mysql_status".to_string(),
            args: None,
            show_progress_type: None,
            payload: None,
        }
    }

    fn exec(
        &self,
        context: &mut CmdContext<impl Write>,
    ) -> Vec<(CmdDef, CmdStatus<Vec<MySQLProcess>>)> {
        let sys = sysinfo::System::new_all();
        let process_list = sys.processes_by_name("mysqld");
        let mut mysql_process_vec: Vec<MySQLProcess> = Vec::new();
        let monograph_conf_list = list_mysql_cnf(None)
            .iter()
            .map(|p| {
                Path::new(p.as_str())
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string()
            })
            .collect::<Vec<_>>();
        for process in process_list {
            //println!("{:?}", process);
            if process.status() == ProcessStatus::Zombie {
                continue;
            }
            let pid = process.pid();
            let process_info = MySQLProcess {
                pid: pid.as_u32(),
                cmd: process.cmd().to_vec(),
            };
            if !process_info.is_monograph_instance(&monograph_conf_list) {
                continue;
            }
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
                data: Some(mysql_process_vec),
            },
        )]
    }
}
