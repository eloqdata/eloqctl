use crate::cmd::base::{CmdContext, CmdDef, CmdStatus, CmdV2};
use crate::cmd::check_mysql_status::CheckMysqlStatus;
use crate::cmd::cmd_utils::cmd_status_ok;
use crate::config::workspace_sub_dir;
use std::io::Write;
use std::path::Path;
use sysinfo::{Pid, PidExt, ProcessExt, SystemExt};
use crate::cmd::mysql_ctl_util::list_mysql_cnf;

#[derive(Clone, Debug)]
pub enum MySQLOpCode {
    Start,
    Stop,
}

impl Into<String> for MySQLOpCode {
    fn into(self) -> String {
        match self {
            MySQLOpCode::Start => "start".to_string(),
            MySQLOpCode::Stop => "stop".to_string(),
        }
    }
}

impl From<String> for MySQLOpCode {
    fn from(op_code_string: String) -> Self {
        match op_code_string.to_lowercase().as_str() {
            "start" => MySQLOpCode::Start,
            "stop" => MySQLOpCode::Stop,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CtlMySQLProcess {
    op_code: MySQLOpCode,
}

impl CmdV2 for CtlMySQLProcess {
    type Executable = CmdDef;
    type StatsData = ();

    fn definition(&self) -> CmdDef {
        CmdDef {
            name: self.op_code.clone().into(),
            args: None,
            show_progress_type: None,
            payload: None,
        }
    }

    fn exec(&self, context: &mut CmdContext<impl Write>) -> Vec<(CmdDef, CmdStatus<()>)> {
        let check_mysql = CheckMysqlStatus {};
        let check_mysql_status = check_mysql.exec(context);
        if !cmd_status_ok(&check_mysql_status) {
            return check_mysql_status
                .iter()
                .map(|(cmd, status)| {
                    let mut new_status: CmdStatus<()> = CmdStatus::default();
                    new_status.success = status.success;
                    new_status.output = status.clone().output;
                    (cmd.clone(), new_status)
                })
                .collect::<Vec<_>>();
        }

        let mut mysql_cnf_list = list_mysql_cnf(None);
        let monnograph_process = check_mysql_status
            .iter()
            .filter(|(_, status)| status.clone().data.unwrap().is_monograph_instance())
            .filter(|(_, status)| status.clone().data.unwrap().config_file().is_some())
            .map(|(_, status)| status.clone().data.unwrap())
            .collect::<Vec<_>>();

        mysql_cnf_list.retain(|cnf| {
            monnograph_process
                .iter()
                .filter(|process| {
                    let start_config = process.config_file().unwrap();
                    let file_name_os_str = Path::new(&start_config).file_name().unwrap();
                    let file_name = file_name_os_str.to_str().unwrap();
                    !cnf.eq(file_name)
                })
                .count()
                > 0
        });
        let mut vec_rs = vec![];
        match self.op_code.clone() {
            MySQLOpCode::Start => {
                for cnf in mysql_cnf_list {
                    println!("start mysql use default_file={}", cnf);
                    let start_script = format!(
                        r#"
                      #!/bin/bash
                      install_dir=${{MONOGRAPH_WORKSPACE_DIR}}/monograph/install
                      now=`date +%F_%H_%M_%S`
                      log_file={}_${{now}}.log
                      start_cmd="${{install_dir}}/bin/mysqld --defaults-file=${{MONOGRAPH_WORKSPACE_DIR}}/monograph/etc/{} > ${{log_file}} 2>&1 &"
                    "#,
                        cnf.replace(".", ""),
                        cnf
                    );
                    let cmd = CmdDef {
                        name: "bash".to_string(),
                        args: Some(vec!["-c".to_string(), start_script]),
                        show_progress_type: None,
                        payload: None,
                    };
                    let status = context.cmd_run(cmd.clone(), |stdout, _| {
                        println!("{}", stdout);
                    });
                    vec_rs.push((cmd, status));
                }
            }
            MySQLOpCode::Stop => {
                let sys = sysinfo::System::new_all();
                for process in monnograph_process {
                    let sys_process = sys.process(Pid::from_u32(process.pid));
                    let mut kill_cmd_status = CmdStatus::default();
                    if let Some(pro) = sys_process {
                        let kill_success = pro.kill();
                        kill_cmd_status.success = kill_success;
                        println!("{:?} stop {}", process, kill_success);
                    } else {
                        let output_msg =
                            format!("{:?} not exit. Maybe the process has exited", process);
                        kill_cmd_status.output = Some(output_msg.clone());
                        println!("{}", output_msg);
                    }
                    vec_rs.push((
                        CmdDef {
                            name: "kill".to_string(),
                            args: Some(vec![process.pid.to_string()]),
                            show_progress_type: None,
                            payload: None,
                        },
                        kill_cmd_status,
                    ))
                }
            }
        }
        vec_rs
    }
}
