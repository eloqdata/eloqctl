use crate::cmd::base::{CmdContext, CmdDef, CmdStatus, CmdV2};
use crate::cmd::check_mysql_status::CheckMysqlStatus;
use crate::cmd::cmd_macro::StoragePrepare;
use crate::cmd::cmd_utils::{cmd_status_ok, wait_storage_status_running};
use crate::cmd::mysql_ctl_util::list_mysql_cnf;
use std::io::Write;
use std::path::Path;
use std::time::Duration;
use sysinfo::{Pid, PidExt, ProcessExt, SystemExt};

#[derive(Clone, Debug)]
pub enum MySQLOpCode {
    Start,
    Stop,
}

impl From<MySQLOpCode> for String {
    fn from(op_code: MySQLOpCode) -> Self {
        match op_code {
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
    pub op_code: MySQLOpCode,
}

impl CmdV2 for CtlMySQLProcess {
    type Executable = CmdDef;
    type StatsData = ();

    fn definition(&self) -> CmdDef {
        CmdDef {
            name: format!("CtlMySQLProcess:{:?}", self.op_code.clone()),
            args: None,
            show_progress_type: None,
            payload: None,
        }
    }

    fn exec(&self, context: &mut CmdContext<impl Write>) -> Vec<(CmdDef, CmdStatus<()>)> {
        println!("Receive OP_CODE={:?}", self.op_code);
        let mut mysql_cnf_list = list_mysql_cnf(None);
        if mysql_cnf_list.is_empty() {
            println!("MySQL config file not exists.in etc directory");
            return vec![(
                CmdDef::default(),
                CmdStatus {
                    success: false,
                    output: Some("not found mysql config in etc".to_string()),
                    data: None,
                },
            )];
        }
        let check_mysql = CheckMysqlStatus {};
        let check_mysql_status = check_mysql.exec(context);
        if !cmd_status_ok(&check_mysql_status) {
            return vec![(
                CmdDef {
                    name: "check_mysql_status".to_string(),
                    args: None,
                    show_progress_type: None,
                    payload: None,
                },
                CmdStatus::default(),
            )];
        }
        let (_, monnograph_process_list) = check_mysql_status.first().unwrap();
        assert!(monnograph_process_list.data.is_some());
        let process_list = monnograph_process_list.data.clone().unwrap();
        for process in process_list {
            let config = process.config_file();
            if config.is_none() {
                continue;
            }
            let cnf = config.unwrap();
            let process_file_name = Path::new(cnf.as_str())
                .file_name()
                .unwrap()
                .to_str()
                .unwrap();
            mysql_cnf_list.retain(|x| {
                let file_name = Path::new(x.as_str()).file_name().unwrap().to_str().unwrap();
                file_name == process_file_name
            });
        }
        let mut vec_rs = vec![];
        match self.op_code.clone() {
            MySQLOpCode::Start => {
                let start_storage_if_need = StoragePrepare {}.exec(context);
                if !cmd_status_ok(&start_storage_if_need) {
                    println!("Storage Service may be not running. start storage service failed.");
                    return start_storage_if_need;
                }
                let storage_status_running =
                    wait_storage_status_running(Duration::from_millis(500));
                if !storage_status_running {
                    println!("cassandra is still unavailable. pleas check");
                    return vec![(self.definition(), CmdStatus::default())];
                }
                println!("use mysql config list = {:?}", mysql_cnf_list);
                for cnf in &mysql_cnf_list {
                    println!("start mysql use default_file={}", cnf);
                    let file_name = Path::new(cnf.as_str())
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap();
                    let start_script = format!(
                        r#"
                      #!/bin/bash
                      install_dir=${{MONOGRAPH_WORKSPACE_DIR}}/monograph/install
                      echo ${{install_dir}}
                      now=`date +%F_%H_%M_%S`
                      log_file={}_${{now}}.log
                      mkdir_log_dir="mkdir -p ${{install_dir}}/logs/"
                      eval ${{mkdir_log_dir}}
                      start_cmd="${{install_dir}}/bin/mysqld --defaults-file={} > ${{install_dir}}/logs/${{log_file}} 2>&1 &"
                      echo ${{start_cmd}}
                      eval ${{start_cmd}}
                    "#,
                        file_name.replace(".cnf", ""),
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
                for process in monnograph_process_list.data.clone().unwrap() {
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
