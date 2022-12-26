use crate::cmd::base::{CmdContext, CmdDef, CmdStatus, CmdV2};
use crate::cmd::mysql_ctl_util::get_mysql_prepare_cmd;
use crate::config::MONOGRAPH_WATER_CONFIG_DIR;
use std::fmt::Debug;
use std::io::Write;
use std::path::Path;

pub struct ExecSqlScript;

impl CmdV2 for ExecSqlScript {
    type Executable = CmdDef;
    type StatsData = ();

    fn definition(&self) -> CmdDef {
        let config_path_rs = std::env::var(MONOGRAPH_WATER_CONFIG_DIR);
        if config_path_rs.is_err() {
            panic!("ENV MONOGRAPH_WATER_CONFIG_DIR not exists.");
        }
        let config_path = config_path_rs.unwrap();
        let script_path = format!("{}/mysql/init.sql", config_path);
        let script_rs = std::fs::read_to_string(Path::new(script_path.as_str()));
        if script_rs.is_err() {
            println!("{}", script_rs.err().unwrap());
            panic!("Load init.sql script Error");
        }
        let script = script_rs.unwrap();
        let sql_vec = script.split(';');
        let mut args = vec![];
        for sql in sql_vec {
            args.push(sql.to_string());
        }
        CmdDef {
            name: "exec_init_sql_script".to_string(),
            args: Some(args),
            show_progress_type: None,
            payload: None,
        }
    }

    fn exec(
        &self,
        context: &mut CmdContext<impl Write>,
    ) -> Vec<(CmdDef, CmdStatus<Self::StatsData>)>
    where
        Self::StatsData: Clone + Debug,
    {
        let sql_cmd = self.definition();
        for sql in sql_cmd.args.unwrap() {
            println!(r#"Exec SQL {}"#, sql.clone());
            let exec_sql_cmd = get_mysql_prepare_cmd(
                "root".to_string(),
                None,
                None,
                vec!["-e".to_string(), sql.to_string()],
            );
            let status: CmdStatus<()> = context.cmd_run(exec_sql_cmd.clone(), |stdout, _| {
                println!("{}", stdout);
            });
            if !status.success {
                return vec![(self.definition(), status)];
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
