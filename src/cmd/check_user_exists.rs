use crate::cmd::base::{CmdContext, CmdDef, CmdStatus, CmdV2};
use crate::cmd::cmd_utils::get_platform_info;
use crate::cmd::mysql_ctl_util::{connect_mysqld_from_cli, list_mysql_cnf};
use indicatif::ProgressBar;
use std::io::Write;

pub struct CheckDBUser;

impl CmdV2 for CheckDBUser {
    type Executable = CmdDef;
    type StatsData = bool;

    fn definition(&self) -> CmdDef {
        let mysql_conf = list_mysql_cnf(None);
        if mysql_conf.is_empty() {
            println!("not found mysql config in $MONOGRAPH_WORKSPACE_DIR");
            return CmdDef::default();
        }
        let mut conn_mysql = connect_mysqld_from_cli(mysql_conf.first().unwrap().to_string());
        conn_mysql.push("-e".to_string());
        conn_mysql.push("select user from mysql.user where length(user)>0".to_string());
        CmdDef {
            name: "sudo".to_string(),
            args: Some(conn_mysql),
            show_progress_type: None,
            payload: None,
        }
    }

    fn exec(&self, context: &mut CmdContext<impl Write>) -> Vec<(CmdDef, CmdStatus<bool>)> {
        let cmd = self.definition();
        if cmd.is_empty() {
            return vec![(
                cmd,
                CmdStatus {
                    success: false,
                    output: None,
                    data: Some(false),
                },
            )];
        }
        let user = get_platform_info(None).user.clone();
        if !user.has_sudo && !user.is_root {
            return vec![(
                cmd,
                CmdStatus {
                    success: false,
                    output: Some("current user whether sudo privileges are available.".to_string()),
                    data: None,
                },
            )];
        }
        let mut user_table = Vec::default();
        let check_user_status =
            context.cmd_run(cmd.clone(), |record: &str, _: Option<ProgressBar>| {
                user_table.push(record.to_string());
                println!("{}", record);
            });
        if !check_user_status.success {
            return vec![(cmd, check_user_status)];
        }
        let has_mono = &user_table[1..]
            .into_iter()
            .filter(|user| user.clone().eq("mono"))
            .count()
            > &0_usize;
        vec![(
            cmd,
            CmdStatus {
                success: true,
                output: None,
                data: Some(has_mono),
            },
        )]
    }
}
