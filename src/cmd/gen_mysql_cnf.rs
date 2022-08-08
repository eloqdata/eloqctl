use crate::cmd::base::{CmdContext, CmdDef, CmdStatus, CmdV2};
use crate::config::workspace_sub_dir;
use crate::extract_config_value;
use std::io::Write;
use std::path::Path;

const MYSQL_INSTANCE_COUNT: usize = 3;
const MARIADB_SECTION: &str = "mariadb";
pub struct GenMySQLConf;

impl CmdV2 for GenMySQLConf {
    type Executable = CmdDef;

    fn definition(&self) -> CmdDef {
        CmdDef {
            name: "GenMySQLConf".to_string(),
            args: None,
            show_progress_type: None,
            payload: None,
        }
    }

    fn exec(&self, context: &mut CmdContext<impl Write>) -> Vec<(CmdDef, CmdStatus)> {
        let mut mysql_cnf = extract_config_value!("mysql", MySQL, None).clone();

        let local_ip = mysql_cnf.get(MARIADB_SECTION, "monograph_local_ip");
        if local_ip.is_none() {
            let err_msg = "not found config key monograph_local_ip";
            context.logging(err_msg.to_string());
            return self.error_status(err_msg);
        }

        println!("monograph_local_ip = {:?}", local_ip);
        let local_ip_rs = local_ip
            .unwrap()
            .split(':')
            .last()
            .unwrap()
            .parse::<usize>();
        if local_ip_rs.is_err() {
            let err_msg = "monograph_local_ip value illegal. The format must be IP:PORT";
            context.logging(err_msg.to_string());
            return self.error_status(err_msg);
        }

        let local_ip_port = local_ip_rs.unwrap();
        let monograph_ip_list = (0..MYSQL_INSTANCE_COUNT)
            .collect::<Vec<_>>()
            .iter()
            .map(|i| {
                let my_port = local_ip_port + (i * 10);
                format!("127.0.0.1:{}", my_port)
            })
            .collect::<Vec<_>>()
            .join(";");

        let workspace_sub_dir = workspace_sub_dir(None);
        let etc_dir = workspace_sub_dir.get("etc").unwrap().clone();
        let data_dir = workspace_sub_dir.get("data").unwrap();
        let install_dir = workspace_sub_dir.get("install").unwrap();

        let service_port = mysql_cnf.get("mariadb", "port").unwrap();
        let mysql_port: usize = service_port.parse().unwrap();

        for idx in 0..MYSQL_INSTANCE_COUNT {
            mysql_cnf.set(
                MARIADB_SECTION,
                "socket",
                Some(format!("/tmp/mysqld{}.sock", mysql_port + idx)),
            );
            mysql_cnf.set(
                MARIADB_SECTION,
                "port",
                Some((mysql_port + idx).to_string()),
            );

            mysql_cnf.set(MARIADB_SECTION, "datadir", Some(data_dir.clone()));
            mysql_cnf.set(
                MARIADB_SECTION,
                "lc_messages_dir",
                Some(format!("{}/share", install_dir.clone())),
            );

            mysql_cnf.set(
                MARIADB_SECTION,
                "monograph_local_ip",
                Some(format!("127.0.0.1:{}", local_ip_port + (idx * 10))),
            );

            mysql_cnf.set(
                MARIADB_SECTION,
                "monograph_ip_list",
                Some(monograph_ip_list.clone()),
            );
            let cnf_location =
                format!("{}/{}-{}.cnf", etc_dir, "my-conf", mysql_port + idx,).clone();
            println!("GenMySQLCnf at {}", cnf_location);

            let write_rs = mysql_cnf.write(Path::new(cnf_location.as_str()));
            if write_rs.is_err() {
                let err_msg = write_rs.err().unwrap();
                println!(
                    "GenMySQLConf save {} error Cause by {}",
                    cnf_location, err_msg
                );
                return vec![(
                    CmdDef::default(),
                    CmdStatus {
                        success: false,
                        output: Some(err_msg.to_string()),
                    },
                )];
            }
        }
        vec![(self.definition(), CmdStatus::default())]
    }
}

impl GenMySQLConf {
    fn error_status(&self, err_msg: &str) -> Vec<(CmdDef, CmdStatus)> {
        vec![(
            self.definition(),
            CmdStatus {
                success: false,
                output: Some(err_msg.to_string()),
            },
        )]
    }
}
