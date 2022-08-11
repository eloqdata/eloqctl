use crate::cmd::base::{CmdContext, CmdDef, CmdEnum, CmdStatus, CmdV2, PipeDef, Platform};
use crate::cmd::cmd_macro::CheckDeps;
use crate::cmd::cmd_utils::get_platform_info;
use std::io::Write;

pub struct InstallDeps;

impl InstallDeps {
    pub fn exec(&self, context: &mut CmdContext<impl Write>) -> Vec<(CmdDef, CmdStatus<()>)> {
        let check_deps = CheckDeps {};
        let check_dep_rs = check_deps.exec(context);
        let mut install_dep_pipe = Vec::new();
        let platform = get_platform_info(None);
        let user_info = platform.clone().user;
        println!("install_dep exec current user {:?}", user_info);
        let use_sudo = !user_info.is_root && user_info.has_sudo;
        for (cmd, status) in &check_dep_rs {
            if !status.success {
                let args = cmd.clone().args.unwrap();
                let dep_name = args.get(1).unwrap();
                let install_dep =
                    install_deps_cmd(dep_name.to_string(), platform.clone(), use_sudo);
                println!("Package {} not install {}", dep_name, install_dep);
                install_dep_pipe.push(install_dep);
            }
        }
        if install_dep_pipe.is_empty() {
            println!("Success, All dependencies are installed.");
            vec![]
        } else {
            context.run_and_record_context(CmdEnum::PipeExec(PipeDef {
                cmd_vec: install_dep_pipe,
            }))
        }
    }
}

fn install_deps_cmd(dep: String, platform: Platform, use_sudo: bool) -> CmdDef {
    match platform.os_type.as_str() {
        "darwin" => CmdDef {
            name: "brew".to_string(),
            args: Some(vec!["install".to_string(), dep]),
            show_progress_type: None,
            payload: None,
        },
        "ubuntu" => {
            let def_cmd_args = vec!["install".to_string(), "-y".to_string(), dep];
            let cmd = if use_sudo {
                (
                    "/usr/bin/sudo".to_string(),
                    vec![&vec!["apt-get".to_string()][..], &def_cmd_args[..]].concat(),
                )
            } else {
                ("apt-get".to_string(), def_cmd_args)
            };
            CmdDef {
                name: cmd.0,
                args: Some(cmd.1),
                show_progress_type: Some("pipe".to_string()),
                payload: None,
            }
        }
        _ => {
            panic!("not support platform");
        }
    }
}
