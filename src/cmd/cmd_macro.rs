use crate::cmd::base::*;
use crate::cmd::cmd_utils::*;
use crate::extract_config_value;
use crate::{build_script, cmd};

#[macro_export]
macro_rules! check_deps_cmds {
    ($platform:expr, $check_cmd:expr, $check_cmd_arg:expr) => {{
        use $crate::cmd::base::CmdDef;
        $platform
            .deps
            .iter()
            .map(|dep| CmdDef {
                name: $check_cmd.to_string(),
                args: Some(vec![$check_cmd_arg.to_string(), dep.to_string()]),
                show_progress_type: Some("pipe".to_string()),
                payload: None,
            })
            .collect::<Vec<_>>()
    }};
}

#[macro_export]
macro_rules! sync_cmd_impl {
    ($cmd_impl:ident, $cmd_obj:ident, $cmd_enum:ident, $cmd_build_closure:expr) => {
        #[derive(Clone, Debug)]
        pub struct $cmd_impl;

        impl Default for $cmd_impl {
            fn default() -> Self {
                $cmd_impl {}
            }
        }

        impl CmdV2 for $cmd_impl {
            type Executable = $cmd_obj;
            type StatsData = ();

            fn definition(&self) -> $cmd_obj {
                $cmd_build_closure()
            }

            fn exec(
                &self,
                context: &mut CmdContext<impl std::io::Write>,
            ) -> Vec<(CmdDef, CmdStatus<()>)> {
                context.run_and_record_context(CmdEnum::$cmd_enum(self.definition()))
            }
        }
    };
}

sync_cmd_impl!(CheckDeps, PipeDef, PipeExec, || {
    let platform = get_platform_info(None);
    println!("current OS Name is {}", platform.os_type);
    match platform.os_type.as_str() {
        "darwin" => PipeDef {
            cmd_vec: check_deps_cmds!(platform.clone(), "brew", "list"),
        },
        "ubuntu" => PipeDef {
            cmd_vec: check_deps_cmds!(platform.clone(), "dpkg", "-s"),
        },
        _ => {
            panic!("not support platform");
        }
    }
});

sync_cmd_impl!(MkdirWorkspace, CmdDef, CmdExec, || {
    use crate::config::{MONOGRAPH_WORKSPACE_DIR, WORKSPACE_LAYOUT};
    let workspace_dir = std::env::var(MONOGRAPH_WORKSPACE_DIR).unwrap();
    let workspace_layout = WORKSPACE_LAYOUT
        .iter()
        .map(|entry| format!("{}/{}", workspace_dir, entry.1))
        .collect::<Vec<_>>();
    let mut cmd_args = vec!["-p".to_string()];
    cmd_args.extend(workspace_layout);

    CmdDef {
        name: "mkdir".to_string(),
        args: Some(cmd_args),
        show_progress_type: None,
        payload: None,
    }
});

sync_cmd_impl!(ExtractTarFile, PipeDef, PipeExec, || {
    use cmd::cmd_const::{CASSANDRA_TAR_FILE_NAME, PROTOBUF_TAR_FILE_NAME};
    let extract_protobuf = extract_tar_cmd(PROTOBUF_TAR_FILE_NAME.to_string());
    let extract_cassandra = extract_tar_cmd(CASSANDRA_TAR_FILE_NAME.to_string());
    PipeDef {
        cmd_vec: vec![extract_protobuf, extract_cassandra],
    }
});

sync_cmd_impl!(LinkMonographSource, CmdDef, CmdExec, || {
    CmdDef {
        name: "bash".to_string(),
        args: Some(vec![
            "-c".to_string(),
            r#"
    #!/bin/bash
    source_dir=${MONOGRAPH_WORKSPACE_DIR}/monograph/source
    monograph_dir=${source_dir}/monograph
    mariadb_dir=${source_dir}/mariadb
    printf "workspace source dir %s \n" ${source_dir}
    printf "workspace monograph source dir %s \n" ${monograph_dir}
    printf "workspace mariadb  source dir %s \n" ${mariadb_dir}
    cd ${mariadb_dir}
    pwd
    echo "MariaDB git submodule init"
    git_submodel_init="git submodule init"
    eval ${git_submodel_init}
    echo "Link Monograph Source"
    ln -nsf ${source_dir}/log_service ${source_dir}/tx_service/log_service
    ln -nsf ${source_dir}/cass ${monograph_dir}/cass
    ln -nsf ${source_dir}/tx_service ${monograph_dir}/tx_service
    ln -nsf ${monograph_dir} ${mariadb_dir}/storage/monograph
"#
            .to_string(),
        ]),
        show_progress_type: None,
        payload: None,
    }
});

sync_cmd_impl!(ProtobufBuild, PipeDef, PipeExec, || {
    build_script!(download, "".to_string(), protobuf)
});

sync_cmd_impl!(GitRepoBuild, PipeDef, PipeExec, || {
    build_script!(git, "".to_string(), brpc, braft, catch2, aws, mariadb)
});

sync_cmd_impl!(BuildMonograph, PipeDef, PipeExec, || {
    build_script!(git, "".to_string(), mariadb)
});

sync_cmd_impl!(MkDataDir, PipeDef, PipeExec, || { mk_data_dir_cmd(3) });
// TODO: fixed hard code
sync_cmd_impl!(CopySchemData, PipeDef, PipeExec, || {
    copy_data_dir_cmd(
        "data_0".to_string(),
        vec![
            "data_1".to_string(),
            "data_2".to_string(),
            "data_3".to_string(),
        ],
    )
});

sync_cmd_impl!(InitMySQLInstance, CmdDef, CmdExec, || {
    let common = extract_config_value!("common", Common, "".to_string()).clone();
    let init_script = common.initialize_script;
    CmdDef {
        name: "bash".to_string(),
        args: Some(vec!["-c".to_string(), init_script]),
        show_progress_type: None,
        payload: None,
    }
});

sync_cmd_impl!(StoragePrepare, PipeDef, PipeExec, || {
    let mut cmd_vec = vec![];
    let cassandra_bin = set_storage_env_cmd(None).unwrap();
    std::env::set_var("CASSANDRA_BIN_DIR", cassandra_bin);
    if !storage_service_running() {
        let start_cassandra = start_storage_service_cmd(None);
        cmd_vec.push(start_cassandra);
    }
    PipeDef { cmd_vec }
});

#[cfg(test)]
mod tests {
    use crate::build_script;
    use crate::cmd::base::*;
    use crate::cmd::cmd_utils::get_platform_info;
    use crate::config::MONOGRAPH_WATER_CONFIG_DIR;
    use crate::extract_config_value;

    // build time too long
    #[test]
    #[ignore]
    pub fn test_monograph_build() {
        let config_path = format!("{}/config", env!("CARGO_MANIFEST_DIR").to_owned());
        std::env::set_var(MONOGRAPH_WATER_CONFIG_DIR, config_path.clone());
        let platform = get_platform_info(Some(config_path));
        println!("platform = {:?}", platform.clone());
        if platform.os_type.eq("ubuntu") {
            println!("current platform is ubuntu");
            sync_cmd_impl!(TestMariaDBBuild, PipeDef, PipeExec, || {
                build_script!(git, "".to_string(), mariadb)
            });

            let stdout = std::io::stdout();
            let mut context = CmdContext::new(stdout);
            TestMariaDBBuild {}.exec(&mut context);
        }
    }
}
