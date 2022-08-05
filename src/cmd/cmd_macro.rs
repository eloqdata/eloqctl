use crate::cmd::base::*;
use crate::cmd::cmd_utils::*;

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

            fn definition(&self) -> $cmd_obj {
                $cmd_build_closure()
            }

            fn exec(
                &self,
                context: &mut CmdContext<impl std::io::Write>,
            ) -> Vec<(CmdDef, CmdStatus)> {
                context.record_context(CmdEnum::$cmd_enum(self.definition()))
            }
        }
    };
}

sync_cmd_impl!(CheckDeps, PipeDef, PipeExec, || { check_deps_as_pipe() });

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

sync_cmd_impl!(LinkMonographSource, CmdDef, CmdExec, || {
    CmdDef {
        name: "bash".to_string(),
        args: Some(vec![
            "-c".to_string(),
            r#"
    #!/bin/bash
    source_dir=${MONOGRAPH_WORKSPACE_DIR}/source
    monograph_dir=${source_dir}/monograph
    mariadb_dir=${source_dir}/mariadb
    echo ${source_dir} ${monograph_dir} ${mariadb_dir}
    cd $mariadb_dir
    echo "MariaDB git submodule init"
    git_submodel_init="git submodule init"
    eval ${git_submodel_init}
    echo "Link Monograph Source"
    ln -s ${monograph_dir} ${mariadb_dir}/storage/monograph
    ln -s ${source_dir}/log_service ${source_dir}/tx_service/log_service
    ln -s ${source_dir}/cass ${monograph_dir}/cass
    ln -s ${source_dir}/tx_service ${monograph_dir}/tx_service
"#
            .to_string(),
        ]),
        show_progress_type: None,
        payload: None,
    }
});
