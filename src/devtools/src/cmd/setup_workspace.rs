use crate::cmd::base::{CmdContext, CmdDef, CmdStatus, CmdV2};
use crate::cmd::cmd_macro::{ExtractTarFile, MkdirWorkspace};
use crate::cmd::cmd_utils::{cmd_status_ok, workspace_is_empty};
use crate::cmd::download_third_party::DownloadThirdParty;
use crate::cmd::git_clone_source::GitCloneSource;
use std::io::Write;

pub struct SetupWorkspace {}

impl SetupWorkspace {
    // TODO use macro impl
    pub async fn exec(&self, context: &mut CmdContext<impl Write>) -> Vec<(CmdDef, CmdStatus<()>)> {
        if !workspace_is_empty(None) {
            println!("current workspace dir is not empty");
            return vec![(
                CmdDef::default(),
                CmdStatus {
                    success: false,
                    output: Some("workspace dir is not empty.please clear it".to_string()),
                    data: None,
                },
            )];
        }
        let mkdir_workspace = MkdirWorkspace {};
        let download_third_party = DownloadThirdParty {};
        let git_clone_source = GitCloneSource {};
        let extract_tar = ExtractTarFile {};
        let mk_workspace = mkdir_workspace.exec(context);
        if !cmd_status_ok(&mk_workspace) {
            mk_workspace
        } else {
            println!("mkdir workspace success.");
            let download_status = download_third_party.exec().await;
            if !cmd_status_ok(&download_status) {
                return download_status;
            }
            println!("download third party resource success.");
            let extract_status = extract_tar.exec(context);
            if !cmd_status_ok(&extract_status) {
                return extract_status;
            }
            println!("extract tar file success.");
            let git_clone_status = git_clone_source.exec().await;
            if !cmd_status_ok(&git_clone_status) {
                return git_clone_status;
            }
            println!("git clone third party source code success.");
            vec![
                &mk_workspace[..],
                &download_status[..],
                &extract_status[..],
                &git_clone_status[..],
            ]
            .concat()
        }
    }
}
