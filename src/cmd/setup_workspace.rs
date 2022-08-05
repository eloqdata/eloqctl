use crate::cmd::base::{CmdContext, CmdDef, CmdStatus, CmdV2};
use crate::cmd::cmd_macro::{LinkMonographSource, MkdirWorkspace};
use crate::cmd::cmd_utils::cmd_status_ok;
use crate::cmd::download_third_party::DownloadThirdParty;
use crate::cmd::git_clone_source::GitCloneSource;
use std::io::Write;

pub struct SetupWorkspace {}

impl SetupWorkspace {
    pub async fn exec(&self, context: &mut CmdContext<impl Write>) -> Vec<(CmdDef, CmdStatus)> {
        let mkdir_workspace = MkdirWorkspace {};
        let download_third_party = DownloadThirdParty {};
        let git_clone_source = GitCloneSource {};
        let mk_workspace = mkdir_workspace.exec(context);
        if !cmd_status_ok(&mk_workspace) {
            mk_workspace
        } else {
            let download_status = download_third_party.exec().await;
            if !cmd_status_ok(&download_status) {
                return download_status;
            }
            let git_clone_status = git_clone_source.exec().await;
            if !cmd_status_ok(&git_clone_status) {
                return git_clone_status;
            }
            let link_source = LinkMonographSource {}.exec(context);
            vec![
                &mk_workspace[..],
                &download_status[..],
                &git_clone_status[..],
                &link_source[..],
            ]
            .concat()
        }
    }
}
