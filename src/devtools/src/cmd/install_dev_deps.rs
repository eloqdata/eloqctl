use crate::cmd::base::{CmdContext, CmdDef, CmdStatus, CmdV2};
use crate::cmd::cmd_macro::ExtractProtobufFile;
use crate::cmd::cmd_utils::cmd_status_ok;
use crate::cmd::download_third_party::{DownloadTarget, DownloadThirdParty};
use crate::cmd::git_clone_source::{GitCloneSource, GitRepository};
use std::io::Write;

pub struct InstallDevDeps;

impl InstallDevDeps {
    pub async fn exec(&self, context: &mut CmdContext<impl Write>) -> Vec<(CmdDef, CmdStatus<()>)> {
        let download_protobuf = DownloadThirdParty::new(DownloadTarget::Protobuf);

        let download_protobuf_status = download_protobuf.exec().await;
        if !cmd_status_ok(&download_protobuf_status) {
            download_protobuf_status
        } else {
            println!("download protobuf success.");
            let extract_protobuf = ExtractProtobufFile {};
            let extract_protobuf_status = extract_protobuf.exec(context);
            if !cmd_status_ok(&extract_protobuf_status) {
                return extract_protobuf_status;
            }
            let git_clone_source = GitCloneSource::new(GitRepository::BuildAndRunDeps);
            let git_clone_status = git_clone_source.exec().await;
            if !cmd_status_ok(&git_clone_status) {
                return git_clone_status;
            }
            println!("git clone build and runtime dependencies success.");
            [
                &download_protobuf_status[..],
                &extract_protobuf_status[..],
                &git_clone_status[..],
            ]
            .concat()
        }
    }
}
