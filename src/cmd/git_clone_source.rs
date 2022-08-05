use crate::cmd::base::{CmdDef, CmdStatus};
use crate::cmd::cmd_utils::cmd_process;
use crate::{extract_config_value, git_clone};
use futures_util::future::join_all;
use std::sync::Arc;
use tokio::sync::Semaphore;

static GIT_CLONE_SEMAPHORE: usize = 3;
pub struct GitCloneSource;

impl GitCloneSource {
    pub async fn exec(&self) -> Vec<(CmdDef, CmdStatus)> {
        let common = extract_config_value!("common", Common, None);
        let git = common.clone().compile.git;
        let git_clone_cmd_vec = git_clone!(
            git,
            brpc,
            braft,
            catch2,
            tx_service,
            log_service,
            monograph,
            aws,
            cass,
            mariadb
        );
        let mut join_all_task_vec = Vec::new();
        let semaphore = Arc::new(Semaphore::new(GIT_CLONE_SEMAPHORE));
        for git_clone_cmd in git_clone_cmd_vec.clone() {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let task_handler = tokio::task::spawn(async move {
                let cmd_status = cmd_process(git_clone_cmd, |stdout| {
                    println!("{}", stdout);
                });
                drop(permit);
                cmd_status
            });
            join_all_task_vec.push(task_handler);
        }
        let git_clone_all_rs = join_all(join_all_task_vec).await;
        println!("{:?}", git_clone_all_rs);
        let git_cmd_err_count = git_clone_all_rs.iter().filter(|rs| rs.is_err()).count();
        if git_cmd_err_count == 0 {
            CmdStatus::default()
        } else {
            CmdStatus {
                success: false,
                output: Some(format!("git clone repo failure {:?}", git_clone_cmd_vec)),
            }
        };

        vec![]
    }
}
