use crate::cmd::base::{CmdDef, CmdStatus};
use crate::cmd::cmd_utils::elapsed_progress_bar;

use crate::cmd::cmd_const::{CASSANDRA_TAR_FILE_NAME, PROTOBUF_TAR_FILE_NAME};
use crate::config::MONOGRAPH_WORKSPACE_DIR;
use crate::extract_config_value;
use futures::stream::StreamExt;
use futures_util::future::join_all;
use indicatif::MultiProgress;
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[macro_export]
macro_rules! download_task {
    ($multi_progress:expr, $extract_closure:expr) => {{
        let extract_tuple = $extract_closure();
        let pb_m = $multi_progress.clone();
        let task_join = tokio::task::spawn(async move {
            let task_rs = DownloadThirdParty::download_async(
                pb_m,
                extract_tuple.0,
                format!("{}", extract_tuple.1),
                format!("{}/{}", extract_tuple.2, "/monograph/third_party"),
            )
            .await;
            if task_rs.is_err() {
                println!("{:?}", task_rs);
            }
        });
        task_join
    }};
}

pub struct DownloadThirdParty;

impl DownloadThirdParty {
    async fn download_async(
        multi_progress: MultiProgress,
        resource_url: String,
        download_file_name: String,
        download_dest_path: String,
    ) -> anyhow::Result<()> {
        let rsp_rs = reqwest::get(resource_url.clone()).await;
        if rsp_rs.is_err() {
            return Err(anyhow::Error::from(rsp_rs.err().unwrap()));
        }
        let rsp = rsp_rs.unwrap();
        let download_file_tmp_path =
            Path::new(download_dest_path.as_str()).join(download_file_name.clone());
        let download_file_rs = File::create(download_file_tmp_path.clone());

        if download_file_rs.is_err() {
            return Err(anyhow::Error::from(download_file_rs.err().unwrap()));
        }

        let total_size = rsp
            .content_length()
            .ok_or(format!(
                "Failed to get content length from '{}'",
                resource_url.clone()
            ))
            .unwrap();

        let pb = multi_progress.add(elapsed_progress_bar(
            Some(total_size),
            Some(download_file_name.clone()),
        ));

        let mut download_stream = rsp.bytes_stream();
        let mut download_file = download_file_rs.unwrap();
        let mut downloaded = 0_u64;
        while let Some(stream_chunk) = download_stream.next().await {
            if stream_chunk.is_err() {
                return Err(anyhow::Error::from(stream_chunk.err().unwrap()));
            }
            let chunk_bytes = stream_chunk.unwrap();
            let write_chunks = download_file.write_all(&chunk_bytes);
            if write_chunks.is_err() {
                return Err(anyhow::Error::from(write_chunks.err().unwrap()));
            }
            let new_progress = std::cmp::min(downloaded + (chunk_bytes.len() as u64), total_size);
            downloaded = new_progress;
            pb.set_position(downloaded);
        }
        pb.finish_with_message(format!("{} download compete", download_file_name.clone()));
        Ok(())
    }

    pub async fn exec(&self) -> Vec<(CmdDef, CmdStatus<()>)> {
        let multi_progress = MultiProgress::new();
        let workspace = std::env::var(MONOGRAPH_WORKSPACE_DIR)
            .unwrap_or_else(|_| panic!("MONOGRAPH_WORKSPACE_DIR not set"));

        let protobuf_download_cl = || {
            let common = extract_config_value!("common", Common, "".to_string());
            (
                common.clone().compile.download.protobuf.url,
                PROTOBUF_TAR_FILE_NAME,
                workspace.clone(),
            )
        };

        let cassandra_download_cl = || {
            let cassandra = extract_config_value!("cassandra", Storage, "".to_string());
            (
                cassandra.clone().download.url,
                CASSANDRA_TAR_FILE_NAME,
                workspace.clone(),
            )
        };
        let join_protobuf = download_task!(multi_progress, protobuf_download_cl);
        let join_cassandra = download_task!(multi_progress, cassandra_download_cl);
        let download_join_all = join_all(vec![join_protobuf, join_cassandra]).await;
        multi_progress.clear().unwrap();
        let status = if download_join_all.is_empty() {
            println!("WARN: Join download task is empty.");
            CmdStatus {
                success: false,
                output: Some("Download task may be failed".to_string()),
                data: None
            }
        } else {
            println!("Download third_party complete");
            CmdStatus::default()
        };

        vec![(
            CmdDef {
                name: "Download Protobuf and Cassandra".to_string(),
                args: None,
                show_progress_type: None,
                payload: None,
            },
            status,
        )]
    }
}
