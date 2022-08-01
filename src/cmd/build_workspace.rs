use crate::cmd::base::{Cmd, CmdStatus};
use crate::cmd::cmd_utils::{cmd_process, elapsed_progress_bar};
use crate::config::ConfigObject;
use crate::extract_config_value;
use async_trait::async_trait;
use futures::future::join_all;
use futures::stream::StreamExt;
use std::fs::File;
use std::io::Write;
use std::path::Path;

static WORKSPACE_LAYOUT: [&str; 6] = [
    "/monograph",
    "/monograph/datafarm",
    "/monograph/etc",
    "/monograph/install",
    "/monograph/source",
    "/monograph/third_party",
];

pub struct SetupWorkspace;

impl SetupWorkspace {
    async fn download_async(resource_url: String, download_path: String) {
        tokio::task::spawn(async move {
            let rsp_rs = reqwest::get(resource_url.clone()).await;

            if rsp_rs.is_err() {
                return Err(anyhow::Error::from(rsp_rs.err().unwrap()));
            }
            let rsp = rsp_rs.unwrap();
            // The files currently downloaded are tar.gz
            let download_file_name = rsp
                .url()
                .path_segments()
                .and_then(|segments| segments.last())
                .and_then(|file_name| {
                    if file_name.is_empty() {
                        None
                    } else {
                        Some(file_name)
                    }
                })
                .unwrap_or("tmp_download_file.tar.gz");

            println!(
                "Downloading from {}, file_name = {}",
                resource_url.clone(),
                download_file_name
            );

            let download_file_tmp_path = Path::new(download_path.as_str()).join(download_file_name);
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

            let pb = elapsed_progress_bar();
            pb.set_length(total_size);
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

                let new_progress =
                    std::cmp::min(downloaded + (chunk_bytes.len() as u64), total_size);
                downloaded = new_progress;
                pb.set_position(new_progress);
            }
            let pb_finish_msg = format!("Downloaded from {} to {}", resource_url, download_path);
            pb.finish_with_message(pb_finish_msg);
            Ok(())
        });
    }
}

#[async_trait]
impl Cmd for SetupWorkspace {
    fn set_up(&self) -> CmdStatus {
        let common_config = extract_config_value!("common", Common, None);
        let workspace_dir = common_config.clone().workspace;
        let workspace_layout = WORKSPACE_LAYOUT
            .to_vec()
            .iter()
            .map(|d| format!("{}/{}", workspace_dir, d))
            .collect::<Vec<_>>();
        let mut cmd_args = vec!["-p".to_string()];
        cmd_args.extend(workspace_layout);
        cmd_process("mkdir".to_string(), Some(cmd_args), |stdout: &str| {
            println!("create workspace {}", stdout);
        })
    }

    async fn exec_async(&self) -> CmdStatus {
        let common_config = extract_config_value!("common", Common, None);
        let download = common_config.clone().compile.download;
        let third_party = format!("{}/{}", common_config.workspace, "/monograph/third_party");
        let protobuf_download =
            SetupWorkspace::download_async(download.protobuf.url, third_party.clone());
        let cassandra = extract_config_value!("cassandra", Storage, None);
        let cassandra_download =
            SetupWorkspace::download_async(cassandra.clone().download.url, third_party);

        let download_rs = join_all(vec![protobuf_download, cassandra_download]).await;
        println!("Download all third party complete {:?}", download_rs);
        CmdStatus::default()
    }
}

#[cfg(test)]
mod tests {}
