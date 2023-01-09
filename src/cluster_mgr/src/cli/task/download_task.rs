use crate::cli::config::{DeploymentConfig, DownloadUrl};
use crate::cli::task::ssh_conn::{SSH_EXEC_CMD, SSH_EXEC_CMD_OUTPUT, SSH_EXEC_CMD_STATUS};
use crate::cli::task::task_base::CmdErr::DownloadErr;
use crate::cli::task::task_base::{
    ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::{download_dir, file_process_progress};
use anyhow::anyhow;
use futures::stream::StreamExt;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use tracing::{error, info};

pub const MONOGRAPH_DEFAULT_FILE_NAME: &str = "monographdb-release-bin.tar.gz";

pub(crate) const DOWNLOAD_URL: &str = "download_url";
pub(crate) const DOWNLOAD_PATH: &str = "download_path";

pub(crate) const MONOGRAPH_DOWNLOAD_TASK: &str = "monogrphdb_download";
pub(crate) const CASSANDRA_DOWNLOAD_TASK: &str = "cassandra_download";
pub(crate) const ALL_DOWNLOAD_TASKS: [&str; 2] = [MONOGRAPH_DOWNLOAD_TASK, CASSANDRA_DOWNLOAD_TASK];

#[derive(Debug, Clone)]
pub struct DownloadFromRemoteTask {
    task_id: TaskId,
}

impl DownloadFromRemoteTask {
    pub fn from_config(
        config: &DeploymentConfig,
    ) -> anyhow::Result<IndexMap<TaskId, TaskInstance>> {
        let deployment_cloned = &config.deployment;
        let mono_download_url_string = deployment_cloned.install_image.clone();
        let mono_download_url = DownloadUrl::from_url_str(mono_download_url_string.as_str())?;

        let mut download_url_vec = vec![];
        if !mono_download_url.is_local() {
            download_url_vec.push(mono_download_url_string);
        }

        if let Some(cassandra) = &config.deployment.storage_service.cassandra {
            let cass_download_url_string = &cassandra.download_url;
            let cass_download_url = DownloadUrl::from_url_str(cass_download_url_string.as_str())?;
            if !cass_download_url.is_local() {
                download_url_vec.push(cass_download_url_string.to_string());
            }
        }
        let download_dir = download_dir();
        let local_ip = local_ip_address::local_ip()?.to_string();
        let download_tasks = download_url_vec
            .into_iter()
            .map(|download_url| {
                let task_name = if download_url.to_lowercase().contains("monograph") {
                    MONOGRAPH_DOWNLOAD_TASK
                } else {
                    CASSANDRA_DOWNLOAD_TASK
                };
                let task_id = TaskId {
                    cmd: "deploy".to_string(),
                    task: task_name.to_string(),
                    host: local_ip.to_string(),
                };
                (
                    task_id.clone(),
                    TaskInstance {
                        task_input: HashMap::from([
                            (DOWNLOAD_URL.to_string(), TaskArgValue::Str(download_url)),
                            (
                                DOWNLOAD_PATH.to_string(),
                                TaskArgValue::Str(download_dir.to_str().unwrap().to_string()),
                            ),
                        ]),
                        task: Box::new(DownloadFromRemoteTask::new(task_id)),
                        task_host: TaskHost::Local,
                    },
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>();
        Ok(download_tasks)
    }

    pub fn new(task_id: TaskId) -> Self {
        Self { task_id }
    }
}

#[async_trait::async_trait]
impl TaskExecutor for DownloadFromRemoteTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        _task_host: TaskHost,
        task_input: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        println!("{} execute.\n", self.task_id.pretty_string());
        let download_url =
            TaskArgValue::into_inner_value::<String>(task_input.get(DOWNLOAD_URL).unwrap().clone());
        let download_dir = TaskArgValue::into_inner_value::<String>(
            task_input.get(DOWNLOAD_PATH).unwrap().clone(),
        );
        let download_path = PathBuf::from(download_dir.as_str());
        info!("DownloadTask start download_url={}", download_url);
        let client = reqwest::Client::new();
        let download_url_cloned = download_url.clone();
        let rsp_rs = client.get(download_url.as_str()).send().await;
        if let Err(rsp_err) = rsp_rs {
            error!(
                "Download cli error cause by http client request error {:?}",
                rsp_err
            );
            return Err(anyhow!(DownloadErr(
                download_url.clone(),
                rsp_err.to_string()
            )));
        } else {
        }

        let http_response = rsp_rs.unwrap();
        let status = http_response.status();

        if !status.is_success() {
            error!(
                "Download cli error cause by http status_code = {:?}",
                status.as_str()
            );
            return Err(anyhow!(DownloadErr(
                download_url_cloned,
                status.to_string()
            )));
        }

        let create_download_path_rs = std::fs::create_dir_all(download_path.as_path());
        if create_download_path_rs.is_err() {
            error!("Download cli create tmp_dir error {:?}", download_path);
            return Err(anyhow!(DownloadErr(
                download_url_cloned,
                create_download_path_rs.err().unwrap().to_string()
            )));
        }

        let download_file_tuple = {
            let download_file_name = http_response
                .url()
                .path_segments()
                .and_then(|segments| segments.last())
                .and_then(|name| if name.is_empty() { None } else { Some(name) })
                .unwrap_or(MONOGRAPH_DEFAULT_FILE_NAME);

            let file_length = http_response.content_length().unwrap();
            (download_file_name, file_length)
        };
        let file_name = download_file_tuple.0.to_string();
        let file_len = download_file_tuple.1;
        let local_file_path = download_path.join(file_name.clone());
        if local_file_path.exists() {
            info!(
                "The local file {:?} exists. please delete it if you want to re-download it first.",
                local_file_path.clone()
            );
            return Ok(None);
        }

        let create_local_file_rs = std::fs::File::create(local_file_path.as_path());
        if create_local_file_rs.is_err() {
            return Err(anyhow!(DownloadErr(
                download_url_cloned,
                create_local_file_rs.err().unwrap().to_string()
            )));
        }

        let mut download_file = create_local_file_rs.unwrap();
        let mut downloaded = 0_u64;
        let pb = file_process_progress(file_len, format!("DOWNLOAD [{}]", file_name), "#>-");

        let mut stream_reader = http_response.bytes_stream();
        while let Some(stream_chunk) = stream_reader.next().await {
            if stream_chunk.is_err() {
                return Err(anyhow!(DownloadErr(
                    download_url_cloned,
                    stream_chunk.err().unwrap().to_string()
                )));
            }
            if let Ok(chunk) = stream_chunk {
                if let Err(write_err) = download_file.write_all(&chunk) {
                    return Err(anyhow!(DownloadErr(
                        download_url_cloned,
                        write_err.to_string()
                    )));
                }
                let new_progress = std::cmp::min(downloaded + (chunk.len() as u64), file_len);
                downloaded = new_progress;
                pb.set_position(downloaded);
            } else {
                let stream_chunk_err = stream_chunk.err().unwrap().to_string();
                error!("DownloadTask write local file error {}", stream_chunk_err);
                return Err(anyhow!(DownloadErr(download_url_cloned, stream_chunk_err)));
            }
        }
        pb.finish_with_message(format!("{} download compete", file_name));

        let mut download_result = HashMap::new();

        download_result.insert(
            SSH_EXEC_CMD.to_string(),
            TaskArgValue::Str(self.task_id.format_string()),
        );
        download_result.insert(
            SSH_EXEC_CMD_OUTPUT.to_string(),
            TaskArgValue::Str(local_file_path.to_str().unwrap().to_string()),
        );

        download_result.insert(SSH_EXEC_CMD_STATUS.to_string(), TaskArgValue::Number(0));

        Ok(Some(download_result))
    }
}
