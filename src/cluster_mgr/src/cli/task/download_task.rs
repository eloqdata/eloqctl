use crate::cli::task::task_base::CmdErr::DownloadErr;
use crate::cli::task::task_base::{
    ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::{file_process_progress, CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeploymentConfig;
use crate::config::deployment::Codis;
use crate::config::DownloadUrl;
use anyhow::{anyhow, Ok};
use futures::stream::StreamExt;
use indexmap::IndexMap;
use indicatif::{MultiProgress, ProgressBar};
use itertools::Itertools;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use tracing::{error, info};

pub(crate) const DOWNLOAD_URL: &str = "download_url";
pub(crate) const DOWNLOAD_FILE_NAME: &str = "download_file_name";
pub(crate) const DOWNLOAD_PATH: &str = "download_path";

#[derive(Debug, Clone)]
pub struct DownloadTask {
    task_id: TaskId,
    pg_bar: ProgressBar,
}

impl DownloadTask {
    pub fn from_config(
        config: &DeploymentConfig,
    ) -> anyhow::Result<IndexMap<TaskId, TaskInstance>> {
        let deployment_ref = &config.deployment;
        let tx_download_url_string = deployment_ref.get_tx_image();
        let tx_download_url = DownloadUrl::from_url_str(tx_download_url_string.as_str())?;

        let mut download_url_vec = vec![];
        if !tx_download_url.is_local() {
            download_url_vec.push(tx_download_url_string);
        }

        if let Some(log_image_url) = deployment_ref.log_image.as_ref() {
            let log_download_url = DownloadUrl::from_url_str(log_image_url.as_str())?;
            if !log_download_url.is_local() {
                download_url_vec.push(log_image_url.to_string());
            }
        }

        if let Some(cassandra) = &config.deployment.storage_service.cassandra {
            let cass_download_url_string = &cassandra.download_url;
            let cass_download_url = DownloadUrl::from_url_str(cass_download_url_string.as_str())?;
            if !cass_download_url.is_local() {
                download_url_vec.push(cass_download_url_string.to_string());
            }
        }

        if let Some(monitor) = &config.deployment.monitor {
            let monitor_download_url_vec = monitor.download_links()?;
            let monitor_download_string_vec = monitor_download_url_vec
                .iter()
                .filter(|url| !url.is_local())
                .map(|download_url| download_url.get_url())
                .collect_vec();
            download_url_vec.extend(monitor_download_string_vec);
        }

        if config.deployment.codis.is_some() {
            download_url_vec.push(Codis::download_url());
        }

        let mpg_bar = MultiProgress::new();
        let local_ip = local_ip_address::local_ip()?.to_string();
        let download_tasks = download_url_vec
            .into_iter()
            .map(|source| {
                let d_url = DownloadUrl::from_url_str(source.as_str()).unwrap();
                let download_dir = d_url.cache_dir().unwrap();
                let filename = d_url.file_name();
                let task_id = TaskId {
                    cmd: "deploy".to_string(),
                    task: format!("{filename}_download"),
                    host: local_ip.to_string(),
                };
                let pb = mpg_bar.add(file_process_progress(
                    format!("DOWNLOAD [{filename}]"),
                    "#>-",
                ));
                (
                    task_id.clone(),
                    TaskInstance {
                        task_input: HashMap::from([
                            (DOWNLOAD_URL.to_string(), TaskArgValue::Str(source)),
                            (DOWNLOAD_FILE_NAME.to_string(), TaskArgValue::Str(filename)),
                            (DOWNLOAD_PATH.to_string(), TaskArgValue::Str(download_dir)),
                        ]),
                        task: Box::new(DownloadTask::new(task_id, pb)),
                        task_host: TaskHost::Local,
                    },
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>();
        Ok(download_tasks)
    }

    pub fn new(task_id: TaskId, pg_bar: ProgressBar) -> Self {
        Self { task_id, pg_bar }
    }
}

#[async_trait::async_trait]
impl TaskExecutor for DownloadTask {
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
        let filename = TaskArgValue::into_inner_value::<String>(
            task_input.get(DOWNLOAD_FILE_NAME).unwrap().clone(),
        );
        let download_path = PathBuf::from(TaskArgValue::into_inner_value::<String>(
            task_input.get(DOWNLOAD_PATH).unwrap().clone(),
        ));

        // create local directory and partial file
        let create_download_path_rs = std::fs::create_dir_all(download_path.as_path());
        if create_download_path_rs.is_err() {
            error!("Download cli create tmp_dir error {:?}", download_path);
            return Err(anyhow!(DownloadErr(
                download_url,
                create_download_path_rs.err().unwrap().to_string()
            )));
        }
        let local_file_path = download_path.join(&filename);
        if local_file_path.exists() {
            info!(
                "The local file {:?} exists. please delete it if you want to re-download it first.",
                local_file_path.clone()
            );
            return Ok(None);
        }
        // TODO(zhanghao): Use HTTP range header to resume download
        let tmp_file = append_ext(local_file_path.clone(), "partial");
        let create_local_file_rs = std::fs::File::create(tmp_file.as_path());
        if create_local_file_rs.is_err() {
            return Err(anyhow!(DownloadErr(
                download_url,
                create_local_file_rs.err().unwrap().to_string()
            )));
        }
        let mut download_file = create_local_file_rs.unwrap();

        // start download
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .build()?;
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
        }
        let http_response = rsp_rs.unwrap();
        let status = http_response.status();
        if !status.is_success() {
            error!(
                "Download cli error cause by http status_code = {:?}",
                status.as_str()
            );
            return Err(anyhow!(DownloadErr(download_url, status.to_string())));
        }
        let file_len = http_response.content_length().unwrap();
        self.pg_bar.set_length(file_len);
        let mut stream_reader = http_response.bytes_stream();
        while let Some(stream_chunk) = stream_reader.next().await {
            if let Err(err) = stream_chunk {
                error!(
                    "DownloadRemote task error file={},msg={}",
                    download_url, err
                );
                return Err(anyhow!(DownloadErr(download_url, err.to_string())));
            }
            let chunk = stream_chunk.unwrap();
            if let Err(write_err) = download_file.write_all(&chunk) {
                error!(
                    "DownloadTask {} write local file error {} ",
                    download_url, write_err
                );
                return Err(anyhow!(DownloadErr(download_url, write_err.to_string())));
            }
            self.pg_bar.inc(chunk.len() as u64);
        }
        if let Err(err) = std::fs::rename(tmp_file, local_file_path.as_path()) {
            return Err(anyhow!(DownloadErr(download_url, err.to_string())));
        }
        self.pg_bar
            .finish_with_message(format!("{filename} download compete"));

        let mut download_result = HashMap::new();
        download_result.insert(
            CMD.to_string(),
            TaskArgValue::Str(self.task_id.format_string()),
        );
        download_result.insert(
            CMD_OUTPUT.to_string(),
            TaskArgValue::Str(local_file_path.to_str().unwrap().to_string()),
        );
        download_result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
        // Ok(Some(download_result))
        Ok(None)
    }
}

pub fn append_ext(path: PathBuf, ext: impl AsRef<OsStr>) -> PathBuf {
    let mut os_string: OsString = path.into();
    os_string.push(".");
    os_string.push(ext.as_ref());
    os_string.into()
}
