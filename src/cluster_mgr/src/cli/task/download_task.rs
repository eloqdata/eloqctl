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
use std::fs;
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
    client: reqwest::Client,
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
            if let Some(cassdp) = cassandra.internal() {
                let cass_download_url_string = &cassdp.download_url;
                let cass_download_url =
                    DownloadUrl::from_url_str(cass_download_url_string.as_str())?;
                if !cass_download_url.is_local() {
                    download_url_vec.push(cass_download_url_string.to_string());
                }
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

        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .build()?;
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
                        task: Box::new(DownloadTask::new(task_id, pb, client.clone())),
                        task_host: TaskHost::Local,
                    },
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>();
        Ok(download_tasks)
    }

    pub fn new(task_id: TaskId, pg_bar: ProgressBar, client: reqwest::Client) -> Self {
        Self {
            task_id,
            pg_bar,
            client,
        }
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
        info!("execute {}", self.task_id.pretty_string());
        let url =
            TaskArgValue::into_inner_value::<String>(task_input.get(DOWNLOAD_URL).unwrap().clone());
        let filename = TaskArgValue::into_inner_value::<String>(
            task_input.get(DOWNLOAD_FILE_NAME).unwrap().clone(),
        );
        let save_dir = PathBuf::from(TaskArgValue::into_inner_value::<String>(
            task_input.get(DOWNLOAD_PATH).unwrap().clone(),
        ));

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|err| DownloadErr(url.clone(), err.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            error!("Download falied http status_code = {:?}", status.as_str());
            return Err(anyhow!(DownloadErr(url, status.to_string())));
        }
        let file_len = response.content_length().unwrap();

        // create local directory and partial file
        if let Err(err) = std::fs::create_dir_all(save_dir.as_path()) {
            error!("Download task: create directory {:?} falied", save_dir);
            return Err(anyhow!(DownloadErr(url, err.to_string())));
        }
        let save_path = save_dir.join(&filename);
        if save_path.exists() {
            // TODO(zhanghao): It would be better to check file SHA
            if file_len == fs::metadata(&save_path).unwrap().len() {
                info!("local file cache {:?} found.", save_path);
                return Ok(None);
            }
        }
        // TODO(zhanghao): Use HTTP range header to resume download
        let part_path = append_ext(save_path.clone(), "partial");
        let mut part_file = std::fs::File::create(part_path.as_path())
            .map_err(|err| DownloadErr(url.clone(), err.to_string()))?;

        // start downloading
        self.pg_bar.set_length(file_len);
        let mut stream_reader = response.bytes_stream();
        while let Some(stream_chunk) = stream_reader.next().await {
            if let Err(err) = stream_chunk {
                error!("DownloadRemote task error file={},msg={}", url, err);
                return Err(anyhow!(DownloadErr(url, err.to_string())));
            }
            let chunk = stream_chunk.unwrap();
            if let Err(write_err) = part_file.write_all(&chunk) {
                error!("DownloadTask {} write local file error {} ", url, write_err);
                return Err(anyhow!(DownloadErr(url, write_err.to_string())));
            }
            self.pg_bar.inc(chunk.len() as u64);
        }
        self.pg_bar
            .finish_with_message(format!("{filename} download compete"));
        fs::rename(part_path, save_path.as_path())
            .map_err(|err| DownloadErr(url, err.to_string()))?;

        let mut download_result = HashMap::new();
        download_result.insert(
            CMD.to_string(),
            TaskArgValue::Str(self.task_id.format_string()),
        );
        download_result.insert(
            CMD_OUTPUT.to_string(),
            TaskArgValue::Str(save_path.to_str().unwrap().to_string()),
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
