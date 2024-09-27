use crate::cli::cmd_base::HTTP_CLIENT;
use crate::cli::task::task_base::CmdErr::DownloadErr;
use crate::cli::task::task_base::{
    ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::util::file_pg_bar;
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeployConfig;
use crate::config::deployment::Codis;
use crate::config::DownloadUrl;
use anyhow::{anyhow, Ok, Result};
use futures::stream::StreamExt;
use indexmap::IndexMap;
use indicatif::{MultiProgress, ProgressBar};
use itertools::Itertools;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use tracing::{error, info};

#[derive(Debug, Clone)]
pub struct DownloadTask {
    task_id: TaskId,
    url: String,
    name: String,
    dir: PathBuf,
    pg_bar: ProgressBar,
}

impl DownloadTask {
    pub fn from_config(config: &DeployConfig) -> Result<IndexMap<TaskId, TaskInstance>> {
        let deployment_ref = &config.deployment;
        let tx_download_str = deployment_ref.tx_image();
        let tx_download_url = DownloadUrl::from_url_str(tx_download_str)?;

        let mut urls = vec![];
        if !tx_download_url.is_local() {
            urls.push(tx_download_str.to_owned());
        }

        if let Some(log_image_url) = deployment_ref.log_image() {
            let log_download_url = DownloadUrl::from_url_str(log_image_url)?;
            if !log_download_url.is_local() {
                urls.push(log_image_url.to_owned());
            }
        }

        if let Some(cassandra) = &config.deployment.storage_service.cassandra {
            if let Some(cassdp) = cassandra.internal() {
                let cass_download_url_string = &cassdp.image_url();
                let cass_download_url =
                    DownloadUrl::from_url_str(cass_download_url_string.as_str())?;
                if !cass_download_url.is_local() {
                    urls.push(cass_download_url_string.to_owned());
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
            urls.extend(monitor_download_string_vec);
        }

        if config.deployment.codis.is_some() {
            urls.push(Codis::download_url());
        }

        Ok(Self::instances(Self::from_urls(urls)))
    }

    pub fn from_urls(urls: Vec<String>) -> Vec<Self> {
        let mpg_bar = MultiProgress::new();
        urls.into_iter()
            .map(|url| {
                let d_url = DownloadUrl::from_url_str(url.as_str()).unwrap();
                let dir = d_url.cache_dir().unwrap();
                let filename = d_url.file_name();
                let task_id = TaskId {
                    cmd: "deploy".to_string(),
                    task: format!("{filename}_download"),
                    host: "127.0.0.1".to_owned(),
                };
                let pg_bar = mpg_bar.add(file_pg_bar());
                DownloadTask {
                    task_id,
                    url,
                    name: filename,
                    dir: PathBuf::from(dir),
                    pg_bar,
                }
            })
            .collect()
    }

    pub fn instances(tasks: Vec<Self>) -> IndexMap<TaskId, TaskInstance> {
        tasks
            .into_iter()
            .map(|task| {
                (
                    task.task_id.clone(),
                    TaskInstance {
                        task_input: HashMap::default(),
                        task: Box::new(task),
                        task_host: TaskHost::Local,
                    },
                )
            })
            .collect::<IndexMap<TaskId, TaskInstance>>()
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
        _task_input: HashMap<String, TaskArgValue>,
    ) -> Result<Option<ExecutionValue>> {
        info!("execute {}", self.task_id.pretty_string());
        let url = &self.url;
        let save_dir = &self.dir;

        let response = HTTP_CLIENT
            .get(&self.url)
            .send()
            .await
            .map_err(|err| DownloadErr(url.clone(), err.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            error!("Download falied http status_code = {:?}", status.as_str());
            return Err(anyhow!(DownloadErr(self.url.clone(), status.to_string())));
        }
        let file_len = response.content_length().unwrap();

        // create local directory and partial file
        if let Err(err) = std::fs::create_dir_all(&self.dir) {
            error!("Download task: create directory {:?} falied", save_dir);
            return Err(anyhow!(DownloadErr(self.url.clone(), err.to_string())));
        }
        let save_path = save_dir.join(&self.name);
        println!("save_path:{}", save_path.display());
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
        self.pg_bar
            .set_message(format!("{} Downloading...", self.name));
        let mut stream_reader = response.bytes_stream();
        while let Some(stream_chunk) = stream_reader.next().await {
            if let Err(err) = stream_chunk {
                error!("DownloadRemote task error file={},msg={}", url, err);
                return Err(anyhow!(DownloadErr(url.clone(), err.to_string())));
            }
            let chunk = stream_chunk.unwrap();
            if let Err(write_err) = part_file.write_all(&chunk) {
                error!("DownloadTask {} write local file error {} ", url, write_err);
                return Err(anyhow!(DownloadErr(url.clone(), write_err.to_string())));
            }
            self.pg_bar.inc(chunk.len() as u64);
        }
        self.pg_bar
            .finish_with_message(format!("{} Downloaded!", self.name));
        fs::rename(part_path, save_path.as_path())
            .map_err(|err| DownloadErr(url.clone(), err.to_string()))?;

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
