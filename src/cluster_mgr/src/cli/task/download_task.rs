use crate::cli::cmd_base::HTTP_CLIENT;
use crate::cli::task::task_base::CmdErr::DownloadErr;
use crate::cli::task::task_base::{
    is_verbose_task_output, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId,
    TaskInstance,
};
use crate::cli::util::file_pg_bar;
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeployConfig;
use crate::config::DownloadUrl;
use anyhow::{anyhow, Result};
use futures::stream::StreamExt;
use indexmap::IndexMap;
use indicatif::{MultiProgress, ProgressBar};
use itertools::Itertools;
use reqwest::header::{ACCEPT, ACCEPT_RANGES, CONNECTION, CONTENT_RANGE, RANGE, USER_AGENT};
use reqwest::StatusCode;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use tokio::task::spawn_blocking;
use tokio::time::timeout;
use tracing::{error, info, warn};

const MAX_DOWNLOAD_ATTEMPTS: usize = 8;
const DOWNLOAD_RETRY_DELAY: Duration = Duration::from_secs(3);
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const DOWNLOAD_PROGRESS_TIMEOUT: Duration = Duration::from_secs(120);
const WRITE_FLUSH_THRESHOLD: usize = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct DownloadTask {
    task_id: TaskId,
    url: String,
    name: String,
    dir: PathBuf,
    pg_bar: ProgressBar,
}

impl DownloadTask {
    async fn resolve_remote_target(&self) -> (Option<u64>, String) {
        match HTTP_CLIENT
            .head(&self.url)
            .header(CONNECTION, "close")
            .timeout(HTTP_REQUEST_TIMEOUT)
            .send()
            .await
        {
            Ok(response) => {
                let effective_url = response.url().to_string();
                if effective_url != self.url {
                    info!(
                        "resolved download URL for {}: {} → {}",
                        self.name, self.url, effective_url
                    );
                }
                (response.content_length(), effective_url)
            }
            Err(err) => {
                info!(
                    "HEAD request failed for {} ({}), falling back to original URL",
                    self.name, err
                );
                (None, self.url.clone())
            }
        }
    }

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

        if let Some(monitor) = &config.deployment.monitor {
            let monitor_download_url_vec = monitor.download_links()?;
            let monitor_download_string_vec = monitor_download_url_vec
                .iter()
                .filter(|url| !url.is_local())
                .map(|download_url| download_url.get_url())
                .collect_vec();
            urls.extend(monitor_download_string_vec);
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

    async fn expected_digest(&self) -> Result<Option<String>> {
        let download_url = DownloadUrl::from_url_str(&self.url)?;
        let DownloadUrl::Remote(url) = download_url else {
            return Ok(None);
        };
        if url.domain() != Some("github.com") {
            return Ok(None);
        }

        let segments = url
            .path_segments()
            .map(|segments| segments.collect_vec())
            .unwrap_or_default();
        if segments.len() < 8
            || segments[0] != "eloqdata"
            || segments[1] != "eloqkv"
            || segments[2] != "releases"
            || segments[3] != "download"
        {
            return Ok(None);
        }

        let tag = segments[4];
        let file_name = segments.last().unwrap();
        let api_url = format!("https://api.github.com/repos/eloqdata/eloqkv/releases/tags/{tag}");
        let response = HTTP_CLIENT
            .get(api_url)
            .timeout(HTTP_REQUEST_TIMEOUT)
            .header(USER_AGENT, "eloqctl")
            .header(ACCEPT, "application/vnd.github+json")
            .send()
            .await?;
        if !response.status().is_success() {
            return Ok(None);
        }

        #[derive(Debug, Deserialize)]
        struct ReleaseResponse {
            assets: Vec<GitHubAssetEntry>,
        }

        #[derive(Debug, Deserialize)]
        struct GitHubAssetEntry {
            name: String,
            digest: Option<String>,
        }

        let release = response.json::<ReleaseResponse>().await?;
        Ok(release
            .assets
            .into_iter()
            .find(|asset| asset.name == *file_name)
            .and_then(|asset| asset.digest))
    }

    fn sha256_file(path: &PathBuf) -> Result<String> {
        let mut file = std::fs::File::open(path)?;
        let mut hasher = Sha256::new();
        let mut buf = [0u8; 8192];
        loop {
            let read = file.read(&mut buf)?;
            if read == 0 {
                break;
            }
            hasher.update(&buf[..read]);
        }
        Ok(format!("sha256:{:x}", hasher.finalize()))
    }

    async fn flush_to_disk(
        mut file: std::fs::File,
        data: Vec<u8>,
        url: &str,
    ) -> Result<std::fs::File> {
        let url_owned = url.to_owned();
        let write_result = spawn_blocking(move || match file.write_all(&data) {
            Ok(()) => Ok(file),
            Err(e) => Err(format!("{}", e)),
        })
        .await;
        match write_result {
            Ok(Ok(f)) => Ok(f),
            Ok(Err(msg)) => {
                error!("DownloadTask {} write local file error {}", url_owned, msg);
                Err(anyhow!(DownloadErr(url_owned, msg)))
            }
            Err(join_err) => {
                error!(
                    "DownloadTask {} spawn_blocking error {}",
                    url_owned, join_err
                );
                Err(anyhow!(DownloadErr(url_owned, join_err.to_string())))
            }
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
        _task_input: HashMap<String, TaskArgValue>,
    ) -> Result<Option<ExecutionValue>> {
        info!("execute {}", self.task_id.format_string());
        let url = &self.url;
        if is_verbose_task_output() {
            println!("download url:{url}");
        }
        let save_dir = &self.dir;

        // create local directory
        if let Err(err) = std::fs::create_dir_all(&self.dir) {
            error!("Download task: create directory {:?} falied", save_dir);
            return Err(anyhow!(DownloadErr(self.url.clone(), err.to_string())));
        }
        let save_path = save_dir.join(&self.name);
        let expected_digest = self.expected_digest().await.ok().flatten();

        // Try HEAD first to check remote content-length for cache validation,
        // and resolve the final URL after redirects (github.com → S3) so
        // subsequent Range requests go directly to the object store.
        let (mut remote_len, mut effective_url) = self.resolve_remote_target().await;

        if save_path.exists() {
            let digest_matches = match &expected_digest {
                Some(expected) => Self::sha256_file(&save_path)
                    .map(|actual| actual == *expected)
                    .unwrap_or(false),
                None => false,
            };

            if digest_matches {
                info!(
                    "local file cache {:?} found (sha256 matches), skipping download.",
                    save_path
                );
                return Ok(None);
            }

            if expected_digest.is_none()
                && remote_len.is_some_and(|expected_len| {
                    fs::metadata(&save_path)
                        .map(|m| m.len() == expected_len)
                        .unwrap_or(false)
                })
            {
                info!(
                    "local file cache {:?} found ({} bytes, matches remote), skipping download.",
                    save_path,
                    remote_len.unwrap()
                );
                return Ok(None);
            }

            if expected_digest.is_none()
                && remote_len.is_none()
                && fs::metadata(&save_path)
                    .map(|m| m.len() > 0)
                    .unwrap_or(false)
            {
                info!(
                    "local file cache {:?} found (remote HEAD unavailable), reusing existing file.",
                    save_path
                );
                return Ok(None);
            }
        }

        let part_path = append_ext(save_path.clone(), "partial");
        if !part_path.exists() && save_path.exists() {
            let existing_len = fs::metadata(&save_path).map(|m| m.len()).unwrap_or(0);
            let should_promote_to_partial = remote_len
                .map(|expected_len| existing_len > 0 && existing_len < expected_len)
                .unwrap_or(false);
            if should_promote_to_partial {
                info!(
                    "existing file cache {:?} is incomplete ({} bytes), resuming via partial file.",
                    save_path, existing_len
                );
                fs::rename(&save_path, &part_path)
                    .map_err(|err| DownloadErr(url.clone(), err.to_string()))?;
            }
        }
        let mut final_file_len = remote_len.unwrap_or(0);
        for attempt in 1..=MAX_DOWNLOAD_ATTEMPTS {
            if attempt > 1 {
                let (attempt_remote_len, attempt_effective_url) =
                    self.resolve_remote_target().await;
                if let (Some(previous), Some(current)) = (remote_len, attempt_remote_len) {
                    if previous != current {
                        warn!(
                            "remote content length changed for {} between retries: {} -> {}",
                            self.name, previous, current
                        );
                    }
                }
                if remote_len.is_none() {
                    remote_len = attempt_remote_len;
                }
                effective_url = attempt_effective_url;
            }

            let mut resume_from = fs::metadata(&part_path).map(|m| m.len()).unwrap_or(0);
            info!(
                "download attempt {attempt}/{MAX_DOWNLOAD_ATTEMPTS} for {}: resume_from={}, expected_len={:?}, partial_path={:?}",
                self.name, resume_from, remote_len, part_path
            );

            if let Some(expected_len) = remote_len {
                if resume_from > expected_len {
                    warn!(
                        "partial file for {} is larger than expected remote size ({} > {}), removing it",
                        self.name, resume_from, expected_len
                    );
                    fs::remove_file(&part_path).ok();
                    resume_from = 0;
                } else if resume_from == expected_len && expected_len > 0 {
                    if let Some(expected) = &expected_digest {
                        let actual = Self::sha256_file(&part_path)
                            .map_err(|err| DownloadErr(url.clone(), err.to_string()))?;
                        if &actual == expected {
                            fs::rename(&part_path, save_path.as_path())
                                .map_err(|err| DownloadErr(url.clone(), err.to_string()))?;
                            info!("partial download {:?} already complete.", save_path);
                            return Ok(None);
                        }
                        fs::remove_file(&part_path).ok();
                        resume_from = 0;
                    } else {
                        fs::rename(&part_path, save_path.as_path())
                            .map_err(|err| DownloadErr(url.clone(), err.to_string()))?;
                        info!("partial download {:?} already complete.", save_path);
                        return Ok(None);
                    }
                }
            }

            let mut request = HTTP_CLIENT.get(&effective_url).header(CONNECTION, "close");
            if resume_from > 0 {
                request = request.header(RANGE, format!("bytes={resume_from}-"));
                info!(
                    "issuing resume request for {} with Range: bytes={resume_from}-",
                    self.name
                );
            }
            // reqwest's per-request timeout spans the entire body transfer,
            // which would cap each attempt at HTTP_REQUEST_TIMEOUT worth of
            // data. Bound only the time-to-response-headers here; the body
            // stream is guarded per chunk by DOWNLOAD_PROGRESS_TIMEOUT below.
            let send_result = match timeout(HTTP_REQUEST_TIMEOUT, request.send()).await {
                Ok(result) => result.map_err(|err| err.to_string()),
                Err(_) => Err(format!(
                    "no response headers within {HTTP_REQUEST_TIMEOUT:?}"
                )),
            };
            let response = match send_result {
                Ok(response) => response,
                Err(err_msg) => {
                    if attempt < MAX_DOWNLOAD_ATTEMPTS {
                        info!(
                            "download attempt {attempt}/{MAX_DOWNLOAD_ATTEMPTS} failed to start for {}: {}",
                            self.name, err_msg
                        );
                        tokio::time::sleep(DOWNLOAD_RETRY_DELAY).await;
                        continue;
                    }
                    return Err(anyhow!(DownloadErr(url.clone(), err_msg)));
                }
            };
            let status = response.status();
            let content_length = response.content_length();
            let accept_ranges = response
                .headers()
                .get(ACCEPT_RANGES)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let content_range = response
                .headers()
                .get(CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            info!(
                "download response for {} attempt {attempt}: status={}, content_length={:?}, accept_ranges={:?}, content_range={:?}, resume_from={}",
                self.name,
                status.as_str(),
                content_length,
                accept_ranges,
                content_range,
                resume_from
            );
            if !(status.is_success() || status == StatusCode::PARTIAL_CONTENT) {
                error!("Download falied http status_code = {:?}", status.as_str());
                return Err(anyhow!(DownloadErr(self.url.clone(), status.to_string())));
            }

            let (mut part_file, file_len, resumed) = if resume_from > 0
                && status == StatusCode::PARTIAL_CONTENT
            {
                match content_range.as_deref() {
                    Some(range) if range.starts_with(&format!("bytes {resume_from}-")) => {
                        info!(
                            "resume response for {} matched requested offset {}",
                            self.name, resume_from
                        );
                    }
                    Some(range) => {
                        warn!(
                                "resume response for {} returned unexpected Content-Range {:?} for requested offset {}",
                                self.name, range, resume_from
                            );
                    }
                    None => {
                        warn!(
                            "resume response for {} returned 206 without Content-Range header",
                            self.name
                        );
                    }
                }
                let total_len =
                    remote_len.unwrap_or_else(|| resume_from + content_length.unwrap_or(0));
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(part_path.as_path())
                    .map_err(|err| DownloadErr(url.clone(), err.to_string()))?;
                info!(
                    "resuming download for {} from byte {}",
                    self.name, resume_from
                );
                (file, total_len, true)
            } else if resume_from > 0 {
                warn!(
                        "resume request for {} returned {} instead of 206; preserving partial file and retrying with a fresh URL",
                        self.name,
                        status.as_str()
                    );
                if attempt < MAX_DOWNLOAD_ATTEMPTS {
                    self.pg_bar.set_message(format!(
                        "{} Resume pending, retrying with preserved partial...",
                        self.name
                    ));
                    tokio::time::sleep(DOWNLOAD_RETRY_DELAY).await;
                    continue;
                }
                return Err(anyhow!(DownloadErr(
                    self.url.clone(),
                    format!(
                        "resume request returned {} instead of 206 after {} attempts",
                        status.as_str(),
                        MAX_DOWNLOAD_ATTEMPTS
                    )
                )));
            } else {
                let file = std::fs::File::create(part_path.as_path())
                    .map_err(|err| DownloadErr(url.clone(), err.to_string()))?;
                (
                    file,
                    content_length.unwrap_or(remote_len.unwrap_or(0)),
                    false,
                )
            };
            final_file_len = file_len;

            // Double-check cache after successful GET (in case of concurrent download)
            if save_path.exists() && file_len > 0 {
                let digest_matches = match &expected_digest {
                    Some(expected) => Self::sha256_file(&save_path)
                        .map(|actual| actual == *expected)
                        .unwrap_or(false),
                    None => false,
                };
                if digest_matches
                    || (expected_digest.is_none()
                        && fs::metadata(&save_path)
                            .map(|m| m.len() == file_len)
                            .unwrap_or(false))
                {
                    info!("local file cache {:?} found.", save_path);
                    return Ok(None);
                }
            }

            self.pg_bar.set_length(file_len);
            if resumed {
                self.pg_bar.set_position(resume_from);
            } else {
                self.pg_bar.set_position(0);
            }
            self.pg_bar
                .set_message(format!("{} Downloading...", self.name));

            let mut stream_reader = response.bytes_stream();
            let mut stream_failed = None;
            let mut write_buf: Vec<u8> = Vec::with_capacity(WRITE_FLUSH_THRESHOLD);
            loop {
                let next_chunk =
                    match timeout(DOWNLOAD_PROGRESS_TIMEOUT, stream_reader.next()).await {
                        Ok(chunk) => chunk,
                        Err(_) => {
                            stream_failed = Some(format!(
                                "download stalled for more than {:?}",
                                DOWNLOAD_PROGRESS_TIMEOUT
                            ));
                            break;
                        }
                    };
                let Some(stream_chunk) = next_chunk else {
                    break;
                };
                if let Err(err) = stream_chunk {
                    error!("DownloadRemote task error file={},msg={}", url, err);
                    stream_failed = Some(err.to_string());
                    break;
                }
                let chunk = stream_chunk.unwrap();
                write_buf.extend_from_slice(&chunk);
                self.pg_bar.inc(chunk.len() as u64);
                if write_buf.len() >= WRITE_FLUSH_THRESHOLD {
                    let data = std::mem::replace(
                        &mut write_buf,
                        Vec::with_capacity(WRITE_FLUSH_THRESHOLD),
                    );
                    part_file = Self::flush_to_disk(part_file, data, url).await?;
                }
            }

            if !write_buf.is_empty() {
                part_file = Self::flush_to_disk(part_file, write_buf, url).await?;
            }

            drop(part_file);

            let current_len = fs::metadata(&part_path).map(|m| m.len()).unwrap_or(0);
            let err_msg = if let Some(err) = stream_failed {
                err
            } else if file_len > 0 && current_len < file_len {
                format!(
                    "incomplete download after attempt {attempt}: expected {file_len} bytes, got {current_len}"
                )
            } else {
                self.pg_bar
                    .finish_with_message(format!("{} Downloaded!", self.name));
                break;
            };

            if attempt < MAX_DOWNLOAD_ATTEMPTS {
                info!(
                    "download attempt {attempt}/{MAX_DOWNLOAD_ATTEMPTS} incomplete for {} (have {current_len}/{file_len} bytes, resumed={}), retrying",
                    self.name,
                    resumed
                );
                tokio::time::sleep(DOWNLOAD_RETRY_DELAY).await;
            } else {
                return Err(anyhow!(DownloadErr(url.clone(), err_msg)));
            }
        }

        if let Some(expected) = &expected_digest {
            let actual = Self::sha256_file(&part_path)
                .map_err(|err| DownloadErr(url.clone(), err.to_string()))?;
            if &actual != expected {
                let _ = fs::remove_file(&part_path);
                return Err(anyhow!(DownloadErr(
                    self.url.clone(),
                    format!("sha256 mismatch: expected {expected}, got {actual}")
                )));
            }
        }

        if final_file_len > 0 {
            let actual_len = fs::metadata(&part_path)
                .map(|m| m.len())
                .map_err(|err| DownloadErr(url.clone(), err.to_string()))?;
            if actual_len != final_file_len {
                return Err(anyhow!(DownloadErr(
                    self.url.clone(),
                    format!("download size mismatch: expected {final_file_len}, got {actual_len}")
                )));
            }
        }

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
