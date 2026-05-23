use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::{download_dir, CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::{
    DeployConfig, ELOQ_FILE_KEY, ELOQ_LOG_FILE_KEY, GRAFANA_FILE_KEY, LOG_SERVICE_HOME,
    NODE_EXPORTER_FILE_KEY, PROMETHEUS_FILE_KEY,
};
use crate::config::DownloadUrl;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use flate2::read::GzDecoder;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use tar::Archive;
use tracing::info;

const ARCHIVE_PATH: &str = "_archive_path";
const EXTRACT_ROOT: &str = "_extract_root";
const UNPACK_DEST: &str = "_unpack_dest";

#[derive(Clone, Debug)]
pub struct LocalExtractTask {
    task_id: TaskId,
}

impl LocalExtractTask {
    pub fn from_config(config: &DeployConfig) -> Result<IndexMap<TaskId, TaskInstance>> {
        let links = config.deployment.all_download_links()?;
        let entries = links
            .iter()
            .filter_map(|(key, url)| {
                Self::unpack_dest_for_key(config, key).map(|dest| (key.clone(), url.clone(), dest))
            })
            .collect_vec();
        Ok(Self::instances(entries))
    }

    pub fn from_urls(
        entries: Vec<(String, DownloadUrl, String)>,
    ) -> IndexMap<TaskId, TaskInstance> {
        Self::instances(entries)
    }

    fn instances(entries: Vec<(String, DownloadUrl, String)>) -> IndexMap<TaskId, TaskInstance> {
        entries
            .into_iter()
            .unique_by(|(_key, url, dest)| (url.file_name(), dest.clone()))
            .map(|(_key, url, unpack_dest)| {
                let file_name = url.file_name();
                let task_id = TaskId {
                    cmd: "extract".to_string(),
                    task: format!("{}_extract", file_name),
                    host: "_local".to_string(),
                };
                let archive_path = Self::cached_archive_path(&url);
                let extract_root = Self::extract_root_for(&url);
                (
                    task_id.clone(),
                    TaskInstance {
                        task_input: HashMap::from([
                            (
                                ARCHIVE_PATH.to_string(),
                                TaskArgValue::Str(archive_path.to_string_lossy().to_string()),
                            ),
                            (
                                EXTRACT_ROOT.to_string(),
                                TaskArgValue::Str(extract_root.to_string_lossy().to_string()),
                            ),
                            (UNPACK_DEST.to_string(), TaskArgValue::Str(unpack_dest)),
                        ]),
                        task: Box::new(Self::new(task_id)),
                        task_host: TaskHost::Local,
                    },
                )
            })
            .collect()
    }

    pub fn new(task_id: TaskId) -> Self {
        Self { task_id }
    }

    pub fn cached_archive_path(url: &DownloadUrl) -> PathBuf {
        match url {
            DownloadUrl::Local(_) => download_dir().join(url.file_name()),
            DownloadUrl::Remote(_) => PathBuf::from(url.cache_dir().unwrap()).join(url.file_name()),
        }
    }

    pub fn extract_root_for(url: &DownloadUrl) -> PathBuf {
        let mut path = match url {
            DownloadUrl::Local(_) => download_dir(),
            DownloadUrl::Remote(_) => PathBuf::from(url.cache_dir().unwrap()),
        };
        path.push("_extracted");
        path.push(url.file_name().trim_end_matches(".tar.gz"));
        path
    }

    pub fn staged_dir_for(url: &DownloadUrl, unpack_dest: &str) -> PathBuf {
        Self::extract_root_for(url).join(unpack_dest)
    }

    pub fn unpack_dest_for_key(config: &DeployConfig, key: &str) -> Option<String> {
        match key {
            ELOQ_FILE_KEY => Some(config.deployment.product().home().to_string()),
            ELOQ_LOG_FILE_KEY => Some(LOG_SERVICE_HOME.to_string()),
            PROMETHEUS_FILE_KEY => Some("prometheus".to_string()),
            GRAFANA_FILE_KEY => Some("grafana".to_string()),
            NODE_EXPORTER_FILE_KEY => Some("node_exporter".to_string()),
            _ => None,
        }
    }

    fn unpack_archive(archive_path: &Path, destination: &Path) -> Result<()> {
        let file = File::open(archive_path)?;
        let decoder = GzDecoder::new(file);
        let mut archive = Archive::new(decoder);

        for entry in archive.entries()? {
            let mut entry = entry?;
            let entry_path = entry.path()?;
            let stripped = entry_path.components().skip(1).collect::<PathBuf>();
            if stripped.as_os_str().is_empty() {
                continue;
            }
            let out_path = destination.join(stripped);
            if let Some(parent) = out_path.parent() {
                fs::create_dir_all(parent)?;
            }
            entry.unpack(&out_path)?;
        }
        Ok(())
    }
}

#[async_trait]
impl TaskExecutor for LocalExtractTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        _task_host: TaskHost,
        task_arg: HashMap<String, TaskArgValue>,
    ) -> Result<Option<ExecutionValue>> {
        info!("execute {}", self.task_id.format_string());
        let archive_path = PathBuf::from(TaskArgValue::into_inner_value::<String>(
            task_arg.get(ARCHIVE_PATH).unwrap().clone(),
        ));
        let extract_root = PathBuf::from(TaskArgValue::into_inner_value::<String>(
            task_arg.get(EXTRACT_ROOT).unwrap().clone(),
        ));
        let unpack_dest =
            TaskArgValue::into_inner_value::<String>(task_arg.get(UNPACK_DEST).unwrap().clone());
        let destination = extract_root.join(&unpack_dest);

        if !archive_path.exists() {
            return Err(anyhow!(CmdErr::CopyTaskErr(format!(
                "archive not found: {}",
                archive_path.display()
            ))));
        }

        if extract_root.exists() {
            fs::remove_dir_all(&extract_root)?;
        }
        fs::create_dir_all(&destination)?;
        Self::unpack_archive(&archive_path, &destination)?;

        let mut result = HashMap::new();
        result.insert(
            CMD.to_string(),
            TaskArgValue::Str(format!("extract {}", archive_path.display())),
        );
        result.insert(CMD_STATUS.to_string(), TaskArgValue::Number(0));
        result.insert(
            CMD_OUTPUT.to_string(),
            TaskArgValue::Str(destination.to_string_lossy().to_string()),
        );
        Ok(Some(result))
    }
}
