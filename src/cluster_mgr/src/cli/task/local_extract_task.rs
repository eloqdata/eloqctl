use crate::cli::task::task_base::{
    CmdErr, ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::util::file_pg_bar;
use crate::cli::{download_dir, CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::{
    DeployConfig, ALERTMANAGER_FILE_KEY, ELOQ_FILE_KEY, ELOQ_LOG_FILE_KEY, GRAFANA_FILE_KEY,
    LOG_SERVICE_HOME, NODE_EXPORTER_FILE_KEY, PROMETHEUSALERT_FILE_KEY, PROMETHEUS_FILE_KEY,
};
use crate::config::monitor::ALERTMANAGER_WEBHOOK_ADAPTER_BINARY;
use crate::config::DownloadUrl;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use flate2::read::GzDecoder;
use indexmap::IndexMap;
use indicatif::{MultiProgress, ProgressBar};
use itertools::Itertools;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io;
use std::path::{Component, Path, PathBuf};
use tar::Archive;
use tracing::info;
use zip::ZipArchive;

const ARCHIVE_PATH: &str = "_archive_path";
const EXTRACT_ROOT: &str = "_extract_root";
const UNPACK_DEST: &str = "_unpack_dest";

#[derive(Clone, Debug)]
pub struct LocalExtractTask {
    task_id: TaskId,
    pg_bar: ProgressBar,
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
        let mpg_bar = MultiProgress::new();
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
                let pg_bar = mpg_bar.add(file_pg_bar());
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
                        task: Box::new(Self::new(task_id, pg_bar)),
                        task_host: TaskHost::Local,
                    },
                )
            })
            .collect()
    }

    pub fn new(task_id: TaskId, pg_bar: ProgressBar) -> Self {
        Self { task_id, pg_bar }
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
        path.push(Self::archive_stem(&url.file_name()));
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
            ALERTMANAGER_FILE_KEY => Some("alertmanager".to_string()),
            GRAFANA_FILE_KEY => Some("grafana".to_string()),
            PROMETHEUSALERT_FILE_KEY => Some(PROMETHEUSALERT_FILE_KEY.to_string()),
            NODE_EXPORTER_FILE_KEY => Some("node_exporter".to_string()),
            _ => None,
        }
    }

    fn unpack_archive(&self, archive_path: &Path, destination: &Path) -> Result<()> {
        let file_name = archive_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        if file_name.ends_with(".zip") {
            return self.unpack_zip_archive(archive_path, destination);
        }
        if file_name.ends_with(".tar.gz") {
            return self.unpack_tar_gz_archive(archive_path, destination);
        }
        self.unpack_raw_binary(archive_path, destination)
    }

    fn unpack_raw_binary(&self, archive_path: &Path, destination: &Path) -> Result<()> {
        let file = File::open(archive_path)?;
        let binary_len = file.metadata()?.len();
        self.pg_bar.set_length(binary_len);
        self.pg_bar.set_position(0);
        self.pg_bar.set_message(format!(
            "{} Extracting...",
            Self::archive_display_name(archive_path)
        ));
        let mut reader = self.pg_bar.wrap_read(file);
        let output_path = destination.join(ALERTMANAGER_WEBHOOK_ADAPTER_BINARY);
        let mut out_file = File::create(&output_path)?;
        io::copy(&mut reader, &mut out_file)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&output_path, fs::Permissions::from_mode(0o755))?;
        }
        self.pg_bar.finish_with_message(format!(
            "{} Extracted!",
            Self::archive_display_name(archive_path)
        ));
        Ok(())
    }

    fn unpack_tar_gz_archive(&self, archive_path: &Path, destination: &Path) -> Result<()> {
        let file = File::open(archive_path)?;
        let compressed_len = file.metadata()?.len();
        self.pg_bar.set_length(compressed_len);
        self.pg_bar.set_position(0);
        self.pg_bar.set_message(format!(
            "{} Extracting...",
            Self::archive_display_name(archive_path)
        ));
        let reader = self.pg_bar.wrap_read(file);
        let decoder = GzDecoder::new(reader);
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
        self.pg_bar.finish_with_message(format!(
            "{} Extracted!",
            Self::archive_display_name(archive_path)
        ));
        Ok(())
    }

    #[cfg(unix)]
    fn apply_post_extract_permissions(destination: &Path, unpack_dest: &str) -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        if unpack_dest != PROMETHEUSALERT_FILE_KEY {
            return Ok(());
        }

        for binary_name in [
            "PrometheusAlert",
            "zabbixclient",
            ALERTMANAGER_WEBHOOK_ADAPTER_BINARY,
        ] {
            let binary_path = destination.join(binary_name);
            if !binary_path.exists() {
                continue;
            }
            let metadata = fs::metadata(&binary_path)?;
            let current_mode = metadata.permissions().mode() & 0o777;
            if current_mode & 0o111 != 0 {
                continue;
            }
            fs::set_permissions(
                &binary_path,
                fs::Permissions::from_mode(current_mode | 0o111),
            )?;
        }
        Ok(())
    }

    #[cfg(not(unix))]
    fn apply_post_extract_permissions(_destination: &Path, _unpack_dest: &str) -> Result<()> {
        Ok(())
    }

    fn unpack_zip_archive(&self, archive_path: &Path, destination: &Path) -> Result<()> {
        let file = File::open(archive_path)?;
        let compressed_len = file.metadata()?.len();
        self.pg_bar.set_length(compressed_len);
        self.pg_bar.set_position(0);
        self.pg_bar.set_message(format!(
            "{} Extracting...",
            Self::archive_display_name(archive_path)
        ));
        let reader = self.pg_bar.wrap_read(file);
        let mut archive = ZipArchive::new(reader)?;

        for idx in 0..archive.len() {
            let mut entry = archive.by_index(idx)?;
            let Some(safe_name) = entry.enclosed_name().map(|p| p.to_path_buf()) else {
                continue;
            };
            let stripped = Self::strip_first_component(&safe_name);
            if stripped.as_os_str().is_empty() {
                continue;
            }
            let out_path = destination.join(stripped);
            if entry.is_dir() {
                fs::create_dir_all(&out_path)?;
                continue;
            }
            if let Some(parent) = out_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut out_file = File::create(&out_path)?;
            io::copy(&mut entry, &mut out_file)?;
            #[cfg(unix)]
            if let Some(mode) = entry.unix_mode() {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&out_path, fs::Permissions::from_mode(mode))?;
            }
        }

        self.pg_bar.finish_with_message(format!(
            "{} Extracted!",
            Self::archive_display_name(archive_path)
        ));
        Ok(())
    }

    fn archive_display_name(archive_path: &Path) -> String {
        archive_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("archive")
            .to_string()
    }

    fn archive_stem(file_name: &str) -> &str {
        file_name
            .strip_suffix(".tar.gz")
            .or_else(|| file_name.strip_suffix(".zip"))
            .unwrap_or(file_name)
    }

    fn strip_first_component(path: &Path) -> PathBuf {
        path.components()
            .skip(1)
            .filter_map(|component| match component {
                Component::Normal(part) => Some(part),
                _ => None,
            })
            .collect()
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
        self.unpack_archive(&archive_path, &destination)?;
        Self::apply_post_extract_permissions(&destination, &unpack_dest)?;

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

#[cfg(test)]
mod tests {
    use super::LocalExtractTask;
    use crate::config::config_base::PROMETHEUSALERT_FILE_KEY;
    use indicatif::ProgressBar;
    use std::fs;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
    use zip::write::SimpleFileOptions;

    fn temp_dir(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cluster-mgr-{name}-{suffix}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn unzip_preserves_unix_executable_mode() {
        let root = temp_dir("zip-mode");
        let archive_path = root.join("linux.zip");
        let dest = root.join("dest");
        fs::create_dir_all(&dest).unwrap();

        let file = File::create(&archive_path).unwrap();
        let mut writer = zip::ZipWriter::new(file);
        let opts = SimpleFileOptions::default().unix_permissions(0o755);
        writer.add_directory("linux/", opts).unwrap();
        writer.start_file("linux/PrometheusAlert", opts).unwrap();
        writer.write_all(b"#!/bin/sh\nexit 0\n").unwrap();
        writer.finish().unwrap();

        let task = LocalExtractTask::new(
            crate::cli::task::task_base::TaskId {
                cmd: "extract".to_string(),
                task: "linux.zip_extract".to_string(),
                host: "_local".to_string(),
            },
            ProgressBar::hidden(),
        );
        task.unpack_archive(&archive_path, &dest).unwrap();

        let extracted = dest.join("PrometheusAlert");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(extracted).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o755);
        }
    }

    #[test]
    fn unzip_webhook_adapter_without_unix_mode_still_becomes_executable() {
        let root = temp_dir("zip-webhook-adapter-no-mode");
        let archive_path = root.join("linux.zip");
        let extract_root = root.join("extract-root");
        let destination = extract_root.join(PROMETHEUSALERT_FILE_KEY);
        fs::create_dir_all(&destination).unwrap();

        let file = File::create(&archive_path).unwrap();
        let mut writer = zip::ZipWriter::new(file);
        let opts = SimpleFileOptions::default();
        writer.add_directory("linux/", opts).unwrap();
        writer.start_file("linux/PrometheusAlert", opts).unwrap();
        writer.write_all(b"binary").unwrap();
        writer.start_file("linux/zabbixclient", opts).unwrap();
        writer.write_all(b"helper").unwrap();
        writer.finish().unwrap();

        let task = LocalExtractTask::new(
            crate::cli::task::task_base::TaskId {
                cmd: "extract".to_string(),
                task: "linux.zip_extract".to_string(),
                host: "_local".to_string(),
            },
            ProgressBar::hidden(),
        );
        task.unpack_archive(&archive_path, &destination).unwrap();
        LocalExtractTask::apply_post_extract_permissions(&destination, PROMETHEUSALERT_FILE_KEY)
            .unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for binary_name in ["PrometheusAlert", "zabbixclient"] {
                let extracted = destination.join(binary_name);
                let mode = fs::metadata(extracted).unwrap().permissions().mode() & 0o777;
                assert_ne!(mode & 0o111, 0, "{binary_name} should be executable");
            }
        }
    }
}
