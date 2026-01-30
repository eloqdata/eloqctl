use crate::cli::ssh::SSHSession;
use crate::cli::task::grpc::cc_request::ClusterBackupResponse;
use crate::cli::task::task_base::TaskHost;
use crate::cli::task::task_utils::{check_pid, parse_process_pid, PID_NOT_FOUND, PROCESS_PID};
use crate::config::config_base::DeployConfig;
use crate::config::storage_service_config::DataStoreServiceBackend;
use crate::config::DeploymentPackage;
use crate::state::snapshot_info_operation::SnapshotEntity;
use anyhow::{Context, Result};
use regex::Regex;
use tracing::{info, warn};

/// Join manifest filenames into comma-separated string
/// Validates that manifest filenames don't contain commas
/// For single manifest, returns as-is (backward compatible)
pub fn join_manifests(manifests: &[String]) -> String {
    if manifests.is_empty() {
        return String::new();
    }

    // Validate no manifest contains commas (safety check)
    for manifest in manifests {
        if manifest.contains(',') {
            tracing::warn!(
                "Manifest filename contains comma (unexpected): {}",
                manifest
            );
        }
    }

    if manifests.len() == 1 {
        manifests[0].clone()
    } else {
        manifests.join(",")
    }
}

/// Split comma-separated manifest string into vector
/// Handles empty strings, single strings, and comma-separated strings
/// Trims whitespace and filters empty strings
pub fn split_manifests(manifest_str: &str) -> Vec<String> {
    if manifest_str.is_empty() {
        return vec![];
    }

    // Trim whitespace from each manifest and filter empty strings
    manifest_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Extract all manifest filenames from ClusterBackupResponse
/// Returns all manifests from all node groups
pub fn extract_all_manifests(response: &ClusterBackupResponse) -> Vec<String> {
    let mut manifests = Vec::new();
    for backup_info in &response.backup_infos {
        for backup_file in &backup_info.backup_files {
            manifests.push(backup_file.clone());
        }
    }
    manifests
}

/// Extract backup_ts from ClusterBackupResponse for EloqStore
/// Returns the first non-zero backup_ts found, or None if not found
/// For EloqStore, backup_files is empty but backup_ts contains the timestamp
pub fn extract_backup_ts(response: &ClusterBackupResponse) -> Option<String> {
    for backup_info in &response.backup_infos {
        if backup_info.backup_ts != 0 {
            return Some(backup_info.backup_ts.to_string());
        }
    }
    None
}

/// Format snapshots for deletion confirmation display
/// cluster_config: Used to determine storage type (RocksDB vs EloqStore)
pub fn format_snapshots_for_deletion(
    snapshots: &[&SnapshotEntity],
    cluster_config: Option<&DeployConfig>,
) -> String {
    if snapshots.is_empty() {
        return "No backups to delete.".to_string();
    }

    // Determine if cluster uses EloqStore cloud storage
    let is_eloqstore_cloud = cluster_config
        .and_then(|config| config.deployment.storage_service.as_ref())
        .map(|storage| {
            storage
                .eloqdss
                .as_ref()
                .map(|dss| {
                    matches!(
                        dss.backend_config(),
                        DataStoreServiceBackend::EloqStore(config) if config.is_cloud_mode()
                    )
                })
                .unwrap_or(false)
        })
        .unwrap_or(false);

    // Use appropriate column header based on storage type
    let column_header = if is_eloqstore_cloud {
        "Timestamp(us)"
    } else {
        "Manifest(s)"
    };

    let mut output = format!(
        "\nThe following {} backup(s) will be deleted:\n\n",
        snapshots.len()
    );
    output.push_str(&format!(
        "Cluster Name | Snapshot Timestamp | Storage Type | {}\n",
        column_header
    ));
    output.push_str("-------------|-------------------|--------------|------------\n");

    for snapshot in snapshots {
        let storage_type = if snapshot.dest_host.is_empty() {
            "cloud (S3)"
        } else {
            "local"
        };

        let manifest_display = if snapshot.dest_host.is_empty() {
            // Cloud storage
            if is_eloqstore_cloud {
                // EloqStore: snapshot_path contains backup_ts (timestamp in microseconds)
                snapshot.snapshot_path.trim().to_string()
            } else {
                // RocksDB: snapshot_path contains manifest filenames (comma-separated)
                let manifests = split_manifests(&snapshot.snapshot_path);
                if manifests.len() == 1 {
                    manifests[0].clone()
                } else {
                    format!("[{} manifests]: {}", manifests.len(), manifests.join(", "))
                }
            }
        } else {
            // Local storage: show path
            snapshot.snapshot_path.clone()
        };

        output.push_str(&format!(
            "{} | {} | {} | {}\n",
            snapshot.cluster_name,
            snapshot.snapshot_ts.format("%Y-%m-%d %H:%M:%S UTC"),
            storage_type,
            manifest_display
        ));
    }

    output.push('\n');
    output
}

/// Format snapshot info for restore confirmation display
/// Shows detailed information about the snapshot to be restored
/// is_eloqstore_cloud: if true, snapshot_path contains backup_ts; if false, contains manifest filenames
pub fn format_snapshot_for_restore(snapshot: &SnapshotEntity, is_eloqstore_cloud: bool) -> String {
    let mut output = String::new();

    output.push_str("\n========================================\n");
    output.push_str("RESTORE OPERATION CONFIRMATION\n");
    output.push_str("========================================\n\n");

    output.push_str("WARNING: This operation will OVERRIDE existing database data!\n");
    output.push_str("The current database state will be replaced with the snapshot data.\n\n");

    output.push_str("Snapshot Details:\n");
    output.push_str(&format!("  Cluster Name: {}\n", snapshot.cluster_name));
    output.push_str(&format!(
        "  Snapshot Timestamp: {}\n",
        snapshot.snapshot_ts.format("%Y-%m-%d %H:%M:%S UTC")
    ));

    let status_str = match snapshot.snapshot_status {
        0 => "Finished",
        1 => "Failed",
        2 => "Running",
        _ => "Unknown",
    };
    output.push_str(&format!("  Status: {}\n", status_str));

    if snapshot.snapshot_status != 0 {
        output.push_str("\n  ⚠️  WARNING: Snapshot status is not 'Finished'. Restore may fail!\n");
    }

    // Display information based on storage type
    if is_eloqstore_cloud {
        // EloqStore: snapshot_path contains backup_ts
        let backup_ts = snapshot.snapshot_path.trim();
        if !backup_ts.is_empty() {
            output.push_str(&format!("  Backup Timestamp: {}\n", backup_ts));
        } else {
            output.push_str("  ⚠️  WARNING: No backup timestamp found in snapshot!\n");
        }
    } else {
        // RocksDB: snapshot_path contains manifest filenames
        let manifests = split_manifests(&snapshot.snapshot_path);
        if !manifests.is_empty() {
            output.push_str(&format!("  Manifest Files: {}\n", manifests.len()));
            for (idx, manifest) in manifests.iter().enumerate() {
                output.push_str(&format!("    {}. {}\n", idx + 1, manifest));
            }
        } else {
            output.push_str("  ⚠️  WARNING: No manifest files found in snapshot!\n");
        }
    }

    output.push('\n');
    output.push_str("========================================\n");

    output
}

/// Check if cluster is stopped by reusing the same process checking logic as status command
/// Returns true if cluster is stopped, false if any service is running
/// This reuses the exact same check_pid infrastructure used by status tasks
pub async fn is_cluster_stopped(config: &DeployConfig) -> Result<bool> {
    info!("Checking if cluster is stopped using status command logic...");

    // Check TxService processes - this is the main service that must be stopped
    let tx_host_ports = config.get_host_port_list(DeploymentPackage::MonographTx);
    if tx_host_ports.is_empty() {
        // No nodes configured, consider stopped
        info!("No TxService nodes configured, cluster is stopped");
        return Ok(true);
    }

    let tx_bin = config.deployment.tx_srv_bin();
    let conn_user = &config.connection.username;
    let ssh_port = config.connection.ssh_port() as usize;
    let auth_key = config
        .connection
        .ssh_auth_key()
        .ok_or_else(|| anyhow::anyhow!("SSH auth key not configured"))?;

    // Check each TxService node
    for host_port in &tx_host_ports {
        let parts: Vec<&str> = host_port.split(':').collect();
        if parts.len() != 2 {
            warn!("Invalid host:port format: {}", host_port);
            continue;
        }
        let host = parts[0];
        let port = parts[1];

        // Create SSH session for this host (reusing same pattern as status tasks)
        let task_host = TaskHost::Remote {
            user: conn_user.to_string(),
            port: ssh_port,
            host: host.to_string(),
        };

        let ssh_session = SSHSession::from_task_host(task_host, auth_key.clone())
            .await
            .with_context(|| format!("Failed to create SSH session to {}", host))?;

        // Use the same status check command pattern as MonographTxCtlTask
        // This is the exact same logic used by the status command
        let status_cmd = format!(
            "ps uxwe -u {} | grep {} | grep {}.ini | grep -v grep | awk '{{print $2}}'",
            conn_user, tx_bin, port
        );

        // Use the same check_pid function that status tasks use
        let result = check_pid(status_cmd, ssh_session.clone(), parse_process_pid).await?;

        ssh_session.close().await?;

        // Check if process is running (same logic as status tasks)
        if let Some(pid_value) = result.get(PROCESS_PID) {
            let pid = crate::cli::task::task_base::TaskArgValue::into_inner_value::<String>(
                pid_value.clone(),
            );
            if !pid.is_empty() && pid != PID_NOT_FOUND {
                info!(
                    "Found running eloqkv process on {}:{} (PID: {})",
                    host, port, pid
                );
                return Ok(false); // Cluster is running
            }
        }
    }

    // Optionally check LogService if configured (similar pattern)
    if let Some(_log_service) = &config.deployment.log_service {
        // For Phase 2, we focus on TxService as the primary check
        // LogService check can be added later if needed
        info!("LogService configured, but checking TxService is sufficient for Phase 2");
    }

    info!("No running processes found - cluster appears to be stopped");
    Ok(true)
}

/// Parse snapshot manifest filename to extract components
/// Format: <snapshot_name>-<ng_id>-<backup_ts>
/// Returns: (snapshot_name, ng_id, backup_ts)
pub fn parse_snapshot_manifest(manifest: &str) -> Result<(String, u32, String)> {
    // Remove CLOUDMANIFEST- prefix if present
    let manifest = manifest.strip_prefix("CLOUDMANIFEST-").unwrap_or(manifest);

    // Pattern: <name>-<ng_id>-<timestamp>
    // Example: snapshot-jepsen-eloqkv-rocksdbcloud-single-127.0.0.1:6389-2025-11-05-03-45-45-0-1762314323392964
    // ng_id is the number before the last dash-separated segment (timestamp)

    let parts: Vec<&str> = manifest.rsplitn(3, '-').collect();
    if parts.len() < 3 {
        return Err(anyhow::anyhow!(
            "Invalid snapshot manifest format: {}. Expected: <name>-<ng_id>-<backup_ts>",
            manifest
        ));
    }

    let backup_ts = parts[0].to_string();
    let ng_id_str = parts[1];
    let snapshot_name = parts[2..]
        .iter()
        .rev()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("-");

    let ng_id = ng_id_str
        .parse::<u32>()
        .context(format!("Failed to parse ng_id from manifest: {}", manifest))?;

    Ok((snapshot_name, ng_id, backup_ts))
}

/// Parse current database manifest filename to extract ng_id and epoch
/// Format: CLOUDMANIFEST-development-<ng_id>-<epoch>
/// Returns: (ng_id, epoch)
pub fn parse_database_manifest(manifest: &str) -> Result<(u32, u64)> {
    // Remove CLOUDMANIFEST- prefix
    let manifest = manifest
        .strip_prefix("CLOUDMANIFEST-")
        .ok_or_else(|| anyhow::anyhow!("Manifest must start with CLOUDMANIFEST-: {}", manifest))?;

    // Pattern: development-<ng_id>-<epoch>
    let re = Regex::new(r"^development-(\d+)-(\d+)$").context("Failed to compile regex")?;

    let caps = re
        .captures(manifest)
        .ok_or_else(|| anyhow::anyhow!("Invalid database manifest format: {}", manifest))?;

    let ng_id = caps[1].parse::<u32>().context("Failed to parse ng_id")?;
    let epoch = caps[2].parse::<u64>().context("Failed to parse epoch")?;

    Ok((ng_id, epoch))
}

/// Find maximum epoch for a given ng_id from list of manifest keys
/// Returns the maximum epoch found, or 0 if none found
pub fn find_max_epoch_for_ng(manifest_keys: &[String], ng_id: u32) -> Result<u64> {
    let prefix = format!("CLOUDMANIFEST-development-{}-", ng_id);
    let mut max_epoch = 0u64;

    for key in manifest_keys {
        // Extract just the filename from the key (handle paths)
        let filename = key.split('/').last().unwrap_or(key);

        if filename.starts_with(&prefix) {
            match parse_database_manifest(filename) {
                Ok((parsed_ng_id, epoch)) if parsed_ng_id == ng_id => {
                    if epoch > max_epoch {
                        max_epoch = epoch;
                    }
                }
                _ => continue,
            }
        }
    }

    Ok(max_epoch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_manifests_empty() {
        assert_eq!(join_manifests(&[]), "");
    }

    #[test]
    fn test_join_manifests_single() {
        assert_eq!(join_manifests(&["manifest1".to_string()]), "manifest1");
    }

    #[test]
    fn test_join_manifests_multiple() {
        let manifests = vec!["manifest1".to_string(), "manifest2".to_string()];
        assert_eq!(join_manifests(&manifests), "manifest1,manifest2");
    }

    #[test]
    fn test_split_manifests_empty() {
        assert_eq!(split_manifests(""), vec![] as Vec<String>);
    }

    #[test]
    fn test_split_manifests_single() {
        assert_eq!(split_manifests("manifest1"), vec!["manifest1"]);
    }

    #[test]
    fn test_split_manifests_multiple() {
        assert_eq!(
            split_manifests("manifest1,manifest2,manifest3"),
            vec!["manifest1", "manifest2", "manifest3"]
        );
    }

    #[test]
    fn test_split_manifests_with_spaces() {
        assert_eq!(
            split_manifests("manifest1, manifest2 , manifest3"),
            vec!["manifest1", "manifest2", "manifest3"]
        );
    }
}
