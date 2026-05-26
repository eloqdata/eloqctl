use crate::cli::task::task_base::{TaskArgValue, TaskResultEnum, TaskResultPair};
use crate::cli::{CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeployConfig;
use itertools::Itertools;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservedStatus {
    Up,
    Down,
    Unknown,
    Error,
    Ok,
}

impl std::fmt::Display for ObservedStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Up => write!(f, "UP"),
            Self::Down => write!(f, "DOWN"),
            Self::Unknown => write!(f, "UNKNOWN"),
            Self::Error => write!(f, "ERROR"),
            Self::Ok => write!(f, "OK"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedServiceStatus {
    pub host: String,
    pub service: String,
    pub port: String,
    pub status: ObservedStatus,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedCluster {
    pub cluster: String,
    pub services: Vec<ObservedServiceStatus>,
}

impl ObservedCluster {
    pub fn from_task_results(cluster: String, task_results: &[TaskResultPair]) -> Self {
        let services = task_results
            .iter()
            .filter_map(observed_service_from_task_result)
            .collect_vec();
        Self { cluster, services }
    }

    pub fn has_errors(&self) -> bool {
        self.critical_services()
            .any(|service| service.status == ObservedStatus::Error)
    }

    pub fn has_running_service(&self, service_name: &str) -> bool {
        self.services
            .iter()
            .any(|service| service.service == service_name && service.status == ObservedStatus::Up)
    }

    pub fn unavailable_services(&self) -> Vec<&ObservedServiceStatus> {
        self.critical_services()
            .filter(|service| {
                matches!(
                    service.status,
                    ObservedStatus::Down | ObservedStatus::Unknown | ObservedStatus::Error
                )
            })
            .collect_vec()
    }

    fn critical_services(&self) -> impl Iterator<Item = &ObservedServiceStatus> {
        self.services.iter().filter(|service| {
            matches!(
                service.service.as_str(),
                "tx" | "standby" | "voter" | "log" | "dss"
            )
        })
    }

    pub fn print(&self) {
        if self.services.is_empty() {
            println!(
                "No live observed service rows found for cluster '{}'.",
                self.cluster
            );
            return;
        }

        println!("Observed live state for cluster '{}':", self.cluster);
        for service in &self.services {
            println!(
                "  - {} {}:{} {} {}",
                service.service, service.host, service.port, service.status, service.detail
            );
        }
    }
}

fn parse_task_id_parts(task_id: &str) -> (String, String, String) {
    let mut host = String::new();
    let mut cmd = String::new();
    let mut task = String::new();
    for part in task_id.split(',') {
        if let Some((k, v)) = part.split_once('=') {
            match k.trim() {
                "host" => host = v.trim().to_string(),
                "cmd" => cmd = v.trim().to_string(),
                "task" => task = v.trim().to_string(),
                _ => {}
            }
        }
    }
    (host, cmd, task)
}

fn parse_monitor_status_task(task: &str) -> Option<(String, String)> {
    [
        ("prometheus-status-", "prometheus"),
        ("alertmanager-status-", "alertmanager"),
        ("grafana-status-", "grafana"),
        (
            "alertmanager-webhook-adapter-status-",
            "alertmanager_webhook_adapter",
        ),
        ("node_exporter-status-", "node_exporter"),
    ]
    .into_iter()
    .find_map(|(prefix, service)| {
        task.strip_prefix(prefix)
            .map(|port| (service.to_string(), port.to_string()))
    })
}

fn service_and_port_from_task(cmd: &str, task: &str) -> Option<(String, String)> {
    let tx_prefix = ["tx-status-", "txservice-status-"]
        .into_iter()
        .find(|prefix| task.starts_with(prefix));
    if let Some(prefix) = tx_prefix {
        Some((
            "tx".to_string(),
            task.trim_start_matches(prefix).to_string(),
        ))
    } else if task.starts_with("standby-status-") {
        Some((
            "standby".to_string(),
            task.trim_start_matches("standby-status-").to_string(),
        ))
    } else if task.starts_with("voter-status-") {
        Some((
            "voter".to_string(),
            task.trim_start_matches("voter-status-").to_string(),
        ))
    } else if cmd.starts_with("eloq_log_") && task == "status" {
        Some(("log".to_string(), "-".to_string()))
    } else if cmd.starts_with("dss_") && task == "status" {
        Some(("dss".to_string(), "-".to_string()))
    } else {
        parse_monitor_status_task(task)
    }
}

fn observed_service_from_task_result(pair: &TaskResultPair) -> Option<ObservedServiceStatus> {
    let (host, cmd, task) = parse_task_id_parts(&pair.task_id);
    if host.is_empty() {
        return None;
    }
    let (service, port) = service_and_port_from_task(&cmd, &task)?;

    match &pair.result {
        TaskResultEnum::Error(err) => Some(ObservedServiceStatus {
            host,
            service,
            port,
            status: ObservedStatus::Error,
            detail: err.replace('\n', " "),
        }),
        TaskResultEnum::Success(Some(ev)) => {
            let status_code = ev
                .get(CMD_STATUS)
                .map(|v| TaskArgValue::into_inner_value::<i32>(v.clone()))
                .unwrap_or(0);
            let detail = ev
                .get(CMD_OUTPUT)
                .map(|v| TaskArgValue::into_inner_value::<String>(v.clone()))
                .unwrap_or_default()
                .replace('\n', " ")
                .trim()
                .to_string();
            let low = detail.to_ascii_lowercase();
            let status = if status_code != 0 {
                ObservedStatus::Error
            } else if low.contains("running") {
                ObservedStatus::Up
            } else if low.contains("down") {
                ObservedStatus::Down
            } else if low.contains("unknown") {
                ObservedStatus::Unknown
            } else {
                ObservedStatus::Ok
            };
            Some(ObservedServiceStatus {
                host,
                service,
                port,
                status,
                detail,
            })
        }
        TaskResultEnum::Success(None) => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileAction {
    SaveClusterIndex,
    RegenerateEloqKvNodeConfig,
    RestartTxWithUpdatedConfig,
    RestartMonitor,
    VerifyClusterStatus,
}

impl ReconcileAction {
    pub fn description(&self) -> &'static str {
        match self {
            Self::SaveClusterIndex => "update local cluster index metadata",
            Self::RegenerateEloqKvNodeConfig => "regenerate EloqKV node config files",
            Self::RestartTxWithUpdatedConfig => "restart tx nodes with updated config",
            Self::RestartMonitor => "restart monitor services",
            Self::VerifyClusterStatus => "verify live cluster status",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReconcilePlan {
    pub cluster: String,
    pub merged_config: DeployConfig,
    pub observed: Option<ObservedCluster>,
    pub changes: Vec<String>,
    pub unsupported_changes: Vec<String>,
    pub tx_field_updates: Vec<String>,
    pub actions: Vec<ReconcileAction>,
}

impl ReconcilePlan {
    pub fn new(cluster: String, merged_config: DeployConfig) -> Self {
        Self {
            cluster,
            merged_config,
            observed: None,
            changes: Vec::new(),
            unsupported_changes: Vec::new(),
            tx_field_updates: Vec::new(),
            actions: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty() && self.tx_field_updates.is_empty() && self.actions.is_empty()
    }

    pub fn add_action(&mut self, action: ReconcileAction) {
        if !self.actions.contains(&action) {
            self.actions.push(action);
        }
    }

    pub fn print(&self) {
        if let Some(observed) = &self.observed {
            observed.print();
        }

        if self.is_empty() {
            println!("No supported configuration changes detected.");
            return;
        }

        println!("Plan for cluster '{}':", self.cluster);

        if !self.changes.is_empty() {
            println!("Supported changes:");
            for change in &self.changes {
                println!("  - {change}");
            }
        }

        if !self.tx_field_updates.is_empty() {
            println!("Tx config updates:");
            for field in &self.tx_field_updates {
                println!("  - {field}");
            }
        }

        if !self.actions.is_empty() {
            println!("Actions:");
            for action in &self.actions {
                println!("  - {}", action.description());
            }
        }

        if !self.unsupported_changes.is_empty() {
            println!("Unsupported changes ignored by current reconcile engine:");
            for change in &self.unsupported_changes {
                println!("  - {change}");
            }
        }
    }
}
