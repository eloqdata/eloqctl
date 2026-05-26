use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::task_base::CmdErr;
use crate::cli::task::task_base::{
    ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::task::task_utils::{check_pid, parse_process_pid, PID_NOT_FOUND, PROCESS_PID};
use crate::cli::{MonitorCommand, SubCommand, CMD_OUTPUT};
use crate::config::config_base::{
    DeployConfig, ALERTMANAGER_FILE_KEY, GRAFANA_FILE_KEY, NODE_EXPORTER_FILE_KEY,
    PROMETHEUSALERT_FILE_KEY, PROMETHEUS_FILE_KEY,
};
use crate::config::monitor::{Monitor, ALERTMANAGER_WEBHOOK_ADAPTER_BINARY};
use crate::config::{
    DeploymentPackage, ALERTMANAGER_CONFIG_FILE, GRAFANA_CONFIG_FILE, PROMETHEUS_CONFIG_FILE,
};
use crate::task_return_value;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, info};

#[derive(Clone, Debug)]
pub enum MonitorComponentCommand {
    NodeExporter { home: String },
    Prometheus { home: String },
    Alertmanager { home: String },
    Grafana { home: String },
    PrometheusAlert { home: String },
}

impl MonitorComponentCommand {
    fn service_name(&self) -> &'static str {
        match self {
            MonitorComponentCommand::NodeExporter { .. } => "node_exporter",
            MonitorComponentCommand::Prometheus { .. } => "prometheus",
            MonitorComponentCommand::Alertmanager { .. } => "alertmanager",
            MonitorComponentCommand::Grafana { .. } => "grafana",
            MonitorComponentCommand::PrometheusAlert { .. } => "alertmanager_webhook_adapter",
        }
    }

    fn log_path(&self) -> &'static str {
        match self {
            MonitorComponentCommand::NodeExporter { .. } => "/tmp/eloq_node_exporter.log",
            MonitorComponentCommand::Prometheus { .. } => "/tmp/eloq_prometheus.log",
            MonitorComponentCommand::Alertmanager { .. } => "/tmp/eloq_alertmanager.log",
            MonitorComponentCommand::Grafana { .. } => "/tmp/eloq_grafana_server.log",
            MonitorComponentCommand::PrometheusAlert { .. } => {
                "/tmp/eloq_alertmanager_webhook_adapter.log"
            }
        }
    }

    fn executable_paths(&self) -> Vec<String> {
        match self {
            MonitorComponentCommand::NodeExporter { home } => vec![format!("{home}/node_exporter")],
            MonitorComponentCommand::Prometheus { home } => vec![format!("{home}/prometheus")],
            MonitorComponentCommand::Alertmanager { home } => {
                vec![format!("{home}/alertmanager")]
            }
            MonitorComponentCommand::Grafana { home } => vec![
                format!("{home}/bin/grafana-server"),
                format!("{home}/bin/grafana"),
            ],
            MonitorComponentCommand::PrometheusAlert { home } => {
                vec![format!("{home}/{ALERTMANAGER_WEBHOOK_ADAPTER_BINARY}")]
            }
        }
    }

    pub fn start(&self, monitor: &Monitor) -> Option<String> {
        match self {
            MonitorComponentCommand::NodeExporter { home } => {
                monitor.node_exporter.as_ref().map(|noex| {
                    format!(
                         r#"{home}/node_exporter --web.listen-address=:{port} > /tmp/eloq_node_exporter.log 2>&1 &"#,
                        home = home,
                        port = noex.port
                    )
                })
            }
            MonitorComponentCommand::Grafana { home } => {
                Some(format!(
                     r#"if [ -x {home}/bin/grafana-server ]; then {home}/bin/grafana-server -homepath {home} -config {home}/conf/{config}; else {home}/bin/grafana server -homepath {home} -config {home}/conf/{config}; fi > /tmp/eloq_grafana_server.log 2>&1 &"#,
                    home = home,
                    config = GRAFANA_CONFIG_FILE
                ))
            }
            MonitorComponentCommand::Alertmanager { home } => monitor.alertmanager.as_ref().map(
                |alertmanager| {
                    format!(
                        r#"{home}/alertmanager --config.file={home}/{config} --storage.path={home}/data --web.listen-address=0.0.0.0:{port} > /tmp/eloq_alertmanager.log 2>&1 &"#,
                        home = home,
                        config = ALERTMANAGER_CONFIG_FILE,
                        port = alertmanager.port
                    )
                },
            ),
            MonitorComponentCommand::Prometheus { home } => {
                monitor.prometheus.as_ref().map(|prom| {
                    let retention_flags = prom.retention_flags();
                    let retention_flags = if retention_flags.is_empty() {
                        String::new()
                    } else {
                        format!(" {retention_flags}")
                    };
                    format!(
                         r#"{home}/prometheus --web.listen-address="0.0.0.0:{port}" --web.enable-lifecycle --storage.tsdb.path={home}/data{retention_flags} --config.file={home}/{PROMETHEUS_CONFIG_FILE} > /tmp/eloq_prometheus.log 2>&1 &"#,
                         home = home,
                         port = prom.port,
                         retention_flags = retention_flags,
                    )
                })
            }
            MonitorComponentCommand::PrometheusAlert { home } => monitor
                .alertmanager
                .as_ref()
                .map(|alertmanager| {
                    format!(
                        r#"sh -c 'cd {home} && exec {home}/{binary} --listen-address=0.0.0.0:{port} --tmpl-dir {home}/templates --tmpl-lang zh --signature EloqKV' > /tmp/eloq_alertmanager_webhook_adapter.log 2>&1 &"#,
                        home = home,
                        binary = ALERTMANAGER_WEBHOOK_ADAPTER_BINARY,
                        port = alertmanager.webhook_adapter_port
                    )
                }),
        }
    }

    pub fn stop(&self, pid: String) -> String {
        format!("kill {pid}")
    }

    pub fn force_stop(&self, pid: String) -> String {
        format!("kill -9 {pid}")
    }

    pub fn reload_url(&self, monitor: &Monitor) -> Option<String> {
        match self {
            MonitorComponentCommand::Prometheus { .. } => monitor
                .prometheus
                .as_ref()
                .map(|prom| format!("http://{}:{}/-/reload", prom.host, prom.port)),
            _ => None,
        }
    }

    pub fn process_info(&self) -> String {
        let monitor_component_home = match &self {
            MonitorComponentCommand::NodeExporter { home } => home,
            MonitorComponentCommand::Prometheus { home } => home,
            MonitorComponentCommand::Alertmanager { home } => home,
            MonitorComponentCommand::Grafana { home } => home,
            MonitorComponentCommand::PrometheusAlert { home } => home,
        };
        format!("ps -e -o pid,cmd --no-headers | grep \"{monitor_component_home}\" | grep -v grep")
    }
}

#[cfg(test)]
mod tests {
    use super::MonitorComponentCommand;
    use crate::config::monitor::{Monitor, Prometheus};

    #[test]
    fn prometheus_start_command_includes_retention_flags() {
        let monitor = Monitor {
            data_dir: None,
            prometheus: Some(Prometheus {
                download_url: "https://example.com/prometheus.tar.gz".to_string(),
                port: 9500,
                host: "127.0.0.1".to_string(),
                retention_time: Some("30d".to_string()),
                retention_size: Some("50GB".to_string()),
                remote_write_urls: None,
                alertmanager_targets: None,
                alert_thresholds: None,
            }),
            alertmanager: None,
            grafana: None,
            node_exporter: None,
            eloq_metrics: None,
        };
        let cmd = MonitorComponentCommand::Prometheus {
            home: "/tmp/prometheus".to_string(),
        }
        .start(&monitor)
        .unwrap();

        assert!(cmd.contains("--web.enable-lifecycle"));
        assert!(cmd.contains(r#"--storage.tsdb.retention.time="30d""#));
        assert!(cmd.contains(r#"--storage.tsdb.retention.size="50GB""#));
    }
}

#[derive(Clone, Debug)]
pub struct MonitorCtlTask {
    config: DeployConfig,
    task_id: TaskId,
    monitor_ctl: MonitorComponentCommand,
    cmd_args: SubCommand,
}

impl MonitorCtlTask {
    async fn start_failure_details(&self, ssh_session: SSHSession) -> String {
        let mut sections = Vec::new();
        let log_cmd = format!(
            "tail -n 40 {} 2>/dev/null || echo '<no startup log output>'",
            self.monitor_ctl.log_path()
        );
        if let Ok(log_rs) = ssh_session.command(&log_cmd, CollectOutput).await {
            let output = TaskArgValue::into_inner_value::<String>(
                log_rs
                    .get(CMD_OUTPUT)
                    .cloned()
                    .unwrap_or(TaskArgValue::Str(String::new())),
            )
            .trim()
            .to_string();
            if !output.is_empty() {
                sections.push(format!("startup log:\n{output}"));
            }
        }

        let files = self.monitor_ctl.executable_paths();
        if !files.is_empty() {
            let inspect_cmd = format!("ls -l {}", files.join(" "));
            if let Ok(file_rs) = ssh_session.command(&inspect_cmd, CollectOutput).await {
                let output = TaskArgValue::into_inner_value::<String>(
                    file_rs
                        .get(CMD_OUTPUT)
                        .cloned()
                        .unwrap_or(TaskArgValue::Str(String::new())),
                )
                .trim()
                .to_string();
                if !output.is_empty() {
                    sections.push(format!("file modes:\n{output}"));
                }
            }
        }

        if sections.is_empty() {
            format!(
                "no additional diagnostics captured for {}",
                self.monitor_ctl.service_name()
            )
        } else {
            sections.join("\n")
        }
    }

    async fn wait_started(
        ssh_session: SSHSession,
        process_info_cmd: String,
    ) -> anyhow::Result<ExecutionValue> {
        for _ in 0..15 {
            let process_rs = check_pid(
                process_info_cmd.clone(),
                ssh_session.clone(),
                parse_process_pid,
            )
            .await?;
            let pid = TaskArgValue::into_inner_value::<String>(
                process_rs.get(PROCESS_PID).unwrap().clone(),
            );
            if pid != PID_NOT_FOUND {
                return Ok(process_rs);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        check_pid(process_info_cmd, ssh_session, parse_process_pid).await
    }

    async fn wait_stopped(
        ssh_session: SSHSession,
        process_info_cmd: String,
    ) -> anyhow::Result<ExecutionValue> {
        for _ in 0..15 {
            let process_rs = check_pid(
                process_info_cmd.clone(),
                ssh_session.clone(),
                parse_process_pid,
            )
            .await?;
            let pid = TaskArgValue::into_inner_value::<String>(
                process_rs.get(PROCESS_PID).unwrap().clone(),
            );
            if pid == PID_NOT_FOUND {
                return Ok(process_rs);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        check_pid(process_info_cmd, ssh_session, parse_process_pid).await
    }

    fn monitor_command_name(command: &MonitorCommand) -> &'static str {
        match command {
            MonitorCommand::Start { .. } => "start",
            MonitorCommand::Stop { .. } => "stop",
            MonitorCommand::Restart { .. } => "restart",
            MonitorCommand::Status { .. } => "status",
            MonitorCommand::Update { .. } => "update",
        }
    }

    pub fn new(
        config: DeployConfig,
        task_id: TaskId,
        ctl_cmd: MonitorComponentCommand,
        cmd_args: SubCommand,
    ) -> Self {
        Self {
            config,
            task_id,
            monitor_ctl: ctl_cmd,
            cmd_args,
        }
    }

    fn build_monitor_task_instance(
        config: DeployConfig,
        ctl_cmd: MonitorComponentCommand,
        task_name: String,
        task_remote_host: TaskHost,
        cmd_arg: SubCommand,
    ) -> (TaskId, TaskInstance) {
        let (_, _, host) = &task_remote_host.ssh_conn_tuple();
        let cmd_ref = &cmd_arg.as_ref().to_string();
        let task_id = TaskId {
            cmd: cmd_ref.to_string(),
            task: task_name.clone(),
            host: host.to_string(),
        };
        (
            task_id.clone(),
            TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(MonitorCtlTask::new(
                    config.clone(),
                    task_id,
                    ctl_cmd,
                    cmd_arg,
                )),
                task_host: task_remote_host,
            },
        )
    }

    fn basic_component_ctl_task(
        cmd_args: SubCommand,
        config: &DeployConfig,
        cmd_component: MonitorComponentCommand,
        monitor_component_host: String,
        task_name: String,
    ) -> IndexMap<TaskId, TaskInstance> {
        let task_host = TaskHost::remote(&config.connection, monitor_component_host);
        let (task_id, task_instance) = MonitorCtlTask::build_monitor_task_instance(
            config.clone(),
            cmd_component,
            task_name,
            task_host,
            cmd_args.clone(),
        );
        let mut index_map = IndexMap::new();
        index_map.insert(task_id, task_instance);
        index_map
    }

    pub fn grafana_ctl_task(
        cmd_arg: SubCommand,
        config: &DeployConfig,
    ) -> IndexMap<TaskId, TaskInstance> {
        let monitor = config.deployment.monitor.as_ref().unwrap();

        // Check if Grafana is configured
        if let Some(component_obj) = &monitor.grafana {
            let component_host = component_obj.host.clone();
            let home = format!("{}/{}", config.install_dir(), GRAFANA_FILE_KEY);
            let cmd = MonitorComponentCommand::Grafana { home };
            let task_name = match &cmd_arg {
                SubCommand::Monitor { command, .. } => format!(
                    "grafana-{}-{}",
                    Self::monitor_command_name(command),
                    component_obj.port
                ),
                _ => unreachable!(),
            };
            MonitorCtlTask::basic_component_ctl_task(
                cmd_arg,
                config,
                cmd,
                component_host,
                task_name,
            )
        } else {
            // Return an empty IndexMap if Grafana is not configured
            IndexMap::new()
        }
    }

    pub fn prometheus_ctl_task(
        cmd_arg: SubCommand,
        config: &DeployConfig,
    ) -> IndexMap<TaskId, TaskInstance> {
        let monitor = config.deployment.monitor.as_ref().unwrap();

        // Check if Prometheus is configured
        if let Some(component_obj) = &monitor.prometheus {
            let component_host = component_obj.host.clone();
            let home = format!("{}/{}", config.install_dir(), PROMETHEUS_FILE_KEY);
            let cmd = MonitorComponentCommand::Prometheus { home };
            let task_name = match &cmd_arg {
                SubCommand::Monitor { command, .. } => format!(
                    "prometheus-{}-{}",
                    Self::monitor_command_name(command),
                    component_obj.port
                ),
                _ => unreachable!(),
            };
            MonitorCtlTask::basic_component_ctl_task(
                cmd_arg,
                config,
                cmd,
                component_host,
                task_name,
            )
        } else {
            // Return an empty IndexMap if Prometheus is not configured
            IndexMap::new()
        }
    }

    pub fn alertmanager_ctl_task(
        cmd_arg: SubCommand,
        config: &DeployConfig,
    ) -> IndexMap<TaskId, TaskInstance> {
        let monitor = config.deployment.monitor.as_ref().unwrap();
        if let Some(component_obj) = &monitor.alertmanager {
            let component_host = component_obj.host.clone();
            let home = format!("{}/{}", config.install_dir(), ALERTMANAGER_FILE_KEY);
            let cmd = MonitorComponentCommand::Alertmanager { home };
            let task_name = match &cmd_arg {
                SubCommand::Monitor { command, .. } => format!(
                    "alertmanager-{}-{}",
                    Self::monitor_command_name(command),
                    component_obj.port
                ),
                _ => unreachable!(),
            };
            MonitorCtlTask::basic_component_ctl_task(
                cmd_arg,
                config,
                cmd,
                component_host,
                task_name,
            )
        } else {
            IndexMap::new()
        }
    }

    pub fn exporter_ctl_task(
        cmd_arg: SubCommand,
        config: &DeployConfig,
    ) -> IndexMap<TaskId, TaskInstance> {
        let cmd_str_ref = match &cmd_arg {
            SubCommand::Monitor { command, .. } => Self::monitor_command_name(command),
            _ => unreachable!(),
        };
        let monitor = config.deployment.monitor.as_ref().unwrap();
        let install_dir = config.install_dir();
        let all_hosts = config.get_host_as_map();
        all_hosts
            .iter()
            .filter(|(pkg, _hosts)| {
                matches!(
                    pkg,
                    DeploymentPackage::EloqTx
                        | DeploymentPackage::EloqStandby
                        | DeploymentPackage::EloqLog
                        | DeploymentPackage::Storage
                )
            })
            .flat_map(|(_pkg, hosts)| {
                hosts
                    .iter()
                    .unique()
                    .map(|host| {
                        let task_remote_host = TaskHost::remote(&config.connection, host);
                        let node_exporter_cmd = MonitorComponentCommand::NodeExporter {
                            home: format!("{install_dir}/{NODE_EXPORTER_FILE_KEY}"),
                        };
                        vec![MonitorCtlTask::build_monitor_task_instance(
                            config.clone(),
                            node_exporter_cmd,
                            format!(
                                "node_exporter-{cmd_str_ref}-{}",
                                monitor.node_exporter.as_ref().unwrap().port
                            ),
                            task_remote_host,
                            cmd_arg.clone(),
                        )]
                    })
                    .collect_vec()
            })
            .flatten()
            .collect::<IndexMap<TaskId, TaskInstance>>()
    }

    pub fn prometheusalert_ctl_task(
        cmd_arg: SubCommand,
        config: &DeployConfig,
    ) -> IndexMap<TaskId, TaskInstance> {
        let monitor = config.deployment.monitor.as_ref().unwrap();
        if let Some(component_obj) = &monitor.alertmanager {
            let component_host = component_obj.host.clone();
            let home = format!("{}/{}", config.install_dir(), PROMETHEUSALERT_FILE_KEY);
            let cmd = MonitorComponentCommand::PrometheusAlert { home };
            let task_name = match &cmd_arg {
                SubCommand::Monitor { command, .. } => format!(
                    "alertmanager-webhook-adapter-{}-{}",
                    Self::monitor_command_name(command),
                    component_obj.webhook_adapter_port
                ),
                _ => unreachable!(),
            };
            MonitorCtlTask::basic_component_ctl_task(
                cmd_arg,
                config,
                cmd,
                component_host,
                task_name,
            )
        } else {
            IndexMap::new()
        }
    }
}

#[async_trait::async_trait]
impl TaskExecutor for MonitorCtlTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        task_host: TaskHost,
        _task_arg: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        info!("execute {}", self.task_id.format_string());
        let ssh_session =
            SSHSession::from_task_host(task_host, self.config.connection.ssh_auth_key().unwrap())
                .await?;

        let cmd_str = match &self.cmd_args {
            SubCommand::Monitor { command, .. } => Self::monitor_command_name(command).to_string(),
            _ => unreachable!(),
        };
        let process_info_cmd = self.monitor_ctl.process_info();
        let process_rs = check_pid(
            process_info_cmd.clone(),
            ssh_session.clone(),
            parse_process_pid,
        )
        .await?;
        let monitor_component_pid =
            TaskArgValue::into_inner_value::<String>(process_rs.get(PROCESS_PID).unwrap().clone());
        let monitor_ref = self.config.deployment.monitor.as_ref().unwrap();

        let monitor_ctl_cmd_result = match cmd_str.as_str() {
            "start" => {
                if monitor_component_pid.eq(PID_NOT_FOUND) {
                    if let Some(start_cmd) = self.monitor_ctl.start(monitor_ref) {
                        debug!(r#"MonitorCtlTask start_cmd={start_cmd:?}"#);
                        ssh_session
                            .command(start_cmd.as_str(), CollectOutput)
                            .await?;
                        let started =
                            Self::wait_started(ssh_session.clone(), process_info_cmd).await?;
                        let pid = TaskArgValue::into_inner_value::<String>(
                            started.get(PROCESS_PID).unwrap().clone(),
                        );
                        if pid == PID_NOT_FOUND {
                            let details = self.start_failure_details(ssh_session.clone()).await;
                            Err(anyhow::anyhow!(
                                "monitor component {} failed to start\n{}",
                                self.task_id.format_string(),
                                details
                            ))
                        } else {
                            Ok(started)
                        }
                    } else {
                        Ok(process_rs)
                    }
                } else if let Some(reload_url) = self.monitor_ctl.reload_url(monitor_ref) {
                    info!("Prometheus reload: POST {reload_url}");
                    let resp = reqwest::Client::new()
                        .post(&reload_url)
                        .timeout(std::time::Duration::from_secs(10))
                        .send()
                        .await;
                    match resp {
                        Ok(r) if r.status().is_success() => {
                            info!("Prometheus reloaded successfully");
                        }
                        Ok(r) => {
                            let status = r.status();
                            let body = r.text().await.unwrap_or_default();
                            info!("Prometheus reload returned {status}: {body}");
                        }
                        Err(e) => {
                            info!("Prometheus reload request failed: {e}");
                        }
                    }
                    Ok(process_rs)
                } else {
                    Ok(process_rs)
                }
            }
            "stop" => {
                if !monitor_component_pid.eq(PID_NOT_FOUND) {
                    let stop_cmd = self.monitor_ctl.stop(monitor_component_pid.clone());
                    ssh_session
                        .command(stop_cmd.as_str(), CollectOutput)
                        .await?;
                    let stop_rs =
                        Self::wait_stopped(ssh_session.clone(), process_info_cmd.clone()).await?;
                    let pid = TaskArgValue::into_inner_value::<String>(
                        stop_rs.get(PROCESS_PID).unwrap().clone(),
                    );
                    if pid == PID_NOT_FOUND {
                        Ok(stop_rs)
                    } else {
                        let force_stop_cmd = self.monitor_ctl.force_stop(pid);
                        ssh_session
                            .command(force_stop_cmd.as_str(), CollectOutput)
                            .await?;
                        let force_stop_rs =
                            Self::wait_stopped(ssh_session.clone(), process_info_cmd).await?;
                        let force_pid = TaskArgValue::into_inner_value::<String>(
                            force_stop_rs.get(PROCESS_PID).unwrap().clone(),
                        );
                        if force_pid == PID_NOT_FOUND {
                            Ok(force_stop_rs)
                        } else {
                            Err(anyhow::anyhow!(
                                "monitor component {} failed to stop after SIGTERM and SIGKILL",
                                self.task_id.format_string()
                            ))
                        }
                    }
                } else {
                    Ok(process_rs)
                }
            }
            "status" => Ok(process_rs),
            _ => {
                unreachable!()
            }
        };
        ssh_session.close().await?;
        let mut ctl_rtn_value = monitor_ctl_cmd_result?;
        if cmd_str == "status" && ctl_rtn_value.contains_key(PROCESS_PID) {
            let pid = TaskArgValue::into_inner_value::<String>(
                ctl_rtn_value.get(PROCESS_PID).unwrap().clone(),
            );
            let service_name = self.monitor_ctl.service_name();
            let output = if pid == PID_NOT_FOUND {
                format!("\n{service_name} is down.")
            } else {
                format!("\n{service_name} is running, pid: {pid}.")
            };
            ctl_rtn_value.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(output));
        }
        task_return_value!(
            ctl_rtn_value,
            |status_code: i32| -> CmdErr {
                CmdErr::MonitorCtlCmdErr(self.task_id.format_string(), status_code.to_string())
            },
            "MonitorCtlTask"
        )
    }
}
