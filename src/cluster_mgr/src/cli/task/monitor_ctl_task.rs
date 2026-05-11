use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::task_base::CmdErr;
use crate::cli::task::task_base::{
    ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::task::task_utils::{
    check_pid, ctl_action_wait_complete, parse_process_pid, PID_NOT_FOUND, PROCESS_PID,
};
use crate::cli::{SubCommand, CMD_OUTPUT};
use crate::config::config_base::{
    DeployConfig, GRAFANA_FILE_KEY, MYSQL_EXPORTER_FILE_KEY, NODE_EXPORTER_FILE_KEY,
    PROMETHEUS_FILE_KEY,
};
use crate::config::deployment::Product;
use crate::config::monitor::Monitor;
use crate::config::DeploymentPackage;
use crate::config::PROMETHEUS_CONFIG_FILE;
use crate::{task_return_value, wait_command_complete};
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::HashMap;
use tracing::{debug, info};

#[derive(Clone, Debug)]
pub enum MonitorComponentCommand {
    NodeExporter { home: String },
    MySqlExporter { home: String, mysql_conf: String },
    Prometheus { home: String },
    Grafana { home: String },
}

impl MonitorComponentCommand {
    pub fn start(&self, monitor: &Monitor) -> Option<String> {
        match self {
            MonitorComponentCommand::NodeExporter { home } => {
                monitor.node_exporter.as_ref().map(|noex| {
                    format!(
                        r#"{home}/node_exporter --web.listen-address=:{port} > /tmp/mono_node_exporter.log 2>&1 &"#,
                        home = home,
                        port = noex.port
                    )
                })
            }
            MonitorComponentCommand::MySqlExporter {
                home,
                mysql_conf: my_conf,
            } => {
                monitor.mysql_exporter.as_ref().map(|mysql_expt| {
                    format!(
                        r#"{home}/mysqld_exporter --web.listen-address=:{port} --config.my-cnf {my_conf} --log.level=info > /tmp/mono_mysql_exporter.log 2>&1 &"#,
                        home = home,
                        port = mysql_expt.port,
                        my_conf = my_conf,
                    )
                })
            }
            MonitorComponentCommand::Grafana { home } => {
                // Assuming Grafana is always configured when this command is used
                Some(format!(
                    r#"{home}/bin/grafana-server -homepath {home} -config {home}/conf/defaults.ini > /tmp/mono_grafana_server.log 2>&1 &"#,
                    home = home
                ))
            }
            MonitorComponentCommand::Prometheus { home } => {
                monitor.prometheus.as_ref().map(|prom| {
                    let retention_flags = prom.retention_flags();
                    let retention_flags = if retention_flags.is_empty() {
                        String::new()
                    } else {
                        format!(" {retention_flags}")
                    };
                    format!(
                        r#"{home}/prometheus --web.listen-address="0.0.0.0:{port}" --storage.tsdb.path={home}/data{retention_flags} --config.file={home}/{PROMETHEUS_CONFIG_FILE} > /tmp/mono_prometheus.log 2>&1 &"#,
                        home = home,
                        port = prom.port,
                        retention_flags = retention_flags,
                    )
                })
            }
        }
    }

    pub fn stop(&self, pid: String) -> String {
        format!("kill {pid}")
    }

    pub fn process_info(&self) -> String {
        let monitor_component_home = match &self {
            MonitorComponentCommand::NodeExporter { home } => home,
            MonitorComponentCommand::MySqlExporter {
                home,
                mysql_conf: _,
            } => home,
            MonitorComponentCommand::Prometheus { home } => home,
            MonitorComponentCommand::Grafana { home } => home,
        };
        let ps_pid = format!(r#"ps uxwe | grep "{monitor_component_home}" | grep -v grep | "#);
        let output_pid = r#"awk '{print $2}'"#;
        format!("{ps_pid} {output_pid}")
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
            }),
            grafana: None,
            node_exporter: None,
            mysql_exporter: None,
            monograph_metrics: None,
            eloq_metrics: None,
        };
        let cmd = MonitorComponentCommand::Prometheus {
            home: "/tmp/prometheus".to_string(),
        }
        .start(&monitor)
        .unwrap();

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
        let conn_user = &config.connection.username;
        let ssh_port = config.connection.ssh_port();
        let task_host = TaskHost::Remote {
            user: conn_user.clone(),
            port: ssh_port as usize,
            host: monitor_component_host,
        };
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
                SubCommand::Monitor { command, .. } => {
                    format!("grafana-{command}-{}", component_obj.port)
                }
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
                SubCommand::Monitor { command, .. } => {
                    format!("prometheus-{command}-{}", component_obj.port)
                }
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

    pub fn exporter_ctl_task(
        cmd_arg: SubCommand,
        config: &DeployConfig,
    ) -> IndexMap<TaskId, TaskInstance> {
        let cmd_str_ref = cmd_arg.as_ref();
        let monitor = config.deployment.monitor.as_ref().unwrap();
        let install_dir = config.install_dir();
        let conn_user = &config.connection.username;
        let ssh_port = config.connection.ssh_port();
        let all_hosts = config.get_host_as_map();
        all_hosts
            .iter()
            .filter(|(pkg, _hosts)| {
                matches!(
                    pkg,
                    DeploymentPackage::MonographTx
                        | DeploymentPackage::MonographStandby
                        | DeploymentPackage::MonographLog
                        | DeploymentPackage::Storage
                )
            })
            .flat_map(|(pkg, hosts)| {
                let mysql_expt = (pkg == &DeploymentPackage::MonographTx
                    || pkg == &DeploymentPackage::MonographStandby)
                    && config.product() == Product::EloqSQL
                    && monitor.mysql_exporter.is_some();

                hosts
                    .iter()
                    .unique()
                    .map(|host| {
                        let task_remote_host = TaskHost::Remote {
                            user: conn_user.clone(),
                            port: ssh_port as usize,
                            host: host.clone(),
                        };
                        let node_exporter_cmd = MonitorComponentCommand::NodeExporter {
                            home: format!("{install_dir}/{NODE_EXPORTER_FILE_KEY}"),
                        };
                        let mut exporter_cmd_vec =
                            vec![MonitorCtlTask::build_monitor_task_instance(
                                config.clone(),
                                node_exporter_cmd,
                                format!(
                                    "node_exporter-{cmd_str_ref}-{}",
                                    monitor.node_exporter.as_ref().unwrap().port
                                ),
                                task_remote_host.clone(),
                                cmd_arg.clone(),
                            )];
                        if mysql_expt {
                            let mysql_exporter_cmd = MonitorComponentCommand::MySqlExporter {
                                home: format!("{install_dir}/{MYSQL_EXPORTER_FILE_KEY}"),
                                mysql_conf: format!("{install_dir}/mysql_exporter_{host}.cnf"),
                            };
                            exporter_cmd_vec.push(MonitorCtlTask::build_monitor_task_instance(
                                config.clone(),
                                mysql_exporter_cmd,
                                format!(
                                    "mysql_exporter-{cmd_str_ref}-{}",
                                    monitor.mysql_exporter.as_ref().unwrap().port
                                ),
                                task_remote_host,
                                cmd_arg.clone(),
                            ));
                        }
                        exporter_cmd_vec
                    })
                    .collect_vec()
            })
            .flatten()
            .collect::<IndexMap<TaskId, TaskInstance>>()
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
            SubCommand::Monitor {
                cluster: _,
                command,
            } => command.to_lowercase(),
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
                        wait_command_complete!(
                            start_cmd.clone(),
                            process_info_cmd,
                            ssh_session.clone(),
                            is_some
                        )
                    } else {
                        // Skip execution and return success
                        Ok(process_rs)
                    }
                } else {
                    Ok(process_rs)
                }
            }
            "stop" => {
                if !monitor_component_pid.eq(PID_NOT_FOUND) {
                    let stop_cmd = self.monitor_ctl.stop(monitor_component_pid);
                    wait_command_complete!(
                        stop_cmd.clone(),
                        process_info_cmd,
                        ssh_session.clone(),
                        is_none
                    )
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
            let service_name = match &self.monitor_ctl {
                MonitorComponentCommand::NodeExporter { .. } => "node_exporter",
                MonitorComponentCommand::MySqlExporter { .. } => "mysql_exporter",
                MonitorComponentCommand::Prometheus { .. } => "prometheus",
                MonitorComponentCommand::Grafana { .. } => "grafana",
            };
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
