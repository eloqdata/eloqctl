use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::task_base::CmdErr;
use crate::cli::task::task_base::{
    ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::task::task_utils::{
    check_pid, ctl_action_wait_complete, parse_process_pid, PROCESS_PID,
};
use crate::cli::SubCommand;
use crate::config::config_base::{
    DeployConfig, GRAFANA_FILE_KEY, MYSQL_EXPORTER_FILE_KEY, NODE_EXPORTER_FILE_KEY,
    PROMETHEUS_FILE_KEY,
};
use crate::config::deployment::Product;
use crate::config::monitor::Monitor;
use crate::config::DeploymentPackage;
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

macro_rules! basic_component_ctl_task {
    ($cmd_args:expr,$config:expr, $cmd_component:ident, $monitor_component:ident,$component_home:expr) => {{
        let monitor = $config.deployment.monitor.as_ref();
        assert!(monitor.is_some());
        let task_name = match $cmd_args.clone() {
            SubCommand::Monitor {
                cluster: _,
                command,
            } => {
                format!("{}_{command}", $component_home)
            }
            _ => unreachable!(),
        };
        let component_obj = &monitor.unwrap().$monitor_component;
        let conn_user = &$config.connection.username;
        let ssh_port = $config.connection.ssh_port();
        let host = TaskHost::Remote {
            user: conn_user.clone(),
            port: ssh_port as usize,
            hosts: component_obj.host.clone(),
        };
        let home = format!("{}/{}", $config.install_dir(), $component_home);
        let cmd = MonitorComponentCommand::$cmd_component { home };
        IndexMap::from([build_monitor_task_instance!(
            $config.clone(),
            cmd,
            task_name,
            host,
            $component_home,
            $cmd_args.clone()
        )])
    }};
}

macro_rules! build_monitor_task_instance {
    // add start/stop ctl_cmd argument.
    ($config:expr,$ctl_cmd:expr,$task_name:expr,$task_remote_host:expr,$component_home:expr, $cmd_arg:expr) => {{
        let (_, _, host) = &$task_remote_host.ssh_conn_tuple();
        let cmd_ref = &$cmd_arg.as_ref().to_string();
        let task_id = TaskId {
            cmd: cmd_ref.to_string(),
            task: $task_name.to_string(),
            host: host.to_string(),
        };
        (
            task_id.clone(),
            TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(MonitorCtlTask::new(
                    $config.clone(),
                    task_id,
                    $ctl_cmd,
                    $cmd_arg,
                )),
                task_host: $task_remote_host,
            },
        )
    }};
}

impl MonitorComponentCommand {
    pub fn start(&self, monitor: Monitor) -> String {
        match self {
            MonitorComponentCommand::NodeExporter { home } => {
                let port = monitor.node_exporter.port;
                format!(
                    r#"{home}/node_exporter --web.listen-address=:{port} > /tmp/mono_node_exporter.log 2>&1 &"#
                )
            }
            MonitorComponentCommand::MySqlExporter {
                home,
                mysql_conf: my_conf,
            } => {
                let port = monitor.mysql_exporter.as_ref().unwrap().port;
                format!(
                    r#"{home}/mysqld_exporter --web.listen-address=:{port} --config.my-cnf {my_conf} --log.level=info > /tmp/mono_mysql_exporter.log 2>&1 &"#
                )
            }
            MonitorComponentCommand::Grafana { home } => {
                format!(
                    r#"{home}/bin/grafana-server -homepath {home} -config {home}/conf/defaults.ini > /tmp/mono_grafana_server.log 2>&1 &"#
                )
            }
            MonitorComponentCommand::Prometheus { home } => {
                let port = monitor.prometheus.port;
                format!(
                    r#"{home}/prometheus --web.listen-address="0.0.0.0:{port}" --storage.tsdb.path={home}/data --config.file={home}/prometheus.yml > /tmp/mono_prometheus.log 2>&1 &"#
                )
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

    pub fn grafana_ctl_task(
        cmd_arg: SubCommand,
        config: &DeployConfig,
    ) -> IndexMap<TaskId, TaskInstance> {
        basic_component_ctl_task!(cmd_arg, config, Grafana, grafana, GRAFANA_FILE_KEY)
    }

    pub fn prometheus_ctl_task(
        cmd_arg: SubCommand,
        config: &DeployConfig,
    ) -> IndexMap<TaskId, TaskInstance> {
        basic_component_ctl_task!(cmd_arg, config, Prometheus, prometheus, PROMETHEUS_FILE_KEY)
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
                let pkg_copy = *pkg;
                pkg_copy.eq(&DeploymentPackage::MonographTx)
                    || pkg_copy.eq(&DeploymentPackage::MonographLog)
                    || pkg_copy.eq(&DeploymentPackage::Storage)
            })
            .flat_map(|(pkg, hosts)| {
                let mysql_expt = pkg.eq(&DeploymentPackage::MonographTx)
                    && config.product() == Product::EloqSQL
                    && monitor.mysql_exporter.is_some();
                hosts
                    .iter()
                    .unique()
                    .map(|host| {
                        let task_remote_host = TaskHost::Remote {
                            user: conn_user.clone(),
                            port: ssh_port as usize,
                            hosts: host.clone(),
                        };
                        let node_exporter_cmd = MonitorComponentCommand::NodeExporter {
                            home: format!("{install_dir}/{NODE_EXPORTER_FILE_KEY}"),
                        };

                        let task_remote_host_cloned = task_remote_host.clone();
                        let mut exporter_cmd_vec = vec![build_monitor_task_instance!(
                            config.clone(),
                            node_exporter_cmd,
                            format!("node_exporter_{cmd_str_ref}"),
                            task_remote_host_cloned,
                            NODE_EXPORTER_FILE_KEY,
                            cmd_arg.clone()
                        )];

                        if mysql_expt {
                            let mysql_exporter_cmd = MonitorComponentCommand::MySqlExporter {
                                home: format!("{install_dir}/{MYSQL_EXPORTER_FILE_KEY}"),
                                mysql_conf: format!(
                                    //"{install_dir}/mysqld_exporter/mysql_exporter_{monograph_host}.cnf"
                                    "{install_dir}/mysql_exporter_{host}.cnf"
                                ),
                            };
                            exporter_cmd_vec.push(build_monitor_task_instance!(
                                config,
                                mysql_exporter_cmd,
                                format!("mysql_exporter_{cmd_str_ref}"),
                                task_remote_host,
                                MYSQL_EXPORTER_FILE_KEY,
                                cmd_arg.clone()
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
        info!("execute {}", self.task_id.pretty_string());
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
                if monitor_component_pid.eq("NONE") {
                    let start_cmd = self.monitor_ctl.start(monitor_ref.clone());
                    debug!(r#"MonitorCtlTask start_cmd={start_cmd:?}"#);
                    wait_command_complete!(
                        start_cmd.clone(),
                        process_info_cmd,
                        ssh_session.clone(),
                        is_some
                    )
                } else {
                    Ok(process_rs)
                }
            }
            "stop" => {
                if !monitor_component_pid.eq("NONE") {
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
            _ => {
                unreachable!()
            }
        };
        ssh_session.close().await?;
        let ctl_rtn_value = monitor_ctl_cmd_result?;
        task_return_value!(
            ctl_rtn_value,
            |status_code: i32| -> CmdErr {
                CmdErr::MonitorCtlCmdErr(self.task_id.format_string(), status_code.to_string())
            },
            "MonitorCtlTask"
        )
    }
}
