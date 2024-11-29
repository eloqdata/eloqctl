use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{Config, MonitorCtlTaskGroup, TaskGroup};
use crate::cli::task::monitor_ctl_task::MonitorCtlTask;
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::SubCommand;
use crate::config::deployment::Product;
use crate::config::DeploymentPackage;
use crate::config::CREATE_MONITOR_USER_SQL_FILE;
use indexmap::IndexMap;

#[async_trait::async_trait]
impl TaskGroup for MonitorCtlTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => {
                return Err(anyhow::anyhow!(
                    "Expected ClusterConfig for MonitorCtlTaskGroup"
                ))
            }
        };

        if cluster_config.deployment.monitor.is_none() {
            return Ok(TaskExecutionContext {
                task_group: format!("control-{}", cmd_arg.as_ref()),
                barrier: None,
                executable: IndexMap::new(),
            });
        }
        let monitor_ctl_cmd = match &cmd_arg {
            SubCommand::Monitor {
                cluster: _,
                command,
            } => command,
            _ => unreachable!(),
        };
        let mut executable = IndexMap::new();
        let mut barrier = vec![];
        if monitor_ctl_cmd.to_lowercase().eq("start")
            && cluster_config.product() == Product::EloqSQL
        {
            let create_monitor_user_cmd = format!(
                "{} < {}/{CREATE_MONITOR_USER_SQL_FILE}",
                cluster_config.client_conn(),
                cluster_config.install_dir()
            );
            let monograph_hosts = cluster_config.get_host_list(DeploymentPackage::MonographTx);
            let pick_mono_instance = monograph_hosts.first().unwrap();
            let create_user_task = ExecCustomCommand::build_task_by_host(
                create_monitor_user_cmd,
                &config,
                vec![pick_mono_instance.to_string()],
                Some("create_monitor_user".to_string()),
            );
            barrier.push(create_user_task.len());
            executable.extend(create_user_task);

            let flush_privileges =
                format!("{} -e  'FLUSH PRIVILEGES'", cluster_config.client_conn());
            let flush_privilege_task = ExecCustomCommand::build_task_by_host(
                flush_privileges,
                &config,
                monograph_hosts,
                Some("flush_privilege".to_string()),
            );
            barrier.push(flush_privilege_task.len());
            executable.extend(flush_privilege_task);
        }

        let exporter_task_instance =
            MonitorCtlTask::exporter_ctl_task(cmd_arg.clone(), &cluster_config);
        let prometheus_task_instance =
            MonitorCtlTask::prometheus_ctl_task(cmd_arg.clone(), &cluster_config);
        let grafana_task_instance =
            MonitorCtlTask::grafana_ctl_task(cmd_arg.clone(), &cluster_config);

        barrier.push(exporter_task_instance.len());
        barrier.push(prometheus_task_instance.len());
        barrier.push(grafana_task_instance.len());

        executable.extend(exporter_task_instance);
        executable.extend(prometheus_task_instance);
        executable.extend(grafana_task_instance);

        let cmd_ref = cmd_arg.as_ref();
        Ok(TaskExecutionContext {
            task_group: format!("control-{cmd_ref}"),
            barrier: Some(barrier),
            executable,
        })
    }
}
