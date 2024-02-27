use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{MonitorCtlTaskGroup, TaskGroup};
use crate::cli::task::monitor_ctl_task::MonitorCtlTask;
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::CommandArgs;
use crate::config::config_base::DeploymentConfig;
use crate::config::deployment::Product;
use crate::config::DeploymentPackage;
use indexmap::IndexMap;

#[async_trait::async_trait]
impl TaskGroup for MonitorCtlTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: CommandArgs,
        config: DeploymentConfig,
    ) -> anyhow::Result<TaskExecutionContext> {
        if config.deployment.monitor.is_none() {
            return Ok(TaskExecutionContext {
                task_group: format!("control-{}", cmd_arg.as_ref()),
                barrier: None,
                executable: IndexMap::new(),
            });
        }
        let monitor_ctl_cmd = match &cmd_arg {
            CommandArgs::Monitor {
                cluster: _,
                command,
            } => command,
            _ => unreachable!(),
        };
        let mut executable = IndexMap::new();
        let mut barrier = vec![];
        if monitor_ctl_cmd.to_lowercase().eq("start") && config.product() == Product::EloqSQL {
            let monitor_opt = config.deployment.monitor.as_ref();
            assert!(monitor_opt.is_some());
            let monitor = monitor_opt.unwrap();
            let install_dir = config.install_dir();
            let mysql_port = config.deployment.cs_conn_port();
            let create_monitor_user_cmd =
                monitor.create_monitor_user_cmd(install_dir.clone(), mysql_port);

            let monograph_hosts = config.get_host_list(DeploymentPackage::MonographTx);
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
                monitor.flush_privileges_for_create_user(install_dir, mysql_port);
            let flush_privilege_task = ExecCustomCommand::build_task_by_host(
                flush_privileges,
                &config,
                monograph_hosts,
                Some("flush_privilege".to_string()),
            );
            barrier.push(flush_privilege_task.len());
            executable.extend(flush_privilege_task);
        }

        let exporter_task_instance = MonitorCtlTask::exporter_ctl_task(cmd_arg.clone(), &config);
        let prometheus_task_instance =
            MonitorCtlTask::prometheus_ctl_task(cmd_arg.clone(), &config);
        let grafana_task_instance = MonitorCtlTask::grafana_ctl_task(cmd_arg.clone(), &config);

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
