use crate::cli::task::group::{Config, MonitorCtlTaskGroup, TaskGroup};
use crate::cli::task::monitor_ctl_task::MonitorCtlTask;
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::task::upload::monitor_upload_builder::MonitorInfraConfUploadBuilder;
use crate::cli::{MonitorCommand, MonitorComponent, SubCommand};
use indexmap::IndexMap;

fn selected_components(command: &MonitorCommand) -> &[MonitorComponent] {
    match command {
        MonitorCommand::Start { components, .. }
        | MonitorCommand::Stop { components, .. }
        | MonitorCommand::Restart { components, .. }
        | MonitorCommand::Status { components, .. } => components.as_slice(),
        MonitorCommand::Update { .. } => &[],
    }
}

fn component_enabled(selected: &[MonitorComponent], component: MonitorComponent) -> bool {
    selected.is_empty() || selected.contains(&component)
}

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
            SubCommand::Monitor { command, .. } => command,
        };
        let mut executable = IndexMap::new();
        let mut barrier = vec![];
        let selected = selected_components(monitor_ctl_cmd);

        let start_like = matches!(
            monitor_ctl_cmd,
            MonitorCommand::Start { .. } | MonitorCommand::Restart { .. }
        );
        if start_like {
            // Re-upload the Prometheus config (and other monitor config files) before
            // starting the services so that any config changes (remote_write_urls,
            // retention_time, etc.) are applied to the running Prometheus instance.
            let monitor_conf_upload =
                MonitorInfraConfUploadBuilder::build_for_cmd(config, "monitor");
            if !monitor_conf_upload.is_empty() {
                barrier.push(monitor_conf_upload.len());
                executable.extend(monitor_conf_upload);
            }
        }

        let cluster_name = match &cmd_arg {
            SubCommand::Monitor { cluster, .. } => {
                cluster.clone().expect("monitor cluster is resolved")
            }
            _ => unreachable!(),
        };
        let stop_cmd = SubCommand::Monitor {
            cluster: Some(cluster_name.clone()),
            command: MonitorCommand::Stop {
                cluster: None,
                components: selected.to_vec(),
            },
        };
        let start_cmd = SubCommand::Monitor {
            cluster: Some(cluster_name),
            command: MonitorCommand::Start {
                cluster: None,
                components: selected.to_vec(),
            },
        };
        let component_cmd = if matches!(monitor_ctl_cmd, MonitorCommand::Restart { .. }) {
            stop_cmd.clone()
        } else {
            cmd_arg.clone()
        };

        let exporter_task_instance = if component_enabled(selected, MonitorComponent::NodeExporter)
        {
            MonitorCtlTask::exporter_ctl_task(component_cmd.clone(), cluster_config)
        } else {
            IndexMap::new()
        };
        let prometheus_task_instance = if component_enabled(selected, MonitorComponent::Prometheus)
        {
            MonitorCtlTask::prometheus_ctl_task(component_cmd.clone(), cluster_config)
        } else {
            IndexMap::new()
        };
        let alertmanager_task_instance =
            if component_enabled(selected, MonitorComponent::Alertmanager) {
                MonitorCtlTask::alertmanager_ctl_task(component_cmd.clone(), cluster_config)
            } else {
                IndexMap::new()
            };
        let grafana_task_instance = if component_enabled(selected, MonitorComponent::Grafana) {
            MonitorCtlTask::grafana_ctl_task(component_cmd.clone(), cluster_config)
        } else {
            IndexMap::new()
        };
        let adapter_task_instance =
            if component_enabled(selected, MonitorComponent::AlertmanagerWebhookAdapter) {
                MonitorCtlTask::prometheusalert_ctl_task(component_cmd.clone(), cluster_config)
            } else {
                IndexMap::new()
            };

        let phase_sets = vec![
            exporter_task_instance,
            prometheus_task_instance,
            alertmanager_task_instance,
            grafana_task_instance,
            adapter_task_instance,
        ];

        for tasks in phase_sets {
            barrier.push(tasks.len());
            executable.extend(tasks);
        }

        if matches!(monitor_ctl_cmd, MonitorCommand::Restart { .. }) {
            let restart_sets = vec![
                if component_enabled(selected, MonitorComponent::NodeExporter) {
                    MonitorCtlTask::exporter_ctl_task(start_cmd.clone(), cluster_config)
                } else {
                    IndexMap::new()
                },
                if component_enabled(selected, MonitorComponent::Prometheus) {
                    MonitorCtlTask::prometheus_ctl_task(start_cmd.clone(), cluster_config)
                } else {
                    IndexMap::new()
                },
                if component_enabled(selected, MonitorComponent::Alertmanager) {
                    MonitorCtlTask::alertmanager_ctl_task(start_cmd.clone(), cluster_config)
                } else {
                    IndexMap::new()
                },
                if component_enabled(selected, MonitorComponent::Grafana) {
                    MonitorCtlTask::grafana_ctl_task(start_cmd.clone(), cluster_config)
                } else {
                    IndexMap::new()
                },
                if component_enabled(selected, MonitorComponent::AlertmanagerWebhookAdapter) {
                    MonitorCtlTask::prometheusalert_ctl_task(start_cmd, cluster_config)
                } else {
                    IndexMap::new()
                },
            ];

            for tasks in restart_sets {
                barrier.push(tasks.len());
                executable.extend(tasks);
            }
        }

        let cmd_ref = cmd_arg.as_ref();
        Ok(TaskExecutionContext {
            task_group: format!("control-{cmd_ref}"),
            barrier: Some(barrier),
            executable,
        })
    }
}
