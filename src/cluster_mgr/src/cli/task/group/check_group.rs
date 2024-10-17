use crate::cli::task::check_task::CheckTask;
use crate::cli::task::group::{CheckTaskGroup, TaskGroup};
use crate::cli::task::task_base::{
    TaskArgValue, TaskExecutionContext, TaskHost, TaskId, TaskInstance,
};
use crate::cli::SubCommand;
use crate::config::config_base::DeployConfig;
use crate::config::{cassandra_used_ports, DeploymentPackage};
use indexmap::IndexMap;
use std::collections::HashMap;
use tracing::info;

macro_rules! make_check_tasks {
    ($pkg:expr, $config:expr, $input:expr, $executable:expr) => {
        let check_tx = $config
            .get_host_list($pkg)
            .into_iter()
            .map(|host| {
                let task = CheckTask::new($pkg, host.clone(), $config.clone());
                let instance = TaskInstance {
                    task_input: $input.clone(),
                    task: Box::new(task),
                    task_host: TaskHost::Remote {
                        user: $config.connection.username.clone(),
                        port: $config.connection.ssh_port() as usize,
                        host,
                    },
                };
                (instance.task.identifier(), instance)
            })
            .collect::<IndexMap<TaskId, TaskInstance>>();
        $executable.extend(check_tx);
    };
}

#[async_trait::async_trait]
impl TaskGroup for CheckTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: DeployConfig,
    ) -> anyhow::Result<TaskExecutionContext> {
        let mut executable = IndexMap::new();
        let input = HashMap::new();
        make_check_tasks!(DeploymentPackage::MonographTx, config, input, executable);
        make_check_tasks!(
            DeploymentPackage::MonographStandby,
            config,
            input,
            executable
        );
        make_check_tasks!(DeploymentPackage::MonographVoter, config, input, executable);
        make_check_tasks!(DeploymentPackage::MonographLog, config, input, executable);
        if let Some(cass) = &config.deployment.storage_service.cassandra {
            if cass.internal().is_some() {
                let input = cassandra_used_ports()
                    .into_iter()
                    .map(|(name, port)| {
                        info!("cassandra used port {} for {}", port, name);
                        (port.to_string(), TaskArgValue::Str(name))
                    })
                    .collect::<HashMap<String, TaskArgValue>>();
                make_check_tasks!(DeploymentPackage::Storage, config, input, executable);
            }
        }
        if config.deployment.monitor.is_some() {
            make_check_tasks!(DeploymentPackage::Prometheus, config, input, executable);
            make_check_tasks!(DeploymentPackage::Grafana, config, input, executable);
        }
        Ok(TaskExecutionContext {
            task_group: cmd_arg.as_ref().to_string(),
            barrier: None,
            executable,
        })
    }
}
