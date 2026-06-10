use crate::cli::task::check_task::CheckTask;
use crate::cli::task::group::{CheckTaskGroup, Config, TaskGroup};
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost, TaskId, TaskInstance};
use crate::cli::SubCommand;
use crate::config::DeploymentPackage;
use indexmap::IndexMap;
use std::collections::HashMap;

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
                    task_host: TaskHost::remote(&$config.connection, host),
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
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        let Config::Cluster(cluster_config) = config;

        let mut executable = IndexMap::new();
        let input = HashMap::new();
        make_check_tasks!(DeploymentPackage::EloqTx, cluster_config, input, executable);
        make_check_tasks!(
            DeploymentPackage::EloqStandby,
            cluster_config,
            input,
            executable
        );
        make_check_tasks!(
            DeploymentPackage::EloqVoter,
            cluster_config,
            input,
            executable
        );
        make_check_tasks!(
            DeploymentPackage::EloqLog,
            cluster_config,
            input,
            executable
        );
        if cluster_config.deployment.monitor.is_some() {
            make_check_tasks!(
                DeploymentPackage::Prometheus,
                cluster_config,
                input,
                executable
            );
            make_check_tasks!(
                DeploymentPackage::Alertmanager,
                cluster_config,
                input,
                executable
            );
            make_check_tasks!(
                DeploymentPackage::Grafana,
                cluster_config,
                input,
                executable
            );
            make_check_tasks!(
                DeploymentPackage::PrometheusAlert,
                cluster_config,
                input,
                executable
            );
        }
        Ok(TaskExecutionContext {
            task_group: cmd_arg.as_ref().to_string(),
            barrier: None,
            executable,
        })
    }
}
