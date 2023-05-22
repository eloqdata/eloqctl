mod bootstrap_group;
mod custom_cmd_group;
mod db_cluster_ctrl_group;
mod deployment_group;
mod install_runtime_deps_group;
mod log_srv_ctl_group;
mod monitor_ctl_group;
mod upgrade_cluster_group;

use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::CommandArgs;
use crate::config::config_base::DeploymentConfig;
use dyn_clone::DynClone;
use std::collections::HashMap;
use std::sync::LazyLock;

/// `TaskGroup` base on different business logic, multiple tasks are organized into task groups,
/// and barriers are inserted between task lists according to dependencies.
#[async_trait::async_trait]
pub trait TaskGroup: Send + Sync + DynClone {
    async fn tasks(
        &self,
        cmd_arg: CommandArgs,
        config: DeploymentConfig,
    ) -> anyhow::Result<TaskExecutionContext>;
}

dyn_clone::clone_trait_object!(TaskGroup);

#[macro_export]
macro_rules! task_group_boxed {
    ($({$task_group:ident}),*) => {
        $(
        #[derive(Clone)]
        struct $task_group;

        impl $task_group {
            fn boxed() -> Box<dyn TaskGroup> {
                Box::new(Self {})
            }
        }
        )*
    };
}

task_group_boxed! {
    {DeploymentTaskGroup},
    {InstallDBTaskGroup},
    {CtrlDBTaskGroup},
    {CustomCmdTaskGroup},
    {InstallRuntimeDepsTaskGroup},
    {MonitorCtlTaskGroup},
    {LogServiceCtlTaskGroup},
    {UpgradeClusterTaskGroup}
}

pub static TASK_GROUP: LazyLock<HashMap<String, Box<dyn TaskGroup>>> = LazyLock::new(|| {
    HashMap::from([
        ("deploy".to_string(), DeploymentTaskGroup::boxed()),
        ("install".to_string(), InstallDBTaskGroup::boxed()),
        ("start".to_string(), CtrlDBTaskGroup::boxed()),
        ("stop".to_string(), CtrlDBTaskGroup::boxed()),
        ("restart".to_string(), CtrlDBTaskGroup::boxed()),
        ("status".to_string(), CtrlDBTaskGroup::boxed()),
        ("exec_cmd".to_string(), CustomCmdTaskGroup::boxed()),
        ("run-deps".to_string(), InstallRuntimeDepsTaskGroup::boxed()),
        ("monitor".to_string(), MonitorCtlTaskGroup::boxed()),
        ("log-srv".to_string(), LogServiceCtlTaskGroup::boxed()),
        ("upgrade".to_string(), UpgradeClusterTaskGroup::boxed()),
    ])
});
