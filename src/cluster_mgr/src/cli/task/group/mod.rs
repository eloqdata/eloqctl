mod backup_group;
mod bootstrap_group;
mod check_group;
mod custom_cmd_group;
mod db_cluster_ctrl_group;
mod deployment_group;
mod failover_group;
mod install_dep_pkg;
mod launch_group;
mod log_srv_ctl_group;
mod monitor_ctl_group;
mod remove_group;
mod scale_group;
mod scale_log_group;
mod update_cluster_group;
mod update_config_group;

use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::SubCommand;
use crate::config::config_base::DeployConfig;
use crate::config::connection::Connection;
use dyn_clone::DynClone;
use once_cell::sync::OnceCell;
use std::collections::HashMap;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum Config {
    Cluster(DeployConfig),
}

impl Config {
    pub fn conn_ref(&self) -> &Connection {
        match self {
            Config::Cluster(cfg) => &cfg.connection,
        }
    }

    pub fn conn_user(&self) -> &str {
        match self {
            Config::Cluster(cfg) => &cfg.connection.username,
        }
    }

    pub fn ssh_port(&self) -> u16 {
        match self {
            Config::Cluster(cfg) => cfg.connection.ssh_port(),
        }
    }

    pub fn ssh_endpoint(&self, host: &str) -> (String, u16) {
        match self {
            Config::Cluster(cfg) => cfg.connection.ssh_endpoint(host),
        }
    }

    pub fn service_endpoint(&self, host: &str, port: u16) -> (String, u16) {
        match self {
            Config::Cluster(cfg) => cfg.connection.service_endpoint(host, port),
        }
    }

    pub fn conn_ssh_auth_key(&self) -> String {
        match self {
            Config::Cluster(cfg) => cfg.connection.ssh_auth_key().unwrap(),
        }
    }

    pub fn get_unique_host_list(&self) -> Vec<String> {
        match self {
            Config::Cluster(cfg) => cfg.get_unique_host_list(),
        }
    }
}

/// `TaskGroup` base on different business logic, multiple tasks are organized into task groups,
/// and barriers are inserted between task lists according to dependencies.
#[async_trait::async_trait]
pub trait TaskGroup: Send + Sync + DynClone {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: &Config,
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
    {InstallDBTaskGroup},
    {DeploymentTaskGroup},
    {CtrlDBTaskGroup},
    {CustomCmdTaskGroup},
    {InstallDepPkgTaskGroup},
    {MonitorCtlTaskGroup},
    {LogServiceCtlTaskGroup},
    {UpdateClusterTaskGroup},
    {UpdateConfigTaskGroup},
    {LaunchTaskGroup},
    {RemoveTaskGroup},
    {CheckTaskGroup},
    {BackupTaskGroup},
    {FailoverTaskGroup},
    {ScaleTaskGroup},
    {ScaleLogTaskGroup}
}

pub static TASK_GROUP: OnceCell<HashMap<String, Box<dyn TaskGroup>>> = OnceCell::new();

pub fn init_task_group() -> &'static HashMap<String, Box<dyn TaskGroup>> {
    TASK_GROUP.get_or_init(|| {
        HashMap::from([
            ("deploy".to_string(), DeploymentTaskGroup::boxed()),
            ("install".to_string(), InstallDBTaskGroup::boxed()),
            ("start".to_string(), CtrlDBTaskGroup::boxed()),
            ("stop".to_string(), CtrlDBTaskGroup::boxed()),
            ("restart".to_string(), CtrlDBTaskGroup::boxed()),
            ("status".to_string(), CtrlDBTaskGroup::boxed()),
            ("exec_cmd".to_string(), CustomCmdTaskGroup::boxed()),
            ("run-deps".to_string(), InstallDepPkgTaskGroup::boxed()),
            ("monitor".to_string(), MonitorCtlTaskGroup::boxed()),
            ("log-srv".to_string(), LogServiceCtlTaskGroup::boxed()),
            ("update".to_string(), UpdateClusterTaskGroup::boxed()),
            ("update-conf".to_string(), UpdateConfigTaskGroup::boxed()),
            ("launch".to_string(), LaunchTaskGroup::boxed()),
            ("remove".to_string(), RemoveTaskGroup::boxed()),
            ("demo".to_string(), LaunchTaskGroup::boxed()),
            ("check".to_string(), CheckTaskGroup::boxed()),
            ("backup".to_string(), BackupTaskGroup::boxed()),
            ("failover".to_string(), FailoverTaskGroup::boxed()),
            ("scale".to_string(), ScaleTaskGroup::boxed()),
            ("scalelog".to_string(), ScaleLogTaskGroup::boxed()),
        ])
    })
}
