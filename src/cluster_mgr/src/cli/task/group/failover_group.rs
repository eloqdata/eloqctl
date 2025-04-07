use crate::cli::task::failover_task_group::failover_task_group;
use crate::cli::task::group::{Config, FailoverTaskGroup};
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::{SubCommand, CMD, CMD_OUTPUT, CMD_STATUS};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use indexmap::IndexMap;
use std::collections::HashMap;
use tracing::info;

#[async_trait]
impl super::TaskGroup for FailoverTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        if !matches!(cmd_arg, SubCommand::Failover { .. }) {
            return Err(anyhow!("Expected Failover command"));
        }

        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => return Err(anyhow!("Expected Cluster config for Failover command")),
        };

        info!("Setting up failover task group");

        let mut barrier = Vec::new();
        let mut executable = IndexMap::new();

        // Set up the failover task group
        failover_task_group(cmd_arg, cluster_config, &mut barrier, &mut executable);

        Ok(TaskExecutionContext {
            task_group: "failover".to_string(),
            barrier: Some(barrier),
            executable,
        })
    }
}
