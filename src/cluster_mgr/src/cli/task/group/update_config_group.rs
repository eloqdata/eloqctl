use crate::cli::task::group::Config;
use crate::cli::task::group::{TaskGroup, UpdateConfigTaskGroup};
use crate::cli::task::monograph_tx_ctl_task::{MonographTxCtlTask, ServerType};
use crate::cli::task::task_base::TaskExecutionContext;
use crate::cli::task::task_utils::stop_with_hot_standby;
use crate::cli::task::upload::upload_task_builder::{upload_tasks, UploadTaskBuilderType};
use crate::cli::SubCommand;
use indexmap::IndexMap;

#[async_trait::async_trait]
impl TaskGroup for UpdateConfigTaskGroup {
    async fn tasks(
        &self,
        cmd_arg: SubCommand,
        config: &Config,
    ) -> anyhow::Result<TaskExecutionContext> {
        let cluster_config = match config {
            Config::Cluster(cfg) => cfg,
            _ => {
                return Err(anyhow::anyhow!(
                    "Expected ClusterConfig for UpdateConfigTaskGroup"
                ))
            }
        };

        let cluster_name = &cluster_config.deployment.cluster_name;
        let need_restart = match cmd_arg {
            SubCommand::UpdateConf { restart, .. } => restart,
            _ => unreachable!(),
        };

        let mut executable = IndexMap::new();
        let mut barrier = vec![];
        executable.extend(upload_tasks(UploadTaskBuilderType::TxConf, &config));
        barrier.push(executable.len());

        if need_restart {
            // stop order: (standby-server -> voter-server ->) tx-server -> log-server -> kv-store
            if cluster_config
                .deployment
                .tx_service
                .standby_host_ports
                .is_some()
            {
                stop_with_hot_standby(
                    SubCommand::Stop {
                        cluster: cluster_name.clone(),
                        tx: Some(true),
                        log: true,
                        store: false,
                        monitor: false,
                        force: false,
                        all: false,
                        password: None,
                        nodes: Vec::new(),
                    },
                    &cluster_config,
                    &mut barrier,
                    &mut executable,
                );
            } else {
                let stop_tx_task = MonographTxCtlTask::from_config(
                    SubCommand::Stop {
                        cluster: cluster_name.clone(),
                        tx: Some(true),
                        log: true,
                        store: false,
                        monitor: false,
                        force: false,
                        all: false,
                        password: None,
                        nodes: Vec::new(),
                    },
                    &cluster_config,
                    ServerType::Tx,
                );
                barrier.push(stop_tx_task.len());
                executable.extend(stop_tx_task);
            }

            let start_tx_task = MonographTxCtlTask::from_config(
                SubCommand::Start {
                    cluster: cluster_name.to_string(),
                    nodes: Vec::new(),
                },
                &cluster_config,
                ServerType::Tx,
            );
            barrier.push(start_tx_task.len());
            executable.extend(start_tx_task);

            if cluster_config
                .deployment
                .tx_service
                .standby_host_ports
                .is_some()
            {
                let start_standby = MonographTxCtlTask::from_config(
                    SubCommand::Start {
                        cluster: cluster_name.to_string(),
                        nodes: Vec::new(),
                    },
                    &cluster_config,
                    ServerType::Standby,
                );
                barrier.push(start_standby.len());
                executable.extend(start_standby);
            }

            if cluster_config
                .deployment
                .tx_service
                .voter_host_ports
                .is_some()
            {
                let start_voter = MonographTxCtlTask::from_config(
                    SubCommand::Start {
                        cluster: cluster_name.to_string(),
                        nodes: Vec::new(),
                    },
                    &cluster_config,
                    ServerType::Voter,
                );
                barrier.push(start_voter.len());
                executable.extend(start_voter);
            }
        }

        Ok(TaskExecutionContext {
            task_group: "update-tx-conf".to_string(),
            barrier: if barrier.is_empty() {
                None
            } else {
                Some(barrier)
            },
            executable,
        })
    }
}
