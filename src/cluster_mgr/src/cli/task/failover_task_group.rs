use crate::cli::task::failover_op_task::FailoverOpTask;
use crate::cli::task::redis_op_task::{ClusterNodes, RedisOpTask};
use crate::cli::task::task_base::{TaskHost, TaskId, TaskInstance};
use crate::cli::SubCommand;
use crate::config::config_base::DeployConfig;
use crate::config::DeploymentPackage;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::info;

pub fn failover_task_group(
    cmd: SubCommand,
    config: &DeployConfig,
    barrier: &mut Vec<usize>,
    executable: &mut indexmap::IndexMap<TaskId, TaskInstance>,
) {
    // Extract the old_leader and new_leader information from the SubCommand
    let (old_leader_host, old_leader_port, new_leader_host, new_leader_port, password) =
        if let SubCommand::Failover {
            old_leader_host,
            old_leader_port,
            new_leader_host,
            new_leader_port,
            ..
        } = &cmd
        {
            (
                old_leader_host.clone(),
                *old_leader_port,
                new_leader_host.clone(),
                *new_leader_port,
                None, // Optional password could be added to the SubCommand if needed
            )
        } else {
            panic!("Invalid command for failover task group");
        };

    // Get all Redis host:port combinations from the config
    let mut redis_host_ports = config.get_host_port_list(DeploymentPackage::MonographTx);
    let standby_host_ports = config.get_host_port_list(DeploymentPackage::MonographStandby);
    redis_host_ports.extend(standby_host_ports);

    // Create channels for communication between tasks
    // Channel for first RedisOpTask to FailoverOpTask
    let (pre_failover_tx, pre_failover_rx) = watch::channel::<ClusterNodes>(ClusterNodes {
        masters: Vec::new(),
        replicas: Vec::new(),
    });

    // Create another channel for the post-failover task
    // This time, we'll keep a reference to both sender and receiver to ensure the channel remains open
    let (post_failover_tx, post_failover_rx) = watch::channel::<ClusterNodes>(ClusterNodes {
        masters: Vec::new(),
        replicas: Vec::new(),
    });

    // Keep the receiver alive to prevent the channel from closing
    // We won't actually use this data, but it keeps the channel open
    let _post_rx_handle = Arc::new(post_failover_rx);

    // 1. Task to get the initial cluster topology
    let pre_failover_task_id = TaskId {
        cmd: "topology".to_string(),
        task: "pre-failover-topology".to_string(),
        host: "_local".to_string(),
    };

    let pre_failover_task = RedisOpTask::new_and_skip_checkpoint(
        pre_failover_task_id.clone(),
        redis_host_ports.clone(),
        "cluster nodes".to_string(),
        pre_failover_tx,
        password.clone(),
        true, // Skip checkpoint
    );

    let pre_failover_instance = TaskInstance {
        task_input: HashMap::default(),
        task: Box::new(pre_failover_task),
        task_host: TaskHost::Local,
    };

    barrier.push(1);
    executable.insert(pre_failover_task_id, pre_failover_instance);

    // 2. Task to perform the failover
    let failover_task_id = TaskId {
        cmd: "failover".to_string(),
        task: "execute-failover".to_string(),
        host: "_local".to_string(),
    };

    let failover_task = FailoverOpTask::new(
        failover_task_id.clone(),
        old_leader_host,
        old_leader_port,
        new_leader_host,
        new_leader_port,
        pre_failover_rx,
        password.clone(),
    );

    let failover_instance = TaskInstance {
        task_input: HashMap::default(),
        task: Box::new(failover_task),
        task_host: TaskHost::Local,
    };

    barrier.push(1);
    executable.insert(failover_task_id, failover_instance);

    // 3. Task to verify the cluster topology after failover
    let post_failover_task_id = TaskId {
        cmd: "topology".to_string(),
        task: "post-failover-topology".to_string(),
        host: "_local".to_string(),
    };

    let post_failover_task = RedisOpTask::new_and_skip_checkpoint(
        post_failover_task_id.clone(),
        redis_host_ports,
        "cluster nodes".to_string(),
        post_failover_tx,
        password,
        true, // Skip checkpoint
    );

    let post_failover_instance = TaskInstance {
        task_input: HashMap::default(),
        task: Box::new(post_failover_task),
        task_host: TaskHost::Local,
    };

    barrier.push(1);
    executable.insert(post_failover_task_id, post_failover_instance);

    info!("Failover task group configured with 3 sequential tasks");
}
