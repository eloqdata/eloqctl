use crate::cli::ssh::SSHCommandOption::CollectOutput;
use crate::cli::ssh::SSHSession;
use crate::cli::task::monograph_tx_ctl_task::{MonographTxCtlTask, ServerType};
use crate::cli::task::redis_op_task::{ClusterNodes, RedisOpTask};
use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskHost, TaskId, TaskInstance};
use crate::cli::upload_dir;
use crate::cli::{SubCommand, CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::{config_base::DeployConfig, DeploymentPackage, MONITOR_DIR};
use crate::state::state_base::StateOperation;
use crate::state::state_mgr::{STATE_MGR, TASK_STATUS_STATE};
use crate::state::task_status_operation::{TaskStatusEntity, TaskStatusOperation};
use anyhow::anyhow;
use configparser::ini::Ini;
use indexmap::IndexMap;
use itertools::Itertools;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::future::Future;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info};

#[derive(Clone, Debug, PartialEq)]
pub enum ScaleOperationType {
    AddNodes = 0,
    RemoveNodes = 1,
}

#[derive(Clone, Debug)]
pub struct ClusterNodesWithConfig {
    pub nodes: ClusterNodes,
    pub cluster_config: Option<String>,
}

#[macro_export]
macro_rules! wait_command_complete {
    ($cmd:expr,$process_cmd:expr,$ssh_session:expr, $check_fn:ident) => {{
        ctl_action_wait_complete(
            $cmd,
            $process_cmd,
            $ssh_session,
            async move |cmd: String, ssh_conn: SSHSession| {
                ssh_conn.command(cmd.as_str(), CollectOutput).await
            },
            |output| -> bool { parse_process_pid(output).$check_fn() },
        )
        .await
    }};
}

pub(crate) const PID_NOT_FOUND: &str = "NONE";
pub(crate) const PROCESS_PID: &str = "_process_pid_";
pub(crate) const PROCESS_PID_LIST: &str = "_process_pid_list_";

#[allow(dead_code)]
pub fn parse_process_pid_as_list(process_info: String) -> Option<Vec<i32>> {
    if process_info.is_empty() {
        None
    } else {
        let output_normal = process_info.trim();
        let pid_vec = ps_cmd_output_extract(output_normal.to_string());
        if pid_vec.is_empty() {
            None
        } else {
            Some(pid_vec)
        }
    }
}

fn ps_cmd_output_extract(cmd_output: String) -> Vec<i32> {
    cmd_output
        .lines()
        .map(|line| line.trim())
        .filter(|output_normal| !output_normal.is_empty())
        .filter(|line| line.chars().all(char::is_numeric))
        .map(|pid_str| pid_str.parse::<i32>().unwrap())
        .unique()
        .collect_vec()
}

pub fn parse_process_pid(process_info: String) -> Option<i32> {
    if process_info.is_empty() {
        None
    } else {
        let output_normal = process_info.trim();
        let pid_vec = ps_cmd_output_extract(output_normal.to_string());
        pid_vec.first().cloned()
    }
}

pub(crate) async fn check_pid<F, T>(
    find_ps_cmd: String,
    ssh_session: SSHSession,
    ps_output_parse: F,
) -> anyhow::Result<ExecutionValue>
where
    F: Fn(String) -> Option<T>,
    T: Any + Debug,
{
    let mut cmd_exec_rs = ssh_session
        .command(find_ps_cmd.as_str(), CollectOutput)
        .await?;
    let cmd_status = cmd_exec_rs.get(CMD_STATUS).unwrap();
    if 0 != TaskArgValue::into_inner_value::<i32>(cmd_status.clone()) {
        error!("check_process_pid fails status={:?}", cmd_status);
        return Err(anyhow!("Cmd {} execution fails", find_ps_cmd));
    }
    let cmd_output_value = cmd_exec_rs.get(CMD_OUTPUT).unwrap();
    let pid_output_string = TaskArgValue::into_inner_value::<String>(cmd_output_value.clone())
        .trim()
        .to_owned();
    if let Some(ref val) = ps_output_parse(pid_output_string) {
        let val_any = val as &dyn Any;
        if val_any.type_id() == TypeId::of::<i32>() {
            match val_any.downcast_ref::<i32>() {
                Some(pid_as_i32) => cmd_exec_rs.insert(
                    PROCESS_PID.to_string(),
                    TaskArgValue::Str(pid_as_i32.to_string()),
                ),
                None => unreachable!(),
            };
        } else if val_any.type_id() == TypeId::of::<Vec<i32>>() {
            match val_any.downcast_ref::<Vec<i32>>() {
                Some(pid_list) => {
                    let cloned_pid_list = pid_list
                        .iter()
                        .map(|pid| pid.clone().to_string())
                        .collect_vec();
                    cmd_exec_rs.insert(
                        PROCESS_PID_LIST.to_string(),
                        TaskArgValue::List(cloned_pid_list),
                    );
                }
                None => unreachable!(),
            }
        } else {
            unreachable!()
        };
    } else {
        cmd_exec_rs.insert(
            PROCESS_PID.to_string(),
            TaskArgValue::Str(PID_NOT_FOUND.to_string()),
        );
        cmd_exec_rs.insert(PROCESS_PID_LIST.to_string(), TaskArgValue::List(vec![]));
    }
    Ok(cmd_exec_rs)
}

pub(crate) async fn ctl_action_wait_complete<F1, F2, Fut2>(
    ctl_cmd: String,
    check_cmd: String,
    ssh_conn: SSHSession,
    ctl_fn: F2,
    check_fn: F1,
) -> anyhow::Result<ExecutionValue>
where
    F1: Fn(String) -> bool,
    F2: Fn(String, SSHSession) -> Fut2,
    Fut2: Future<Output = anyhow::Result<ExecutionValue>> + 'static,
{
    let mut ctl_action_rs = ctl_fn(ctl_cmd.clone(), ssh_conn.clone()).await?;
    let process_ready =
        wait_process_complete(check_cmd, ssh_conn, Duration::from_secs(5 * 60), check_fn).await?;
    if let Some(output) = ctl_action_rs.get(CMD_OUTPUT) {
        let final_output = format!(
            r#"output={},check control func return={}"#,
            TaskArgValue::into_inner_value::<String>(output.clone()),
            process_ready
        );
        ctl_action_rs.insert(CMD.to_string(), TaskArgValue::Str(ctl_cmd));
        ctl_action_rs.insert(CMD_OUTPUT.to_string(), TaskArgValue::Str(final_output));
    } else {
        ctl_action_rs.insert(CMD.to_string(), TaskArgValue::Str(ctl_cmd));
        ctl_action_rs.insert(
            CMD_OUTPUT.to_string(),
            TaskArgValue::Str(format!("check control func return={process_ready}")),
        );
    }
    Ok(ctl_action_rs)
}

pub(crate) async fn wait_process_complete<F>(
    check_status_cmd: String,
    ssh_conn: SSHSession,
    wait_timeout: Duration,
    parser_output: F,
) -> anyhow::Result<bool>
where
    F: Fn(String) -> bool,
{
    let sleep_duration = Duration::from_secs(1);
    let mut timeout_remaining = wait_timeout;
    let mut process_ready = false;
    loop {
        if timeout_remaining.as_secs() == 0 {
            info!("CheckStatus timeout");
            break;
        }
        let rs = ssh_conn
            .command(check_status_cmd.as_str(), CollectOutput)
            .await;
        info!("check_status_cmd = {rs:#?}");
        if rs.as_ref().is_err() {
            let err_msg = rs.err().unwrap().to_string();
            error!(
                "CheckStatus return failed. {} {}",
                err_msg, check_status_cmd
            );
            return Err(anyhow!(err_msg));
        }
        let exec_rs = rs.as_ref().unwrap();
        let output_value = exec_rs.get(CMD_OUTPUT).unwrap();
        let output_string = TaskArgValue::into_inner_value::<String>(output_value.clone());
        process_ready = parser_output(output_string.clone());
        if process_ready {
            break;
        } else {
            std::thread::sleep(sleep_duration);
            timeout_remaining -= sleep_duration;
        }
    }
    Ok(process_ready)
}

pub(crate) async fn save_task_status(
    task_status_entity: TaskStatusEntity,
    execution_result: Option<ExecutionValue>,
) -> anyhow::Result<Option<ExecutionValue>> {
    let state_operation = STATE_MGR.get_state_operation::<TaskStatusOperation>(TASK_STATUS_STATE);

    let put_rs = state_operation.put(task_status_entity).await;
    if let Err(put_err) = put_rs {
        let err_string = put_err.to_string();
        Err(anyhow!(err_string))
    } else {
        Ok(execution_result)
    }
}

pub fn stop_with_hot_standby(
    cmd: SubCommand,
    config: &DeployConfig,
    barrier: &mut Vec<usize>,
    executable: &mut IndexMap<TaskId, TaskInstance>,
) {
    let mut is_force_stop = false;
    let mut redis_op_password: Option<String> = None;
    if let SubCommand::Stop {
        password, force, ..
    } = &cmd
    {
        redis_op_password = password.clone();
        is_force_stop = *force;
    }

    if is_force_stop {
        // Set up standby tasks
        let stop_standby = MonographTxCtlTask::from_config_with_channel(
            cmd.clone(),
            config,
            ServerType::Standby,
            None,
        )
        .expect("stop standby error");

        barrier.push(stop_standby.len());
        executable.extend(stop_standby);

        // Set up voter tasks if applicable
        if config.deployment.tx_service.voter_host_ports.is_some() {
            let stop_voter =
                MonographTxCtlTask::from_config(cmd.clone(), config, ServerType::Voter);
            barrier.push(stop_voter.len());
            executable.extend(stop_voter);
        }

        // Set up transaction tasks
        let stop_tx =
            MonographTxCtlTask::from_config_with_channel(cmd.clone(), config, ServerType::Tx, None)
                .expect("stop tx error");

        barrier.push(stop_tx.len());
        executable.extend(stop_tx);
    } else {
        // Check if any node configuration has enable_data_store set to false. if so, skip the checkpoint tasks
        let skip_checkpoint = check_whether_to_skip_checkpoint(&config.deployment.cluster_name);

        let mut redis_host_ports = config.get_host_port_list(DeploymentPackage::MonographTx);
        let standby_host_ports = config.get_host_port_list(DeploymentPackage::MonographStandby);
        redis_host_ports.extend(standby_host_ports);

        let task_id = TaskId {
            cmd: "topology".to_string(),
            task: "check-topology".to_string(),
            host: "_local".to_string(),
        };

        let redis_cmd = "cluster nodes".to_string();

        // Use a channel to pass the result of RedisOpTask to MonographTxCtlTask
        let (tx_channel, rx_standby) = watch::channel::<ClusterNodes>(ClusterNodes {
            masters: Vec::new(),
            replicas: Vec::new(),
        });
        let rx_tx = tx_channel.subscribe();

        // Add flag to specify if checkpoint tasks should be skipped
        let topology_task = RedisOpTask::new(
            task_id.clone(),
            redis_host_ports,
            redis_cmd,
            tx_channel,
            redis_op_password,
            skip_checkpoint,
        );

        let task_instance = TaskInstance {
            task_input: HashMap::default(),
            task: Box::new(topology_task),
            task_host: TaskHost::Local,
        };

        barrier.push(1);
        executable.insert(task_id, task_instance);

        // Set up standby tasks
        let stop_standby = MonographTxCtlTask::from_config_with_channel(
            cmd.clone(),
            config,
            ServerType::Standby,
            Some(rx_standby),
        )
        .expect("stop standby error");

        barrier.push(stop_standby.len());
        executable.extend(stop_standby);

        // Set up voter tasks if applicable
        if config.deployment.tx_service.voter_host_ports.is_some() {
            let stop_voter =
                MonographTxCtlTask::from_config(cmd.clone(), config, ServerType::Voter);
            barrier.push(stop_voter.len());
            executable.extend(stop_voter);
        }

        // Set up transaction tasks
        let stop_tx = MonographTxCtlTask::from_config_with_channel(
            cmd.clone(),
            config,
            ServerType::Tx,
            Some(rx_tx),
        )
        .expect("stop tx error");

        barrier.push(stop_tx.len());
        executable.extend(stop_tx);
    }
}

pub fn stop_with_failover(
    cmd: SubCommand,
    config: &DeployConfig,
    barrier: &mut Vec<usize>,
    executable: &mut IndexMap<TaskId, TaskInstance>,
) {
    let mut redis_op_password: Option<String> = None;
    let mut nodes_to_stop: Vec<String> = Vec::new();
    if let SubCommand::Stop {
        password, nodes, ..
    } = &cmd
    {
        redis_op_password = password.clone();
        nodes_to_stop = nodes.clone();
    }

    // Check if any node configuration has enable_data_store set to false. if so, skip the checkpoint tasks
    let skip_checkpoint = check_whether_to_skip_checkpoint(&config.deployment.cluster_name);

    // Set up topology task to get cluster information
    let topology_task_id = TaskId {
        cmd: "topology".to_string(),
        task: "check-topology".to_string(),
        host: "_local".to_string(),
    };

    let redis_cmd = "cluster nodes".to_string();

    let (topology_tx, failover_rx) = watch::channel::<ClusterNodes>(ClusterNodes {
        masters: Vec::new(),
        replicas: Vec::new(),
    });

    // Create additional receivers that will get the same data
    let stop_nodes_rx = failover_rx.clone();

    // Create the topology task to get cluster information
    let topology_task = RedisOpTask::new(
        topology_task_id.clone(),
        nodes_to_stop.clone(),
        redis_cmd,
        topology_tx,
        redis_op_password.clone(),
        skip_checkpoint,
    );

    let topology_instance = TaskInstance {
        task_input: HashMap::default(),
        task: Box::new(topology_task),
        task_host: TaskHost::Local,
    };

    barrier.push(1);
    executable.insert(topology_task_id, topology_instance);

    // Add failover tasks for leader nodes
    // We'll create one failover task for each potential leader node in the nodes_to_stop list
    // These tasks will initiate failover if needed
    let mut failover_task_ids = Vec::new();

    // Use the same ReceiverOpTask for all failover tasks to get cluster info
    for node_addr in &nodes_to_stop {
        if let Some((host, port_str)) = node_addr.split_once(':') {
            if let Ok(port) = port_str.parse::<u16>() {
                let failover_task_id = TaskId {
                    cmd: "failover".to_string(),
                    task: format!("failover-check-{}", port_str),
                    host: host.to_string(),
                };

                // We will create a dummy replica address for now - the actual FailoverOpTask will
                // determine if this node is a leader, and if so, choose an appropriate replica
                // The task will be a no-op if this node is not a leader
                let failover_task = crate::cli::task::failover_op_task::FailoverOpTask::new(
                    failover_task_id.clone(),
                    host.to_string(),
                    port,
                    // These values will be dynamically determined by the task itself based on cluster info
                    "".to_string(), // Will be filled by the task if needed
                    0,              // Will be filled by the task if needed
                    failover_rx.clone(),
                    redis_op_password.clone(),
                );

                let failover_instance = TaskInstance {
                    task_input: HashMap::default(),
                    task: Box::new(failover_task),
                    task_host: TaskHost::Local, // Failover coordination happens locally
                };

                failover_task_ids.push(failover_task_id.clone());
                executable.insert(failover_task_id, failover_instance);
            }
        }
    }

    barrier.push(failover_task_ids.len());

    // Add stop tasks for the nodes
    // These tasks will execute after the failover tasks have completed
    let stop_nodes = MonographTxCtlTask::from_config_with_channel(
        cmd.clone(),
        config,
        ServerType::Node,
        Some(stop_nodes_rx),
    )
    .expect("stop nodes error");

    barrier.push(stop_nodes.len());
    executable.extend(stop_nodes);
}

fn check_whether_to_skip_checkpoint(cluster_name: &str) -> bool {
    // TODO(ZX) !!! should load and check ini config info from internal sqlite db, instead of checking the ini file in upload dir

    let upload_path = upload_dir().join(cluster_name);
    if !upload_path.exists() {
        return true;
    }

    let mut skip_ckpt = false;

    // First check the root EloqKv.ini file
    let root_ini_path = upload_path.join("EloqKv.ini");
    if root_ini_path.exists() {
        let mut ini = Ini::new();
        if let Ok(_) = ini.load(root_ini_path.to_str().unwrap()) {
            if let Some(value) = ini.get("LOCAL", "enable_data_store") {
                if value.to_lowercase() == "false" {
                    info!(
                        "Found enable_data_store=false in root {}",
                        root_ini_path.display()
                    );
                    return true;
                }
            }
        }
    }

    // // Check monitor directory
    // let monitor_path = upload_path.join(MONITOR_DIR);
    // if monitor_path.exists() {
    //     if let Ok(monitor_entries) = fs::read_dir(&monitor_path) {
    //         for file_entry in monitor_entries.flatten() {
    //             let file_path = file_entry.path();
    //             if file_path.extension().and_then(|e| e.to_str()) == Some("ini") {
    //                 // Found an INI file, check its content
    //                 let mut ini = Ini::new();
    //                 if let Ok(_) = ini.load(file_path.to_str().unwrap()) {
    //                     if let Some(value) = ini.get("LOCAL", "enable_data_store") {
    //                         if value.to_lowercase() == "false" {
    //                             info!("Found enable_data_store=false in {}", file_path.display());
    //                             return true;
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // }

    // Walk through all directories and check .ini files
    if let Ok(entries) = fs::read_dir(upload_path) {
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                let dir_path = entry.path();
                // Skip the monitor directory as we already checked it
                if dir_path.file_name().and_then(|f| f.to_str()) == Some(MONITOR_DIR) {
                    continue;
                }

                // Check for .ini files in this directory
                if let Ok(dir_entries) = fs::read_dir(&dir_path) {
                    for file_entry in dir_entries.flatten() {
                        let file_path = file_entry.path();
                        if file_path.extension().and_then(|e| e.to_str()) == Some("ini") {
                            // Found an INI file, check its content
                            let mut ini = Ini::new();
                            if let Ok(_) = ini.load(file_path.to_str().unwrap()) {
                                if let Some(value) = ini.get("LOCAL", "enable_data_store") {
                                    if value.to_lowercase() == "false" {
                                        info!(
                                            "Found enable_data_store=false in {}",
                                            file_path.display()
                                        );
                                        skip_ckpt = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                if skip_ckpt {
                    break;
                }
            }
        }
    }

    skip_ckpt
}

/// Node configuration with node ID, IP, port, and candidate status
#[derive(Debug, Clone)]
pub struct NgNodeConfig {
    pub node_id: u32,
    pub ip: String,
    pub port: u16,
    pub is_candidate: bool,
}

type NodeId = u32;
type NodeGroupId = u32;

/// Parse node group configuration from string lists
///
/// # Arguments
///
/// * `ip_port_list` - Comma-separated list of primary nodes (one per node group)
/// * `standby_ip_port_list` - Comma-separated list of standby nodes, with pipe-delimited nodes per group
/// * `voter_ip_port_list` - Comma-separated list of voter nodes, with pipe-delimited nodes per group
/// * `port_delta` - Optional port adjustment (default: 0)
///
/// # Returns
///
/// HashMap of node group configurations
pub fn parse_ng_config(
    tx_ip_port_list: &str,
    standby_ip_port_list: &str,
    voter_ip_port_list: &str,
    port_delta: Option<i16>,
) -> anyhow::Result<HashMap<NodeGroupId, Vec<NgNodeConfig>>> {
    let port_delta = port_delta.unwrap_or(0);
    const NG_DELIMITER: char = ',';
    const NODE_DELIMITER: char = '|';

    // Contains explicitly set members
    let mut ng_configs = HashMap::new();
    let mut node_map = HashMap::new();
    let mut ng_cnt: NodeGroupId = 0;

    // Parse primary nodes (one per node group)
    for token in tx_ip_port_list.split(NG_DELIMITER) {
        if token.trim().is_empty() {
            continue;
        }

        let c_idx = match token.find(':') {
            Some(idx) => idx,
            None => return Err(anyhow!("Port missing in ip_port_list: {}", tx_ip_port_list)),
        };

        // Check for duplicate nodes
        if node_map.contains_key(token) {
            return Err(anyhow!("Node repeated in config ip_port_list: {}", token));
        }

        let port_str = &token[(c_idx + 1)..];
        let port = match port_str.parse::<u16>() {
            Ok(p) => (p as i32 + port_delta as i32) as u16,
            Err(_) => return Err(anyhow!("Invalid port in ip_port_list: {}", port_str)),
        };

        let ip = token[..c_idx].to_string();
        let ng_id = ng_cnt;
        let node_id = ng_id;

        node_map.insert(token.to_string(), node_id);
        ng_cnt += 1;

        // Create new node group and add the first node
        ng_configs.insert(
            ng_id,
            vec![NgNodeConfig {
                node_id,
                ip,
                port,
                is_candidate: true,
            }],
        );
    }

    // Parse standby nodes
    if !standby_ip_port_list.trim().is_empty() {
        let mut s_ng_idx = 0;
        for token in standby_ip_port_list.split(NG_DELIMITER) {
            if s_ng_idx >= ng_cnt || token.trim().is_empty() {
                continue;
            }

            // Get the node group vector for this index
            if let Some(members_vec) = ng_configs.get_mut(&s_ng_idx) {
                // Process pipe-delimited standby nodes for this group
                for token2 in token.split(NODE_DELIMITER) {
                    if token2.trim().is_empty() {
                        continue;
                    }

                    let c_idx = match token2.find(':') {
                        Some(idx) => idx,
                        None => {
                            return Err(anyhow!("Port missing in standby_ip_port_list: {}", token2))
                        }
                    };

                    // Check for duplicate nodes across primary and standby
                    if node_map.contains_key(token2) {
                        return Err(anyhow!(
                            "Node in standby_ip_port_list also appears in ip_port_list: {}",
                            token2
                        ));
                    }

                    let port_str = &token2[(c_idx + 1)..];
                    let port = match port_str.parse::<u16>() {
                        Ok(p) => (p as i32 + port_delta as i32) as u16,
                        Err(_) => {
                            return Err(anyhow!(
                                "Invalid port in standby_ip_port_list: {}",
                                port_str
                            ))
                        }
                    };

                    let ip = token2[..c_idx].to_string();
                    let node_id = node_map.len() as u32;
                    node_map.insert(token2.to_string(), node_id);

                    members_vec.push(NgNodeConfig {
                        node_id,
                        ip,
                        port,
                        is_candidate: true,
                    });
                }
            }
            s_ng_idx += 1;
        }
    }

    // Parse voter nodes
    if !voter_ip_port_list.trim().is_empty() {
        let mut v_ng_idx = 0;
        for token in voter_ip_port_list.split(NG_DELIMITER) {
            if v_ng_idx >= ng_cnt || token.trim().is_empty() {
                continue;
            }

            // Get the node group vector for this index
            if let Some(members_vec) = ng_configs.get_mut(&v_ng_idx) {
                // Process pipe-delimited voter nodes for this group
                for token2 in token.split(NODE_DELIMITER) {
                    if token2.trim().is_empty() {
                        continue;
                    }

                    let c_idx = match token2.find(':') {
                        Some(idx) => idx,
                        None => {
                            return Err(anyhow!("Port missing in voter_ip_port_list: {}", token2))
                        }
                    };

                    let port_str = &token2[(c_idx + 1)..];
                    let port = match port_str.parse::<u16>() {
                        Ok(p) => (p as i32 + port_delta as i32) as u16,
                        Err(_) => {
                            return Err(anyhow!("Invalid port in voter_ip_port_list: {}", port_str))
                        }
                    };

                    let ip = token2[..c_idx].to_string();

                    // Check if this node already exists in the node map
                    let node_id = match node_map.get(token2) {
                        Some(&id) => id,
                        None => {
                            let id = node_map.len() as u32;
                            node_map.insert(token2.to_string(), id);
                            id
                        }
                    };

                    // Check if this node already exists in the member vector (same group)
                    if members_vec.iter().any(|m_node| m_node.node_id == node_id) {
                        return Err(anyhow!("Voter node appeared in the same group: {}", token2));
                    }

                    members_vec.push(NgNodeConfig {
                        node_id,
                        ip,
                        port,
                        is_candidate: false,
                    });
                }
            }
            v_ng_idx += 1;
        }
    }

    // Calculate required replica count for each node group based on its size
    let mut replica_num_list = HashMap::new();
    for (ng_id, explicit_members) in &ng_configs {
        // Calculate the required replica count for this node group
        let explicit_members_count = explicit_members.len() as u32;
        // Ensure replica_num is at least 3 for high availability
        let replica_num = std::cmp::max(explicit_members_count, 3);
        replica_num_list.insert(*ng_id, replica_num);
    }

    info!(
        "Generated replica counts per node group: {:?}",
        replica_num_list
    );
    info!("Initial node configuration: {:#?}", ng_configs);

    // Adjust node groups to ensure each has sufficient replicas
    adjust_ng_configs(&mut ng_configs, &replica_num_list)?;

    Ok(ng_configs)
}

/// Ensure each node group has sufficient members based on its replica_num
///
/// This function will borrow nodes from other groups if necessary
fn adjust_ng_configs(
    ng_configs: &mut HashMap<NodeGroupId, Vec<NgNodeConfig>>,
    replica_num_list: &HashMap<NodeGroupId, u32>,
) -> anyhow::Result<()> {
    let ng_cnt = ng_configs.len() as u32;
    if ng_cnt == 0 {
        return Ok(());
    }

    for ng_id in 0..ng_cnt {
        // Get the replica count for this specific node group
        let replica_num = replica_num_list.get(&ng_id).cloned().unwrap_or(3);

        if let Some(members_set_in_deploy_config_explicitly) = ng_configs.get(&ng_id) {
            if members_set_in_deploy_config_explicitly.len() >= replica_num as usize {
                continue;
            }

            // Calculate how many replicas to borrow
            let left_rep_cnt = replica_num - members_set_in_deploy_config_explicitly.len() as u32;
            let left_rep_cnt = std::cmp::min(left_rep_cnt, ng_cnt - 1);

            // Make a mutable copy of the current members
            let mut updated_members = members_set_in_deploy_config_explicitly.clone();

            // Borrow nodes from other groups
            for idx in 1..=left_rep_cnt {
                let borrow_ng_id = (ng_id + idx) % ng_cnt;

                if let Some(borrow_members) = ng_configs.get(&borrow_ng_id) {
                    if !borrow_members.is_empty() {
                        let mut tmp_conf = borrow_members[0].clone();
                        tmp_conf.is_candidate = false;
                        updated_members.push(tmp_conf);
                    }
                }
            }

            // Update the node group with the new members
            ng_configs.insert(ng_id, updated_members);
        }
    }

    info!("Adjusted node configuration: {:#?}", ng_configs);

    Ok(())
}
