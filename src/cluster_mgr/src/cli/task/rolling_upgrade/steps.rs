use super::Step;
use crate::cli::task::download_task::DownloadTask;
use crate::cli::task::eloq_log_ctl_task::EloqLogCtlTask;
use crate::cli::task::eloq_log_probe_task::EloqLogProbeTask;
use crate::cli::task::eloq_store_data_clean_task::EloqStoreDataCleanTask;
use crate::cli::task::eloq_tx_ctl_task::{EloqTxCtlTask, ServerType};
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::failover_op_task::FailoverOpTask;
use crate::cli::task::group::Config;
use crate::cli::task::local_extract_task::LocalExtractTask;
use crate::cli::task::redis_op_task::{ClusterNodes, RedisOpTask};
use crate::cli::task::task_base::{
    TaskArgValue, TaskExecutionContext, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::task::wait_replica_ready_task::WaitReplicaReadyTask;
use crate::cli::{SubCommand, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::{DeployConfig, ELOQ_FILE_KEY, ELOQ_LOG_FILE_KEY};
use crate::config::storage_service_config::DataStoreServiceBackend;
use crate::config::DeploymentPackage;
use anyhow::bail;
use async_trait::async_trait;
use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tracing::info;

// ── Helpers ──────────────────────────────────────────────────────────────────

fn single_barrier_ctx(
    task_group: &str,
    executable: IndexMap<TaskId, TaskInstance>,
) -> TaskExecutionContext {
    let len = executable.len();
    TaskExecutionContext {
        task_group: task_group.to_string(),
        barrier: (len > 0).then(|| vec![len]),
        executable,
    }
}

fn node_addr(host: &str, port: u16) -> String {
    format!("{host}:{port}")
}

fn hosts_from_host_ports(host_ports: &[String]) -> Vec<String> {
    host_ports
        .iter()
        .filter_map(|host_port| host_port.split_once(':').map(|(host, _)| host.to_string()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

fn connected_managed_nodes(
    nodes: &[crate::cli::task::redis_op_task::NodeInfo],
    managed_nodes: &HashSet<String>,
) -> Vec<String> {
    nodes.iter()
        .filter(|node| node.connected)
        .map(|node| node_addr(&node.ip, node.port))
        .filter(|node| managed_nodes.contains(node))
        .collect()
}

fn validate_host_port(value: &str, field: &str) -> anyhow::Result<()> {
    let Some((host, port_str)) = value.split_once(':') else {
        bail!("invalid {field} host:port: '{value}'");
    };
    if host.trim().is_empty() {
        bail!("invalid {field} host:port with empty host: '{value}'");
    }
    port_str
        .parse::<u16>()
        .map_err(|_| anyhow::anyhow!("invalid {field} port in host:port: '{value}'"))?;
    Ok(())
}

fn select_first_batch_nodes(
    ctx: &UpgradeContext,
    current_masters: &[String],
    current_replicas: &[String],
) -> anyhow::Result<Vec<String>> {
    if ctx.current_master_nodes.is_empty() {
        return Ok(current_replicas.to_vec());
    }

    for node in &ctx.current_master_nodes {
        validate_host_port(node, "--current-master-nodes")?;
    }

    let current_master_set: HashSet<String> = current_masters.iter().cloned().collect();
    let managed_nodes = ctx.managed_tx_and_standby_set();
    let mut provided_master_set = HashSet::new();
    for node in &ctx.current_master_nodes {
        if !managed_nodes.contains(node) {
            bail!(
                "--current-master-nodes entry '{}' is not in configured tx/standby nodes",
                node
            );
        }
        if !current_master_set.contains(node) {
            bail!(
                "--current-master-nodes entry '{}' is not a connected current master; current masters={:?}",
                node,
                current_masters
            );
        }
        provided_master_set.insert(node.clone());
    }

    let missing_masters: Vec<String> = current_masters
        .iter()
        .filter(|node| !provided_master_set.contains(*node))
        .cloned()
        .collect();
    if !missing_masters.is_empty() {
        bail!(
            "--current-master-nodes must include all connected current masters; missing={:?}, current masters={:?}",
            missing_masters,
            current_masters
        );
    }

    let first_batch_nodes: Vec<String> = ctx
        .managed_tx_and_standby_nodes()
        .into_iter()
        .filter(|node| !provided_master_set.contains(node))
        .collect();
    if first_batch_nodes.is_empty() {
        bail!(
            "--current-master-nodes covers all configured tx/standby nodes; no standby node remains for the first rolling-update batch"
        );
    }

    let current_replica_set: HashSet<String> = current_replicas.iter().cloned().collect();
    let offline_or_unknown: Vec<String> = first_batch_nodes
        .iter()
        .filter(|node| !current_replica_set.contains(*node))
        .cloned()
        .collect();
    if !offline_or_unknown.is_empty() {
        bail!(
            "nodes outside --current-master-nodes must be connected current replicas before first restart; invalid={:?}, current replicas={:?}",
            offline_or_unknown,
            current_replicas
        );
    }

    Ok(first_batch_nodes)
}

async fn fetch_cluster_nodes(ctx: &UpgradeContext, task_name: &str) -> anyhow::Result<ClusterNodes> {
    let task_id = TaskId {
        cmd: "topology".to_string(),
        task: task_name.to_string(),
        host: "_local".to_string(),
    };
    let (topology_tx, _) = watch::channel(ClusterNodes {
        masters: Vec::new(),
        replicas: Vec::new(),
    });
    let result = RedisOpTask::new(
        task_id,
        ctx.redis_cluster_startup_nodes(),
        "cluster topology".to_string(),
        topology_tx,
        ctx.redis_password.clone(),
        true,
    )
    .with_service_endpoints(ctx.deploy.connection.service_endpoints.clone())
    .execute(TaskHost::Local, HashMap::default())
    .await?;

    let values = result.ok_or_else(|| anyhow::anyhow!("missing topology task result"))?;
    let status = values
        .get(CMD_STATUS)
        .cloned()
        .unwrap_or(TaskArgValue::Number(1));
    let output = values
        .get(CMD_OUTPUT)
        .cloned()
        .unwrap_or_else(|| TaskArgValue::Str("missing cluster topology output".to_string()));

    match (status, output) {
        (TaskArgValue::Number(0), TaskArgValue::Str(json)) => {
            Ok(serde_json::from_str::<ClusterNodes>(&json)?)
        }
        (_, TaskArgValue::Str(err)) => Err(anyhow::anyhow!(err)),
        _ => Err(anyhow::anyhow!("unexpected topology task output")),
    }
}

fn build_stop_node_tasks(
    ctx: &UpgradeContext,
    task_group: &str,
    nodes: Vec<String>,
) -> anyhow::Result<TaskExecutionContext> {
    if nodes.is_empty() {
        return Ok(TaskExecutionContext::dummy());
    }
    let stop = EloqTxCtlTask::from_config_with_channel(
        SubCommand::Stop {
            cluster: ctx.cluster.clone(),
            tx: Some(true),
            log: true,
            store: false,
            monitor: false,
            force: true,
            all: false,
            password: ctx.redis_password.clone(),
            nodes,
        },
        &ctx.deploy,
        ServerType::Node,
        None,
    )?;
    Ok(single_barrier_ctx(task_group, stop))
}

fn build_start_node_tasks(
    ctx: &UpgradeContext,
    task_group: &str,
    nodes: Vec<String>,
) -> TaskExecutionContext {
    if nodes.is_empty() {
        return TaskExecutionContext::dummy();
    }
    let start = EloqTxCtlTask::from_config(
        SubCommand::Start {
            cluster: ctx.cluster.clone(),
            nodes,
        },
        &ctx.deploy,
        ServerType::Node,
    );
    single_barrier_ctx(task_group, start)
}

fn build_wait_replica_ready_tasks(
    ctx: &UpgradeContext,
    task_group: &str,
    task_prefix: &str,
    source_master: &str,
    target_replicas: &[String],
) -> anyhow::Result<TaskExecutionContext> {
    if target_replicas.is_empty() {
        return Ok(TaskExecutionContext::dummy());
    }

    let Some((source_host, source_port_str)) = source_master.split_once(':') else {
        bail!("invalid host:port in current master: '{source_master}'");
    };
    let Ok(source_port) = source_port_str.parse::<u16>() else {
        bail!("invalid port in current master: '{source_master}'");
    };

    let mut executable = IndexMap::new();
    for target_addr in target_replicas {
        let Some((target_host, target_port_str)) = target_addr.split_once(':') else {
            bail!("invalid host:port in target replica list: '{target_addr}'");
        };
        let Ok(target_port) = target_port_str.parse::<u16>() else {
            bail!("invalid port in target replica list: '{target_addr}'");
        };
        let task_id = TaskId {
            cmd: "topology".to_string(),
            task: format!("{task_prefix}-{target_port}"),
            host: target_host.to_string(),
        };
        executable.insert(
            task_id.clone(),
            TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(
                    WaitReplicaReadyTask::new(
                        task_id,
                        ctx.redis_cluster_startup_nodes(),
                        source_host.to_string(),
                        source_port,
                        target_host.to_string(),
                        target_port,
                        ctx.redis_password.clone(),
                    )
                    .with_service_endpoints(ctx.deploy.connection.service_endpoints.clone()),
                ),
                task_host: TaskHost::Local,
            },
        );
    }

    Ok(single_barrier_ctx(task_group, executable))
}

#[derive(Clone, Default)]
struct RollingUpdateState {
    first_batch_nodes: Vec<String>,
    second_batch_nodes: Vec<String>,
}

// ── Context ───────────────────────────────────────────────────────────────────

/// All configuration extracted upfront from CLI args + deploy config.
/// Each Step reads this to construct its TaskExecutionContext.
#[derive(Clone)]
pub struct UpgradeContext {
    pub config: Config,
    pub deploy: DeployConfig,
    pub cluster: String,
    pub redis_password: Option<String>,
    pub force: bool,
    pub skip_log_restart: bool,
    pub current_master_nodes: Vec<String>,
    update_state: Arc<Mutex<RollingUpdateState>>,
}

impl UpgradeContext {
    /// Create an `UpgradeContext` from the CLI args and cluster config.
    /// Panics if `config` is not `Config::Cluster` — callers must guarantee this.
    pub(crate) fn new(cmd_arg: &SubCommand, config: Config) -> Self {
        let Config::Cluster(ref deploy) = config;
        let deploy = deploy.clone();
        let (redis_password, force, skip_log_restart, current_master_nodes) = match cmd_arg {
            SubCommand::Update {
                password,
                force,
                skip_log_restart,
                current_master_nodes,
                ..
            } => (
                deploy.redis_password(password.clone()),
                *force,
                *skip_log_restart,
                current_master_nodes.clone(),
            ),
            SubCommand::UpdateConf { password, .. } => {
                (deploy.redis_password(password.clone()), false, false, Vec::new())
            }
            _ => (None, false, false, Vec::new()),
        };
        Self {
            cluster: deploy.deployment.cluster_name.clone(),
            config,
            deploy,
            redis_password,
            force,
            skip_log_restart,
            current_master_nodes,
            update_state: Arc::new(Mutex::new(RollingUpdateState::default())),
        }
    }

    pub fn has_standby(&self) -> bool {
        self.deploy
            .deployment
            .tx_service
            .standby_host_ports
            .is_some()
    }

    pub fn has_voter(&self) -> bool {
        self.deploy.deployment.tx_service.voter_host_ports.is_some()
    }

    pub fn has_log(&self) -> bool {
        self.deploy.deployment.log_service.is_some()
    }

    pub fn tx_host_ports(&self) -> Vec<String> {
        self.deploy.get_host_port_list(DeploymentPackage::EloqTx)
    }

    pub fn standby_host_ports(&self) -> Vec<String> {
        self.deploy
            .get_host_port_list(DeploymentPackage::EloqStandby)
    }

    pub fn voter_host_ports(&self) -> Vec<String> {
        self.deploy.get_host_port_list(DeploymentPackage::EloqVoter)
    }

    pub fn redis_cluster_startup_nodes(&self) -> Vec<String> {
        let mut host_ports = self.tx_host_ports();
        host_ports.extend(self.standby_host_ports());
        host_ports
    }

    fn managed_tx_and_standby_nodes(&self) -> Vec<String> {
        let mut host_ports = self.tx_host_ports();
        host_ports.extend(self.standby_host_ports());
        host_ports
    }

    fn managed_tx_and_standby_set(&self) -> HashSet<String> {
        self.managed_tx_and_standby_nodes().into_iter().collect()
    }

    fn set_first_batch_nodes(&self, nodes: Vec<String>) {
        self.update_state
            .lock()
            .expect("rolling update state poisoned")
            .first_batch_nodes = nodes;
    }

    fn first_batch_nodes(&self) -> Vec<String> {
        self.update_state
            .lock()
            .expect("rolling update state poisoned")
            .first_batch_nodes
            .clone()
    }

    fn set_second_batch_nodes(&self, nodes: Vec<String>) {
        self.update_state
            .lock()
            .expect("rolling update state poisoned")
            .second_batch_nodes = nodes;
    }

    fn second_batch_nodes(&self) -> Vec<String> {
        self.update_state
            .lock()
            .expect("rolling update state poisoned")
            .second_batch_nodes
            .clone()
    }
}

// ── Helper: build a round of topo→failover→stop ─────────────────────────────

pub struct StopStandbyOnly {
    ctx: UpgradeContext,
}

impl StopStandbyOnly {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for StopStandbyOnly {
    fn name(&self) -> &str {
        "StopStandbyOnly"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        if !self.ctx.has_standby() {
            return Ok(TaskExecutionContext::dummy());
        }
        let topology = fetch_cluster_nodes(&self.ctx, "rolling-update-initial-topology").await?;
        let managed_nodes = self.ctx.managed_tx_and_standby_set();
        let current_masters = connected_managed_nodes(&topology.masters, &managed_nodes);
        let current_replicas = connected_managed_nodes(&topology.replicas, &managed_nodes);
        let first_batch_nodes =
            select_first_batch_nodes(&self.ctx, &current_masters, &current_replicas)?;
        if first_batch_nodes.is_empty() {
            bail!(
                "rolling update requires a connected replica/standby node, but topology reported masters={:?}, replicas={:?}",
                topology.masters,
                topology.replicas
            );
        }
        info!(
            "Rolling update first batch nodes selected from current replicas: {:?}",
            first_batch_nodes
        );
        self.ctx.set_first_batch_nodes(first_batch_nodes.clone());
        build_stop_node_tasks(&self.ctx, "stop-standby", first_batch_nodes)
    }
}

pub struct FailoverAndStopOldMaster {
    ctx: UpgradeContext,
}

impl FailoverAndStopOldMaster {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for FailoverAndStopOldMaster {
    fn name(&self) -> &str {
        "FailoverAndStopOldMaster"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        if !self.ctx.has_standby() {
            let stop_tx = EloqTxCtlTask::from_config(
                SubCommand::Stop {
                    cluster: self.ctx.cluster.clone(),
                    tx: Some(true),
                    log: true,
                    store: false,
                    monitor: false,
                    force: true,
                    all: false,
                    password: self.ctx.redis_password.clone(),
                    nodes: Vec::new(),
                },
                &self.ctx.deploy,
                ServerType::Tx,
            );
            return Ok(single_barrier_ctx("stop-old-master", stop_tx));
        }

        let topology = fetch_cluster_nodes(&self.ctx, "rolling-update-pre-failover-topology").await?;
        let managed_nodes = self.ctx.managed_tx_and_standby_set();
        let current_masters = connected_managed_nodes(&topology.masters, &managed_nodes);
        let current_replicas = connected_managed_nodes(&topology.replicas, &managed_nodes);
        if current_masters.is_empty() {
            bail!(
                "rolling update could not find a connected current master before failover; topology reported masters={:?}, replicas={:?}",
                topology.masters,
                topology.replicas
            );
        }
        if current_replicas.is_empty() {
            bail!(
                "rolling update could not find a connected current replica to fail over to; topology reported masters={:?}, replicas={:?}",
                topology.masters,
                topology.replicas
            );
        }
        info!(
            "Rolling update failover sources selected from current masters: {:?}; current replicas: {:?}",
            current_masters,
            current_replicas
        );
        self.ctx.set_second_batch_nodes(current_masters.clone());
        let all_nodes = self.ctx.managed_tx_and_standby_nodes();

        build_round(
            "failover-stop-master",
            &current_masters,
            &current_masters,
            &all_nodes,
            &self.ctx,
        )
    }
}

fn build_round(
    round_label: &str,
    nodes_to_failover: &[String],
    nodes_to_stop: &[String],
    all_topology_nodes: &[String],
    ctx: &UpgradeContext,
) -> anyhow::Result<TaskExecutionContext> {
    let mut barrier = vec![];
    let mut executable = IndexMap::new();

    let topo_task_id = TaskId {
        cmd: "topology".to_string(),
        task: format!("check-topology-{round_label}"),
        host: "_local".to_string(),
    };
    let (topo_tx, failover_rx) = watch::channel::<ClusterNodes>(ClusterNodes {
        masters: Vec::new(),
        replicas: Vec::new(),
    });
    let stop_rx = failover_rx.clone();

    executable.insert(
        topo_task_id.clone(),
        TaskInstance {
            task_input: HashMap::default(),
            task: Box::new(
                RedisOpTask::new(
                    topo_task_id,
                    all_topology_nodes.to_vec(),
                    "cluster topology".to_string(),
                    topo_tx,
                    ctx.redis_password.clone(),
                    true,
                )
                .with_service_endpoints(ctx.deploy.connection.service_endpoints.clone()),
            ),
            task_host: TaskHost::Local,
        },
    );
    barrier.push(1);

    let mut failover_count = 0usize;
    for node_addr in nodes_to_failover {
        let Some((host, port_str)) = node_addr.split_once(':') else {
            bail!("invalid host:port in failover list: '{node_addr}'");
        };
        let Ok(port) = port_str.parse::<u16>() else {
            bail!("invalid port in failover list: '{node_addr}'");
        };
        let fid = TaskId {
            cmd: "failover".to_string(),
            task: format!("failover-check-{round_label}-{port_str}"),
            host: host.to_string(),
        };
        executable.insert(
            fid.clone(),
            TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(
                    FailoverOpTask::new(
                        fid,
                        host.to_string(),
                        port,
                        String::new(),
                        0u16,
                        failover_rx.clone(),
                        ctx.redis_password.clone(),
                    )
                    .with_service_endpoints(ctx.deploy.connection.service_endpoints.clone()),
                ),
                task_host: TaskHost::Local,
            },
        );
        failover_count += 1;
    }
    barrier.push(failover_count);

    let stop_tasks = EloqTxCtlTask::from_config_with_channel(
        SubCommand::Stop {
            cluster: ctx.cluster.clone(),
            tx: Some(true),
            log: true,
            store: false,
            monitor: false,
            force: true,
            all: false,
            password: ctx.redis_password.clone(),
            nodes: nodes_to_stop.to_vec(),
        },
        &ctx.deploy,
        ServerType::Node,
        Some(stop_rx),
    )?;
    barrier.push(stop_tasks.len());
    executable.extend(stop_tasks);

    Ok(TaskExecutionContext {
        task_group: format!("rolling-restart-{round_label}"),
        barrier: Some(barrier),
        executable,
    })
}

// ── Concrete Steps ──────────────────────────────────────────────────────────

pub struct DownloadAndExtract {
    ctx: UpgradeContext,
}

impl DownloadAndExtract {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for DownloadAndExtract {
    fn name(&self) -> &str {
        "DownloadAndExtract"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        let mut downloads = vec![];

        downloads.push(self.ctx.deploy.deployment.tx_image().to_owned());
        if let Some(img) = self.ctx.deploy.deployment.log_image() {
            downloads.push(img.to_owned());
        }

        let download_task = DownloadTask::instances(DownloadTask::from_urls(downloads));
        let extract_task = LocalExtractTask::from_config_keys(
            &self.ctx.deploy,
            &[ELOQ_FILE_KEY, ELOQ_LOG_FILE_KEY],
        )?;
        let barrier: Vec<usize> = [download_task.len(), extract_task.len()]
            .into_iter()
            .filter(|&n| n > 0)
            .collect();
        let mut executable = IndexMap::new();
        executable.extend(download_task);
        executable.extend(extract_task);

        Ok(TaskExecutionContext {
            task_group: "download-and-extract".to_string(),
            barrier: if barrier.is_empty() {
                None
            } else {
                Some(barrier)
            },
            executable,
        })
    }
}

pub struct UploadToStandby {
    ctx: UpgradeContext,
}

impl UploadToStandby {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for UploadToStandby {
    fn name(&self) -> &str {
        "UploadToStandby"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        let all_uploads =
            crate::cli::task::upload::eloq_upload_builder::EloqUpload::eloq_image_upload(
                &self.ctx.deploy.deployment,
            );

        let first_batch_nodes = self.ctx.first_batch_nodes();
        let standby_hosts: HashSet<String> = hosts_from_host_ports(&first_batch_nodes)
            .into_iter()
            .collect();

        let log_hosts: std::collections::HashSet<String> = self
            .ctx
            .deploy
            .deployment
            .log_service
            .as_ref()
            .map(|srv| {
                srv.log_host_unique()
                    .iter()
                    .map(|h| h.to_string())
                    .collect()
            })
            .unwrap_or_default();

        let filtered: Vec<_> = all_uploads
            .into_iter()
            .filter(|u| standby_hosts.contains(&u.host) || log_hosts.contains(&u.host))
            .collect();

        if filtered.is_empty() {
            return Ok(TaskExecutionContext::dummy());
        }

        let upload_tasks = crate::cli::task::upload::eloq_upload_builder::EloqUpload::build_tasks(
            &self.ctx.config,
            "update",
            "upload_to_standby",
            filtered,
        );

        Ok(single_barrier_ctx("upload-to-standby", upload_tasks))
    }
}

pub struct UploadToMaster {
    ctx: UpgradeContext,
}

impl UploadToMaster {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for UploadToMaster {
    fn name(&self) -> &str {
        "UploadToMaster"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        let all_uploads =
            crate::cli::task::upload::eloq_upload_builder::EloqUpload::eloq_image_upload(
                &self.ctx.deploy.deployment,
            );

        let second_batch_nodes = self.ctx.second_batch_nodes();
        let tx_hosts: HashSet<String> = hosts_from_host_ports(&second_batch_nodes)
            .into_iter()
            .collect();

        let voter_hosts: std::collections::HashSet<String> = self
            .ctx
            .voter_host_ports()
            .iter()
            .filter_map(|hp| hp.split(':').next().map(|h| h.to_string()))
            .collect();

        let filtered: Vec<_> = all_uploads
            .into_iter()
            .filter(|u| tx_hosts.contains(&u.host) || voter_hosts.contains(&u.host))
            .collect();

        if filtered.is_empty() {
            return Ok(TaskExecutionContext::dummy());
        }

        let upload_tasks = crate::cli::task::upload::eloq_upload_builder::EloqUpload::build_tasks(
            &self.ctx.config,
            "update",
            "upload_to_master",
            filtered,
        );

        Ok(single_barrier_ctx("upload-to-master", upload_tasks))
    }
}

pub struct StopTxNodes {
    ctx: UpgradeContext,
}

impl StopTxNodes {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for StopTxNodes {
    fn name(&self) -> &str {
        "StopTxNodes"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        if !self.ctx.has_standby() {
            let stop_tx = EloqTxCtlTask::from_config(
                SubCommand::Stop {
                    cluster: self.ctx.cluster.clone(),
                    tx: Some(true),
                    log: true,
                    store: false,
                    monitor: false,
                    force: true,
                    all: false,
                    password: self.ctx.redis_password.clone(),
                    nodes: Vec::new(),
                },
                &self.ctx.deploy,
                ServerType::Tx,
            );
            return Ok(single_barrier_ctx("stop-tx-nodes", stop_tx));
        }

        // Has standby: failover masters → stop them
        let tx_host_ports = self.ctx.tx_host_ports();
        let standby_host_ports = self.ctx.standby_host_ports();
        let mut all_nodes = tx_host_ports.clone();
        all_nodes.extend(standby_host_ports);

        build_round(
            "round1",
            &tx_host_ports,
            &tx_host_ports,
            &all_nodes,
            &self.ctx,
        )
    }
}

pub struct StopLog {
    ctx: UpgradeContext,
}

impl StopLog {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for StopLog {
    fn name(&self) -> &str {
        "StopLog"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        if !self.ctx.has_log() {
            return Ok(TaskExecutionContext::dummy());
        }
        let stop_log = EloqLogCtlTask::from_config(
            SubCommand::Stop {
                cluster: self.ctx.cluster.clone(),
                tx: Some(true),
                log: true,
                store: false,
                monitor: false,
                force: true,
                all: false,
                password: self.ctx.redis_password.clone(),
                nodes: Vec::new(),
            },
            &self.ctx.deploy,
        );
        Ok(single_barrier_ctx("stop-log", stop_log))
    }
}

pub struct CleanEloqStoreData {
    ctx: UpgradeContext,
}

impl CleanEloqStoreData {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for CleanEloqStoreData {
    fn name(&self) -> &str {
        "CleanEloqStoreData"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        let deployment = &self.ctx.deploy.deployment;
        let start_cmd = SubCommand::Start {
            cluster: self.ctx.cluster.clone(),
            nodes: Vec::new(),
        };

        if let Some(storage_service) = &deployment.storage_service {
            if let Some(dss) = &storage_service.eloqdss {
                match dss.backend_config() {
                    DataStoreServiceBackend::EloqStore(eloq_store_config) => {
                        if eloq_store_config.is_cloud_mode() {
                            let should_skip_cleanup = eloq_store_config
                                .get_cloud_config()
                                .and_then(|cc| cc.eloq_store_reuse_local_files)
                                .unwrap_or(false);
                            if !should_skip_cleanup {
                                let first_batch_hosts =
                                    hosts_from_host_ports(&self.ctx.first_batch_nodes());
                                let clean_tasks = EloqStoreDataCleanTask::build_tasks(
                                    start_cmd,
                                    &self.ctx.config,
                                    if first_batch_hosts.is_empty() {
                                        None
                                    } else {
                                        Some(first_batch_hosts.as_slice())
                                    },
                                );
                                if !clean_tasks.is_empty() {
                                    let len = clean_tasks.len();
                                    return Ok(TaskExecutionContext {
                                        task_group: "clean-eloq-store-data".to_string(),
                                        barrier: Some(vec![len]),
                                        executable: clean_tasks,
                                    });
                                }
                            } else {
                                info!(
                                    "Skipping EloqStore data cleanup (reuse_local_files enabled)"
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(TaskExecutionContext::dummy())
    }
}

pub struct StartLogAndWait {
    ctx: UpgradeContext,
}

impl StartLogAndWait {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for StartLogAndWait {
    fn name(&self) -> &str {
        "StartLogAndWait"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        if !self.ctx.has_log() {
            return Ok(TaskExecutionContext::dummy());
        }
        let start_cmd = SubCommand::Start {
            cluster: self.ctx.cluster.clone(),
            nodes: Vec::new(),
        };
        let start_log = EloqLogCtlTask::from_config(start_cmd, &self.ctx.deploy);
        let probe = EloqLogProbeTask::from_config(&self.ctx.deploy);

        let mut barrier = vec![];
        let mut executable = IndexMap::new();

        if !start_log.is_empty() {
            barrier.push(start_log.len());
            executable.extend(start_log);
        }
        if !probe.is_empty() {
            barrier.push(probe.len());
            executable.extend(probe);
        }

        Ok(TaskExecutionContext {
            task_group: "start-log-and-wait".to_string(),
            barrier: if barrier.is_empty() {
                None
            } else {
                Some(barrier)
            },
            executable,
        })
    }
}

pub struct StartTx {
    ctx: UpgradeContext,
}

impl StartTx {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for StartTx {
    fn name(&self) -> &str {
        "StartTx"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        let mut start_tx = if self.ctx.has_standby() {
            build_start_node_tasks(&self.ctx, "start-tx", self.ctx.second_batch_nodes()).executable
        } else {
            EloqTxCtlTask::from_config(
                SubCommand::Start {
                    cluster: self.ctx.cluster.clone(),
                    nodes: Vec::new(),
                },
                &self.ctx.deploy,
                ServerType::Tx,
            )
        };

        if self.ctx.has_voter() {
            let start_voter = EloqTxCtlTask::from_config(
                SubCommand::Start {
                    cluster: self.ctx.cluster.clone(),
                    nodes: Vec::new(),
                },
                &self.ctx.deploy,
                ServerType::Voter,
            );
            start_tx.extend(start_voter);
        }

        Ok(single_barrier_ctx("start-tx", start_tx))
    }
}

pub struct WaitCurrentMaster {
    ctx: UpgradeContext,
}

impl WaitCurrentMaster {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for WaitCurrentMaster {
    fn name(&self) -> &str {
        "WaitCurrentMaster"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        let task_id = TaskId {
            cmd: "topology".to_string(),
            task: "wait-current-master".to_string(),
            host: "_local".to_string(),
        };
        let (topology_tx, _) = watch::channel(ClusterNodes {
            masters: Vec::new(),
            replicas: Vec::new(),
        });
        let mut executable = IndexMap::new();
        executable.insert(
            task_id.clone(),
            TaskInstance {
                task_input: HashMap::default(),
                task: Box::new(
                    RedisOpTask::new(
                        task_id,
                        self.ctx.redis_cluster_startup_nodes(),
                        "cluster topology".to_string(),
                        topology_tx,
                        self.ctx.redis_password.clone(),
                        true,
                    )
                    .with_service_endpoints(self.ctx.deploy.connection.service_endpoints.clone()),
                ),
                task_host: TaskHost::Local,
            },
        );
        Ok(single_barrier_ctx("wait-current-master", executable))
    }
}

pub struct FailoverBackAndStopStandby {
    ctx: UpgradeContext,
}

impl FailoverBackAndStopStandby {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

pub struct WaitTxReplicaReady {
    ctx: UpgradeContext,
}

impl WaitTxReplicaReady {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for WaitTxReplicaReady {
    fn name(&self) -> &str {
        "WaitTxReplicaReady"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        if !self.ctx.has_standby() {
            return Ok(TaskExecutionContext::dummy());
        }
        let targets = self.ctx.second_batch_nodes();
        let topology = fetch_cluster_nodes(&self.ctx, "rolling-update-wait-second-batch-topology")
            .await?;
        let managed_nodes = self.ctx.managed_tx_and_standby_set();
        let current_masters = connected_managed_nodes(&topology.masters, &managed_nodes);
        let Some(source_master) = current_masters.first() else {
            bail!(
                "rolling update could not find current master while waiting for updated old master replicas; topology reported masters={:?}, replicas={:?}",
                topology.masters,
                topology.replicas
            );
        };
        build_wait_replica_ready_tasks(
            &self.ctx,
            "wait-tx-replica-ready",
            "wait-tx-replica-ready",
            source_master,
            &targets,
        )
    }
}

pub struct WaitStandbyReplicaReady {
    ctx: UpgradeContext,
}

impl WaitStandbyReplicaReady {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for WaitStandbyReplicaReady {
    fn name(&self) -> &str {
        "WaitStandbyReplicaReady"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        if !self.ctx.has_standby() {
            return Ok(TaskExecutionContext::dummy());
        }
        let targets = self.ctx.first_batch_nodes();
        let topology = fetch_cluster_nodes(&self.ctx, "rolling-update-wait-first-batch-topology")
            .await?;
        let managed_nodes = self.ctx.managed_tx_and_standby_set();
        let current_masters = connected_managed_nodes(&topology.masters, &managed_nodes);
        let Some(source_master) = current_masters.first() else {
            bail!(
                "rolling update could not find current master while waiting for updated replica; topology reported masters={:?}, replicas={:?}",
                topology.masters,
                topology.replicas
            );
        };
        build_wait_replica_ready_tasks(
            &self.ctx,
            "wait-standby-replica-ready",
            "wait-standby-replica-ready",
            source_master,
            &targets,
        )
    }
}

#[async_trait]
impl Step for FailoverBackAndStopStandby {
    fn name(&self) -> &str {
        "FailoverBackAndStopStandby"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        if !self.ctx.has_standby() {
            return Ok(TaskExecutionContext::dummy());
        }

        let standby_host_ports = self.ctx.standby_host_ports();
        let tx_host_ports = self.ctx.tx_host_ports();
        let mut all_nodes = standby_host_ports.clone();
        all_nodes.extend(tx_host_ports);

        build_round(
            "round2",
            &standby_host_ports,
            &standby_host_ports,
            &all_nodes,
            &self.ctx,
        )
    }
}

pub struct StartStandby {
    ctx: UpgradeContext,
}

impl StartStandby {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for StartStandby {
    fn name(&self) -> &str {
        "StartStandby"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        if !self.ctx.has_standby() {
            return Ok(TaskExecutionContext::dummy());
        }
        Ok(build_start_node_tasks(
            &self.ctx,
            "start-standby",
            self.ctx.first_batch_nodes(),
        ))
    }
}

pub struct StopVoters {
    ctx: UpgradeContext,
}

impl StopVoters {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for StopVoters {
    fn name(&self) -> &str {
        "StopVoters"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        if !self.ctx.has_voter() {
            return Ok(TaskExecutionContext::dummy());
        }
        let stop = EloqTxCtlTask::from_config(
            SubCommand::Stop {
                cluster: self.ctx.cluster.clone(),
                tx: None,
                log: false,
                store: false,
                monitor: false,
                force: true,
                all: false,
                password: self.ctx.redis_password.clone(),
                nodes: Vec::new(),
            },
            &self.ctx.deploy,
            ServerType::Voter,
        );
        Ok(single_barrier_ctx("stop-voters", stop))
    }
}

pub struct StartVoters {
    ctx: UpgradeContext,
}

impl StartVoters {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for StartVoters {
    fn name(&self) -> &str {
        "StartVoters"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        if !self.ctx.has_voter() {
            return Ok(TaskExecutionContext::dummy());
        }
        let start = EloqTxCtlTask::from_config(
            SubCommand::Start {
                cluster: self.ctx.cluster.clone(),
                nodes: Vec::new(),
            },
            &self.ctx.deploy,
            ServerType::Voter,
        );
        Ok(single_barrier_ctx("start-voters", start))
    }
}

pub struct VerifyVersion {
    ctx: UpgradeContext,
}

impl VerifyVersion {
    pub fn new(ctx: UpgradeContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl Step for VerifyVersion {
    fn name(&self) -> &str {
        "VerifyVersion"
    }

    async fn build(&self) -> anyhow::Result<TaskExecutionContext> {
        let tx_dir = self.ctx.deploy.deployment.tx_srv_home();
        let tx_bin = self.ctx.deploy.deployment.tx_srv_bin();
        let tasks = ExecCustomCommand::build_task_by_host(
            format!("cd {tx_dir}; export LD_LIBRARY_PATH={tx_dir}/lib:$LD_LIBRARY_PATH; {tx_bin} --version"),
            &self.ctx.config,
            self.ctx.deploy.deployment.tx_service.merge_hosts(),
            Some("check_eloqkv_version".to_string()),
        );
        Ok(single_barrier_ctx("verify-version", tasks))
    }
}

// ── Builder ──────────────────────────────────────────────────────────────────

/// Build the list of steps for a rolling binary upgrade (`eloqctl update`).
///
/// Strategy: update standby first, then failover, then update old master.
/// This minimizes downtime because the master continues serving during
/// the standby update phase.
pub fn build_upgrade_steps(ctx: UpgradeContext) -> Vec<Box<dyn Step>> {
    let mut steps: Vec<Box<dyn Step>> = vec![
        Box::new(DownloadAndExtract::new(ctx.clone())),
        Box::new(StopStandbyOnly::new(ctx.clone())),
        Box::new(UploadToStandby::new(ctx.clone())),
    ];

    if ctx.skip_log_restart {
        info!("Skipping log service restart during rolling update (--skip-log-restart)");
    } else {
        steps.push(Box::new(StopLog::new(ctx.clone())));
    }

    steps.push(Box::new(CleanEloqStoreData::new(ctx.clone())));

    if !ctx.skip_log_restart {
        steps.push(Box::new(StartLogAndWait::new(ctx.clone())));
    }

    steps.push(Box::new(StartStandby::new(ctx.clone())));
    steps.push(Box::new(WaitStandbyReplicaReady::new(ctx.clone())));
    steps.push(Box::new(FailoverAndStopOldMaster::new(ctx.clone())));
    steps.push(Box::new(UploadToMaster::new(ctx.clone())));
    steps.push(Box::new(StartTx::new(ctx.clone())));
    steps.push(Box::new(WaitTxReplicaReady::new(ctx.clone())));
    steps.push(Box::new(VerifyVersion::new(ctx)));

    steps
}

/// Build the list of steps for a rolling config restart (`eloqctl update-conf --restart`).
///
/// Strategy: restart standby first with new config, then failover, then restart old master.
pub fn build_config_restart_steps(ctx: UpgradeContext) -> Vec<Box<dyn Step>> {
    vec![
        Box::new(StopStandbyOnly::new(ctx.clone())),
        Box::new(FailoverAndStopOldMaster::new(ctx.clone())),
        Box::new(StartTx::new(ctx.clone())),
        Box::new(WaitTxReplicaReady::new(ctx.clone())),
    ]
}
