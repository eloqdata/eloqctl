//! Rolling upgrade engine for zero-downtime cluster upgrades.
//!
//! Design philosophy: **the cluster's own state is the single source of truth**.
//!
//! Every Step exposes an `is_done()` predicate that queries the live cluster.
//! If a step is already complete (e.g. process already stopped, binary already
//! the right version, topology already correct) it is skipped silently.
//!
//! This means the upgrade is fully idempotent: if it fails halfway through, fix
//! the underlying problem and re-run — completed steps are skipped automatically,
//! execution resumes from the first incomplete step.
//!
//! No external state store is involved. No "resume from checkpoint" complexity.

pub mod steps;

use anyhow::anyhow;
use async_trait::async_trait;
use std::time::Instant;
use tracing::info;

// ── Context ──────────────────────────────────────────────────────────────────

/// Runtime context passed to every Step.
/// Steps read it to check current cluster state and write to it to share
/// information (e.g. discovered topology) with subsequent steps.
#[derive(Debug, Clone)]
pub struct ClusterContext {
    /// SSH private key path
    pub ssh_key: String,
    /// SSH user
    pub ssh_user: String,
    /// SSH port
    pub ssh_port: u16,
    /// Redis password (if any)
    pub redis_password: Option<String>,
    /// Install directory on remote hosts (e.g. `/data/eloqkv-cluster-01`)
    pub install_dir: String,
    /// Target version string being upgraded to (e.g. `1.2.2`)
    pub target_version: String,
    /// tx node host:port list
    pub tx_nodes: Vec<NodeAddr>,
    /// standby node host:port list (empty if no standby)
    pub standby_nodes: Vec<NodeAddr>,
    /// voter node host:port list (empty if no voters)
    pub voter_nodes: Vec<NodeAddr>,
    /// log service host list (empty if no log service)
    pub log_nodes: Vec<String>,
    /// Topology discovered at runtime; populated by QueryTopology step
    pub topology: Option<ClusterTopology>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeAddr {
    pub host: String,
    pub port: u16,
}

impl NodeAddr {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// Parse from "host:port" string.
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        let (host, port_str) = s
            .split_once(':')
            .ok_or_else(|| anyhow!("invalid host:port '{s}'"))?;
        let port = port_str
            .parse::<u16>()
            .map_err(|_| anyhow!("invalid port in '{s}'"))?;
        Ok(Self::new(host, port))
    }

    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Clone)]
pub struct ClusterTopology {
    pub masters: Vec<NodeAddr>,
    pub replicas: Vec<NodeAddr>,
}

impl ClusterTopology {
    pub fn is_master(&self, node: &NodeAddr) -> bool {
        self.masters.iter().any(|m| m == node)
    }
    pub fn is_replica(&self, node: &NodeAddr) -> bool {
        self.replicas.iter().any(|r| r == node)
    }
}

// ── Step trait ────────────────────────────────────────────────────────────────

/// A single step in the rolling upgrade.
///
/// Implementors must be `Send + Sync` so the runner can drive them from an
/// async context.
#[async_trait]
pub trait Step: Send + Sync {
    /// Human-readable name shown in progress output.
    fn name(&self) -> &str;

    /// Returns `true` if this step has already been completed and can be
    /// skipped.  The implementation should query the live cluster state — no
    /// external bookkeeping.
    ///
    /// If checking fails (e.g. SSH down) return `false` so the step is
    /// attempted and the real error surfaces in `run()`.
    async fn is_done(&self, ctx: &ClusterContext) -> bool;

    /// Execute the step.  Called only when `is_done()` returns `false`.
    async fn run(&self, ctx: &mut ClusterContext) -> anyhow::Result<()>;
}

// ── Runner ────────────────────────────────────────────────────────────────────

pub struct RollingUpgrade {
    steps: Vec<Box<dyn Step>>,
}

impl RollingUpgrade {
    pub fn new(steps: Vec<Box<dyn Step>>) -> Self {
        Self { steps }
    }

    /// Execute all steps in order, skipping already-completed ones.
    pub async fn execute(&self, ctx: &mut ClusterContext) -> anyhow::Result<()> {
        let total = self.steps.len();
        println!("[upgrade] {} steps total", total);

        for (i, step) in self.steps.iter().enumerate() {
            let n = i + 1;
            let name = step.name();

            if step.is_done(ctx).await {
                println!("[{n}/{total}] skip (already done): {name}");
                info!("Rolling upgrade step {n}/{total} '{name}' already done, skipping");
                continue;
            }

            println!("[{n}/{total}] running: {name}");
            info!("Rolling upgrade step {n}/{total} '{name}' starting");
            let t = Instant::now();

            step.run(ctx).await.map_err(|e| {
                eprintln!("[{n}/{total}] FAILED: {name} -- {e}");
                anyhow!("step '{name}' failed: {e}")
            })?;

            println!(
                "[{n}/{total}] done ({:.1}s): {name}",
                t.elapsed().as_secs_f32()
            );
            info!(
                "Rolling upgrade step {n}/{total} '{name}' done in {:.1}s",
                t.elapsed().as_secs_f32()
            );
        }

        println!("[upgrade] all steps complete");
        Ok(())
    }
}
