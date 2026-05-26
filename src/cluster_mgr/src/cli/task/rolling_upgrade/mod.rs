//! Rolling upgrade engine for zero-downtime cluster upgrades.
//!
//! Design: **idempotent, retryable steps executed sequentially**.
//!
//! Each Step builds a `TaskExecutionContext` (same as existing TaskGroup impls)
//! and drives it through a freshly-created `TaskController`.  If a step fails,
//! the whole upgrade aborts with a clear error message.  Re-running the upgrade
//! command from scratch is safe because every step is idempotent by nature
//! (stop an already-stopped process → no-op, unpack already-correct binary → ok).
//!
//! Both `update` (binary upgrade) and `update-conf --restart` (config rolling
//! restart) share the same Step implementations — no more duplicated code.

pub mod steps;

use crate::cli::task::group::Config;
use crate::cli::task::task_base::{
    is_verbose_task_output, TaskExecutionContext, TaskResultEnum, TaskResultPair,
};
use crate::cli::task::task_controller::TaskController;
use anyhow::{anyhow, bail};
use async_trait::async_trait;
use std::time::Instant;
use tracing::info;

// ── Step trait ────────────────────────────────────────────────────────────────

/// A single step in the rolling upgrade.
#[async_trait]
pub trait Step: Send + Sync {
    /// Human-readable name shown in progress output.
    fn name(&self) -> &str;

    /// Build the tasks for this step and return a `TaskExecutionContext`.
    /// Called by the runner; do not call directly.
    async fn build(&self) -> anyhow::Result<TaskExecutionContext>;
}

// ── Helper: execute one TaskExecutionContext ──────────────────────────────────

/// Execute a `TaskExecutionContext` synchronously (sequentially by barrier,
/// parallel within each barrier group).  Returns an error if any task fails.
pub async fn run_step_context(ctx: TaskExecutionContext, config: Config) -> anyhow::Result<()> {
    if ctx.executable.is_empty() {
        return Ok(());
    }
    // Leak a fresh TaskController so we get a &'static reference as required
    // by run_all_tasks.  The controller is small (~2 channel endpoints) and
    // this happens at most once per Step, so total leakage is bounded.
    let controller: &'static TaskController = Box::leak(Box::new(TaskController::new()));

    let results: Vec<TaskResultPair> = controller.run_all_tasks(ctx, config, true).await?;

    // Check for any task-level errors (run_all_tasks with err_brk=true already
    // bails on the first error, but we double-check for clarity).
    for pair in &results {
        if let TaskResultEnum::Error(msg) = &pair.result {
            bail!("task '{}' failed: {}", pair.task_id, msg);
        }
    }
    Ok(())
}

// ── Runner ────────────────────────────────────────────────────────────────────

/// Sequential rolling-upgrade executor.
///
/// Steps are executed in the order they are provided.  Execution aborts on the
/// first step that fails.  Every step builds its own `TaskExecutionContext` and
/// is driven through an independent `TaskController`.
///
/// Steps are expected to be **idempotent** — re-running after a failure should
/// be safe because stopping an already-stopped process, unpacking an
/// already-correct binary, etc. are no-ops by design.
pub struct RollingUpgrade {
    steps: Vec<Box<dyn Step>>,
    config: Config,
}

impl RollingUpgrade {
    /// Create a new upgrade runner with the given ordered steps and cluster config.
    pub fn new(steps: Vec<Box<dyn Step>>, config: Config) -> Self {
        Self { steps, config }
    }

    /// Execute all steps in order.  Stops at the first failure.
    pub async fn execute(&self) -> anyhow::Result<()> {
        let total = self.steps.len();
        println!("Rolling restart: {total} steps");

        for (i, step) in self.steps.iter().enumerate() {
            let n = i + 1;
            let name = step.name();

            println!("[{n}/{total}] {}", friendly_step_name(name));
            info!("Rolling upgrade step {n}/{total} '{name}' starting");
            let t = Instant::now();

            let ctx = step
                .build()
                .await
                .map_err(|e| anyhow!("step '{name}' build failed: {e}"))?;

            run_step_context(ctx, self.config.clone())
                .await
                .map_err(|e| {
                    if is_verbose_task_output() {
                        eprintln!("[{n}/{total}] FAILED: {name} -- {e}");
                    } else {
                        eprintln!("[{n}/{total}] failed: {}", friendly_step_name(name));
                    }
                    anyhow!("step '{name}' failed: {e}")
                })?;

            if is_verbose_task_output() {
                println!(
                    "[{n}/{total}] done ({:.1}s): {name}",
                    t.elapsed().as_secs_f32()
                );
            } else {
                println!("  done");
            }
            info!(
                "Rolling upgrade step {n}/{total} '{name}' done in {:.1}s",
                t.elapsed().as_secs_f32()
            );
        }

        println!("Rolling restart complete");
        Ok(())
    }
}

fn friendly_step_name(name: &str) -> &str {
    match name {
        "StopTxNodes" => "Move traffic away from the current master",
        "StartTx" => "Start the original master node",
        "WaitCurrentMaster" => "Wait for a serving master",
        "WaitTxReplicaReady" => "Wait for the original master to rejoin as replica",
        "FailoverBackAndStopStandby" => "Move traffic back and restart standby",
        "StartStandby" => "Start standby nodes",
        "StopVoters" => "Stop voter nodes",
        "StartVoters" => "Start voter nodes",
        "DownloadAndUpload" => "Prepare upgrade package",
        "StopLog" => "Stop log service",
        "UnpackTxLog" => "Install upgrade package",
        "CleanEloqStoreData" => "Clean EloqStore data",
        "StartLogAndWait" => "Start log service",
        "VerifyVersion" => "Verify upgraded version",
        _ => name,
    }
}
