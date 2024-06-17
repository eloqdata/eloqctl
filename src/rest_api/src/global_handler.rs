use crate::{listen_exit_signal, RequestPayload};
use cluster_mgr::cli::cmd_base::CommandExecutor;
use std::sync::Arc;
use tracing::{error, info};

#[derive(Clone)]
pub struct GlobalCommandHandler {
    cmd_executor: Arc<CommandExecutor>,
    tx: crossbeam_channel::Sender<RequestPayload>,
    rx: crossbeam_channel::Receiver<RequestPayload>,
}

impl GlobalCommandHandler {
    pub async fn new(cmd_executor: CommandExecutor) -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        let handler = GlobalCommandHandler {
            cmd_executor: Arc::new(cmd_executor),
            tx,
            rx,
        };
        let handler_arc = Arc::new(handler.clone());
        let handler_clone = Arc::clone(&handler_arc);
        tokio::spawn(async move {
            listen_exit_signal(handler_clone, |handler_clone| async move {
                handler_clone.close()
            })
            .await;
        });
        tokio::spawn(async move {
            let _ = handler_arc.handle().await;
        });
        handler
    }

    pub fn get_command_executor(&self) -> &CommandExecutor {
        &self.cmd_executor
    }

    fn close(&self) {
        info!("GlobalCommandHandler will exit.");
        self.tx
            .send(RequestPayload {
                command: None,
                config: None,
            })
            .unwrap();
    }

    pub fn submit(&self, payload: RequestPayload) {
        self.tx.send(payload).unwrap();
    }

    pub async fn handle(&self) -> anyhow::Result<()> {
        let cmd_executor = Box::leak(Box::new(self.cmd_executor.clone()));
        while let Ok(payload) = self.rx.recv() {
            let cmd_opt = payload.command;
            if cmd_opt.is_none() {
                break;
            }
            let cmd = cmd_opt.unwrap();
            let cmd_str = cmd.as_ref().to_owned();
            info!("Global handler process command={cmd_str}");
            match cmd.as_ref() {
                "deploy" | "run-deps" => {
                    let config = payload.config.unwrap();
                    if let Err(err) = cmd_executor.run(cmd, Some(config)).await {
                        error!("command {cmd_str} failed: {err}");
                    }
                }
                _ => {
                    if let Err(err) = cmd_executor.run(cmd, None).await {
                        error!("command {cmd_str} failed: {err}");
                    }
                }
            }
        }
        Ok(())
    }
}
