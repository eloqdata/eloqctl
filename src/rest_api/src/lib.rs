// #![feature(async_closure)]
// #![feature(once_cell)]

use actix_web::error;
use anyhow::Error;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

use cluster_mgr::cli::SubCommand;
use cluster_mgr::config::config_base::DeployConfig;
use derive_more::Error;
use error::ResponseError;
use serde_json::Value;
use tokio::signal::unix::{signal, SignalKind};
use tracing::info;

mod global_handler;
pub mod handler;
pub mod server;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MonographConnInfo {
    pub user: String,
    pub password: String,
}

#[derive(Clone, Debug)]
pub struct RequestPayload {
    pub command: Option<SubCommand>,
    pub config: Option<DeployConfig>,
}

#[derive(Debug, Error)]
pub struct WebHandleError {
    err: anyhow::Error,
}

impl Display for WebHandleError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "handler error: {}", self.err)
    }
}

impl ResponseError for WebHandleError {}

impl From<Error> for WebHandleError {
    fn from(value: Error) -> Self {
        WebHandleError { err: value }
    }
}

#[derive(Deserialize, Serialize)]
pub struct ResponseData {
    code: usize,
    msg: Option<String>,
    data: Option<Value>,
}

#[derive(Parser, Default, Debug)]
#[command(author, version = "0.0.0", about = "EloqData cluster manager REST API")]
#[command(next_line_help = true)]
pub struct ServerCommandArgs {
    #[arg(long, value_name = "HOME_DIR")]
    pub home: Option<PathBuf>,
    #[arg(short, long, value_name = "addr")]
    pub addr: Option<String>,
    #[arg(short, long, value_name = "port")]
    pub port: Option<u16>,
}

pub(crate) async fn listen_exit_signal<F, Fut, T>(t: T, call_back: F)
where
    T: Send + 'static,
    F: Fn(T) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut interrupt = signal(SignalKind::interrupt()).unwrap();
    let mut terminate = signal(SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = interrupt.recv() => {
           info!("Recv interrupt.");
           call_back(t).await;
        }
        _ = terminate.recv() => {
           info!("Recv terminate.");
           call_back(t).await;
        }
    }
}
