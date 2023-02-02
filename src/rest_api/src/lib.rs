#![feature(async_closure)]
#![feature(once_cell)]

use actix_web::error;
use anyhow::Error;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

use derive_more::Error;
use error::ResponseError;
use serde_json::Value;

pub mod handler;
pub mod server;

pub(crate) static SUPPORT_CMD: [&str; 5] = ["deploy", "install", "start", "stop", "status"];

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
pub struct Response {
    code: usize,
    msg: Option<String>,
    data: Option<Value>,
}

#[derive(Parser, Default, Debug)]
#[command(
    author,
    version = "1.0.0",
    about = "MonographDB Cluster Manager REST API"
)]
#[command(next_line_help = true)]
pub struct ServerCommandArgs {
    #[arg(short, long, value_name = "config")]
    pub config: PathBuf,
    #[arg(short, long, value_name = "addr")]
    pub addr: Option<String>,
    #[arg(short, long, value_name = "port")]
    pub port: Option<u16>,
}
