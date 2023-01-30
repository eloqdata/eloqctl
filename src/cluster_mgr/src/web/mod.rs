use actix_web::error;
use anyhow::Error;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

use derive_more::Error;
use error::ResponseError;
use serde_json::Value;

pub mod server;
mod web_handler;

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
