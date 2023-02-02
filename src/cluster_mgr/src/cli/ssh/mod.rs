use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskHost};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use futures::AsyncWriteExt;
use russh::client::Session;
use russh::*;
use russh_keys::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info};

#[derive(Clone)]
pub struct SSHClient {}

impl client::Handler for SSHClient {
    type Error = russh::Error;
    type FutureBool = futures::future::Ready<Result<(Self, bool), Self::Error>>;
    type FutureUnit = futures::future::Ready<Result<(Self, Session), Self::Error>>;

    fn finished_bool(self, b: bool) -> Self::FutureBool {
        futures::future::ready(Ok((self, b)))
    }

    fn finished(self, session: Session) -> Self::FutureUnit {
        futures::future::ready(Ok((self, session)))
    }

    fn check_server_key(self, server_public_key: &key::PublicKey) -> Self::FutureBool {
        info!("SSHClient check_server_key: {}", server_public_key.name());
        self.finished_bool(true)
    }
}

#[derive(Clone, Debug)]
pub enum SSHCommandOption {
    CollectOutput,
    None,
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct SSHSession {
    session: Arc<Mutex<client::Handle<SSHClient>>>,
    user: String,
    host: String,
    port: usize,
}

impl SSHSession {
    pub async fn from_task_host(host: TaskHost, key_path: String) -> anyhow::Result<Self> {
        match host {
            TaskHost::Remote { user, port, hosts } => {
                SSHSession::connect(key_path, user.as_str(), hosts.as_str(), port).await
            }
            _ => {
                unreachable!()
            }
        }
    }

    pub async fn connect<P: AsRef<Path>>(
        key_path: P,
        user: &str,
        host: &str,
        port: usize,
    ) -> anyhow::Result<Self> {
        let ssh_config = Arc::new(client::Config {
            connection_timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        });
        let ssh_client = SSHClient {};
        let ssh_addr = SocketAddr::from_str(format!("{host}:{port}").as_str())?;
        let mut session = client::connect(ssh_config, ssh_addr, ssh_client).await?;
        let key_pair = load_secret_key(key_path, None)?;
        let auth_rs = session
            .authenticate_publickey(user, Arc::new(key_pair))
            .await?;
        assert!(auth_rs);
        info!(
            "SSHSession connect remote host success host={:?},user={:?}",
            host, user
        );
        Ok(Self {
            session: Arc::new(Mutex::new(session)),
            user: user.to_string(),
            host: host.to_string(),
            port,
        })
    }

    pub fn ssh_conn_info(&self) -> (String, String) {
        (self.host.clone(), self.user.clone())
    }

    pub async fn command(
        &self,
        command: &str,
        cmd_option: SSHCommandOption,
    ) -> anyhow::Result<ExecutionValue> {
        let mut session = self.session.lock().await;
        let mut channel = session.channel_open_session().await?;
        channel.exec(true, command).await?;
        let mut output = Vec::new();
        let mut status_code = 0;
        while let Some(chanel_msg) = channel.wait().await {
            match chanel_msg {
                ChannelMsg::Data { ref data } => {
                    if let SSHCommandOption::CollectOutput = cmd_option {
                        output.write_all(data).await?;
                    }
                }
                ChannelMsg::ExitStatus { exit_status } => {
                    status_code = exit_status;
                }
                ChannelMsg::Failure => {
                    error!("ssh channel receive failure msg.");
                }
                _ => {}
            }
        }
        let output_str = String::from_utf8_lossy(&output).into_owned();
        let cmd_res = HashMap::from([
            (CMD.to_string(), TaskArgValue::Str(command.to_string())),
            (
                CMD_STATUS.to_string(),
                TaskArgValue::Number(status_code as usize),
            ),
            (CMD_OUTPUT.to_string(), TaskArgValue::Str(output_str)),
        ]);
        channel.eof().await?;
        Ok(cmd_res)
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        info!("SSHSession close() invoke.");
        let session = self.session.lock().await;
        session
            .disconnect(Disconnect::ByApplication, "", "English")
            .await?;
        Ok(())
    }
}
