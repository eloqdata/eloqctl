use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskHost};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use anyhow::anyhow;
use async_trait::async_trait;
use futures::AsyncWriteExt;
use itertools::Itertools;
use russh::*;
use russh_keys::*;
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info};

#[derive(Clone)]
pub struct SSHClient {}

#[async_trait]
impl client::Handler for SSHClient {
    type Error = russh::Error;

    async fn check_server_key(
        self,
        _server_public_key: &key::PublicKey,
    ) -> Result<(Self, bool), Self::Error> {
        // println!("ssh client check_server_key = {server_public_key:?}");
        Ok((self, true))
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
        let ssh_socket_addr_rs = format!("{host}:{port}").to_socket_addrs();
        if ssh_socket_addr_rs.is_err() {
            panic!("SSHSession build SocketAddr error from [{host}:{port}]. Please check deployment.yaml");
        }
        let ssh_socket_add_ref = ssh_socket_addr_rs.unwrap();
        let ssh_addr_vec = ssh_socket_add_ref
            .as_slice()
            .iter()
            .filter(|add| add.is_ipv4() && !add.to_string().contains("0.0.0.0"))
            .collect_vec();
        let ssh_addr = ssh_addr_vec.as_slice().first().unwrap();
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
        let session = self.session.lock().await;
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
        // println!("SSHSession output = {output_str}");
        let cmd_res = HashMap::from([
            (CMD.to_string(), TaskArgValue::Str(command.to_string())),
            (
                CMD_STATUS.to_string(),
                TaskArgValue::Number(status_code as usize),
            ),
            (CMD_OUTPUT.to_string(), TaskArgValue::Str(output_str)),
        ]);
        channel.close().await?;
        Ok(cmd_res)
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        let session = self.session.lock().await;
        let close_rs = session
            .disconnect(Disconnect::ByApplication, "", "English")
            .await;
        if let Err(close_err) = close_rs {
            error!("SSHSession close error cause by {}", close_err.to_string());
            Err(anyhow!(close_err.to_string()))
        } else {
            println!("SSHSession close by monograph waiter cluster_mgr");
            Ok(())
        }
    }
}
