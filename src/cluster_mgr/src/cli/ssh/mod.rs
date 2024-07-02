use crate::cli::task::task_base::{ExecutionValue, TaskArgValue, TaskHost};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use anyhow::bail;
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
use std::vec;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct SSHClient {}

#[async_trait]
impl client::Handler for SSHClient {
    type Error = russh::Error;

    async fn check_server_key(
        self,
        _server_public_key: &key::PublicKey,
    ) -> Result<(Self, bool), Self::Error> {
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
                info!("ssh connect key {key_path}");
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
            keepalive_interval: Some(Duration::from_secs(3)),
            ..Default::default()
        });
        let ssh_client = SSHClient {};
        let ssh_socket_addr_rs = format!("{host}:{port}").to_socket_addrs();
        if ssh_socket_addr_rs.is_err() {
            bail!("SSHSession build SocketAddr error from [{host}:{port}]. Please check deployment.yaml");
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
        if !session
            .authenticate_publickey(user, Arc::new(key_pair))
            .await?
        {
            bail!("ssh auth failed {user}@{ssh_addr}")
        }
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
        let mut status_code = 0_i32;
        while let Some(chanel_msg) = channel.wait().await {
            match chanel_msg {
                ChannelMsg::Data { ref data } => {
                    if let SSHCommandOption::CollectOutput = cmd_option {
                        output.write_all(data).await?;
                    }
                }
                ChannelMsg::ExitStatus { exit_status } => {
                    status_code = exit_status as i32;
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
            (CMD_STATUS.to_string(), TaskArgValue::Number(status_code)),
            (CMD_OUTPUT.to_string(), TaskArgValue::Str(output_str)),
        ]);
        if status_code != 0 {
            let conn_info = (self.host.clone(), self.port);
            warn!("SSHSession Failed execute command. ssh_info={conn_info:?}, {cmd_res:#?}] ");
        }
        channel.close().await?;
        Ok(cmd_res)
    }

    pub async fn execute(&self, command: &str) -> anyhow::Result<(i32, String)> {
        let ret = self
            .command(command, SSHCommandOption::CollectOutput)
            .await?;
        let code = TaskArgValue::into_inner_value::<i32>(ret.get(CMD_STATUS).unwrap().clone());
        let output = TaskArgValue::into_inner_value::<String>(ret.get(CMD_OUTPUT).unwrap().clone())
            .trim()
            .to_owned();
        Ok((code, output))
    }

    pub async fn used_tcp_ports(&self) -> anyhow::Result<Vec<u16>> {
        let output = self
            .execute("ss -tnl | awk 'NR>1 {print $4}' | awk -F':' '{print $NF}'")
            .await?
            .1
            .replace('\n', ",");
        info!("socket {}:{} is already used", self.host, output);
        let used = output
            .split(',')
            .filter_map(|s| match s.parse::<u16>() {
                Ok(port) => Some(port),
                Err(err) => {
                    warn!("can't parse port number {s}: {err}");
                    None
                }
            })
            .unique()
            .collect();
        Ok(used)
    }

    pub async fn parallel(
        key: String,
        user: &str,
        port: usize,
        hosts: Vec<String>,
        content: &str,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let mut joins = vec![];
        hosts.into_iter().for_each(|host| {
            let task_host = TaskHost::Remote {
                user: user.to_owned(),
                port,
                hosts: host.clone(),
            };
            let key_path = key.clone();
            let c = content.to_owned();
            let join = tokio::task::spawn(async move {
                let sess = Self::from_task_host(task_host, key_path).await?;
                let output = sess.execute(&c).await?.1;
                sess.close().await?;
                anyhow::Ok((host, output))
            });
            joins.push(join);
        });
        let mut all_out = vec![];
        for join_res in futures::future::join_all(joins).await {
            let out = join_res??;
            all_out.push(out);
        }
        Ok(all_out)
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        let session = self.session.lock().await;
        session
            .disconnect(Disconnect::ByApplication, "", "English")
            .await?;
        Ok(())
    }
}
