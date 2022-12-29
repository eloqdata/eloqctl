use crate::cli::config::Auth;
use crate::cli::task::task_base::{CmdErr, ExecutionValue, TaskArgValue, TaskHost};
use anyhow::anyhow;
use ssh2::{Channel, Session};
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::io::Read;
use std::net::TcpStream;
use std::path::Path;
use tracing::{error, info};

pub(crate) const SSH_EXEC_CMD: &str = "_SSH_EXEC_CMD_";
pub(crate) const SSH_EXEC_CMD_OUTPUT: &str = "_SSH_EXEC_CMD_OUTPUT_";
pub(crate) const SSH_EXEC_CMD_STATUS: &str = "_SSH_EXEC_CMD_STATUS_";
pub(crate) const SSH_CHECK_PROCESS_PID: &str = "_SSH_CHECK_PROCESS_PID_";

#[derive(Clone, Debug)]
pub enum SSHAuth {
    Password(String),
    KeyPair(String),
}

impl SSHAuth {
    pub fn build_from_connection_auth(conn_auth: Auth) -> SSHAuth {
        if let Some(pwd) = conn_auth.password {
            SSHAuth::Password(pwd)
        } else {
            SSHAuth::KeyPair(conn_auth.keypair.unwrap())
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum RemoteCmdOutput {
    None,
    Sync,
    Stream,
}

#[derive(Clone)]
pub struct SSHConn {
    session: Session,
}

#[macro_export]
macro_rules! ssh_conn_info {
    ($conn_auth:expr, $remote_task_host:expr, $ssh_conn:ident,$conn_user_var:ident, $conn_host_var:ident) => {
        let conn_auth_conf = $conn_auth.auth;
        let remote_host = $remote_task_host;
        let ($conn_user_var, _port, $conn_host_var) = remote_host.ssh_conn_tuple();
        let $ssh_conn = $crate::cli::task::ssh_conn::SSHConn::build_from_remote_host(
            remote_host,
            conn_auth_conf,
        );
    };
}

impl SSHConn {
    pub fn build_from_remote_host(
        remote_host: TaskHost,
        conn_auth_conf: Auth,
    ) -> anyhow::Result<Self> {
        Ok(
            if let TaskHost::Remote { user, port, hosts } = remote_host {
                SSHConn::new(
                    SSHAuth::build_from_connection_auth(conn_auth_conf),
                    format!("{}:{}", hosts, port),
                    user,
                )?
            } else {
                unreachable!()
            },
        )
    }

    pub fn new(
        ssh_auth: SSHAuth,
        host_and_port: String,
        conn_user: String,
    ) -> anyhow::Result<Self> {
        let tcp_conn = TcpStream::connect(host_and_port.clone());
        if tcp_conn.is_err() {
            let conn_err = tcp_conn.err().unwrap().to_string();
            error!(
                "SSHConn connect to remote host [{}] error. cause by {}",
                host_and_port, conn_err
            );
            return Err(anyhow!(CmdErr::SSHConnErr(
                format!("{}@{}", conn_user.as_str(), host_and_port),
                conn_err
            )));
        }
        let session_rs = Session::new();
        if session_rs.is_err() {
            error!(
                "SSHConn failed to establish ssh connection remote_host:{:?}",
                host_and_port
            );
            return Err(anyhow!(CmdErr::SSHConnErr(
                format!("{}@{}", conn_user.as_str(), host_and_port),
                session_rs.err().unwrap().to_string()
            )));
        }
        let mut session = session_rs.unwrap();
        session.set_tcp_stream(tcp_conn.unwrap());
        let handshake_rs = session.handshake();
        if handshake_rs.is_err() {
            let handshake_err = handshake_rs.err().unwrap();
            error!(
                "SSHConn handshake error cause by {:?}",
                handshake_err.code()
            );
            let ssh_err = CmdErr::SSHConnErr(
                format!("{}@{}", conn_user.as_str(), host_and_port),
                handshake_err.to_string(),
            );
            panic!("{}", ssh_err.to_string().as_str());
            // return Err(anyhow!(CmdErr::SSHConnErr(
            //     format!("{}@{}", conn_user.as_str(), host_and_port),
            //     handshake_err.to_string()
            // )));
        }

        let auth_rs = SSHConn::ssh_auth(&mut session, conn_user.clone(), ssh_auth);
        if auth_rs.is_err() {
            return Err(anyhow!(CmdErr::SSHConnErr(
                format!("{}@{}", conn_user.as_str(), host_and_port),
                auth_rs.err().unwrap().to_string()
            )));
        }
        Ok(Self {
            session: session.clone(),
        })
    }

    fn ssh_auth(session: &mut Session, conn_user: String, ssh_auth: SSHAuth) -> anyhow::Result<()> {
        let auth_rs = match ssh_auth.clone() {
            SSHAuth::Password(password) => {
                session.userauth_password(conn_user.as_str(), password.as_str())
            }
            SSHAuth::KeyPair(private_key) => session.userauth_pubkey_file(
                conn_user.as_str(),
                None,
                Path::new(private_key.as_str()),
                None,
            ),
        };
        if auth_rs.is_err() {
            return Err(anyhow!(auth_rs.err().unwrap().to_string()));
        }
        if session.authenticated() {
            // info!("SSHConn Authentication success. {:?}", ssh_auth);
            Ok(())
        } else {
            let auth_err = anyhow!(format!(
                "SSHConn auth error ssh_auth {:?}, conn_user={}",
                ssh_auth, conn_user
            ));
            error!(
                "SSHConn Authentication failure cause by {}",
                auth_err.to_string()
            );
            Err(auth_err)
        }
    }

    fn run_cmd_with_env(
        &self,
        cmd: String,
        cmd_output: RemoteCmdOutput,
        env: Option<HashMap<String, String>>,
    ) -> anyhow::Result<ExecutionValue> {
        let session_channel_rs = self.session.channel_session();
        if session_channel_rs.is_err() {
            let new_channel_err = session_channel_rs.err().unwrap();
            let err = new_channel_err.to_string();
            error!(
                "SSH run_cmd new session channel error {:?}, cmd = {}",
                err, cmd
            );
            return Err(anyhow!(CmdErr::SSHRemoteCmdErr(cmd, err)));
        }
        let mut channel = session_channel_rs.unwrap();
        if let Some(entry) = env {
            for (key, val) in entry.iter() {
                info!(
                    "SSH set env {}={} to remote host",
                    key.as_str(),
                    val.as_str()
                );
                let set_env = channel.setenv(key.as_str(), val.as_str());
                if set_env.is_err() {
                    let set_env_err = set_env.err().unwrap().to_string();
                    error!(
                        "SSH run_cmd channel set_env error={:?}, cmd = {}",
                        set_env_err, cmd
                    );
                    return Err(anyhow!(CmdErr::SSHRemoteCmdErr(cmd, set_env_err)));
                }
            }
        }

        let exec_rs = channel.exec(cmd.as_str());
        if exec_rs.is_err() {
            let exec_rs_err = exec_rs.err().unwrap();
            let run_cmd_err = exec_rs_err.to_string();
            error!(
                "SSH run_cmd exec cmd  error {:?}, cmd = {}",
                run_cmd_err, cmd
            );
            return Err(anyhow!(CmdErr::SSHRemoteCmdErr(cmd, run_cmd_err)));
        }
        let cmd_output = match cmd_output {
            RemoteCmdOutput::Stream | RemoteCmdOutput::None => {
                self.read_output_to_buf(channel.borrow_mut(), cmd_output)
            }
            _ => {
                let mut output = Vec::new();
                channel.read_to_end(&mut output)?;
                String::from_utf8(output)?
            }
        };

        let wait_close_rs = channel.wait_close();
        if wait_close_rs.is_err() {
            let wait_close_err = wait_close_rs.err().unwrap().to_string();
            error!(
                "SSH run_cmd wait_close_rs  error {:?}, cmd = {}",
                wait_close_err, cmd
            );
            return Err(anyhow!(wait_close_err));
        }
        let exec_status_rs = channel.exit_status();
        if exec_status_rs.is_err() {
            return Err(anyhow!(CmdErr::SSHRemoteCmdErr(
                cmd,
                exec_status_rs.err().unwrap().to_string()
            )));
        }
        let mut cmd_exec_rtn = HashMap::new();
        cmd_exec_rtn.insert(SSH_EXEC_CMD.to_string(), TaskArgValue::Str(cmd));
        cmd_exec_rtn.insert(
            SSH_EXEC_CMD_STATUS.to_string(),
            TaskArgValue::Number(exec_status_rs.unwrap() as usize),
        );
        cmd_exec_rtn.insert(
            SSH_EXEC_CMD_OUTPUT.to_string(),
            TaskArgValue::Str(cmd_output),
        );
        Ok(cmd_exec_rtn)
    }

    pub fn read_output_to_buf(
        &self,
        channel: &mut Channel,
        output_enum: RemoteCmdOutput,
    ) -> String {
        let mut cmd_output = String::new();
        let mut buffer = [0; 512];
        loop {
            let read_len_rs = channel.read(&mut buffer[..]);
            if read_len_rs.is_err() {
                break;
            }
            let read_len = read_len_rs.unwrap();
            if read_len == 0 {
                break;
            }
            if output_enum != RemoteCmdOutput::None {
                let output = String::from_utf8(buffer[0..read_len].to_vec()).unwrap();
                println!("{}", output);
                cmd_output.push_str(String::from_utf8(buffer.to_vec()).unwrap().as_str());
            }
        }
        cmd_output.clone()
    }

    pub fn run_cmd(&self, cmd: String, collect_output: bool) -> anyhow::Result<ExecutionValue> {
        let cmd_output = if collect_output {
            RemoteCmdOutput::Stream
        } else {
            RemoteCmdOutput::None
        };
        self.run_cmd_with_env(cmd, cmd_output, None)
    }

    pub fn run_cmd_sync_output(&self, cmd: String) -> anyhow::Result<ExecutionValue> {
        self.run_cmd_with_env(cmd, RemoteCmdOutput::Sync, None)
    }
}
