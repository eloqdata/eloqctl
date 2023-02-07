use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Connection {
    pub username: String,
    pub auth_type: String,
    pub auth: Auth,
    pub port: Option<u16>,
}

impl Connection {
    pub fn ssh_port(&self) -> u16 {
        if let Some(ssh_port) = self.port {
            ssh_port
        } else {
            22_u16
        }
    }

    pub fn ssh_auth_key(&self) -> Option<String> {
        self.auth.clone().keypair
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Auth {
    pub password: Option<String>,
    pub keypair: Option<String>,
}
