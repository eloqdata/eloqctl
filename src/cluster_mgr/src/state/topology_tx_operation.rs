use crate::{
    cli::task::task_utils::{NodeGroupId, NodeId},
    state_operation_impl, StateValue, Stateful,
};
use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{from_str, to_string};
use sqlx::{sqlite::SqliteRow, FromRow, Row};

pub(crate) const TOPOLOGY_TX_SELECT: &str = r#"select
    cluster_name,
    node_group_count,
    node_group_id,
    node_id,
    role,
    host,
    port,
    ini_config,
    create_timestamp,
    update_timestamp
from t_topology_tx"#;

pub(crate) const TOPOLOGY_TX_UPDATE: [&str; 2] = [
    r#"insert into t_topology_tx(
        cluster_name,
        node_group_count,
        node_group_id,
        node_id,
        role,
        host,
        port,
        ini_config,
        create_timestamp,
        update_timestamp
    ) values("#,
    r#") on conflict(cluster_name, node_group_id, host, port) do update set
        node_group_count=excluded.node_group_count,
        node_id=excluded.node_id,
        role=excluded.role,
        ini_config=excluded.ini_config,
        update_timestamp=excluded.update_timestamp
    "#,
];

pub(crate) const TOPOLOGY_TX_DELETE: &str = r#"delete from t_topology_tx"#;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigJson {
    pub eloq_data_path: String,
    pub enable_data_store: bool,
    pub enable_wal: bool,
    #[serde(default)]
    pub enable_io_uring: bool,
    pub checkpoint_interval: Option<u32>,
    pub enable_cache_replacement: Option<bool>,
    #[serde(default)]
    pub additional_settings: std::collections::HashMap<String, String>,
}

impl From<ConfigJson> for String {
    fn from(config: ConfigJson) -> Self {
        to_string(&config).unwrap_or_default()
    }
}

impl TryFrom<&str> for ConfigJson {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        from_str(value).context("Failed to parse ConfigJson from string")
    }
}

#[derive(Debug, Clone)]
pub struct TopologyTxEntity {
    pub cluster_name: String,
    pub node_group_count: u32,
    pub node_group_id: NodeGroupId,
    pub node_id: NodeId,
    pub role: i32,
    pub host: String,
    pub port: u16,
    pub ini_config: ConfigJson,
    pub create_timestamp: DateTime<Utc>,
    pub update_timestamp: DateTime<Utc>,
}

impl<'r> FromRow<'r, SqliteRow> for TopologyTxEntity {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        let ini_config_str: String = row.try_get("ini_config")?;
        let ini_config = match from_str::<ConfigJson>(&ini_config_str) {
            Ok(config) => config,
            Err(e) => {
                return Err(sqlx::Error::ColumnDecode {
                    index: "ini_config".to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to parse ConfigJson: {}", e),
                    )),
                });
            }
        };

        Ok(TopologyTxEntity {
            cluster_name: row.try_get("cluster_name")?,
            node_group_count: row.try_get("node_group_count")?,
            node_group_id: row.try_get("node_group_id")?,
            node_id: row.try_get("node_id")?,
            role: row.try_get("role")?,
            host: row.try_get("host")?,
            port: row.try_get("port")?,
            ini_config,
            create_timestamp: row.try_get("create_timestamp")?,
            update_timestamp: row.try_get("update_timestamp")?,
        })
    }
}

impl Stateful for TopologyTxEntity {
    fn to_values(&self) -> Vec<StateValue> {
        let ini_config_str: String = self.ini_config.clone().into();

        vec![
            StateValue::Varchar(self.cluster_name.clone()),
            StateValue::Integer(self.node_group_count as i32),
            StateValue::Integer(self.node_group_id as i32),
            StateValue::Integer(self.node_id as i32),
            StateValue::Integer(self.role),
            StateValue::Varchar(self.host.clone()),
            StateValue::Integer(self.port as i32),
            StateValue::Varchar(ini_config_str),
            StateValue::Timestamp(self.create_timestamp),
            StateValue::Timestamp(self.update_timestamp),
        ]
    }
}

state_operation_impl! {
    {TopologyTxOperation, TopologyTxEntity, TOPOLOGY_TX_SELECT, TOPOLOGY_TX_UPDATE, TOPOLOGY_TX_DELETE}
}
