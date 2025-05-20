use crate::{state_operation_impl, StateValue, Stateful};
use chrono::{DateTime, Utc};
use sqlx::FromRow;

pub(crate) const TOPOLOGY_TX_SELECT: &str = r#"select
    cluster_name,
    node_group_count,
    node_group_id,
    node_id,
    role,
    host,
    port,
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
        create_timestamp,
        update_timestamp
    ) values("#,
    r#") on conflict(cluster_name, host, port) do update set
        node_group_count=excluded.node_group_count,
        node_group_id=excluded.node_group_id,
        node_id=excluded.node_id,
        role=excluded.role,
        update_timestamp=excluded.update_timestamp
    "#,
];

pub(crate) const TOPOLOGY_TX_DELETE: &str = r#"delete from t_topology_tx"#;

#[derive(Debug, Clone, FromRow)]
pub struct TopologyTxEntity {
    pub cluster_name: String,
    pub node_group_count: i32,
    pub node_group_id: i32,
    pub node_id: String,
    pub role: i32,
    pub host: String,
    pub port: i32,
    pub create_timestamp: DateTime<Utc>,
    pub update_timestamp: DateTime<Utc>,
}

impl Stateful for TopologyTxEntity {
    fn to_values(&self) -> Vec<StateValue> {
        vec![
            StateValue::Varchar(self.cluster_name.clone()),
            StateValue::Integer(self.node_group_count),
            StateValue::Integer(self.node_group_id),
            StateValue::Varchar(self.node_id.clone()),
            StateValue::Integer(self.role),
            StateValue::Varchar(self.host.clone()),
            StateValue::Integer(self.port),
            StateValue::Timestamp(self.create_timestamp),
            StateValue::Timestamp(self.update_timestamp),
        ]
    }
}

state_operation_impl! {
    {TopologyTxOperation, TopologyTxEntity, TOPOLOGY_TX_SELECT, TOPOLOGY_TX_UPDATE, TOPOLOGY_TX_DELETE}
}
