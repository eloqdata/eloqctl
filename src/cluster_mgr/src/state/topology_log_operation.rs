use crate::{state_operation_impl, StateValue, Stateful};
use chrono::{DateTime, Utc};
use sqlx::FromRow;

pub(crate) const TOPOLOGY_LOG_SELECT: &str = r#"select
    cluster_name,
    node_group_count,
    node_group_id,
    node_id,
    host,
    port,
    data_dirs,
    create_timestamp,
    update_timestamp
from t_topology_log"#;

pub(crate) const TOPOLOGY_LOG_UPDATE: [&str; 2] = [
    r#"insert into t_topology_log(
        cluster_name,
        node_group_count,
        node_group_id,
        node_id,
        host,
        port,
        data_dirs,
        create_timestamp,
        update_timestamp
    ) values("#,
    r#") on conflict(cluster_name, host, port) do update set
        node_group_count=excluded.node_group_count,
        node_group_id=excluded.node_group_id,
        node_id=excluded.node_id,
        data_dirs=excluded.data_dirs,
        update_timestamp=excluded.update_timestamp
    "#,
];

pub(crate) const TOPOLOGY_LOG_DELETE: &str = r#"delete from t_topology_log"#;

#[derive(Debug, Clone, FromRow)]
pub struct TopologyLogEntity {
    pub cluster_name: String,
    pub node_group_count: i32,
    pub node_group_id: i32,
    pub node_id: String,
    pub host: String,
    pub port: i32,
    pub data_dirs: Option<String>,
    pub create_timestamp: DateTime<Utc>,
    pub update_timestamp: DateTime<Utc>,
}

impl Stateful for TopologyLogEntity {
    fn to_values(&self) -> Vec<StateValue> {
        vec![
            StateValue::Varchar(self.cluster_name.clone()),
            StateValue::Integer(self.node_group_count),
            StateValue::Integer(self.node_group_id),
            StateValue::Varchar(self.node_id.clone()),
            StateValue::Varchar(self.host.clone()),
            StateValue::Integer(self.port),
            match &self.data_dirs {
                Some(d) => StateValue::Varchar(d.clone()),
                None => StateValue::Varchar("".to_string()),
            },
            StateValue::Timestamp(self.create_timestamp),
            StateValue::Timestamp(self.update_timestamp),
        ]
    }
}

state_operation_impl! {
    {TopologyLogOperation, TopologyLogEntity, TOPOLOGY_LOG_SELECT, TOPOLOGY_LOG_UPDATE, TOPOLOGY_LOG_DELETE}
}
