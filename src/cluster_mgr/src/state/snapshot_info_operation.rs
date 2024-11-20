use crate::{state_operation_impl, StateValue, Stateful};
use chrono::Utc;
use sqlx::FromRow;

pub(crate) const SNAPSHOT_INFO_SELECT: &str = r#"select cluster_name,snapshot_ts,snapshot_status,snapshot_path,dest_host,dest_user from t_snapshot_info "#;

pub(crate) const SNAPSHOT_INFO_UPSERT: [&str; 2] = [
    r#"insert into t_snapshot_info(cluster_name,snapshot_ts,snapshot_status,snapshot_path,dest_host,dest_user) values( "#,
    r#" )on CONFLICT (cluster_name,snapshot_ts) DO UPDATE SET snapshot_status = excluded.snapshot_status"#,
];

pub(crate) const SNAPSHOT_INFO_DELETE: &str = r#"delete from t_snapshot_info"#;

#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct SnapshotEntity {
    pub cluster_name: String,
    pub snapshot_ts: chrono::DateTime<Utc>,
    pub snapshot_status: i64,
    pub snapshot_path: String,
    pub dest_host: String,
    pub dest_user: String,
}

impl Stateful for SnapshotEntity {
    fn to_values(&self) -> Vec<StateValue> {
        let self_cloned = self.clone();
        vec![
            StateValue::Varchar(self_cloned.cluster_name),
            StateValue::Timestamp(self_cloned.snapshot_ts),
            StateValue::Bigint(self_cloned.snapshot_status as i64),
            StateValue::Varchar(self_cloned.snapshot_path),
            StateValue::Varchar(self_cloned.dest_host),
            StateValue::Varchar(self_cloned.dest_user),
        ]
    }
}

state_operation_impl! {
    { SnapshotOperation, SnapshotEntity, SNAPSHOT_INFO_SELECT, SNAPSHOT_INFO_UPSERT ,SNAPSHOT_INFO_DELETE}
}
