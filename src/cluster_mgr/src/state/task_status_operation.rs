use crate::{state_operation_impl, StateValue, Stateful};
use chrono::Utc;
use sqlx::FromRow;

pub(crate) const TASK_STATUS_SELECT: &str = r#"select cluster_name,task,command,task_host,task_status,
                                           create_timestamp,update_timestamp
                                    from t_task_status "#;

pub(crate) const TASK_STATUS_UPSERT: [&str; 2] = [
    r#"insert into t_task_status(cluster_name,task,command,task_host,task_status,create_timestamp,update_timestamp) values( "#,
    r#" )on CONFLICT (cluster_name, task, command, task_host) DO UPDATE SET task_status = excluded.task_status"#,
];

#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct TaskStatusEntity {
    pub cluster_name: String,
    pub task: String,
    pub command: String,
    pub task_host: String,
    pub task_status: u16,
    pub create_timestamp: chrono::DateTime<Utc>,
    pub update_timestamp: chrono::DateTime<Utc>,
}

impl Stateful for TaskStatusEntity {
    fn to_values(&self) -> Vec<StateValue> {
        let self_cloned = self.clone();
        vec![
            StateValue::Varchar(self_cloned.cluster_name),
            StateValue::Varchar(self_cloned.task),
            StateValue::Varchar(self_cloned.command),
            StateValue::Varchar(self_cloned.task_host),
            StateValue::Bigint(self_cloned.task_status as i64),
            StateValue::Timestamp(self_cloned.create_timestamp),
            StateValue::Timestamp(self_cloned.update_timestamp),
        ]
    }
}

state_operation_impl! {
    { TaskStatusOperation, TaskStatusEntity, TASK_STATUS_SELECT, TASK_STATUS_UPSERT }
}
