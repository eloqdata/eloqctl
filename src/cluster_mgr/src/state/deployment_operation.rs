use crate::{state_operation_impl, StateValue, Stateful};
use chrono::Utc;
use sqlx::FromRow;

pub(crate) const DEPLOYMENT_SELECT: &str = r#"select cluster_name, deployment_config, host_list,
                                          create_timestamp, update_timestamp
                                    from  t_deployment"#;

pub(crate) const DEPLOYMENT_UPSERT: [&str; 2] = [
    r#"insert into t_deployment (cluster_name, deployment_config, host_list, create_timestamp, update_timestamp) values ("#,
    r#" )on CONFLICT (cluster_name) DO UPDATE SET deployment_config = excluded.deployment_config,host_list=excluded.host_list"#,
];

#[derive(Eq, PartialEq, Clone, Debug, FromRow)]
pub struct DeploymentEntity {
    pub cluster_name: String,
    pub deployment_config: String,
    pub host_list: String,
    pub create_timestamp: chrono::DateTime<Utc>,
    pub update_timestamp: chrono::DateTime<Utc>,
}

impl Stateful for DeploymentEntity {
    fn to_values(&self) -> Vec<StateValue> {
        let self_cloned = self.clone();
        vec![
            StateValue::Varchar(self_cloned.cluster_name),
            StateValue::Varchar(self_cloned.deployment_config),
            StateValue::Varchar(self_cloned.host_list),
            StateValue::Timestamp(self_cloned.create_timestamp),
            StateValue::Timestamp(self_cloned.update_timestamp),
        ]
    }
}

state_operation_impl! {
    { DeploymentOperation, DeploymentEntity, DEPLOYMENT_SELECT, DEPLOYMENT_UPSERT }
}
