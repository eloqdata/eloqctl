use crate::{state_operation_impl, StateValue, Stateful};
use chrono::Utc;
use sqlx::FromRow;

pub(crate) const PROXY_SELECT: &str = r#"select proxy_name, proxy_config, proxy_host_list,
                                          create_timestamp, update_timestamp
                                    from  t_proxy"#;

pub(crate) const PROXY_UPSERT: [&str; 2] = [
    r#"insert into t_proxy (proxy_name, proxy_config, proxy_host_list, create_timestamp, update_timestamp) values ("#,
    r#" )on CONFLICT (proxy_name) DO UPDATE SET proxy_config = excluded.proxy_config, proxy_host_list = excluded.proxy_host_list"#,
];

pub(crate) const PROXY_DELETE: &str = r#"delete from t_proxy"#;

#[derive(Eq, PartialEq, Clone, Debug, FromRow)]
pub struct ProxyEntity {
    pub proxy_name: String,
    pub proxy_config: String,
    pub proxy_host_list: String,
    pub create_timestamp: chrono::DateTime<Utc>,
    pub update_timestamp: chrono::DateTime<Utc>,
}

impl Stateful for ProxyEntity {
    fn to_values(&self) -> Vec<StateValue> {
        let self_cloned = self.clone();
        vec![
            StateValue::Varchar(self_cloned.proxy_name),
            StateValue::Varchar(self_cloned.proxy_config),
            StateValue::Varchar(self_cloned.proxy_host_list),
            StateValue::Timestamp(self_cloned.create_timestamp),
            StateValue::Timestamp(self_cloned.update_timestamp),
        ]
    }
}

state_operation_impl! {
    { ProxyOperation, ProxyEntity, PROXY_SELECT, PROXY_UPSERT, PROXY_DELETE }
}
