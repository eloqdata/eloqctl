use crate::{StateTypeInto, StateValue, Stateful};
use anyhow::anyhow;
use async_trait::async_trait;
use chrono::Utc;
use sqlx::Error;
use std::any::Any;
use std::fmt::Debug;
use tracing::error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryCondition {
    pub cond_text: String,
    pub bind_values: Vec<StateValue>,
}

#[macro_export]
macro_rules! state_type_into {
    ($({$type_var:ident, $state_type:ty}),*) => {
       $(impl StateTypeInto for $state_type {
           fn type_value(state_type: StateValue) -> Self {
              match state_type {
                 StateValue::$type_var(value) => value,
                 _ => unreachable!(),
              }
           }
        })*
    };
}

state_type_into! {
    {Varchar, String},
    {Integer, i32},
    {Bigint, i64},
    {Timestamp, chrono::DateTime<Utc>}
}

pub trait StateOperationAny: 'static + Send + Sync {
    fn to_any(&self) -> &dyn Any;
}

#[async_trait]
pub trait StateOperation: StateOperationAny {
    type StateObject: Stateful;
    async fn load<F>(&self, cond_supplier: F) -> anyhow::Result<Vec<Self::StateObject>>
    where
        F: Send,
        F: Fn() -> Option<QueryCondition>;
    async fn put(&self, obj: Self::StateObject) -> anyhow::Result<u64>;
}

pub(crate) fn handle_execute_result<T>(result: Result<T, Error>) -> anyhow::Result<T> {
    if let Ok(t) = result {
        Ok(t)
    } else {
        let err_string = result.err().unwrap().to_string();
        error!("Handle execute ResultSet Error. {:?}", err_string);
        Err(anyhow!(err_string))
    }
}

#[macro_export]
macro_rules! state_operation_impl {
    ($({$operation_struct:ident,$entity_name:ty,$select:expr,$upsert:expr} ),*) => {
        use $crate::state::state_base::{StateOperation, StateOperationAny, handle_execute_result,
             QueryCondition};
        use std::any::Any;
        use sqlx::{Sqlite, Pool, QueryBuilder};
        use sqlx::sqlite::SqliteQueryResult;
        $(#[derive(Debug, Clone)]
        pub struct $operation_struct {
            db_instance: Pool<Sqlite>,
        }

        impl $operation_struct {
            pub fn new(db_instance: Pool<Sqlite>) -> Self {
                Self {
                    db_instance
                }
            }

            pub fn boxed(db_instance: Pool<Sqlite>) -> Box<Self> {
                Box::new($operation_struct::new(db_instance))
            }
        }

        impl StateOperationAny for $operation_struct {
            fn to_any(&self) -> &dyn Any {
                self
            }
        }

        #[async_trait::async_trait]
        impl StateOperation for $operation_struct {
            type StateObject = $entity_name;

            async fn load<F>(
                &self,
                cond_supplier: F,
            ) -> anyhow::Result<Vec<$entity_name>> where F: Send, F : Fn()-> Option<QueryCondition> {
                let query_cond = cond_supplier();
                let result_set = if let Some(query_condition) = query_cond {
                    let query_text = format!(r#"{} where {}"#, $select, query_condition.cond_text);
                    let mut query = sqlx::query_as::<Sqlite, $entity_name>(query_text.as_str());
                    for bind_value in query_condition.bind_values.iter()  {
                        query = match bind_value.as_ref() {
                              "String" => {query.bind(StateValue::into_inner_value::<String>(bind_value.clone()))}
                              "i32" => query.bind(StateValue::into_inner_value::<i32>(bind_value.clone())),
                              "i64" => query.bind(StateValue::into_inner_value::<i64>(bind_value.clone())),
                              "chrono::DateTime<Utc>" => query.bind(StateValue::into_inner_value::<chrono::DateTime<Utc>,>(bind_value.clone())),
                              _ => unreachable!(),
                        };
                    }
                    query.fetch_all(&self.db_instance).await
                } else {
                    sqlx::query_as::<Sqlite, $entity_name>($select).fetch_all(&self.db_instance).await
                };
                handle_execute_result::<Vec<$entity_name>>(result_set)
            }

            async fn put(&self, entity: $entity_name) -> anyhow::Result<u64> {
                let bind_values = entity.to_values();
                let mut sql_builder = QueryBuilder::new($upsert[0]);
                let mut separated = sql_builder.separated(",");
                bind_values.iter().for_each(|bind_value| {
                   match bind_value.as_ref() {
                        "String" => {
                            separated.push_bind(StateValue::into_inner_value::<String>(bind_value.clone()))
                        }
                        "i32" => separated.push_bind(StateValue::into_inner_value::<i32>(bind_value.clone())),
                        "i64" => separated.push_bind(StateValue::into_inner_value::<i64>(bind_value.clone())),
                        "chrono::DateTime<Utc>" => separated.push_bind(StateValue::into_inner_value::<chrono::DateTime<Utc>,>(bind_value.clone())),
                        _ => unreachable!(),
                    };
                });
                separated.push_unseparated($upsert[1]);
                let exec_rs = sql_builder.build().execute(&self.db_instance).await;
                let result = handle_execute_result::<SqliteQueryResult>(exec_rs)?;
                Ok(result.rows_affected())
            }
        })*
    };
}

#[cfg(test)]
mod tests {
    use crate::state::state_base;
    use state_base::StateValue;
    use std::convert::AsRef;

    #[test]
    pub fn test_state_type_macro() {
        let varchar_value = StateValue::Varchar("str_value".to_string());
        let str_value = StateValue::into_inner_value::<String>(varchar_value.clone());
        println!(
            "type_value is = {}, {:?}",
            varchar_value.as_ref(),
            str_value
        );
        assert_eq!("str_value", str_value);
    }
}
