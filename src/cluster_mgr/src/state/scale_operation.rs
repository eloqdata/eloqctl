use crate::{state_operation_impl, StateValue, Stateful};
use anyhow::{anyhow, Result};
use chrono::Utc;
use sqlx::FromRow;
use tracing::{error, info};

/// SQL select statement for scale operations
pub const SCALE_SELECT: &str = r#"select event_id, cluster_name, operation_type, nodes_list, is_candidate, stage, error_message, create_timestamp, update_timestamp from t_scale_tx_nodes"#;

/// SQL upsert statements for scale operations
pub const SCALE_UPSERT: [&str; 2] = [
    r#"insert into t_scale_tx_nodes (event_id, cluster_name, operation_type, nodes_list, is_candidate, stage, error_message, create_timestamp, update_timestamp) values ("#,
    r#" )on CONFLICT (event_id) DO UPDATE SET stage = excluded.stage, error_message = excluded.error_message, update_timestamp = excluded.update_timestamp"#,
];

/// SQL delete statement for scale operations
pub const SCALE_DELETE: &str = r#"delete from t_scale_tx_nodes"#;

/// Entity representing a scale operation stored in SQLite
#[derive(Clone, Debug, PartialEq, Eq, FromRow)]
pub struct ScaleEntity {
    pub event_id: String,
    pub cluster_name: String,
    pub operation_type: i32,
    pub nodes_list: String,
    pub is_candidate: Option<String>,
    pub stage: i32,
    pub error_message: Option<String>,
    pub create_timestamp: chrono::DateTime<Utc>,
    pub update_timestamp: chrono::DateTime<Utc>,
}

impl Stateful for ScaleEntity {
    fn to_values(&self) -> Vec<StateValue> {
        vec![
            StateValue::Varchar(self.event_id.clone()),
            StateValue::Varchar(self.cluster_name.clone()),
            StateValue::Integer(self.operation_type),
            StateValue::Varchar(self.nodes_list.clone()),
            StateValue::Varchar(self.is_candidate.clone().unwrap_or_default()),
            StateValue::Integer(self.stage),
            // Default to empty string if no error
            StateValue::Varchar(self.error_message.clone().unwrap_or_default()),
            StateValue::Timestamp(self.create_timestamp.clone()),
            StateValue::Timestamp(self.update_timestamp.clone()),
        ]
    }
}

state_operation_impl! {
    { ScaleOperation, ScaleEntity, SCALE_SELECT, SCALE_UPSERT, SCALE_DELETE }
}

/// Additional methods for ScaleOperation
impl ScaleOperation {
    /// Update the stage field for a scale operation identified by event_id
    pub async fn update_stage(&self, event_id: &str, new_stage: i32) -> Result<()> {
        info!(
            "Updating scale operation stage to {} for event_id: {}",
            new_stage, event_id
        );

        // First, load the existing operation
        let existing_ops = self
            .load(move || -> Option<QueryCondition> {
                Some(QueryCondition {
                    cond_text: "event_id = $1".to_string(),
                    bind_values: vec![StateValue::Varchar(event_id.to_string())],
                })
            })
            .await?;

        if existing_ops.is_empty() {
            error!("No scale operation found with event_id: {}", event_id);
            return Err(anyhow!(
                "No scale operation found with event_id: {}",
                event_id
            ));
        }

        // Get the first (and should be only) operation
        let mut op = existing_ops[0].clone();

        // Update the stage and timestamp
        op.stage = new_stage;
        op.update_timestamp = Utc::now();

        // Use put method which uses SCALE_UPSERT under the hood
        match self.put(op).await {
            Ok(_) => {
                info!(
                    "Successfully updated scale operation stage to {} for event_id: {}",
                    new_stage, event_id
                );
                Ok(())
            }
            Err(e) => {
                error!("Failed to update scale operation stage: {}", e);
                Err(anyhow!("Database error: {}", e))
            }
        }
    }
}
