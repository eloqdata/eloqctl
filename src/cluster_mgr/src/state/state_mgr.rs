use crate::cli::config::CONFIG_PATH_DIR;
use crate::state::deployment_operation::DeploymentOperation;
use crate::state::state_base::{StateOperation, StateOperationAny};
use crate::state::task_status_operation::TaskStatusOperation;
use anyhow::{anyhow, Result};
use sqlx::migrate::MigrateDatabase;
use sqlx::sqlite::{SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Pool, QueryBuilder, Sqlite, SqlitePool};
use std::any::Any;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use std::{env, fs};
use tracing::{error, info};

pub static DEPLOYMENT_STATE: &str = "Deployment";
pub static TASK_STATUS_STATE: &str = "TaskStatus";

pub(crate) static CLUSTER_MGR_CLI_DB: &str = "cluster_mgr_state.db";
pub(crate) static MONO_CLUSTER_MGR_SCHEMA_PATH: &str = "MONO_CLUSTER_MGR_SCHEMA_PATH";

pub static STATE_MGR: LazyLock<StateMgr> = LazyLock::new(|| {
    futures::executor::block_on(async move {
        let config_path = std::env::var(CONFIG_PATH_DIR);
        assert!(config_path.is_ok());
        let config_path_str = config_path.unwrap();
        info!("StateMgr init config_path={}", config_path_str);
        // For now only support Sqlite State.
        let schema_path_buf = PathBuf::from(config_path_str.as_str()).join("deployment_sqlite.sql");
        let schema_path = schema_path_buf.to_str().unwrap().to_string();
        let state_mgr = StateMgr::new(schema_path).await;
        info!("StateMgr init success.");
        state_mgr.unwrap()
    })
});

type StateMap = HashMap<String, Arc<&'static dyn StateOperationAny>>;

#[derive(Clone)]
pub struct StateMgr {
    state_map: Arc<StateMap>,
    db_pool: Pool<Sqlite>,
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
pub struct TableName {
    name: String,
}

impl StateMgr {
    #[allow(dead_code)]
    async fn list_tables(&self) -> Vec<String> {
        let table_result = QueryBuilder::new(
            r#"SELECT
               name from sqlite_schema where type ='table'
               AND name NOT LIKE 'sqlite_%'"#,
        )
        .build_query_as::<TableName>()
        .fetch_all(&self.db_pool)
        .await;
        assert!(table_result.is_ok());
        table_result
            .unwrap()
            .into_iter()
            .filter(|table_name| !table_name.name.is_empty())
            .map(|table_name| table_name.name)
            .collect::<Vec<String>>()
    }

    fn get_or_init_db_location() -> Result<PathBuf> {
        let db_location = PathBuf::from("./")
            .join(".data")
            .join("mono_cluster_mgr_cli");
        info!(
            "StateMgr get_or_init_db_location db_location = {:?}",
            db_location
        );
        if !db_location.exists() {
            info!("StateMgr db_location not exists create it.");
            let create_db_location_rs = fs::create_dir_all(db_location.as_path());
            if create_db_location_rs.is_err() {
                let db_location_create_err = create_db_location_rs.err().unwrap();
                error!(
                    "StateMgr db_location create db_location error. {:?}",
                    db_location_create_err.to_string()
                );
                return Err(anyhow!(
                    "StateMgr create data_location error {:?} {:?}",
                    db_location,
                    db_location_create_err.to_string()
                ));
            }
        }
        info!("StateMgr db_location exists do nothing");
        Ok(db_location)
    }

    async fn create_schema_if_need(db_url: String, schema_path: String) -> Result<()> {
        info!(
            "StatMgr create_schema_if_need {} {}",
            db_url.clone(),
            schema_path.clone()
        );
        if !Sqlite::database_exists(db_url.as_str())
            .await
            .unwrap_or(false)
        {
            info!(
                "StateMgr found database_url={} not exists create it",
                db_url.clone()
            );
            Sqlite::create_database(db_url.as_str()).await?;
            let instance_pool = SqlitePool::connect(db_url.as_str()).await?;
            let db_schema = StateMgr::load_schema_script(Path::new(schema_path.as_str()))?;
            let exec_rs = sqlx::query(db_schema.as_str())
                .execute(&instance_pool)
                .await?;
            info!("StateMgr create_schema execute_rs = {:?}", exec_rs);
            Ok(())
        } else {
            info!("StateMgr found database exists do nothing.");
            Ok(())
        }
    }

    fn load_schema_script(schema_file_path: &Path) -> Result<String> {
        let content = fs::read_to_string(schema_file_path)?;
        Ok(content)
    }

    pub fn get_state_operation<T: StateOperation>(&self, state_value_key: &str) -> Arc<&T> {
        assert!(self.state_map.contains_key(state_value_key));
        let state_operation = self.state_map.get(state_value_key).unwrap();
        let any_ref: &dyn Any = state_operation.to_any();
        match any_ref.downcast_ref::<T>() {
            Some(state) => Arc::new(state),
            None => panic!("can't match any StateOperation"),
        }
    }

    pub async fn db_conn_pool_init(schema_path: String) -> Result<Pool<Sqlite>> {
        let db_path = StateMgr::get_or_init_db_location()?;
        let db_path_string = db_path.as_path().to_str().unwrap().to_string();
        let db_url = format!("sqlite://{}/{}", db_path_string, CLUSTER_MGR_CLI_DB);
        StateMgr::create_schema_if_need(db_url.clone(), schema_path).await?;
        let connection_options = sqlx::sqlite::SqliteConnectOptions::from_str(db_url.as_str())?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(std::time::Duration::from_secs(30));

        let sqlite_pool = SqlitePoolOptions::new()
            .max_connections(100_u32)
            .connect_with(connection_options)
            .await?;

        sqlx::query("pragma temp_store = memory;")
            .execute(&sqlite_pool)
            .await?;
        sqlx::query("pragma page_size = 4096;")
            .execute(&sqlite_pool)
            .await?;
        info!("StateMgr init sqlite conn pool success.");
        Ok(sqlite_pool)
    }

    pub async fn new(schema_path: String) -> Result<Self> {
        info!("StateMgr init schema_path={}", schema_path);
        env::set_var(MONO_CLUSTER_MGR_SCHEMA_PATH, schema_path.clone());
        let db_conn_pool_rs = StateMgr::db_conn_pool_init(schema_path).await;
        if let Ok(db_conn_pool) = db_conn_pool_rs {
            let deployment_opt_ref = Box::leak(DeploymentOperation::boxed(db_conn_pool.clone()))
                as &dyn StateOperationAny;

            let task_status_opt_ref = Box::leak(TaskStatusOperation::boxed(db_conn_pool.clone()))
                as &dyn StateOperationAny;

            let state_map: HashMap<String, Arc<&'static dyn StateOperationAny>> = HashMap::from([
                (DEPLOYMENT_STATE.to_string(), Arc::new(deployment_opt_ref)),
                (TASK_STATUS_STATE.to_string(), Arc::new(task_status_opt_ref)),
            ]);
            info!("StateMgr init success.");
            Ok(Self {
                state_map: Arc::new(state_map),
                db_pool: db_conn_pool,
            })
        } else {
            let init_err = db_conn_pool_rs.err().unwrap().to_string();
            error!("StateMgr init failure. cause by {}", init_err);
            Err(anyhow::format_err!(init_err))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::state::deployment_operation::DeploymentOperation;
    use crate::state::state_base::{QueryCondition, StateOperation};
    use crate::state::state_mgr::{StateMgr, DEPLOYMENT_STATE, MONO_CLUSTER_MGR_SCHEMA_PATH};
    use sqlx::testing::TestTermination;
    use std::path::PathBuf;
    use std::sync::LazyLock;
    use tracing::Level;

    static SETUP: LazyLock<()> = LazyLock::new(|| {
        tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .init();
    });

    fn setup() {
        assert!(SETUP.is_success());
    }

    pub fn schema_path() -> String {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let config_path = manifest_dir.join("config");
        let schema_path = format!("{}/deployment_sqlite.sql", config_path.to_str().unwrap());
        schema_path
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    pub async fn test_db_init() {
        setup();
        let schema_path = schema_path();
        println!("schema_path {:?}", schema_path);
        std::env::set_var(MONO_CLUSTER_MGR_SCHEMA_PATH, schema_path.clone());
        let state_mgr = StateMgr::new(schema_path).await;
        assert!(state_mgr.is_ok());
        let all_tables = state_mgr.unwrap().list_tables().await;
        println!("{:?}", all_tables);
        assert_eq!(
            all_tables,
            vec![
                "t_deployment",
                "t_task_status",
                "t_service_instance",
                "t_service_config"
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    pub async fn test_state_load() {
        setup();
        let schema_path = schema_path();
        println!("schema_path {:?}", schema_path);
        std::env::set_var(MONO_CLUSTER_MGR_SCHEMA_PATH, schema_path.clone());
        let state_mgr_rs = StateMgr::new(schema_path).await;
        assert!(state_mgr_rs.is_ok());
        let state_mgr = state_mgr_rs.unwrap();
        let deployment_mgr = state_mgr.get_state_operation::<DeploymentOperation>(DEPLOYMENT_STATE);
        let deployment_result = deployment_mgr
            .load(|| -> Option<QueryCondition> { None })
            .await;
        assert!(deployment_result.is_ok());
        let deployment_vec = deployment_result.unwrap();
        assert!(deployment_vec.is_empty());
    }
}
