use crate::cli::HOME_DIR;
use crate::config::config_base::DeployConfig;
use crate::config::CONFIG_PATH_DIR;
use crate::state::cluster_index_operation::{ClusterIndexEntity, ClusterIndexOperation};
use crate::state::deployment_operation::DeploymentOperation;
use crate::state::snapshot_info_operation::{SnapshotEntity, SnapshotOperation};
use crate::state::state_base::{QueryCondition, StateOperation, StateOperationAny};
use crate::state::task_status_operation::{TaskStatusEntity, TaskStatusOperation};
use crate::state::topology_log_operation::{TopologyLogEntity, TopologyLogOperation};
use crate::state::topology_tx_operation::{TopologyTxEntity, TopologyTxOperation};
use crate::StateValue;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
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

pub const DEPLOYMENT_STATE: &str = "Deployment";
pub const CLUSTER_INDEX_STATE: &str = "ClusterIndex";
pub const TASK_STATUS_STATE: &str = "TaskStatus";
pub const SNAPSHOT_STATUS_STATE: &str = "SnapshotStatus";
pub const TOPOLOGY_TX_STATE: &str = "TopologyTx";
pub const TOPOLOGY_LOG_STATE: &str = "TopologyLog";

pub(crate) static CLUSTER_MGR_CLI_DB: &str = "cluster_mgr_state.db";
pub(crate) static ELOQ_CLUSTER_MGR_SCHEMA_PATH: &str = "ELOQ_CLUSTER_MGR_SCHEMA_PATH";

pub static STATE_MGR: LazyLock<StateMgr> = LazyLock::new(|| {
    // CAUTION: block_on inside LazyLock can deadlock if the initialization
    // path triggers another LazyLock that also uses block_on in an async context.
    futures::executor::block_on(async move {
        let config_path = env::var(CONFIG_PATH_DIR);
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
    fn cluster_topology_path(cluster: &str) -> Result<PathBuf> {
        let home = PathBuf::from(env::var(HOME_DIR)?);
        Ok(home.join("clusters").join(cluster).join("topology.yaml"))
    }

    fn read_topology(path: &str) -> Result<DeployConfig> {
        DeployConfig::load(Some(path.to_string()))
    }

    pub async fn save_deployment_config(&self, config: &DeployConfig, upsert: bool) -> Result<()> {
        let cluster = &config.deployment.cluster_name;
        let cluster_index_state =
            self.get_state_operation::<ClusterIndexOperation>(CLUSTER_INDEX_STATE);

        let cluster_index = cluster_index_state
            .load(|| -> Option<QueryCondition> {
                Some(QueryCondition {
                    cond_text: "cluster_name = $1".to_string(),
                    bind_values: vec![StateValue::Varchar(cluster.clone())],
                })
            })
            .await?;
        if !cluster_index.is_empty() && !upsert {
            return Err(anyhow!("cluster {cluster} already exists"));
        }

        let topology_path = Self::cluster_topology_path(cluster)?;
        if let Some(parent) = topology_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&topology_path, config.to_yaml()?)?;

        let now = Utc::now();
        let create_timestamp = cluster_index
            .first()
            .map(|entity| entity.create_timestamp)
            .unwrap_or(now);
        let all_hosts = config.get_unique_host_list().join(";");
        cluster_index_state
            .put(ClusterIndexEntity {
                cluster_name: cluster.clone(),
                topology_path: topology_path.to_string_lossy().to_string(),
                host_list: all_hosts,
                create_timestamp,
                update_timestamp: now,
            })
            .await?;
        Ok(())
    }

    pub async fn load_task_status_from_state(
        &self,
        cluster_name: String,
        status: Option<i32>,
        command: Option<Vec<String>>,
    ) -> Result<Vec<TaskStatusEntity>> {
        let task_state_operation =
            self.get_state_operation::<TaskStatusOperation>(TASK_STATUS_STATE);

        let mut bind_index = 1;
        let mut cond_text = format!("cluster_name=${bind_index} ");
        let mut bind_values = vec![StateValue::Varchar(cluster_name.clone())];

        if let Some(task_status) = status {
            bind_index += 1;
            cond_text.push_str(format!(" and task_status=${bind_index}").as_str());
            bind_values.push(StateValue::Integer(task_status))
        }
        if let Some(cmd_vec) = command {
            cond_text.push_str(" and command in (");
            let cmd_len = cmd_vec.len();
            cmd_vec.iter().enumerate().for_each(|(idx, cmd)| {
                bind_index += 1;
                cond_text.push_str(format!("${bind_index}").as_str());
                if idx < cmd_len - 1 {
                    cond_text.push(',');
                }
                bind_values.push(StateValue::Varchar(cmd.to_string()))
            });
            cond_text.push(')');
        }
        let task_status_entity = task_state_operation
            .load(|| -> Option<QueryCondition> {
                Some(QueryCondition {
                    cond_text: cond_text.clone(),
                    bind_values: bind_values.clone(),
                })
            })
            .await?;
        Ok(task_status_entity)
    }

    pub async fn list_deployments(&self) -> Result<Vec<DeployConfig>> {
        let cluster_index_state =
            self.get_state_operation::<ClusterIndexOperation>(CLUSTER_INDEX_STATE);
        let cluster_index_vec = cluster_index_state
            .load(|| -> Option<QueryCondition> { None })
            .await?;
        cluster_index_vec
            .iter()
            .map(|cluster| Self::read_topology(&cluster.topology_path))
            .collect()
    }

    pub async fn list_cluster_indexes(&self) -> Result<Vec<ClusterIndexEntity>> {
        let cluster_index_state =
            self.get_state_operation::<ClusterIndexOperation>(CLUSTER_INDEX_STATE);
        cluster_index_state
            .load(|| -> Option<QueryCondition> { None })
            .await
    }

    pub async fn list_snapshots(&self, cluster: String) -> Result<Vec<SnapshotEntity>> {
        let snapshot_info_operation =
            self.get_state_operation::<SnapshotOperation>(SNAPSHOT_STATUS_STATE);

        let snapshot_status_entity = snapshot_info_operation
            .load(|| -> Option<QueryCondition> {
                Some(QueryCondition {
                    cond_text: "cluster_name = $1".to_string(),
                    bind_values: vec![StateValue::Varchar(cluster.clone())],
                })
            })
            .await?;
        Ok(snapshot_status_entity)
    }

    pub async fn get_from_snapshot_path(&self, path: String) -> Result<Vec<SnapshotEntity>> {
        let snapshot_info_operation =
            self.get_state_operation::<SnapshotOperation>(SNAPSHOT_STATUS_STATE);

        let snapshot_status_entity = snapshot_info_operation
            .load(|| -> Option<QueryCondition> {
                Some(QueryCondition {
                    cond_text: "snapshot_path  = $1".to_string(),
                    bind_values: vec![StateValue::Varchar(path.clone())],
                })
            })
            .await?;
        Ok(snapshot_status_entity)
    }

    pub async fn get_snapshot_by_ts(
        &self,
        cluster: &str,
        snapshot_ts: DateTime<Utc>,
    ) -> Result<Option<SnapshotEntity>> {
        let snapshot_info_operation =
            self.get_state_operation::<SnapshotOperation>(SNAPSHOT_STATUS_STATE);

        let snapshot_status_entity = snapshot_info_operation
            .load(|| -> Option<QueryCondition> {
                Some(QueryCondition {
                    cond_text: "cluster_name = $1 AND snapshot_ts = $2".to_string(),
                    bind_values: vec![
                        StateValue::Varchar(cluster.to_string()),
                        StateValue::Timestamp(snapshot_ts),
                    ],
                })
            })
            .await?;

        Ok(snapshot_status_entity.first().cloned())
    }

    pub async fn load_deployment_from_state(&self, cluster: &str) -> Result<Option<DeployConfig>> {
        let cluster_index_state =
            self.get_state_operation::<ClusterIndexOperation>(CLUSTER_INDEX_STATE);
        let cluster_index_vec = cluster_index_state
            .load(|| -> Option<QueryCondition> {
                Some(QueryCondition {
                    cond_text: "cluster_name = $1".to_string(),
                    bind_values: vec![StateValue::Varchar(cluster.to_string())],
                })
            })
            .await?;

        if let Some(cluster_index) = cluster_index_vec.first() {
            let config = Self::read_topology(&cluster_index.topology_path)?;
            Ok(Some(config))
        } else {
            Ok(None)
        }
    }

    pub async fn delete_cluster(&self, cluster: &str) -> Result<u64> {
        let cond = QueryCondition {
            cond_text: "cluster_name = $1".to_string(),
            bind_values: vec![StateValue::Varchar(cluster.to_owned())],
        };

        let mut rows = self
            .get_state_operation::<TaskStatusOperation>(TASK_STATUS_STATE)
            .del(|| -> Option<QueryCondition> { Some(cond.clone()) })
            .await?;
        rows += self
            .get_state_operation::<ClusterIndexOperation>(CLUSTER_INDEX_STATE)
            .del(|| -> Option<QueryCondition> { Some(cond.clone()) })
            .await?;
        let topology_path = Self::cluster_topology_path(cluster)?;
        if let Some(cluster_dir) = topology_path.parent() {
            if cluster_dir.exists() {
                fs::remove_dir_all(cluster_dir)?;
            }
        }

        // Delete entries from t_topology_tx
        rows += self
            .get_state_operation::<TopologyTxOperation>(TOPOLOGY_TX_STATE)
            .del(|| -> Option<QueryCondition> { Some(cond.clone()) })
            .await?;

        // Delete entries from t_topology_log
        rows += self
            .get_state_operation::<TopologyLogOperation>(TOPOLOGY_LOG_STATE)
            .del(|| -> Option<QueryCondition> { Some(cond.clone()) })
            .await?;

        Ok(rows)
    }

    pub async fn remove_snapshots(
        &self,
        cluster: &str,
        cutoff_datetime: &Option<DateTime<Utc>>,
    ) -> Result<u64> {
        let mut cond_text = "cluster_name=$1 ".to_string();
        let mut bind_values = vec![StateValue::Varchar(cluster.to_owned())];

        if cutoff_datetime.is_some() {
            cond_text.push_str(" and snapshot_ts<$2".to_string().as_str());
            bind_values.push(StateValue::Timestamp(cutoff_datetime.unwrap()))
        }

        let cond = QueryCondition {
            cond_text: cond_text.clone(),
            bind_values: bind_values.clone(),
        };

        let rows = self
            .get_state_operation::<SnapshotOperation>(SNAPSHOT_STATUS_STATE)
            .del(|| -> Option<QueryCondition> { Some(cond.clone()) })
            .await?;

        Ok(rows)
    }

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

    pub fn get_or_init_db_location() -> Result<PathBuf> {
        let db_location = PathBuf::from(env::var(HOME_DIR).context("HOME_DIR not set")?).join("db");
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
            let exec_rs = sqlx::raw_sql(db_schema.as_str())
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
        let db_url = format!("sqlite://{db_path_string}/{CLUSTER_MGR_CLI_DB}");
        StateMgr::create_schema_if_need(db_url.clone(), schema_path.clone()).await?;
        let connection_options = sqlx::sqlite::SqliteConnectOptions::from_str(db_url.as_str())?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(std::time::Duration::from_secs(30));

        let sqlite_pool = SqlitePoolOptions::new()
            .max_connections(100_u32)
            .connect_with(connection_options)
            .await?;

        let db_schema = StateMgr::load_schema_script(Path::new(&schema_path))?;
        sqlx::raw_sql(db_schema.as_str())
            .execute(&sqlite_pool)
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
        env::set_var(ELOQ_CLUSTER_MGR_SCHEMA_PATH, schema_path.clone());
        let db_conn_pool_rs = StateMgr::db_conn_pool_init(schema_path).await;
        if let Ok(db_conn_pool) = db_conn_pool_rs {
            let deployment_opt_ref = Box::leak(DeploymentOperation::boxed(db_conn_pool.clone()))
                as &dyn StateOperationAny;

            let cluster_index_opt_ref =
                Box::leak(ClusterIndexOperation::boxed(db_conn_pool.clone()))
                    as &dyn StateOperationAny;

            let task_status_opt_ref = Box::leak(TaskStatusOperation::boxed(db_conn_pool.clone()))
                as &dyn StateOperationAny;

            let snapshot_status_opt_ref =
                Box::leak(SnapshotOperation::boxed(db_conn_pool.clone())) as &dyn StateOperationAny;

            let topo_tx_opt_ref = Box::leak(TopologyTxOperation::boxed(db_conn_pool.clone()))
                as &dyn StateOperationAny;
            let topo_log_opt_ref = Box::leak(TopologyLogOperation::boxed(db_conn_pool.clone()))
                as &dyn StateOperationAny;

            let state_map: HashMap<String, Arc<&'static dyn StateOperationAny>> = HashMap::from([
                (DEPLOYMENT_STATE.to_string(), Arc::new(deployment_opt_ref)),
                (
                    CLUSTER_INDEX_STATE.to_string(),
                    Arc::new(cluster_index_opt_ref),
                ),
                (TASK_STATUS_STATE.to_string(), Arc::new(task_status_opt_ref)),
                (
                    SNAPSHOT_STATUS_STATE.to_string(),
                    Arc::new(snapshot_status_opt_ref),
                ),
                (TOPOLOGY_TX_STATE.to_string(), Arc::new(topo_tx_opt_ref)),
                (TOPOLOGY_LOG_STATE.to_string(), Arc::new(topo_log_opt_ref)),
            ]);
            info!("Create StateMgr instance success.");
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

    /// Re-run the SQLite schema script to create any missing tables.
    pub async fn upgrade_schema(&self) -> Result<()> {
        // Load schema path from environment and read script
        let schema_path = env::var(ELOQ_CLUSTER_MGR_SCHEMA_PATH)?;
        let db_schema = StateMgr::load_schema_script(Path::new(&schema_path))?;
        // Execute all statements in the script
        let exec_rs = sqlx::raw_sql(db_schema.as_str())
            .execute(&self.db_pool)
            .await?;
        info!("StateMgr upgrade_schema execute_rs = {:?}", exec_rs);
        self.migrate_legacy_deployments().await?;
        Ok(())
    }

    pub async fn migrate_legacy_deployments(&self) -> Result<usize> {
        let deployment_state = self.get_state_operation::<DeploymentOperation>(DEPLOYMENT_STATE);
        let legacy_deployments = deployment_state
            .load(|| -> Option<QueryCondition> { None })
            .await?;
        let mut migrated = 0;
        for legacy in legacy_deployments {
            let config = DeployConfig::load_from_string(legacy.deployment_config.clone())?;
            self.save_deployment_config(&config, true).await?;
            migrated += 1;
        }
        Ok(migrated)
    }

    /// Load TX topology entries for the given cluster
    pub async fn load_topology_tx_from_state(
        &self,
        cluster_name: String,
    ) -> Result<Vec<TopologyTxEntity>> {
        let topo_tx_op = self.get_state_operation::<TopologyTxOperation>(TOPOLOGY_TX_STATE);
        let entries = topo_tx_op
            .load(|| -> Option<QueryCondition> {
                Some(QueryCondition {
                    cond_text: "cluster_name = $1".to_string(),
                    bind_values: vec![StateValue::Varchar(cluster_name.clone())],
                })
            })
            .await?;
        Ok(entries)
    }

    /// Load log topology entries for the given cluster
    pub async fn load_topology_log_from_state(
        &self,
        cluster_name: String,
    ) -> Result<Vec<TopologyLogEntity>> {
        let topo_log_op = self.get_state_operation::<TopologyLogOperation>(TOPOLOGY_LOG_STATE);
        let entries = topo_log_op
            .load(|| -> Option<QueryCondition> {
                Some(QueryCondition {
                    cond_text: "cluster_name = $1".to_string(),
                    bind_values: vec![StateValue::Varchar(cluster_name.clone())],
                })
            })
            .await?;
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use crate::cli::HOME_DIR;
    use crate::state::cluster_index_operation::ClusterIndexOperation;
    use crate::state::state_base::{QueryCondition, StateOperation};
    use crate::state::state_mgr::{StateMgr, CLUSTER_INDEX_STATE, ELOQ_CLUSTER_MGR_SCHEMA_PATH};
    use sqlx::testing::TestTermination;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{LazyLock, Mutex, MutexGuard};
    use tracing::Level;
    use uuid::Uuid;

    static SETUP: LazyLock<()> = LazyLock::new(|| {
        tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .init();
    });
    static TEST_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn setup() {
        assert!(SETUP.is_success());
    }

    fn test_guard() -> MutexGuard<'static, ()> {
        TEST_MUTEX.lock().expect("test mutex poisoned")
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvVarGuard {
        fn set_path(key: &'static str, value: &std::path::Path) -> Self {
            let previous = std::env::var(key).ok();
            std::env::set_var(key, value);
            Self { key, previous }
        }

        fn set_string(key: &'static str, value: &str) -> Self {
            let previous = std::env::var(key).ok();
            std::env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(prev) = &self.previous {
                std::env::set_var(self.key, prev);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    fn setup_test_home() -> (PathBuf, EnvVarGuard) {
        let home = std::env::temp_dir().join(format!("eloqctl-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&home).expect("create test home");
        let guard = EnvVarGuard::set_path(HOME_DIR, &home);
        (home, guard)
    }

    pub fn schema_path() -> String {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let config_path = manifest_dir.join("config");
        let schema_path = format!("{}/deployment_sqlite.sql", config_path.to_str().unwrap());
        schema_path
    }

    #[allow(clippy::await_holding_lock)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    pub async fn test_db_init() {
        let _guard = test_guard();
        setup();
        let (home, _home_guard) = setup_test_home();
        let schema_path = schema_path();
        println!("schema_path {schema_path:?}");
        let _schema_guard = EnvVarGuard::set_string(ELOQ_CLUSTER_MGR_SCHEMA_PATH, &schema_path);
        let state_mgr = StateMgr::new(schema_path).await;
        assert!(state_mgr.is_ok());
        let all_tables = state_mgr.unwrap().list_tables().await;
        println!("{all_tables:?}");
        assert_eq!(
            all_tables,
            vec![
                "t_cluster_index",
                "t_deployment",
                "t_task_status",
                "t_snapshot_info",
                "t_topology_tx",
                "t_topology_log",
            ]
        );
        fs::remove_dir_all(home).ok();
    }

    #[allow(clippy::await_holding_lock)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    pub async fn test_state_load() {
        let _guard = test_guard();
        setup();
        let (home, _home_guard) = setup_test_home();
        let schema_path = schema_path();
        println!("schema_path {schema_path:?}");
        let _schema_guard = EnvVarGuard::set_string(ELOQ_CLUSTER_MGR_SCHEMA_PATH, &schema_path);
        let state_mgr_rs = StateMgr::new(schema_path).await;
        assert!(state_mgr_rs.is_ok());
        let state_mgr = state_mgr_rs.unwrap();
        let cluster_index_mgr =
            state_mgr.get_state_operation::<ClusterIndexOperation>(CLUSTER_INDEX_STATE);
        let deployment_result = cluster_index_mgr
            .load(|| -> Option<QueryCondition> { None })
            .await;
        assert!(deployment_result.is_ok());
        let deployment_vec = deployment_result.unwrap();
        assert!(deployment_vec.is_empty());
        fs::remove_dir_all(home).ok();
    }
}
