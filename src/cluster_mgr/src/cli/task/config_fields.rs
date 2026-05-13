use once_cell::sync::Lazy;
/// Configuration field definitions for the MonographDB system.
///
/// This file defines the available configuration fields that can be updated
/// using the `eloqctl update-conf` command, along with metadata about their
/// update scope (node-specific or cluster-wide) and other properties.
use std::collections::HashMap;

/// Represents the scope of a configuration field update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldScope {
    /// Field can be updated on a specific node without affecting others
    NodeSpecific,

    /// Field must be updated across all nodes for consistency
    ClusterWide,
}

/// Holds metadata about a configuration field
#[derive(Debug, Clone)]
pub struct FieldMetadata {
    /// Description of the field's purpose
    pub description: &'static str,

    /// The update scope (node-specific or cluster-wide)
    pub scope: FieldScope,

    /// Example valid value
    pub example: &'static str,

    /// Value type (for validation)
    pub value_type: FieldValueType,

    /// Default value if not specified
    pub default_value: &'static str,
}

/// Represents the type of a configuration field value
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldValueType {
    /// String value (path, name, etc.)
    String,

    /// Boolean value (true/false)
    Boolean,

    /// Integer value
    Integer,
}

/// Registry of all available configuration fields with their metadata.
///
/// This allows the update-conf command to validate field names and properly
/// determine which fields require cluster-wide updates.
pub static AVAILABLE_FIELDS: Lazy<HashMap<&'static str, FieldMetadata>> = Lazy::new(|| {
    let mut fields = HashMap::new();

    // ===================================================================
    // CLUSTER CONFIGURATION
    // ===================================================================
    fields.insert(
        "cluster_name",
        FieldMetadata {
            description: "Name of the MonographDB cluster",
            scope: FieldScope::ClusterWide,
            example: "prod-cluster",
            value_type: FieldValueType::String,
            default_value: "default-cluster",
        },
    );

    fields.insert(
        "replication_factor",
        FieldMetadata {
            description: "Number of replicas for each master",
            scope: FieldScope::ClusterWide,
            example: "2",
            value_type: FieldValueType::Integer,
            default_value: "1",
        },
    );

    fields.insert(
        "tx_nodegroup_replica_num",
        FieldMetadata {
            description: "Replica number of one txservice node group",
            scope: FieldScope::ClusterWide,
            example: "5",
            value_type: FieldValueType::Integer,
            default_value: "3",
        },
    );

    fields.insert(
        "cluster_config_file",
        FieldMetadata {
            description: "Path for cluster config file",
            scope: FieldScope::ClusterWide,
            example: "/etc/monographdb/cluster.conf",
            value_type: FieldValueType::String,
            default_value: "",
        },
    );

    // ===================================================================
    // NETWORK AND CONNECTION SETTINGS
    // ===================================================================
    fields.insert(
        "ip",
        FieldMetadata {
            description: "Redis IP",
            scope: FieldScope::NodeSpecific,
            example: "192.168.1.100",
            value_type: FieldValueType::String,
            default_value: "127.0.0.1",
        },
    );

    fields.insert(
        "port",
        FieldMetadata {
            description: "Redis Port",
            scope: FieldScope::NodeSpecific,
            example: "6380",
            value_type: FieldValueType::Integer,
            default_value: "6379",
        },
    );

    fields.insert(
        "max_connections",
        FieldMetadata {
            description: "Maximum number of client connections",
            scope: FieldScope::NodeSpecific,
            example: "10000",
            value_type: FieldValueType::Integer,
            default_value: "5000",
        },
    );

    fields.insert(
        "timeout",
        FieldMetadata {
            description: "Client connection timeout in seconds",
            scope: FieldScope::NodeSpecific,
            example: "300",
            value_type: FieldValueType::Integer,
            default_value: "60",
        },
    );

    fields.insert(
        "maxclients",
        FieldMetadata {
            description: "Maximum number of clients",
            scope: FieldScope::NodeSpecific,
            example: "20000",
            value_type: FieldValueType::Integer,
            default_value: "10000",
        },
    );

    fields.insert(
        "auto_redirect",
        FieldMetadata {
            description: "Auto redirect request to remote node if key not on local",
            scope: FieldScope::NodeSpecific,
            example: "true",
            value_type: FieldValueType::Boolean,
            default_value: "true",
        },
    );

    fields.insert(
        "ip_port_list",
        FieldMetadata {
            description: "Redis server cluster ip port list",
            scope: FieldScope::ClusterWide,
            example: "192.168.1.100:6379,192.168.1.101:6379",
            value_type: FieldValueType::String,
            default_value: "",
        },
    );

    // ===================================================================
    // SECURITY SETTINGS
    // ===================================================================
    fields.insert(
        "tls_enabled",
        FieldMetadata {
            description: "Whether to enable TLS encryption",
            scope: FieldScope::ClusterWide,
            example: "true",
            value_type: FieldValueType::Boolean,
            default_value: "false",
        },
    );

    fields.insert(
        "auth_required",
        FieldMetadata {
            description: "Whether authentication is required for connections",
            scope: FieldScope::ClusterWide,
            example: "true",
            value_type: FieldValueType::Boolean,
            default_value: "false",
        },
    );

    // ===================================================================
    // STORAGE CONFIGURATION
    // ===================================================================
    fields.insert(
        "eloq_data_path",
        FieldMetadata {
            description: "Path to the data directory for the EloqKV instance",
            scope: FieldScope::NodeSpecific,
            example: "/home/eloq/my_cluster/EloqKV/data/port-6379",
            value_type: FieldValueType::String,
            default_value: "/home/eloq/{cluster}/EloqKV/data/port-{port}",
        },
    );

    fields.insert(
        "enable_data_store",
        FieldMetadata {
            description: "Whether to enable persistent data storage",
            scope: FieldScope::NodeSpecific,
            example: "true",
            value_type: FieldValueType::Boolean,
            default_value: "true",
        },
    );

    fields.insert(
        "enable_wal",
        FieldMetadata {
            description: "Whether to enable Write Ahead Log for durability",
            scope: FieldScope::NodeSpecific,
            example: "true",
            value_type: FieldValueType::Boolean,
            default_value: "false",
        },
    );

    fields.insert(
        "enable_io_uring",
        FieldMetadata {
            description: "Whether to enable io_uring for async I/O operations",
            scope: FieldScope::NodeSpecific,
            example: "true",
            value_type: FieldValueType::Boolean,
            default_value: "false",
        },
    );

    fields.insert(
        "data_store_config_file",
        FieldMetadata {
            description: "Data store configuration file path",
            scope: FieldScope::NodeSpecific,
            example: "/etc/monographdb/datastore.ini",
            value_type: FieldValueType::String,
            default_value: "./data_store_config.ini",
        },
    );

    fields.insert(
        "tx_service_data_path",
        FieldMetadata {
            description: "Path for tx_service data",
            scope: FieldScope::NodeSpecific,
            example: "/var/lib/monographdb/tx_service",
            value_type: FieldValueType::String,
            default_value: "",
        },
    );

    fields.insert(
        "log_service_data_path",
        FieldMetadata {
            description: "Path for log_service data",
            scope: FieldScope::NodeSpecific,
            example: "/var/lib/monographdb/log_service",
            value_type: FieldValueType::String,
            default_value: "",
        },
    );

    // ===================================================================
    // MEMORY MANAGEMENT
    // ===================================================================
    fields.insert(
        "max_memory",
        FieldMetadata {
            description: "Maximum memory usage in megabytes",
            scope: FieldScope::NodeSpecific,
            example: "4096",
            value_type: FieldValueType::Integer,
            default_value: "2048",
        },
    );

    fields.insert(
        "maxmemory_policy",
        FieldMetadata {
            description: "Policy for memory eviction when max memory is reached",
            scope: FieldScope::NodeSpecific,
            example: "volatile-lru",
            value_type: FieldValueType::String,
            default_value: "noeviction",
        },
    );

    fields.insert(
        "enable_cache_replacement",
        FieldMetadata {
            description: "Enable cache replacement",
            scope: FieldScope::NodeSpecific,
            example: "true",
            value_type: FieldValueType::Boolean,
            default_value: "true",
        },
    );

    fields.insert(
        "node_memory_limit_mb",
        FieldMetadata {
            description: "TxService node memory limit in MB",
            scope: FieldScope::NodeSpecific,
            example: "16384",
            value_type: FieldValueType::Integer,
            default_value: "8192",
        },
    );

    fields.insert(
        "node_log_limit_mb",
        FieldMetadata {
            description: "TxService node log limit in MB",
            scope: FieldScope::NodeSpecific,
            example: "16384",
            value_type: FieldValueType::Integer,
            default_value: "8192",
        },
    );

    fields.insert(
        "enable_heap_defragment",
        FieldMetadata {
            description: "Enable heap defragmentation",
            scope: FieldScope::NodeSpecific,
            example: "true",
            value_type: FieldValueType::Boolean,
            default_value: "false",
        },
    );

    // ===================================================================
    // DYNAMODB/AWS SETTINGS
    // ===================================================================
    fields.insert(
        "dynamodb_endpoint",
        FieldMetadata {
            description: "Endpoint of KvStore Dynamodb",
            scope: FieldScope::ClusterWide,
            example: "https://dynamodb.ap-northeast-1.amazonaws.com",
            value_type: FieldValueType::String,
            default_value: "",
        },
    );

    fields.insert(
        "dynamodb_keyspace",
        FieldMetadata {
            description: "KeySpace of Dynamodb KvStore",
            scope: FieldScope::ClusterWide,
            example: "my_keyspace",
            value_type: FieldValueType::String,
            default_value: "eloq_kv",
        },
    );

    fields.insert(
        "dynamodb_region",
        FieldMetadata {
            description: "Region of the used table in DynamoDB",
            scope: FieldScope::ClusterWide,
            example: "us-west-2",
            value_type: FieldValueType::String,
            default_value: "ap-northeast-1",
        },
    );

    fields.insert(
        "aws_access_key_id",
        FieldMetadata {
            description: "AWS SDK access key id",
            scope: FieldScope::ClusterWide,
            example: "AKIAIOSFODNN7EXAMPLE",
            value_type: FieldValueType::String,
            default_value: "",
        },
    );

    fields.insert(
        "aws_secret_key",
        FieldMetadata {
            description: "AWS SDK secret key",
            scope: FieldScope::ClusterWide,
            example: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            value_type: FieldValueType::String,
            default_value: "",
        },
    );

    // ===================================================================
    // PROCESSING AND PERFORMANCE
    // ===================================================================
    fields.insert(
        "core_number",
        FieldMetadata {
            description: "Number of TxProcessors",
            scope: FieldScope::NodeSpecific,
            example: "8",
            value_type: FieldValueType::Integer,
            default_value: "4",
        },
    );

    fields.insert(
        "cc_notify",
        FieldMetadata {
            description: "Notify the txrequest sender when cc request finishes",
            scope: FieldScope::NodeSpecific,
            example: "true",
            value_type: FieldValueType::Boolean,
            default_value: "true",
        },
    );

    fields.insert(
        "checkpoint_interval",
        FieldMetadata {
            description: "Interval time (seconds) of checkpoint",
            scope: FieldScope::NodeSpecific,
            example: "20",
            value_type: FieldValueType::Integer,
            default_value: "10",
        },
    );

    fields.insert(
        "cluster_mode",
        FieldMetadata {
            description: "Enable cluster mode for EloqKV",
            scope: FieldScope::NodeSpecific,
            example: "true",
            value_type: FieldValueType::Boolean,
            default_value: "false",
        },
    );

    fields.insert(
        "snapshot_sync_worker_num",
        FieldMetadata {
            description: "Snapshot sync worker num",
            scope: FieldScope::NodeSpecific,
            example: "4",
            value_type: FieldValueType::Integer,
            default_value: "0",
        },
    );

    // ===================================================================
    // ROCKSDB CONFIGURATION
    // ===================================================================
    fields.insert(
        "txlog_rocksdb_sst_files_size_limit",
        FieldMetadata {
            description: "The total RocksDB sst files size before purge",
            scope: FieldScope::NodeSpecific,
            example: "1GB",
            value_type: FieldValueType::String,
            default_value: "500MB",
        },
    );

    fields.insert(
        "txlog_rocksdb_scan_threads",
        FieldMetadata {
            description: "The number of rocksdb scan threads",
            scope: FieldScope::NodeSpecific,
            example: "4",
            value_type: FieldValueType::Integer,
            default_value: "1",
        },
    );

    fields.insert(
        "txlog_rocksdb_max_write_buffer_number",
        FieldMetadata {
            description: "Max write buffer number",
            scope: FieldScope::NodeSpecific,
            example: "16",
            value_type: FieldValueType::Integer,
            default_value: "8",
        },
    );

    fields.insert(
        "txlog_rocksdb_max_background_jobs",
        FieldMetadata {
            description: "Max background jobs",
            scope: FieldScope::NodeSpecific,
            example: "24",
            value_type: FieldValueType::Integer,
            default_value: "12",
        },
    );

    fields.insert(
        "txlog_rocksdb_target_file_size_base",
        FieldMetadata {
            description: "Target file size base for rocksdb",
            scope: FieldScope::NodeSpecific,
            example: "128MB",
            value_type: FieldValueType::String,
            default_value: "64MB",
        },
    );

    // ===================================================================
    // LOGGING AND MONITORING
    // ===================================================================
    fields.insert(
        "slow_log_threshold",
        FieldMetadata {
            description: "Threshold for logging a query as slow query (microseconds)",
            scope: FieldScope::NodeSpecific,
            example: "5000",
            value_type: FieldValueType::Integer,
            default_value: "10000",
        },
    );

    fields.insert(
        "slow_log_max_length",
        FieldMetadata {
            description: "Max number of logs kept in slow query log",
            scope: FieldScope::NodeSpecific,
            example: "256",
            value_type: FieldValueType::Integer,
            default_value: "128",
        },
    );

    fields.insert(
        "log_file_name_prefix",
        FieldMetadata {
            description: "Sets the prefix for log files",
            scope: FieldScope::NodeSpecific,
            example: "monograph.log",
            value_type: FieldValueType::String,
            default_value: "eloqkv.log",
        },
    );

    fields.insert(
        "enable_redis_stats",
        FieldMetadata {
            description: "Enable to collect redis statistics",
            scope: FieldScope::NodeSpecific,
            example: "false",
            value_type: FieldValueType::Boolean,
            default_value: "true",
        },
    );

    fields.insert(
        "enable_brpc_builtin_services",
        FieldMetadata {
            description: "Enable to show brpc builtin services through http",
            scope: FieldScope::NodeSpecific,
            example: "false",
            value_type: FieldValueType::Boolean,
            default_value: "true",
        },
    );

    // ===================================================================
    // TRANSACTION CONFIGURATION
    // ===================================================================
    fields.insert(
        "isolation_level",
        FieldMetadata {
            description: "Isolation level of simple commands",
            scope: FieldScope::ClusterWide,
            example: "Serializable",
            value_type: FieldValueType::String,
            default_value: "ReadCommitted",
        },
    );

    fields.insert(
        "protocol",
        FieldMetadata {
            description: "Concurrency control protocol of simple commands",
            scope: FieldScope::ClusterWide,
            example: "MVCC",
            value_type: FieldValueType::String,
            default_value: "OccRead",
        },
    );

    fields.insert(
        "txn_isolation_level",
        FieldMetadata {
            description: "Isolation level of MULTI/EXEC and Lua transactions",
            scope: FieldScope::ClusterWide,
            example: "Serializable",
            value_type: FieldValueType::String,
            default_value: "RepeatableRead",
        },
    );

    fields.insert(
        "txn_protocol",
        FieldMetadata {
            description: "Concurrency control protocol of MULTI/EXEC and Lua transactions",
            scope: FieldScope::ClusterWide,
            example: "MVCC",
            value_type: FieldValueType::String,
            default_value: "OCC",
        },
    );

    fields.insert(
        "retry_on_occ_error",
        FieldMetadata {
            description: "Retry transaction on OCC caused error",
            scope: FieldScope::ClusterWide,
            example: "false",
            value_type: FieldValueType::Boolean,
            default_value: "true",
        },
    );

    fields.insert(
        "enable_cmd_sort",
        FieldMetadata {
            description: "Enable to sort command in Multi-Exec",
            scope: FieldScope::ClusterWide,
            example: "true",
            value_type: FieldValueType::Boolean,
            default_value: "false",
        },
    );

    // ===================================================================
    // HOST MANAGER CONFIGURATION
    // ===================================================================
    fields.insert(
        "hm_ip",
        FieldMetadata {
            description: "Host manager IP address",
            scope: FieldScope::NodeSpecific,
            example: "127.0.0.1",
            value_type: FieldValueType::String,
            default_value: "",
        },
    );

    fields.insert(
        "hm_port",
        FieldMetadata {
            description: "Host manager port",
            scope: FieldScope::NodeSpecific,
            example: "7000",
            value_type: FieldValueType::Integer,
            default_value: "0",
        },
    );

    fields.insert(
        "hm_bin",
        FieldMetadata {
            description:
                "Host manager binary path if forking host manager process from main process",
            scope: FieldScope::NodeSpecific,
            example: "/usr/bin/hm_manager",
            value_type: FieldValueType::String,
            default_value: "",
        },
    );

    fields.insert(
        "fork_host_manager",
        FieldMetadata {
            description: "Fork host manager process",
            scope: FieldScope::NodeSpecific,
            example: "false",
            value_type: FieldValueType::Boolean,
            default_value: "true",
        },
    );

    fields
});

/// Helper function to check if a field exists in the registry
pub fn field_exists(field_name: &str) -> bool {
    AVAILABLE_FIELDS.contains_key(field_name)
}

/// Helper function to get field metadata if it exists
pub fn get_field_metadata(field_name: &str) -> Option<&FieldMetadata> {
    AVAILABLE_FIELDS.get(field_name)
}

/// Helper function to determine if a field requires a cluster-wide update
pub fn is_cluster_wide_field(field_name: &str) -> bool {
    AVAILABLE_FIELDS
        .get(field_name)
        .is_some_and(|metadata| metadata.scope == FieldScope::ClusterWide)
}
