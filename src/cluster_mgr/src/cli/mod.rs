use crate::config::{deployment::Product, StorageProvider};
use chrono::{DateTime, Duration, Local, NaiveDateTime, TimeZone, Utc};
use clap::{Parser, Subcommand};
use humantime::Duration as HumanDuration;
use itertools::Itertools;
use std::{env, fs::create_dir_all, path::PathBuf};
use strum_macros::AsRefStr;

pub mod cmd_base;
mod cmd_printer;
pub mod reconcile;
pub mod ssh;
pub mod task;
pub mod util;

pub const CMD_STATUS: &str = "_cmd_status_";
pub const CMD_OUTPUT: &str = "_cmd_output_";
pub const CMD: &str = "_cmd_";

#[derive(Parser, Default, Debug)]
#[command(author, version, about = "EloqData cluster management tool")]
#[command(next_line_help = true)]
pub struct Command {
    #[arg(long, value_name = "home-dir", global = true)]
    pub home: Option<PathBuf>,
    #[arg(short, long, default_value_t = false, global = true)]
    pub quiet: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Show verbose task execution logs",
        global = true
    )]
    pub verbose: bool,
    #[command(subcommand)]
    pub subcmd: Option<SubCommand>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, clap::ValueEnum)]
pub enum UpdateMonitorComponent {
    Grafana,
    Prometheus,
    Alertmanager,
    #[value(hide = true)]
    Prometheusalert,
    NodeExporter,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, clap::ValueEnum)]
pub enum MonitorComponent {
    Grafana,
    Prometheus,
    Alertmanager,
    AlertmanagerWebhookAdapter,
    NodeExporter,
}

#[derive(Subcommand, Clone, Debug, Hash, PartialEq, Eq)]
pub enum MonitorCommand {
    #[command(
        long_about = "Start monitor components.\n\nExamples:\n  eloqctl monitor start --cluster test-e2e\n  eloqctl monitor start --cluster test-e2e --component grafana"
    )]
    Start {
        #[arg(long = "cluster", help = "Existing cluster name")]
        cluster: Option<String>,
        #[arg(
            long = "component",
            value_enum,
            value_delimiter = ',',
            help = "Monitor component(s) to operate on; omit to target all monitor components"
        )]
        components: Vec<MonitorComponent>,
    },
    #[command(
        long_about = "Stop monitor components.\n\nExamples:\n  eloqctl monitor stop --cluster test-e2e\n  eloqctl monitor stop --cluster test-e2e --component alertmanager"
    )]
    Stop {
        #[arg(long = "cluster", help = "Existing cluster name")]
        cluster: Option<String>,
        #[arg(
            long = "component",
            value_enum,
            value_delimiter = ',',
            help = "Monitor component(s) to operate on; omit to target all monitor components"
        )]
        components: Vec<MonitorComponent>,
    },
    #[command(
        long_about = "Restart monitor components.\n\nExamples:\n  eloqctl monitor restart --cluster test-e2e\n  eloqctl monitor restart --cluster test-e2e --component alertmanager-webhook-adapter"
    )]
    Restart {
        #[arg(long = "cluster", help = "Existing cluster name")]
        cluster: Option<String>,
        #[arg(
            long = "component",
            value_enum,
            value_delimiter = ',',
            help = "Monitor component(s) to operate on; omit to target all monitor components"
        )]
        components: Vec<MonitorComponent>,
    },
    #[command(
        long_about = "Check monitor component status.\n\nExamples:\n  eloqctl monitor status --cluster test-e2e\n  eloqctl monitor status --cluster test-e2e --component grafana"
    )]
    Status {
        #[arg(long = "cluster", help = "Existing cluster name")]
        cluster: Option<String>,
        #[arg(
            long = "component",
            value_enum,
            value_delimiter = ',',
            help = "Monitor component(s) to inspect; omit to inspect all monitor components"
        )]
        components: Vec<MonitorComponent>,
    },
    #[command(
        long_about = "Update one monitor component without touching EloqKV.\n\nExamples:\n  eloqctl monitor update --cluster test-e2e --component grafana --url <tarball>\n  eloqctl monitor update --cluster test-e2e --component alertmanager --feishu-robot-url <hook-url>"
    )]
    Update {
        #[arg(long = "cluster", help = "Existing cluster name")]
        cluster: Option<String>,
        #[arg(long, value_enum, help = "Monitor component to update")]
        component: UpdateMonitorComponent,
        #[arg(
            long,
            help = "Override the tarball URL for the selected monitor component"
        )]
        url: Option<String>,
        #[arg(
            long,
            value_delimiter = ',',
            help = "Configure one or more Feishu robot webhook URLs for Alertmanager notifications"
        )]
        feishu_robot_url: Vec<String>,
    },
}

#[derive(Subcommand, Clone, Debug, Hash, PartialEq, Eq, AsRefStr)]
#[command(next_line_help = true)]
pub enum SubCommand {
    #[command(
        long_about = "Launch a demo cluster on the local machine for quick evaluation.\n\n\
Examples:\n\
  eloqctl demo EloqKV\n\
  eloqctl demo EloqKV --store rocksdb --version latest\n\
  eloqctl demo EloqKV --skip-deps --no-monitor\n\n\
Behavior:\n\
  - This is a shortcut that bundles deploy + launch for a single-node demo.\n\
  - --store selects the storage backend (default: rocksdb).\n\
  - --skip-deps skips installing system package dependencies.\n\
  - --unlimited disables resource limits; requires the appropriate kernel capabilities.\n\
  - --no-monitor skips deploying Prometheus, Grafana, and Node Exporter.\n\
  - --joint-wal enables joint WAL mode for the storage engine."
    )]
    #[strum(serialize = "demo")]
    Demo {
        product: Product,
        #[arg(short, long, default_value = "rocksdb")]
        store: StorageProvider,
        #[arg(short, long, default_value = "latest")]
        version: String,
        #[arg(long, default_value_t = false)]
        skip_deps: bool,
        #[arg(long, default_value_t = false)]
        unlimited: bool,
        #[arg(long, default_value_t = false)]
        no_monitor: bool,
        #[arg(long, default_value_t = false)]
        joint_wal: bool,
    },

    #[command(long_about = "Validate a topology YAML file before deployment.\n\n\
Examples:\n\
  eloqctl check topology.yaml\n\n\
Behavior:\n\
  - Checks that all required fields are present in the topology file.\n\
  - Validates host reachability and SSH connectivity.\n\
  - Reports any issues that would prevent a successful launch or deploy.")]
    #[strum(serialize = "check")]
    Check { topology_file: String },
    #[command(long_about = "Launch a cluster from a topology YAML file.\n\n\
Examples:\n\
  eloqctl launch topology.yaml\n\
  eloqctl launch topology.yaml --skip-deps\n\n\
Behavior:\n\
  - Reads the topology file, resolves versions, and deploys all services.\n\
  - Equivalent to running `deploy` followed by service startup.\n\
  - --skip-deps skips installing system package dependencies on remote hosts.\n\
  - The cluster configuration is saved to local state for future management.")]
    #[strum(serialize = "launch")]
    Launch {
        topology_file: String,
        #[arg(short, long, default_value_t = false)]
        skip_deps: bool,
    },
    #[strum(serialize = "start")]
    #[command(long_about = "Start EloqKV services for an existing cluster.\n\n\
Examples:\n\
  eloqctl start eloqkv-cluster\n\
  eloqctl start eloqkv-cluster --nodes 10.0.0.12:6379\n\n\
Behavior:\n\
  - Without --nodes, starts tx, standby, voter, log, and managed storage services from the saved topology.\n\
  - With --nodes, starts only the specified existing EloqKV node(s) by host:port.\n\
  - Use this for nodes that are already present in the saved topology.\n\
  - To add a brand-new standby or tx node to the cluster, use `eloqctl scale --add-nodes ...` instead.")]
    Start {
        cluster: String,
        #[arg(
            long,
            value_delimiter = ',',
            value_name = "nodes",
            value_parser = parse_host_port,
            help = "Start only these existing EloqKV node(s) from the saved topology, in host:port form"
        )]
        nodes: Vec<String>,
    },
    #[command(long_about = "Stop cluster components.\n\n\
Examples:\n\
  eloqctl stop eloqkv-cluster --all --force\n\
  eloqctl stop eloqkv-cluster --nodes 10.0.0.12:6379 --force\n\
  eloqctl stop eloqkv-cluster --log\n\
  eloqctl stop eloqkv-cluster --store\n\n\
Behavior:\n\
  - With --nodes, stops only the specified existing EloqKV node(s).\n\
  - Without --nodes, controls component groups such as tx, log, store, and monitor.\n\
  - --all expands to tx + log + store + monitor.\n\
  - Use --force when graceful shutdown is impossible or the cluster is already unhealthy.")]
    #[strum(serialize = "stop")]
    Stop {
        cluster: String,
        #[arg(long, default_value = "true", help = "Stop tx/standby/voter services")]
        tx: Option<bool>,
        #[arg(long, default_value_t = false, help = "Stop log service")]
        log: bool,
        #[arg(long, default_value_t = false, help = "Stop managed storage service")]
        store: bool,
        #[arg(long, default_value_t = false, help = "Stop monitor components")]
        monitor: bool,
        #[arg(
            short,
            long,
            default_value_t = false,
            help = "Force-stop matching processes"
        )]
        force: bool,
        #[arg(
            short,
            long,
            default_value_t = false,
            help = "Stop tx + log + store + monitor"
        )]
        all: bool,
        #[arg(long, value_name = "cluster password")]
        password: Option<String>,
        #[arg(
            long,
            value_delimiter = ',',
            value_name = "nodes",
            value_parser = parse_host_port,
            help = "Stop only these existing EloqKV node(s), in host:port form"
        )]
        nodes: Vec<String>,
    },

    #[command(long_about = "Restart all services of the specified cluster.\n\n\
Examples:\n\
  eloqctl restart eloqkv-cluster\n\n\
Behavior:\n\
  - Performs a rolling restart of all tx, standby, voter, log, and storage services.\n\
  - The cluster configuration is reloaded from saved state.\n\
  - Monitor components (Prometheus, Grafana, etc.) are NOT restarted;\n\
    use `eloqctl monitor restart --cluster <name>` for those.")]
    #[strum(serialize = "restart")]
    Restart { cluster: String },

    #[command(long_about = "Check the health and runtime status of a cluster.\n\n\
Examples:\n\
  eloqctl status eloqkv-cluster\n\
  eloqctl status eloqkv-cluster --wait 60\n\
  eloqctl status eloqkv-cluster --detail\n\
  eloqctl status eloqkv-cluster --user admin --password mypass\n\n\
Behavior:\n\
  - Shows a table of all services with host, port, status, and detail.\n\
  - --wait <seconds>: block until the cluster reports healthy or the timeout expires.\n\
  - --detail: show the full cluster topology including node roles and replication info.\n\
  - --user / --password: authenticate with the EloqKV cluster (required if requirepass is set).\n\
  - Prints a ready-to-use redis-cli connect command for convenience.")]
    #[strum(serialize = "status")]
    Status {
        cluster: String,
        #[arg(short, long, value_name = "EloqKV user")]
        user: Option<String>,
        #[arg(short, long, value_name = "EloqKV password")]
        password: Option<String>,
        #[arg(short, long, value_name = "Wait cluster ready")]
        wait: Option<u16>,
        #[arg(long, default_value_t = false, help = "Show detailed cluster topology")]
        detail: bool,
    },

    #[command(long_about = "Update an existing cluster to a target version.\n\
                      By default this performs a rolling upgrade that stops, replaces, and restarts services.\n\
                      Use `--download-only` to resolve the requested version and download the required EloqKV tarballs\n\
                      into the local cache without changing remote hosts or cluster state.")]
    #[strum(serialize = "update")]
    Update {
        #[arg(help = "Existing cluster name; required for rolling updates and download-only")]
        cluster: Option<String>,
        #[arg(help = "Target EloqKV version or `latest`")]
        version: Option<String>,
        #[arg(
            long,
            default_value_t = false,
            help = "Download the target release tarballs into the local cache only; do not update remote hosts. Use as: `eloqctl update <cluster> <version> --download-only`"
        )]
        download_only: bool,
        #[arg(
            long,
            value_name = "password for graceful shutdown and redis operations"
        )]
        password: Option<String>,
        #[arg(
            short,
            long,
            value_name = "enable this when graceful shutdown is impossible or the cluster is already down",
            default_value_t = false
        )]
        force: bool,
    },

    #[command(
        long_about = "Update the EloqKV node configuration (Eloqkv.ini) and optionally restart services.\n\n\
Examples:\n\
  eloqctl update-conf eloqkv-cluster --fields checkpointer_interval:300,cluster_mode:true\n\
  eloqctl update-conf eloqkv-cluster --fields checkpointer_interval:300 --restart\n\
  eloqctl update-conf eloqkv-cluster --fields checkpointer_interval:300 --tx-node-id 1\n\n\
Behavior:\n\
  - Uploads the local ~/.eloqctl/upload/<cluster>/Eloqkv.ini to each node.\n\
  - --fields: comma-separated key:value pairs to update in the INI config.\n\
  - --restart: restart tx services after the config is pushed.\n\
  - --password: required if the cluster has requirepass set.\n\
  - --tx-node-id: target a specific tx node by ID instead of all nodes."
    )]
    #[strum(serialize = "update-conf")]
    UpdateConf {
        cluster: String,
        #[arg(long, default_value_t = false)]
        restart: bool,
        #[arg(long, help = "Password for Redis operations")]
        password: Option<String>,
        #[arg(
            long,
            help = "Fields to update in format field_1:value_1,field_2:value_2"
        )]
        fields: Vec<String>,
        #[arg(long, help = "Specific tx node ID to update configuration for")]
        tx_node_id: Option<u32>,
    },

    #[command(
        long_about = "Apply supported changes from a topology YAML to an existing cluster.\n\
                      Currently supports tx config fields, Prometheus retention settings,\n\
                      and storage_service configuration changes (full INI regeneration + restart).\n\
                      Other unsupported changes are ignored and reported."
    )]
    #[strum(serialize = "apply")]
    Apply { topology_file: String },

    #[command(
        long_about = "Preview supported changes from a topology YAML without changing the cluster"
    )]
    #[strum(serialize = "plan")]
    Plan { topology_file: String },

    #[command(long_about = "Tear down a cluster and remove it from local state.\n\n\
Examples:\n\
  eloqctl remove eloqkv-cluster\n\
  eloqctl remove eloqkv-cluster --force\n\n\
Behavior:\n\
  - Stops all services on all hosts, cleans up install directories, and deletes cluster state.\n\
  - Without --force: prompts for confirmation before removing.\n\
  - With --force: skips confirmation and deletes local state even if remote hosts are unreachable.\n\
  - If remote hosts are down, manual cleanup may still be needed; the command prints help for that.")]
    #[strum(serialize = "remove")]
    Remove {
        cluster: String,
        #[arg(long, default_value_t = false)]
        force: bool,
    },

    #[command(
        long_about = "Export the latest cluster topology as a launch-compatible YAML file.\n\
                      The output can be used directly with `eloqctl launch`."
    )]
    #[strum(serialize = "export")]
    Export {
        cluster: String,
        #[arg(short, long, help = "Output file path (default: <cluster>.yaml)")]
        output: Option<String>,
    },

    #[command(long_about = "Print a redis-cli connect command for the cluster.\n\n\
Examples:\n\
  eloqctl connect eloqkv-cluster\n\n\
Behavior:\n\
  - Outputs a ready-to-use `redis-cli -h <host> -p <port>` command.\n\
  - If a password is configured, the -a flag is included.\n\
  - The connection targets the first tx host:port in the cluster topology.")]
    #[strum(serialize = "connect")]
    Connect { cluster: String },

    #[command(
        long_about = "List all clusters that have been created with eloqctl.\n\n\
Examples:\n\
  eloqctl list\n\n\
Behavior:\n\
  - Reads from local state (SQLite) to display cluster names, versions, and host summaries.\n\
  - Only shows clusters that were launched/deployed via the current eloqctl home directory."
    )]
    #[strum(serialize = "list")]
    List,

    #[command(long_about = "List available EloqKV release versions from GitHub.\n\n\
Examples:\n\
  eloqctl versions\n\
  eloqctl versions --product EloqKV --store rocksdb\n\n\
Behavior:\n\
  - Fetches release tags from the GitHub releases page.\n\
  - --product: filter by product name (default: all).\n\
  - --store: filter by storage backend (e.g. rocksdb, rocks_s3).")]
    #[strum(serialize = "versions")]
    Versions {
        #[arg(long)]
        product: Option<Product>,
        #[arg(long)]
        store: Option<StorageProvider>,
    },

    #[strum(serialize = "upgrade")]
    #[command(long_about = "Run the SQLite schema script to create any missing tables")]
    Upgrade,

    #[command(long_about = "Manage monitor components without touching EloqKV.\n\n\
Examples:\n\
  eloqctl monitor start --cluster eloqkv-cluster\n\
  eloqctl monitor start --cluster eloqkv-cluster --component grafana\n\
  eloqctl monitor stop --cluster eloqkv-cluster --component prometheus\n\
  eloqctl monitor status --cluster eloqkv-cluster --component node-exporter\n\n\
Behavior:\n\
  - Omit --component to target all configured monitor components.\n\
  - Use --component to operate on exactly one or more monitor components such as grafana, prometheus, alertmanager, or node-exporter.")]
    #[strum(serialize = "monitor")]
    Monitor {
        #[arg(long, global = true, help = "Existing cluster name")]
        cluster: Option<String>,
        #[command(subcommand)]
        command: MonitorCommand,
    },

    #[command(long_about = "Control standalone log service nodes.\n\n\
Examples:\n\
  eloqctl log-srv start eloqkv-cluster\n\
  eloqctl log-srv stop eloqkv-cluster\n\n\
This command controls the log service only. It does not start or stop tx/standby/voter services.")]
    #[strum(serialize = "log-srv")]
    LogService { command: String, cluster: String },

    #[command(
        long_about = "Run a custom shell command on all hosts defined in a topology file.\n\n\
Examples:\n\
  eloqctl exec 'df -h' topology.yaml\n\
  eloqctl exec 'systemctl status eloqkv' topology.yaml\n\n\
Behavior:\n\
  - Parses the topology YAML to discover all target hosts.\n\
  - Executes the given command via SSH on each host and streams the output.\n\
  - Useful for troubleshooting, gathering diagnostics, or running ad-hoc administration tasks."
    )]
    #[strum(serialize = "exec_cmd")]
    Exec {
        command: String,
        topology_file: String,
    },

    #[command(
        long_about = "Install required system packages on hosts defined in a topology file.\n\n\
Examples:\n\
  eloqctl run-deps topology.yaml\n\n\
Behavior:\n\
  - Reads the topology file and installs OS-level dependencies (e.g. libssl, jemalloc)\n\
    on each host via the system package manager.\n\
  - Must be run before `launch` or `deploy` if the hosts are freshly provisioned.\n\
  - Idempotent: re-running is safe and will skip already-installed packages."
    )]
    #[strum(serialize = "run-deps")]
    RunDeps { topology_file: String },

    #[strum(serialize = "deploy")]
    #[command(
        long_about = "Deploy cluster binaries and configuration from a topology file without starting services.\n\n\
Examples:\n\
  eloqctl deploy topology.yaml\n\n\
Behavior:\n\
  - Downloads the required EloqKV release tarballs and uploads them to each target host.\n\
  - Unpacks binaries and generates configuration files (Eloqkv.ini, etc.).\n\
  - Does NOT start any services; use `eloqctl start <cluster>` afterwards.\n\
  - Unlike `launch`, deploy is a two-step workflow: deploy → start.\n\
  - Useful when you want to inspect or modify configs before starting."
    )]
    Deploy { topology_file: String },

    #[strum(serialize = "install")]
    #[command(
        long_about = "Bootstrap a deployed cluster by initializing its catalog and metadata.\n\n\
Examples:\n\
  eloqctl install eloqkv-cluster\n\n\
Behavior:\n\
  - Connects to the cluster's tx nodes and initializes the internal catalog (Raft group membership).\n\
  - Must be run after `deploy` and before the first `start`.\n\
  - If the cluster already has a running tx service, this step is skipped automatically.\n\
  - The `launch` command runs this step as part of its workflow."
    )]
    Install { cluster: String },

    #[strum(serialize = "scale")]
    #[command(long_about = "Scale a cluster by adding or removing EloqKV nodes.\n\n\
Examples:\n\
  eloqctl scale eloqkv-cluster --add-nodes 10.0.0.12:6379 --ng-id 1 --is-candidate false\n\
  eloqctl scale eloqkv-cluster --add-nodes 10.0.0.13:6379 --ng-id 1 --is-candidate true\n\
  eloqctl scale eloqkv-cluster --remove-nodes 10.0.0.12:6379\n\n\
Behavior:\n\
  - --is-candidate false adds a non-candidate node, which is the standby form.\n\
  - --is-candidate true adds a candidate/tx node.\n\
  - Use `eloqctl start --nodes ...` only for nodes that already exist in the saved topology.\n\
  - Use `eloqctl scale --add-nodes ...` to introduce a brand-new node into the cluster.")]
    Scale {
        /// The name of the cluster to scale
        cluster: String,
        /// Nodes to add in host format (comma-separated list)
        #[arg(
            long,
            value_delimiter = ',',
            value_name = "nodes",
            help = "Add these new node(s) in host:port form"
        )]
        add_nodes: Vec<String>,
        /// Nodes to remove in host format (comma-separated list)
        #[arg(
            long,
            value_delimiter = ',',
            value_name = "nodes",
            help = "Remove these existing node(s) in host:port form"
        )]
        remove_nodes: Vec<String>,
        /// Node group ID for adding nodes (required when add_nodes is specified)
        #[arg(long, help = "Node group ID for new nodes; required with --add-nodes")]
        ng_id: Option<u32>,
        /// Candidate status for added nodes: true for candidate, false for non-candidate
        #[arg(
            long,
            value_delimiter = ',',
            help = "Candidate flag(s) for new node(s): true=candidate/tx, false=standby"
        )]
        is_candidate: Option<Vec<bool>>,
        /// Optional password for Redis operations
        #[arg(long, value_name = "cluster password")]
        password: Option<String>,
        /// Version to use for newly added nodes (requires --add-nodes)
        #[arg(long, value_name = "version")]
        version: Option<String>,
    },

    #[strum(serialize = "scalelog")]
    #[command(
        long_about = "Scale the log service by adding or removing log nodes.\n\n\
Examples:\n\
  eloqctl scalelog eloqkv-cluster --add-nodes 10.0.0.21:9000 --log-ng-id 1\n\
  eloqctl scalelog eloqkv-cluster --remove-nodes 10.0.0.21:9000"
    )]
    ScaleLog {
        /// The name of the cluster to scalelog
        cluster: String,
        /// Log nodes to add in host:port format
        #[arg(long, value_delimiter = ',', value_name = "nodes")]
        add_nodes: Vec<String>,
        /// Log nodes to remove in host:port format
        #[arg(long, value_delimiter = ',', value_name = "nodes")]
        remove_nodes: Vec<String>,
        /// Log group ID for adding log nodes (required when adding nodes, must not be provided when removing nodes)
        #[arg(long)]
        log_ng_id: Option<u32>,
    },

    #[command(long_about = "Manage cluster snapshots and backups.\n\n\
Examples:\n\
  eloqctl backup eloqkv-cluster start --path /data/backups\n\
  eloqctl backup eloqkv-cluster start --path /data/backups --password mypass\n\
  eloqctl backup eloqkv-cluster start --path /data/backups --dest-host 10.0.0.100 --dest-user admin\n\
  eloqctl backup eloqkv-cluster list\n\
  eloqctl backup eloqkv-cluster remove --until '7 days'\n\
  eloqctl backup eloqkv-cluster dump-aof --rocksdb-path /data/eloqkv/db --output-file-dir /tmp/aof\n\
  eloqctl backup eloqkv-cluster dump-rdb --rocksdb-path /data/eloqkv/db --output-file-dir /tmp/rdb\n\
  eloqctl backup eloqkv-cluster restore --snapshot-ts '2025-11-05T03:45:45Z'\n\n\
Behavior:\n\
  - start: Trigger a cluster-wide snapshot (via gRPC); requires a healthy cluster.\n\
  - list: Display all recorded snapshots from the local metadata table (t_snapshot_info).\n\
  - remove: Delete snapshots older than --until (relative duration) or --before (absolute timestamp).\n\
    Snapshots in progress (status=2) are skipped to avoid data inconsistency.\n\
    Use --force to delete metadata records even when S3/file deletion fails.\n\
  - dump-aof / dump-rdb: Offline conversion tools that read RocksDB data directly and\n\
    produce AOF or RDB format files. The cluster must be stopped before running these.\n\
  - restore: Recover the cluster state to a previous snapshot timestamp.\n\
    The snapshot_ts must match an existing entry in t_snapshot_info. The cluster must\n\
    be stopped before running this command. Currently only supports cloud storage (S3).\n\
  - Omitting --path on start triggers cloud (S3) storage mode; the backup is stored\n\
    in the configured S3 bucket instead of a local directory.\n\
  - Provide --dest-host and --dest-user to specify where local backup files are stored;\n\
    defaults to the current host and user if omitted.")]
    #[strum(serialize = "backup")]
    Backup {
        cluster: String,
        #[command(subcommand)]
        command: BackupCommand,
    },

    #[command(
        long_about = "Perform a manual failover by switching the leader role from one node to another.\n\n\
Examples:\n\
  eloqctl failover eloqkv-cluster --old-leader-host 10.0.0.11 --old-leader-port 6379 --new-leader-host 10.0.0.12 --new-leader-port 6379\n\
  eloqctl failover eloqkv-cluster --old-leader-host 10.0.0.11 --old-leader-port 6379 --new-leader-host 10.0.0.12 --new-leader-port 6379 --password mypass\n\n\
Behavior:\n\
  - Demotes the current leader (old-leader-host:old-leader-port) and promotes the target node\n\
    (new-leader-host:new-leader-port) to become the new leader.\n\
  - Requires a healthy cluster with the old leader still reachable.\n\
  - Provide --password if the cluster has requirepass set."
    )]
    #[strum(serialize = "failover")]
    Failover {
        cluster: String,
        #[arg(long)]
        old_leader_host: String,
        #[arg(long)]
        old_leader_port: u16,
        #[arg(long)]
        new_leader_host: String,
        #[arg(long)]
        new_leader_port: u16,
        #[arg(long, value_name = "cluster password")]
        password: Option<String>,
    },

    #[command(long_about = "Generate shell completion scripts")]
    #[strum(serialize = "completion")]
    Completion {
        #[arg(value_enum)]
        shell: CompletionShell,
        #[arg(short, long, value_name = "FILE")]
        output: Option<PathBuf>,
    },

    #[command(hide = true)]
    #[strum(serialize = "__complete-clusters")]
    CompleteClusters,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, clap::ValueEnum)]
pub enum CompletionShell {
    Bash,
    Zsh,
    Fish,
}

impl CompletionShell {
    pub fn as_clap_shell(&self) -> clap_complete::Shell {
        match self {
            Self::Bash => clap_complete::Shell::Bash,
            Self::Zsh => clap_complete::Shell::Zsh,
            Self::Fish => clap_complete::Shell::Fish,
        }
    }
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    s.parse::<HumanDuration>()
        .map(|d| Duration::from_std(*d).unwrap())
        .map_err(|e| e.to_string())
}

fn parse_host_port(s: &str) -> Result<String, String> {
    let Some((host, port)) = s.split_once(':') else {
        return Err(format!("invalid node '{s}': expected 'host:port'"));
    };

    if host.is_empty() {
        return Err(format!("invalid node '{s}': host must not be empty"));
    }

    if port.is_empty() {
        return Err(format!("invalid node '{s}': port must not be empty"));
    }

    port.parse::<u16>()
        .map_err(|_| format!("invalid node '{s}': port must be a valid number"))?;

    Ok(s.to_string())
}

fn parse_datetime(s: &str) -> Result<DateTime<Utc>, String> {
    // Try parsing with timezone information (RFC 3339)
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }
    // Try parsing without timezone, format "%Y-%m-%d %H:%M:%S", assuming local time
    if let Ok(naive_dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        let local_dt = Local
            .from_local_datetime(&naive_dt)
            .single()
            .ok_or_else(|| format!("Invalid or ambiguous local datetime: '{}'", s))?;
        return Ok(local_dt.with_timezone(&Utc));
    }
    // Try parsing without timezone, format "%Y-%m-%dT%H:%M:%S", assuming local time
    if let Ok(naive_dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        let local_dt = Local
            .from_local_datetime(&naive_dt)
            .single()
            .ok_or_else(|| format!("Invalid or ambiguous local datetime: '{}'", s))?;
        return Ok(local_dt.with_timezone(&Utc));
    }
    // If all parsing attempts fail, return an error
    Err(format!(
        "Invalid timestamp '{}'. Accepted formats are:\n\
        - RFC 3339: '2024-11-14T15:01:00Z'\n\
        - 'YYYY-MM-DD HH:MM:SS': '2024-11-14 15:01:00' (assumed local time)\n\
        - 'YYYY-MM-DDTHH:MM:SS': '2024-11-14T15:01:00' (assumed local time)",
        s
    ))
}

#[derive(Subcommand, Clone, Debug, Hash, PartialEq, Eq, AsRefStr)]
#[command(next_line_help = true)]
pub enum BackupCommand {
    #[command(long_about = "Start a new cluster-wide snapshot backup.\n\n\
Examples:\n\
  eloqctl backup mycluster start --path /data/backups\n\
  eloqctl backup mycluster start --path /data/backups --password mypass\n\
  eloqctl backup mycluster start --path /data/backups --dest-host 10.0.0.100 --dest-user admin\n\
  eloqctl backup mycluster start  (cloud/S3 mode, no --path)\n\n\
Behavior:\n\
  - With --path: backup files are written to <path>/<cluster>/<timestamp>/ on the destination host.\n\
  - Without --path: backup is stored in the configured cloud (S3) bucket.\n\
  - --dest-host and --dest-user default to the current host and user if omitted (local mode only).\n\
  - The cluster must be healthy; all tx/standby services must be running.\n\
  - Snapshot metadata is recorded in the local t_snapshot_info table.")]
    #[strum(serialize = "start")]
    Start {
        #[arg(
            long,
            help = "The full path to where the backup is stored. Required for local storage, optional for cloud (S3) storage."
        )]
        path: Option<String>,
        #[arg(long, value_name = "cluster password")]
        password: Option<String>,
        #[arg(long, value_name = "destination host")]
        dest_host: Option<String>,
        #[arg(long, value_name = "destination username")]
        dest_user: Option<String>,
    },
    #[command(long_about = "List all recorded snapshots for the cluster.\n\n\
Reads from the local t_snapshot_info metadata table and displays:\n\
  - cluster_name\n\
  - snapshot timestamp (snapshot_ts)\n\
  - snapshot path (local path, manifest list, or backup_ts for cloud storage)\n\
  - destination host and user\n\
  - storage type (local or cloud/S3)\n\n\
Only snapshots with status=0 (completed successfully) are shown.")]
    #[strum(serialize = "list")]
    List {
        // #[arg(long)]
        // before_datetime: String,
    },
    #[command(long_about = "Delete snapshots older than a given threshold.\n\n\
Examples:\n\
  eloqctl backup mycluster remove --until '7 days'\n\
  eloqctl backup mycluster remove --until '2h'\n\
  eloqctl backup mycluster remove --before '2025-01-01T00:00:00Z'\n\
  eloqctl backup mycluster remove --until '30 days' --force\n\n\
Behavior:\n\
  - --until: relative duration from now (e.g. '7 days', '24h', '1 week').\n\
  - --before: absolute UTC timestamp; mutually exclusive with --until.\n\
  - Snapshots with status=2 (in progress) are skipped to avoid corrupting running backups.\n\
  - When the snapshot path is local, the corresponding directory is deleted from the host.\n\
  - When the snapshot path is S3, the S3 objects are deleted.\n\
  - --force: delete the metadata row even when S3/file deletion fails.")]
    #[strum(serialize = "remove")]
    Remove {
        #[arg(
            long,
            value_name = "PERIOD",
            help = "Deletes all snapshots older than the specified period.\n\
            Accepted formats:\n\
            - '2 days'\n\
            - '15h'\n\
            - '1 week'\n\
            - '3 months'\n\
            - '1y 6mo 2w 4d 3h 5m 7s'\n\
            See https://docs.rs/humantime/latest/humantime/fn.parse_duration.html for more details.",
            value_parser = parse_duration
        )]
        until: Option<Duration>,

        #[arg(
            long,
            value_name = "TIMESTAMP",
            help = "Deletes all snapshots created before this timestamp.\n\
            Accepted formats:\n\
            - RFC 3339: '2024-11-14T15:01:00Z'\n\
            - 'YYYY-MM-DD HH:MM:SS' (assumed local time zone)\n\
            - 'YYYY-MM-DDTHH:MM:SS' (assumed local time zone)",
            value_parser = parse_datetime
        )]
        before: Option<chrono::DateTime<chrono::Utc>>,

        #[arg(
            long,
            help = "Force deletion: Delete records from meta data table regardless of S3/file deletion result"
        )]
        force: bool,
    },
    #[command(
        long_about = "Offline dump of RocksDB data into AOF (Append-Only File) format.\n\n\
Examples:\n\
  eloqctl backup mycluster dump-aof --rocksdb-path /data/eloqkv/db --output-file-dir /tmp/aof\n\
  eloqctl backup mycluster dump-aof --rocksdb-path /data/eloqkv/db --output-file-dir /tmp/aof --thread-count 4\n\n\
Behavior:\n\
  - Reads the RocksDB data directory directly and converts all key-value pairs into AOF commands.\n\
  - The cluster must be stopped before running this command; otherwise data corruption may occur.\n\
  - --thread-count controls the number of parallel read threads (default: auto).\n\
  - Output files are written to --output-file-dir."
    )]
    #[strum(serialize = "dump-aof")]
    DumpAOF {
        #[arg(long)]
        rocksdb_path: String,
        #[arg(long)]
        output_file_dir: String,
        #[arg(long)]
        thread_count: Option<String>,
    },
    #[command(long_about = "Offline dump of RocksDB data into RDB format.\n\n\
Examples:\n\
  eloqctl backup mycluster dump-rdb --rocksdb-path /data/eloqkv/db --output-file-dir /tmp/rdb\n\
  eloqctl backup mycluster dump-rdb --rocksdb-path /data/eloqkv/db --output-file-dir /tmp/rdb --thread-count 4\n\n\
Behavior:\n\
  - Reads the RocksDB data directory directly and produces an RDB-format snapshot.\n\
  - The cluster must be stopped before running this command; otherwise data corruption may occur.\n\
  - --thread-count controls the number of parallel read threads (default: auto).\n\
  - Output files are written to --output-file-dir.")]
    #[strum(serialize = "dump-rdb")]
    DumpRDB {
        #[arg(long)]
        rocksdb_path: String,
        #[arg(long)]
        output_file_dir: String,
        #[arg(long)]
        thread_count: Option<String>,
    },
    #[command(long_about = "Restore the cluster state from a previous snapshot.\n\n\
Examples:\n\
  eloqctl backup mycluster restore --snapshot-ts '2025-11-05T03:45:45Z'\n\
  eloqctl backup mycluster restore --snapshot-ts '2025-11-05 03:45:45'\n\n\
Behavior:\n\
  - Restores the cluster data directory to the state captured at the given snapshot timestamp.\n\
  - The snapshot_ts must match a completed snapshot (status=0) in t_snapshot_info.\n\
  - The cluster must be stopped before running this command. Stop it first with `eloqctl stop <cluster> --all --force`.\n\
  - Currently only supports cloud storage (S3) snapshots; local storage restore is not yet available.\n\
  - This is a destructive operation; existing data will be replaced by the restored snapshot.\n\
  - Use `eloqctl backup <cluster> list` to see available snapshots before restoring.")]
    #[strum(serialize = "restore")]
    Restore {
        #[arg(
            long,
            value_name = "SNAPSHOT_TS",
            help = "Snapshot timestamp to restore. Must match a snapshot_name in t_snapshot_info table.\n\
            Accepted formats:\n\
            - RFC 3339: '2024-11-14T15:01:00Z'\n\
            - 'YYYY-MM-DD HH:MM:SS' (assumed UTC)\n\
            - 'YYYY-MM-DDTHH:MM:SS' (assumed UTC)\n\
            Example: '2025-11-05T03:45:45Z'",
            value_parser = parse_datetime
        )]
        snapshot_ts: chrono::DateTime<chrono::Utc>,
    },
}

pub const HOME_DIR: &str = "ELOQCTL_HOME";

pub fn home_path() -> PathBuf {
    match env::var(HOME_DIR) {
        Ok(home) => PathBuf::from(home),
        Err(_) => {
            let home = env::current_dir().expect("current dir should be available");
            env::set_var(HOME_DIR, &home);
            home
        }
    }
}

pub fn download_dir() -> PathBuf {
    home_path().join("download")
}

pub fn download_file_path(download_files: Vec<String>) -> Vec<PathBuf> {
    let download_dir = download_dir();
    download_files
        .iter()
        .map(|file| download_dir.join(file.as_str()))
        .collect_vec()
}

pub fn upload_dir() -> PathBuf {
    home_path().join("upload")
}

pub fn create_upload_cluster_dir(dir: &str) -> PathBuf {
    let dir_buf = upload_dir().join(dir);
    create_dir_all(dir_buf.as_path()).expect("create upload directory for host");
    dir_buf
}
