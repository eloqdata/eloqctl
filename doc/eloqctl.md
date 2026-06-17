# EloqKV `eloqctl`

`eloqctl` deploys and operates EloqKV clusters on SSH-accessible machines. The current cluster manager supports EloqKV only.
This document is the user-facing command reference for `eloqctl`.

## Core Concepts

1. **Cluster**: a named EloqKV deployment. Cluster names must be unique on the control host.
2. **Topology YAML**: the desired cluster shape used by `check`, `run-deps`, `deploy`, `launch`, `plan`, `apply`, and `exec`.
3. **Cluster index**: local SQLite metadata that maps a cluster name to its saved topology file.
4. **Live observed state**: process, Redis/EloqKV, log service, DSS, and monitor status collected from the real deployment.
5. **Mutation lock**: a local lock under `$ELOQCTL_HOME/locks/<cluster>.lock` that prevents concurrent mutations on the same cluster from one control host.

## State Layout

`eloqctl` stores launch-compatible topology files under:

```text
${ELOQCTL_HOME:-$HOME/.eloqctl}/clusters/<cluster>/topology.yaml
```

SQLite stores local operational metadata only, including the cluster index, locks, task history, and backup metadata. It is not the authority for whether a remote service is running.

## Common Workflow

Prepare each target host before first deployment:

```sh
curl -fsSL https://raw.githubusercontent.com/eloqdata/eloq_waiter/main/scripts/setup-host.sh | sudo bash
ssh-copy-id -i ~/.ssh/id_rsa.pub eloq@<target-host>
```

The setup script installs basic host dependencies, creates the `eloq` user if needed, enables passwordless sudo for that user, configures SSH, raises file-descriptor and core-dump limits, and prepares `/var/crash`.

For field meanings and current YAML examples, see the EloqKV website docs: [Deployment YAML Reference](https://eloqdata.github.io/eloq-website/eloqkv/topology-reference) and [Deploy High Availability Cluster with MinIO](https://eloqdata.github.io/eloq-website/eloqkv/quick-start-ha-local-storage).

```sh
eloqctl check /path/to/topology.yaml
eloqctl launch /path/to/topology.yaml
eloqctl status eloqkv-cluster --wait 60
eloqctl plan /path/to/topology-v2.yaml
eloqctl apply /path/to/topology-v2.yaml
eloqctl export eloqkv-cluster --output eloqkv-cluster.yaml
eloqctl stop eloqkv-cluster --all --force
eloqctl remove eloqkv-cluster --force
```

`status`, `connect`, `list`, and `export` operate by cluster name and do not require the original YAML path.

## Commands

| Command | Purpose |
| --- | --- |
| `check <topology.yaml>` | Validate topology and deployment prerequisites that can be checked locally. |
| `run-deps <topology.yaml>` | Install runtime packages on target hosts. Ubuntu runtime dependencies are listed in `src/cluster_mgr/config/runtime_deps_ubuntu`; JDK is not required. |
| `deploy <topology.yaml>` | Upload and unpack EloqKV artifacts and generated config. |
| `install <cluster>` | Bootstrap EloqKV catalog. Skips bootstrap if live tx service is already running. |
| `start <cluster>` | Start tx, standby, voter, log, and storage services described by the saved topology. |
| `launch <topology.yaml>` | Run dependency installation, deployment, bootstrap, start, monitor setup, topology update, and final status checks. |
| `status <cluster> [--wait seconds] [--detail]` | Observe live status using saved topology. Normal output is user-facing; use global `--verbose` for task details. |
| `connect <cluster>` | Print an EloqKV client command for the cluster. |
| `plan <topology.yaml>` | Preview supported changes without mutating local state or remote hosts. |
| `apply <topology.yaml>` | Execute the same plan shown by `plan`, gated by live critical-service health and verified afterward. |
| `update <cluster> <version>` | Rolling version update for an existing cluster. Version lookup and tarball selection come from GitHub Releases for `eloqdata/eloqkv`. `--download-only` resolves and caches the required tarballs without touching remote hosts. |
| `update-conf <cluster>` | Apply selected config fields and optionally restart tx nodes. |
| `scale <cluster>` | Add or remove EloqKV tx/standby nodes. Duplicate add/remove requests are no-ops. |
| `scalelog <cluster>` | Add or remove log service nodes. Duplicate add/remove requests are no-ops. |
| `failover <cluster>` | Move leadership from an old leader to a requested new leader. |
| `monitor start|stop|status <cluster>` | Manage Prometheus, Grafana, and node exporter when monitor config is present. |
| `monitor update <cluster> --component <name> [--url <tarball>]` | Update exactly one monitor component without touching EloqKV. If the selected component is not running yet, the command installs/unpacks it and then starts it. |
| `log-service start|stop <cluster>` | Manage standalone log service nodes. |
| `backup <cluster> ...` | Create, list, remove, restore, and dump backups. |
| `export <cluster> [--output file]` | Write the saved launch-compatible topology YAML. |
| `list` | List locally registered clusters. |
| `versions` | List available EloqKV versions by scanning GitHub Release assets for `eloqdata/eloqkv`. |
| `completion <shell>` | Generate shell completion scripts. |
| `upgrade` | Run local SQLite schema upgrades. |
| `remove <cluster> [--force]` | Remove local cluster metadata and perform best-effort remote cleanup. |

## Targeted Component Operations

`eloqctl` can operate on one node or one component at a time, but the basic command table above does not spell out the exact CLI shapes.

Start only one EloqKV node that already exists in the saved topology:

```sh
eloqctl start eloqkv-cluster --nodes 10.0.0.12:6379
```

This is the right command when you want to bring back just one tx, standby, or voter node by `host:port` without starting the whole cluster.

Stop only one EloqKV node:

```sh
eloqctl stop eloqkv-cluster --nodes 10.0.0.12:6379 --force
```

Stop only tx services for the cluster, without touching monitor or storage:

```sh
eloqctl stop eloqkv-cluster --tx true
```

Stop only the log service:

```sh
eloqctl stop eloqkv-cluster --log
```

Stop only the storage service when it is managed by `eloqctl`:

```sh
eloqctl stop eloqkv-cluster --store
```

Stop everything in the cluster:

```sh
eloqctl stop eloqkv-cluster --all --force
```

Operate on monitor components one at a time:

```sh
eloqctl monitor start --cluster eloqkv-cluster --component grafana
eloqctl monitor stop --cluster eloqkv-cluster --component prometheus
eloqctl monitor status --cluster eloqkv-cluster --component node-exporter
```

Operate on all monitor components together:

```sh
eloqctl monitor start --cluster eloqkv-cluster
eloqctl monitor stop --cluster eloqkv-cluster
eloqctl monitor status --cluster eloqkv-cluster
```

Operate on standalone log service nodes:

```sh
eloqctl log-service start eloqkv-cluster
eloqctl log-service stop eloqkv-cluster
```

Add a new standby node:

```sh
eloqctl scale eloqkv-cluster \
  --add-nodes 10.0.0.12:6379 \
  --ng-id 1 \
  --is-candidate false
```

Add a new candidate/tx node:

```sh
eloqctl scale eloqkv-cluster \
  --add-nodes 10.0.0.13:6379 \
  --ng-id 1 \
  --is-candidate true
```

Remove one existing tx/standby/voter node:

```sh
eloqctl scale eloqkv-cluster --remove-nodes 10.0.0.12:6379
```

Important distinctions:

1. `start <cluster> --nodes ...` only starts nodes that are already present in the saved topology.
2. `scale --add-nodes ...` changes cluster membership and is the correct command for adding a new standby or tx node.
3. `monitor ... --component ...` targets monitor processes such as Grafana, Prometheus, Alertmanager, or node-exporter, not EloqKV tx/standby nodes.

## SSH And Endpoints

Topology `connection.ssh_endpoints` can map a deployment host to the host/port used by the control machine for SSH. This is required for Docker E2E and useful behind bastions or port forwarding.

Topology `connection.service_endpoints` can map service ports, such as Redis and gRPC ports, to control-machine reachable endpoints. Readiness, topology discovery, backup, scale, and failover use service endpoint mapping.

## EloqKV HA Topology

The default HA topology is:

1. One tx/master node.
2. One standby node.
3. One voter node.

For Redis Cluster compatible readiness checks, startup nodes are tx plus standby nodes. Voters are not used as client startup nodes.

## Output

Default command output is concise and intended for operators. Add global `--verbose` to show task identifiers, internal details, and richer diagnostics:

```sh
eloqctl --verbose status eloqkv-cluster
```
