# EloqKV Cluster Manager

`eloqctl` deploys and operates EloqKV clusters on SSH-accessible machines. The current cluster manager supports EloqKV only.

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
| `update <cluster> <version>` | Rolling version update for an existing cluster. Version lookup and tarball selection come from GitHub Releases for `eloqdata/eloqkv`. `--download-only` resolves and caches the required tarballs without touching remote hosts. `--monitor` updates only one monitor component. |
| `update-conf <cluster>` | Apply selected config fields and optionally restart tx nodes. |
| `scale <cluster>` | Add or remove EloqKV tx/standby nodes. Duplicate add/remove requests are no-ops. |
| `scalelog <cluster>` | Add or remove log service nodes. Duplicate add/remove requests are no-ops. |
| `failover <cluster>` | Move leadership from an old leader to a requested new leader. |
| `monitor start|stop <cluster>` | Manage Prometheus, Grafana, and node exporter when monitor config is present. |
| `log-service start|stop <cluster>` | Manage standalone log service nodes. |
| `backup <cluster> ...` | Create, list, remove, restore, and dump backups. |
| `export <cluster> [--output file]` | Write the saved launch-compatible topology YAML. |
| `list` | List locally registered clusters. |
| `versions` | List available EloqKV versions by scanning GitHub Release assets for `eloqdata/eloqkv`. |
| `completion <shell>` | Generate shell completion scripts. |
| `upgrade` | Run local SQLite schema upgrades. |
| `remove <cluster> [--force]` | Remove local cluster metadata and perform best-effort remote cleanup. |

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
