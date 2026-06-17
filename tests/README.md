# E2E Tests

This test suite is documented in two common workflows.

## Workflow 1: run the full test flow with the CLI

Build the local `eloqctl` first:

```sh
cd /path/to/eloq_waiter
cargo build -p cluster_mgr --bin eloqctl
```

### Run the full flow step by step (equivalent to `tests/e2e/devctl.sh full`)

```sh
# 1. Start the Docker environment
tests/e2e/devctl.sh env-up

# 2. Copy the current eloqctl build into the control node
tests/e2e/devctl.sh install-control

# 3. Render the selected topology inside the control node
tests/e2e/devctl.sh render-topology

# 4. Launch the default E2E cluster from the control node
tests/e2e/devctl.sh launch
```

Or use the shortcut for all steps at once:

```sh
tests/e2e/devctl.sh full
```

To test a specific EloqKV version, set `ELOQKV_VERSION` before `render-topology` or `full`, for example:

```sh
ELOQKV_VERSION=1.3.0 tests/e2e/devctl.sh full
```

Use the default RocksDB+MinIO topology unless you override it. For local `eloqstore cloud + minio` testing:

```sh
E2E_TOPOLOGY_TEMPLATE=tests/e2e/topology.eloqstore-cloud.yaml tests/e2e/devctl.sh full
```

### Other useful commands

```sh
# Show cluster and monitor status
tests/e2e/devctl.sh status

# Upgrade Grafana only
tests/e2e/devctl.sh grafana-update

# Run the full stress test suite
tests/e2e/devctl.sh stress

# Run only SDK stress tests; traffic stays inside containers, not on the host
tests/e2e/devctl.sh stress py-stress,go-stress,ts-stress

# Run only the RESP compatibility tests (EloqKV vs Redis 7.0 by default)
tests/e2e/devctl.sh stress resp-compat

# Start / pause / resume / stop manual cluster-client traffic
tests/e2e/devctl.sh traffic-start
tests/e2e/devctl.sh traffic-pause
tests/e2e/devctl.sh traffic-resume
tests/e2e/devctl.sh traffic-stop
tests/e2e/devctl.sh traffic-status

# Run the backup E2E test (snapshot start, list, remove, list)
tests/e2e/devctl.sh backup

# Override the Redis target version used by the compatibility suite
RESP_COMPAT_VERSION=6.2.0 tests/e2e/devctl.sh stress resp-compat

# Remove the Docker environment
tests/e2e/devctl.sh env-down
```

Notes:

- `devctl.sh stress` calls `tests/e2e/cmd_stress_test.sh`.
- GitHub Actions uses the same E2E step list and includes `resp-compat` by default.
- Python stress runs in `stress-python`.
- Go stress runs in `stress-go`.
- TypeScript stress runs in `stress-ts`.
- RESP compatibility runs in `resp-compat`.
- `env-up` reuses existing local images by default. To force a rebuild, run `FORCE_DOCKER_BUILD=1 tests/e2e/devctl.sh env-up`.
- `env-up` only creates a fresh Docker test environment. The `eloqctl` runtime directory inside the control node is also reset to a clean state.

## Workflow 2: start the environment, then run `eloqctl` manually inside the control node

### 1. Start the environment

```sh
cd /path/to/eloq_waiter
cargo build -p cluster_mgr --bin eloqctl
tests/e2e/devctl.sh env-up
tests/e2e/devctl.sh install-control
tests/e2e/devctl.sh render-topology
```

To switch the topology template in either workflow, set `E2E_TOPOLOGY_TEMPLATE`, for example:

```sh
export E2E_TOPOLOGY_TEMPLATE=tests/e2e/topology.eloqstore-cloud.yaml
```

For local `eloqstore cloud + minio` manual testing, use a fixed version explicitly:

```sh
export E2E_TOPOLOGY_TEMPLATE=tests/e2e/topology.eloqstore-cloud.yaml
export ELOQKV_VERSION=1.3.0
```

### 2. Log in to the control node

```sh
tests/e2e/devctl.sh control-shell
```

Equivalent command:

```sh
ssh -i tests/docker_ha/id_ed25519 -p 2224 eloq@127.0.0.1
```

Important paths inside the control node:

- Repository: `/workspace/eloq_waiter`
- `eloqctl`: `/usr/local/bin/eloqctl`
- `ELOQCTL_HOME`: `/home/eloq/.eloqctl`
- Rendered topology: `/home/eloq/topology.generated.yaml`

### 3. Launch, update, and inspect the cluster manually

Run inside the control node:

```sh
eloqctl stop test-e2e --all --force || true
eloqctl remove test-e2e --force || true
eloqctl launch --skip-deps /home/eloq/topology.generated.yaml
```

Check status:

```sh
eloqctl status test-e2e --wait 180
eloqctl monitor status --cluster test-e2e
```

### 3.1 Manual `eloqstore cloud + minio` test flow

Host side:

```sh
cd /path/to/eloq_waiter
cargo build -p cluster_mgr --bin eloqctl
export E2E_TOPOLOGY_TEMPLATE=tests/e2e/topology.eloqstore-cloud.yaml
export ELOQKV_VERSION=1.3.0
tests/e2e/devctl.sh env-up
tests/e2e/devctl.sh install-control
tests/e2e/devctl.sh render-topology
tests/e2e/devctl.sh control-shell
```

Inside the control node:

```sh
sed -n '1,20p' /home/eloq/topology.generated.yaml
eloqctl stop test-e2e --all --force || true
eloqctl remove test-e2e --force || true
eloqctl launch --skip-deps /home/eloq/topology.generated.yaml
eloqctl status test-e2e --wait 180
eloqctl monitor status --cluster test-e2e
```

Inspect MinIO objects from the host:

```sh
docker run --rm --network host --entrypoint /bin/sh \
  minio/mc:RELEASE.2025-05-21T01-59-54Z -lc \
  'mc alias set e2e http://127.0.0.1:19000 minioadmin minioadmin >/dev/null && \
   mc ls --recursive e2e/storeeloqservice'
```

Cleanup after manual testing:

```sh
tests/e2e/devctl.sh env-down
```

`env-down` now clears objects under the MinIO bucket used by this E2E environment before removing the Docker environment.

### 4. Backup E2E testing (manual steps inside the control node)

Run the backup E2E flow manually:

```sh
# Create backup directory
mkdir -p /home/eloq/backups

# Create a snapshot (local storage)
eloqctl backup test-e2e start --path /home/eloq/backups --password testpass

# List all snapshots
eloqctl backup test-e2e list

# Remove snapshots older than 1 second (cleanup)
eloqctl backup test-e2e remove --until 1s --force

# Verify snapshots removed
eloqctl backup test-e2e list
```

To test cloud backup (when storage is configured as S3):

```sh
# Create a cloud snapshot (no --path)
eloqctl backup test-e2e start --password testpass

# List snapshots
eloqctl backup test-e2e list

# Remove by timestamp
eloqctl backup test-e2e remove --before "2025-01-01 00:00:00" --force
```

To restore from a cloud snapshot (cluster must be stopped first):

```sh
# Stop the cluster
eloqctl stop test-e2e --all --force

# Restore from a snapshot timestamp
eloqctl backup test-e2e restore --snapshot-ts "2025-06-01T12:00:00Z"
```

To dump a backup to AOF or RDB format:

```sh
# Dump local backup to AOF
eloqctl backup test-e2e dump-aof \
  --rocksdb-path /home/eloq/backups/test-e2e/2025-06-01-12-00-00 \
  --output-file-dir /home/eloq/aof-output

# Dump local backup to RDB
eloqctl backup test-e2e dump-rdb \
  --rocksdb-path /home/eloq/backups/test-e2e/2025-06-01-12-00-00 \
  --output-file-dir /home/eloq/rdb-output
```

View help for all backup subcommands:

```sh
eloqctl backup --help
```

### 5. Manual failover testing

```sh
# Discover masters
eloqctl status test-e2e --wait 180

# Failover from old leader to new leader
eloqctl failover test-e2e \
  --old-leader-host 172.28.10.11 --old-leader-port 6379 \
  --new-leader-host 172.28.10.12 --new-leader-port 6379 \
  --password testpass
```

### 6. Scale testing (add/remove nodes)

```sh
# Add a standby node
eloqctl scale test-e2e \
  --add-nodes 172.28.10.13:6379 \
  --ng-id 1 \
  --is-candidate false \
  --password testpass

# Add a candidate/tx node
eloqctl scale test-e2e \
  --add-nodes 172.28.10.13:6379 \
  --ng-id 1 \
  --is-candidate true

# Remove a node
eloqctl scale test-e2e --remove-nodes 172.28.10.13:6379

# Scale log service
eloqctl scalelog test-e2e --add-nodes 172.28.10.21:9000 --log-ng-id 1
eloqctl scalelog test-e2e --remove-nodes 172.28.10.21:9000
```

### 7. Config update and rolling update testing

```sh
# Update config fields without restart
eloqctl update-conf test-e2e --fields "maxclients:20000,slowlog-log-slower-than:10000"

# Update config fields and restart
eloqctl update-conf test-e2e --fields "maxclients:20000" --restart

# Perform a rolling update to a new version
eloqctl update test-e2e 1.2.3 --password testpass
```

Upgrade Grafana manually:

```sh
eloqctl monitor update --cluster test-e2e \
  --component grafana \
  --url 'https://dl.grafana.com/grafana/release/13.0.1+security-01/grafana_13.0.1+security-01_25720641773_linux_amd64.tar.gz'
```

Install Alertmanager on an existing cluster:

```sh
eloqctl monitor update --cluster test-e2e \
  --component alertmanager \
  --url 'https://github.com/prometheus/alertmanager/releases/download/v0.32.1/alertmanager-0.32.1.linux-amd64.tar.gz'
```

Re-run the same Alertmanager update:

```sh
eloqctl monitor update --cluster test-e2e \
  --component alertmanager \
  --url 'https://github.com/prometheus/alertmanager/releases/download/v0.32.1/alertmanager-0.32.1.linux-amd64.tar.gz'
```

Install Alertmanager together with `alertmanager-webhook-adapter`:

```sh
eloqctl monitor update --cluster test-e2e \
  --component alertmanager \
  --url 'https://github.com/prometheus/alertmanager/releases/download/v0.32.1/alertmanager-0.32.1.linux-amd64.tar.gz'
```

Install Alertmanager and enable Feishu forwarding at the same time:

```sh
eloqctl monitor update --cluster test-e2e \
  --component alertmanager \
  --url 'https://github.com/prometheus/alertmanager/releases/download/v0.32.1/alertmanager-0.32.1.linux-amd64.tar.gz' \
  --feishu-robot-url 'https://open.feishu.cn/open-apis/bot/v2/hook/xxx'
```

This also deploys `alertmanager-webhook-adapter` and ships the built-in Chinese Feishu template:

- Template language: `zh`
- Default signature: `EloqKV`
- Template file: `src/cluster_mgr/config/feishu.zh.tmpl`
- Remote deployment path: `/home/eloq/test-e2e/alertmanager-webhook-adapter/templates/feishu.zh.tmpl`

Re-run the same command to update Alertmanager again or recover from a failed installation:

```sh
eloqctl monitor update --cluster test-e2e \
  --component alertmanager \
  --url 'https://github.com/prometheus/alertmanager/releases/download/v0.32.1/alertmanager-0.32.1.linux-amd64.tar.gz' \
  --feishu-robot-url 'https://open.feishu.cn/open-apis/bot/v2/hook/xxx'
```

Check monitor status again after installation:

```sh
eloqctl monitor status --cluster test-e2e
```

If you previously deployed the legacy standalone `PrometheusAlert`, clean up leftover processes and directories from the control node with:

```sh
ssh -i /home/eloq/.ssh/id_ed25519 eloq@172.28.10.14 \
  "pkill -f '/home/eloq/test-e2e/prometheusalert/PrometheusAlert' || true; \
   rm -rf /home/eloq/test-e2e/prometheusalert"
```

This cleanup is only for legacy leftovers. The new Feishu alerting chain is deployed under `/home/eloq/test-e2e/alertmanager-webhook-adapter`.

Export topology:

```sh
eloqctl export test-e2e --output /home/eloq/test-e2e-export.yaml
```

Start or stop only one existing EloqKV node from the saved topology:

```sh
eloqctl start test-e2e --nodes 172.28.10.12:6379
eloqctl stop test-e2e --nodes 172.28.10.12:6379 --force
```

Operate on one monitor component at a time:

```sh
eloqctl monitor start --cluster test-e2e --component grafana
eloqctl monitor stop --cluster test-e2e --component prometheus
eloqctl monitor status --cluster test-e2e --component node-exporter
```

Add or remove one cluster node manually:

```sh
eloqctl scale test-e2e --add-nodes 172.28.10.12:6379 --ng-id 1 --is-candidate false
eloqctl scale test-e2e --remove-nodes 172.28.10.12:6379
```

### 8. Keep background cluster traffic running while you mutate the cluster

Recommended: use `devctl.sh` to run the Python stress process in the background
inside `stress-python`, then pause/resume it manually as needed.

Run on the host in a separate terminal after the cluster is healthy:

```sh
cd /path/to/eloq_waiter

# Start cluster-client-only traffic in the background
tests/e2e/devctl.sh traffic-start

# Show PID, process state, and recent log output
tests/e2e/devctl.sh traffic-status

# Pause and resume the same process
tests/e2e/devctl.sh traffic-pause
tests/e2e/devctl.sh traffic-resume

# Stop it completely
tests/e2e/devctl.sh traffic-stop
```

Notes:

- `traffic-start` runs the Python stress client with `--client-mode cluster-only`.
- `--duration 0` keeps the workload running until you stop it manually.
- `--read-from-replicas` allows read traffic to hit standby nodes while writes continue to go to the master.
- Use a second terminal to run manual `eloqctl` operations such as `failover`, `update`, `scale`, or `monitor update`.
- If you want a lighter workload, update the defaults in `tests/e2e/devctl.sh` or run the Python script manually.

If you still want to run the Python script manually, prefer cluster-only mode:

```sh
docker compose -f tests/docker_ha/docker-compose.yaml exec -it stress-python \
python3 -u tests/e2e/cmd_stress_py/main.py \
  --startup-node 172.28.10.11:6379 \
  --startup-node 172.28.10.12:6379 \
  --password testpass \
  --tls \
  --read-from-replicas \
  --client-mode cluster-only \
  --workers 16 \
  --inflight 50 \
  --repeat 10 \
  --key-count 256 \
  --cmd-timeout 5 \
  --progress-interval 5 \
  --duration 0
```

Use `-it` instead of `-T` if you want Ctrl+C to reach the foreground Python process.

### 9. Open the monitor UIs

From the host browser:

- Grafana: `http://127.0.0.1:13301`
- Prometheus: `http://127.0.0.1:19500`
- Alertmanager: `http://127.0.0.1:19093` after `alertmanager` is installed
- Alertmanager Webhook Adapter: `http://127.0.0.1:18080` after `alertmanager` is installed

Default Grafana credentials:

```text
admin / admin
```

You can also validate the endpoints with commands:

```sh
curl -fsS http://127.0.0.1:13301/login >/dev/null
curl -fsS http://127.0.0.1:19500/-/healthy
curl -fsS http://127.0.0.1:19093/-/healthy
curl -fsS http://127.0.0.1:18080 >/dev/null
```

### 10. Tear down the environment

Run on the host:

```sh
tests/e2e/devctl.sh env-down
```

## Common stress test variables

`tests/e2e/cmd_stress_test.sh` supports these common overrides:

| Variable | Default |
|----------|---------|
| `STEPS` | `launch,cluster-update,monitor-update,eloqctl-mutate,py-stress,go-stress,ts-stress,resp-compat,remove` |
| `DURATION_SECONDS` | `300` |
| `INFO_ONLY_DURATION_SECONDS` | `300` |
| `WORKERS` | `16` |
| `INFLIGHT` | `4` |
| `KEY_COUNT` | `256` |
| `CMD_TIMEOUT` | `5` |
| `TLS_ENABLED` | `1` |
| `SKIP_DEPS` | `1` |
| `RESP_COMPAT_VERSION` | `7.0.0` |

Example:

```sh
STEPS=py-stress,go-stress,ts-stress \
  DURATION_SECONDS=15 \
  INFO_ONLY_DURATION_SECONDS=15 \
  WORKERS=4 \
  INFLIGHT=2 \
  tests/e2e/devctl.sh stress
```
