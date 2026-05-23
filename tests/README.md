# E2E Tests

Deploys an EloqKV cluster in Docker containers via `eloqctl launch`, then runs
multi-SDK stress workloads against it. Every test uses both standalone and cluster
Redis clients with TLS enabled.

## Quick Start

```sh
cd /home/starrysky/workspace/eloqdata-kernel/eloq_waiter

# Build eloqctl (one time)
scripts/install-dev.sh

# Run everything: launch → Python → Go → TS → remove
bash tests/e2e/cmd_stress_test.sh
```

The default stress duration is now `300` seconds per SDK step. The appended
high-concurrency `INFO` burst in each SDK step also defaults to `300` seconds.

## Run specific steps

```sh
# Launch only
STEPS=launch bash tests/e2e/cmd_stress_test.sh

# Stress only (against already-running cluster)
STEPS=py-stress,go-stress,ts-stress bash tests/e2e/cmd_stress_test.sh

# Override the default 300-second stress duration
DURATION_SECONDS=120 INFO_ONLY_DURATION_SECONDS=120 \
  STEPS=py-stress,go-stress,ts-stress bash tests/e2e/cmd_stress_test.sh
```

## Control Node Usage

The Docker E2E environment uses `eloq-node-4` as the control node. Install the
current host-built `eloqctl` into that container from the host shell. The
Docker E2E environment must already be running:

```sh
bash tests/install_control_eloqctl.sh
```

If the environment is not running yet, start it first:

```sh
export DOCKER_E2E_DIR=/home/starrysky/workspace/eloqdata-kernel/eloq_waiter/tests/docker_ha
export REPO_ROOT=/home/starrysky/workspace/eloqdata-kernel/eloq_waiter
source tests/docker_env.sh
start_docker_env
bash tests/install_control_eloqctl.sh
```

Then log in directly:

```sh
ssh -i "tests/docker_ha/id_ed25519" -p 2224 eloq@127.0.0.1
```

Inside `node4`, `eloqctl` is installed at `/usr/local/bin/eloqctl` and uses
`/home/eloq/.eloqctl` as `ELOQCTL_HOME`.

### Generate launch topology inside `node4`

The template topology lives at `tests/e2e/topology.yaml`. From a shell inside
`node4`, generate the control-node-ready topology and launch from it:

```sh
cd /workspace/eloq_waiter
export DOCKER_E2E_DIR=/workspace/eloq_waiter/tests/docker_ha
export REPO_ROOT=/workspace/eloq_waiter
source tests/docker_env.sh
render_topology_for_control tests/e2e/topology.yaml tests/e2e/topology.generated.yaml
eloqctl launch --skip-deps tests/e2e/topology.generated.yaml
```

To use a different EloqKV version, set `ELOQKV_VERSION` before sourcing:

```sh
export ELOQKV_VERSION=1.3.0
```

### Check status and connect

After launch, `eloqctl status <cluster>` prints both the status table and a
ready-to-copy `redis-cli` command:

```sh
eloqctl status test-e2e
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `STEPS` | `launch,py-stress,go-stress,ts-stress,remove` | Comma-separated steps |
| `WORKERS` | `16` | Total workers (split evenly: standalone / cluster client) |
| `DURATION_SECONDS` | `300` | Main stress duration per SDK step |
| `INFO_ONLY_DURATION_SECONDS` | `300` | Duration of the appended high-concurrency `INFO` burst |
| `INFO_ONLY_WORKERS` | `64` | Worker count for the appended `INFO` burst |
| `INFO_ONLY_INFLIGHT` | `16` | Inflight slots per worker for the appended `INFO` burst |
| `INFO_ONLY_REPEAT` | `50` | Repeat count per round for the appended `INFO` burst |
| `KEY_COUNT` | `256` | Preloaded key count |
| `CMD_TIMEOUT` | `5` | Per-command timeout (seconds) |
| `PROGRESS_INTERVAL` | `5` | Progress report interval (seconds) |
| `TLS_ENABLED` | `1` | Enable TLS on cluster and clients |
| `SKIP_DEPS` | `1` | Skip OS dep installation on nodes |

## Directory Layout

```
tests/
├── README.md
├── docker_env.sh                   # shared helpers: Docker Compose, SSH, MinIO
├── docker_ha/
│   ├── docker-compose.yaml         # 4-node Ubuntu + MinIO + stress containers
│   ├── Dockerfile                  # SSH image for eloq nodes
│   ├── Dockerfile.stress           # Python 3.13 + redis-py
│   ├── Dockerfile.stress_go        # Go 1.24 + go-redis/v9
│   ├── Dockerfile.stress_ts        # Node 22 + ioredis
│   ├── id_ed25519 / id_ed25519.pub # auto-generated SSH key
│   └── authorized_keys
└── e2e/
    ├── cmd_stress_test.sh          # main entry point ★
    ├── cmd_stress_py/
    │   ├── main.py                 # Python full-command stress client
    │   └── requirements.txt        # pinned redis-py dependency (5.0.1)
    ├── cmd_stress_go/
    │   ├── main.go                 # Go full-command stress client
    │   ├── go.mod / go.sum
    ├── cmd_stress_ts/
    │   ├── main.ts                 # TypeScript full-command stress client
    │   ├── package.json / package-lock.json / tsconfig.json
    └── topology.yaml               # cluster topology template
```

## Command Coverage

Each SDK stress test covers **104 Redis commands** across all families
(string, hash, list, set, sorted-set, generic/key, server/connection).

Every test runs **half the workers with a standalone client** (direct to master)
and **half with a cluster-aware client** (auto slot routing). Results for both
modes are reported separately. The standalone client always targets the master.
Only the cluster-aware client is configured to read from replicas.

After the normal mixed-command phase, each SDK appends a dedicated,
high-concurrency `INFO` burst.

TLS is enabled by default with self-signed certs (`rejectUnauthorized: false` /
`ssl_cert_reqs=CERT_NONE` / `InsecureSkipVerify`).

## Troubleshooting

If launch fails, rebuild Docker images:

```sh
cd tests/docker_ha && docker compose build --no-cache
```

Check cluster health:

```sh
eloqctl status test-e2e --wait 30
```

Logs auto-clean unless `KEEP_LOGS=1` is set.

## Connecting from Host

The cluster nodes are exposed on localhost ports. After `docker compose up` and
a successful `eloqctl launch`:

| Node | Host Port | Docker IP |
|------|-----------|-----------|
| node-1 | `127.0.0.1:16371` | `172.28.10.11:6379` |
| node-2 | `127.0.0.1:16372` | `172.28.10.12:6379` |
| node-3 | `127.0.0.1:16373` | `172.28.10.13:6379` |

### Check cluster topology

```sh
redis-cli -h 127.0.0.1 -p 16371 --tls --insecure -a testpass CLUSTER NODES
```

The output shows which node is `master` and which is `slave`:

```
<id> 172.28.10.11:6379@16380 myself,slave ...
<id> 172.28.10.12:6379@16380 master ...
```

### Connect with redis-py (Python)

```python
import ssl, redis
TLS = {'ssl': True, 'ssl_cert_reqs': ssl.CERT_NONE, 'ssl_check_hostname': False}

# standalone — connect directly to one node
r = redis.Redis(host='127.0.0.1', port=16371, password='testpass', **TLS)

# cluster mode — auto-routes to correct node
from redis.cluster import RedisCluster, ClusterNode
rc = RedisCluster(startup_nodes=[
    ClusterNode('127.0.0.1', 16371),
    ClusterNode('127.0.0.1', 16372),
], password='testpass', **TLS)
```

### Connect with go-redis (Go)

```go
// standalone
c := redis.NewClient(&redis.Options{
    Addr: "127.0.0.1:16371", Password: "testpass",
    TLSConfig: &tls.Config{InsecureSkipVerify: true},
})

// cluster
cc := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs: []string{"127.0.0.1:16371", "127.0.0.1:16372"},
    Password: "testpass",
    TLSConfig: &tls.Config{InsecureSkipVerify: true},
})
```

### Connect with ioredis (TypeScript)

```typescript
// standalone
const r = new Redis({
    host: "127.0.0.1", port: 16371, password: "testpass",
    tls: { rejectUnauthorized: false },
});

// cluster
const c = new Cluster([
    { host: "127.0.0.1", port: 16371 },
    { host: "127.0.0.1", port: 16372 },
], { redisOptions: { password: "testpass",
    tls: { rejectUnauthorized: false } } });
```
