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

## Run specific steps

```sh
# Launch only
STEPS=launch bash tests/e2e/cmd_stress_test.sh

# Stress only (against already-running cluster)
STEPS=py-stress,go-stress,ts-stress bash tests/e2e/cmd_stress_test.sh
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `STEPS` | `launch,py-stress,go-stress,ts-stress,remove` | Comma-separated steps |
| `WORKERS` | `16` | Total workers (split evenly: standalone / cluster client) |
| `DURATION_SECONDS` | `60` | Stress duration |
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
    │   └── main.py                 # Python full-command stress client
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
modes are reported separately.

TLS is enabled by default with self-signed certs (`rejectUnauthorized: false` /
`ssl_cert_reqs=CERT_NONE` / `InsecureSkipVerify`).

## Troubleshooting

If launch fails, rebuild Docker images:

```sh
cd tests/docker_ha && docker compose build --no-cache
```

Check cluster health:

```sh
~/.eloqctl/bin/eloqctl status test-e2e --wait 30
```

Logs auto-clean unless `KEEP_LOGS=1` is set.
