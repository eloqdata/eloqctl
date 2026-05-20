# E2E Tests

A single script (`tests/e2e/test.sh`) starts one Docker environment, deploys one EloqKV cluster, then runs all scenarios sequentially against it. The whole suite completes in ~5 minutes.

## Architecture

```
tests/
├── e2e/
│   ├── test.sh            ← single test entry point
│   └── topology.yaml      ← topology for the e2e cluster
├── docker_ha/              ← shared Docker infrastructure
│   ├── Dockerfile          ← Ubuntu 24.04 + SSH server
│   ├── docker-compose.yaml ← 3-node bridge network (172.28.10.x)
│   ├── topology.yaml       ← topology template (referenced by docker_env.sh)
│   ├── id_ed25519          ← SSH key pair for eloq user
│   └── authorized_keys
└── docker_env.sh           ← shared shell helpers
```

## How It Works

1. **Docker env starts once** — `docker_env.sh` builds an Ubuntu image, starts 3 containers on a bridge network, waits for SSH
2. **Cluster deploys once** — `eloqctl launch` installs deps, deploys EloqKV, boots one tx + one standby + one voter
3. **All scenarios run sequentially** — every test scenario operates on the same running cluster:
   - status, versions, list, export, connect
   - rolling update via `plan` + `apply`
   - scale add / remove
   - stop → check topology → start
   - failover (promote standby to master)
   - monitor status, log-service status
   - exec (remote shell command)
   - schema upgrade
   - remove (cleanup verification)
4. **Docker env destroyed once** — trap cleans up on exit or failure

## Prerequisites

```sh
# Build and install the dev eloqctl binary
scripts/install-dev.sh
```

Requires Docker and Rust toolchain (see repo README).

## Running

```sh
# Single script — runs everything
bash tests/e2e/test.sh

# Full push gate (includes format, build, clippy, E2E)
scripts/test-before-push.sh

# Install git pre-push hook (auto-runs test-before-push.sh)
scripts/install-git-hooks.sh
```

Override the binary path:
```sh
ELOQCTL=/custom/path/to/cluster_mgr bash tests/e2e/test.sh
```

## Command Coverage

### Covered by E2E

| Command | How tested |
|---------|-----------|
| `launch` | Full cluster deploy with deps, config, bootstrap, start |
| `status --wait` | After every mutation: verifies serving master + process health |
| `versions` | Lists available versions, output contains version numbers |
| `list` | Registered clusters include the test cluster |
| `export` | Exported YAML contains cluster_name |
| `connect` | Client command string exits successfully |
| `plan` + `apply` | Rolling update via checkpoint_interval change |
| `scale --add-nodes` | Adds new standby, verifies cluster stays healthy |
| `scale --remove-nodes` | Removes old standby, verifies cluster stays healthy |
| `stop + start` | Full stop/start cycle with intermediate `check` validation |
| `check` | Topology validation on stopped cluster (ports free etc.) |
| `failover` | Promotes standby to master, verifies cluster recovers |
| `monitor status` | Reports correctly even without monitor configured |
| `log-service status` | Reports correctly even without log-service configured |
| `exec` | Runs remote shell command on cluster hosts |
| `upgrade` | Schema upgrade exits successfully |
| `remove --force` | Cluster removed, no longer present in `list` |

### Not Covered

| Command | Reason |
|---------|--------|
| `deploy`, `install`, `run-deps` | Tested implicitly via `launch` |
| `restart` | Equivalent to `stop` + `start`, covered separately |
| `scalelog` | Requires `log_service` config with log binaries |
| `backup start/list/remove` | SSH endpoint mapping bug in `--dest-node` path |
| `update` | Requires version download from remote PostgreSQL |
| `proxy` | Legacy product, not active |
| `demo` | Interactive, not suitable for automated testing |
| `completion` | Shell generation, no runtime state to verify |

## Test Output

Passing output looks like:
```
[1/6] Launch cluster ... OK
[2/6] Verify cluster status ... OK
[3/6] Test read-only commands ... OK
[4/6] Rolling update ... OK
[5/6] Scale add standby ... OK
[6/6] Scale remove old standby ... OK
[7/14] Stop cluster and validate topology ... OK
[8/14] Restart cluster ... OK
[9/14] Failover standby to master ... OK
[10/14] Monitor and log-service status ... OK
[11/14] Exec custom remote command ... OK
[12/14] Schema upgrade ... OK
[13/14] Remove cluster ... OK
PASS: all E2E tests completed
```

## Adding New Tests

Add a new step to `tests/e2e/test.sh` following the existing pattern:
```sh
echo "[N/TOTAL] Your test description"
"${ELOQCTL}" your-command ... > "${SCRIPT_DIR}/your-command.log" 2>&1 || {
    echo "FAIL: your test"
    cat "${SCRIPT_DIR}/your-command.log"
    exit 1
}
echo "  OK"
```
