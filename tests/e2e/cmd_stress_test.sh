#!/bin/bash
# E2E multi-language command stress test for eloqkv cluster.
#
# Each language SDK runs inside its own Docker container (same subnet as cluster).
#   - Python  → stress-client   (redis-py)
#   - Go      → stress-go       (go-redis/v9)
#   - TS      → stress-ts       (ioredis)
#
# Env overrides:
#   STEPS=launch,py-stress,go-stress,ts-stress,remove
#   DURATION_SECONDS=60   WORKERS=8   KEY_COUNT=256
#   CMD_TIMEOUT=5         PROGRESS_INTERVAL=5
#   TLS_ENABLED=1         SKIP_DEPS=1
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../" && pwd)"
source "${REPO_ROOT}/tests/docker_env.sh"

CLUSTER="test-e2e"
TOPO="${SCRIPT_DIR}/topology.generated.yaml"
CONTROL_TOPO="${CONTROL_REPO_ROOT}/tests/e2e/topology.generated.yaml"
STEPS="${STEPS:-launch,py-stress,go-stress,ts-stress,remove}"

DURATION="${DURATION_SECONDS:-60}"
WORKERS="${WORKERS:-16}"
REPEAT="${REPEAT:-10}"
KEY_COUNT="${KEY_COUNT:-256}"
CMD_TIMEOUT="${CMD_TIMEOUT:-5}"
PROGRESS_INTERVAL="${PROGRESS_INTERVAL:-5}"
TLS_ENABLED="${TLS_ENABLED:-1}"
SKIP_DEPS="${SKIP_DEPS:-1}"

PASSWD="testpass"
N1="172.28.10.11"
N2="172.28.10.12"
STARTUP_NODES="${N1}:6379,${N2}:6379"
MASTER=""  # discovered after cluster launch
REPLICA=""

TLS_FLAG=""
[ "${TLS_ENABLED}" = "1" ] && TLS_FLAG="--tls"

control_eloqctl_cmd() {
    control_exec env ELOQCTL_HOME="${CONTROL_ELOQCTL_HOME}" "${CONTROL_ELOQCTL}" "$@"
}

cleanup() {
    rc=$?
    if [ "${KEEP_LOGS:-0}" != "1" ]; then
        rm -f "${SCRIPT_DIR}/cmd-stress-"*.log "${SCRIPT_DIR}/launch-cmd-stress.log" "${TOPO}"
    fi
    exit "${rc}"
}
trap cleanup EXIT

step() {
    local name="$1"; shift
    if [[ ",${STEPS}," == *",${name},"* ]]; then
        echo "=== step: ${name} ==="
        "$@"
    else
        echo "[skip] ${name}"
    fi
}

# ── Launch ──
do_launch() {
    echo "=== Launch cluster ==="
    render_topology_for_control "${SCRIPT_DIR}/topology.yaml" "${TOPO}"
    start_docker_env
    control_eloqctl_cmd stop "${CLUSTER}" --all --force >/dev/null 2>&1 || true
    control_eloqctl_cmd remove "${CLUSTER}" --force >/dev/null 2>&1 || true
    run_with_progress 420 "${SCRIPT_DIR}/launch-cmd-stress.log" \
        docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" exec -T -u eloq \
            "${CONTROL_NODE_SERVICE}" env HOME=/home/eloq ELOQCTL_HOME="${CONTROL_ELOQCTL_HOME}" \
            "${CONTROL_ELOQCTL}" launch $([ "${SKIP_DEPS}" = "1" ] && echo "--skip-deps") "${CONTROL_TOPO}" \
        || { echo "FAIL: launch failed"; dump_failure_diagnostics "${SCRIPT_DIR}/launch-cmd-stress.log"; exit 1; }
    run_with_progress 240 "${SCRIPT_DIR}/launch-cmd-stress.log" \
        docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" exec -T -u eloq \
            "${CONTROL_NODE_SERVICE}" env HOME=/home/eloq ELOQCTL_HOME="${CONTROL_ELOQCTL_HOME}" \
            "${CONTROL_ELOQCTL}" status "${CLUSTER}" --wait 180 >/dev/null 2>&1 \
        || { echo "FAIL: cluster not healthy after launch"; exit 1; }
    echo "  cluster ready"

    # Dynamically discover actual master from CLUSTER NODES
    echo "  discovering cluster topology ..."
    local nodes_info
    nodes_info=$(compose exec -T stress-client python3 -c "
import ssl
TLS={'ssl':True,'ssl_cert_reqs':ssl.CERT_NONE,'ssl_check_hostname':False}
from redis import Redis
r=Redis(host='${N1}',port=6379,password='${PASSWD}',socket_timeout=5,**TLS)
print(r.execute_command('CLUSTER','NODES').decode())
r.close()
" 2>/dev/null)
    while IFS= read -r line; do
        local addr role
        addr=$(echo "$line" | awk '{print $2}' | cut -d@ -f1)
        role=$(echo "$line" | awk '{print $3}')
        if echo "$role" | grep -q 'master'; then
            MASTER="${addr}"
        elif echo "$role" | grep -q 'slave'; then
            REPLICA="${addr}"
        fi
    done <<< "$nodes_info"
    if [ -z "$MASTER" ]; then
        echo "FAIL: could not discover master from CLUSTER NODES"
        exit 1
    fi
    echo "  master=${MASTER} replica=${REPLICA}"
    echo "  waiting for cluster stabilization ..."
    sleep 5
}

# ── Python stress (inside stress-client container) ──
do_py_stress() {
    echo "=== Python command stress (redis-py) ==="
    compose exec -T stress-client python3 -u tests/e2e/cmd_stress_py/main.py \
        --startup-node "${MASTER}" --startup-node "${REPLICA}" \
        --password "${PASSWD}" --cmd-timeout "${CMD_TIMEOUT}" \
        --progress-interval "${PROGRESS_INTERVAL}" --key-count "${KEY_COUNT}" \
        --workers "${WORKERS}" --duration "${DURATION}" --read-from-replicas \
        --repeat "${REPEAT}" \
        ${TLS_FLAG} 2>&1 | tee "${SCRIPT_DIR}/cmd-stress-py.log"
    local rc=${PIPESTATUS[0]}
    [ "${rc}" -eq 0 ] || { echo "FAIL: Python stress failed (rc=${rc})"; exit 1; }
    echo "  Python stress PASS"
}

# ── Go stress (inside stress-go container) ──
do_go_stress() {
    echo "=== Go command stress (go-redis/v9) ==="
    # Download Go module deps inside container (cached in GOPATH pkg dir)
    echo "  installing Go deps ..."
    compose exec -T stress-go bash -c \
        'cd tests/e2e/cmd_stress_go && go mod download' 2>&1 || true
    # Build and run
    compose exec -T stress-go bash -c \
        "cd tests/e2e/cmd_stress_go && go run . \
            --startup-nodes '${STARTUP_NODES}' \
            --password '${PASSWD}' \
            --workers ${WORKERS} \
            --duration ${DURATION}s \
            --progress-interval ${PROGRESS_INTERVAL}s \
            --key-count ${KEY_COUNT} \
            --repeat ${REPEAT} \
            --cmd-timeout ${CMD_TIMEOUT}s \
            $([ "${TLS_ENABLED}" = "1" ] && echo '--tls-insecure')" \
        2>&1 | tee "${SCRIPT_DIR}/cmd-stress-go.log"
    local rc=${PIPESTATUS[0]}
    [ "${rc}" -eq 0 ] || { echo "FAIL: Go stress failed (rc=${rc})"; exit 1; }
    echo "  Go stress PASS"
}

# ── TypeScript stress (inside stress-ts container) ──
do_ts_stress() {
    echo "=== TypeScript command stress (ioredis) ==="
    # Install npm deps inside container (cached in node_modules)
    echo "  installing npm deps ..."
    compose exec -T stress-ts bash -c \
        'cd tests/e2e/cmd_stress_ts && npm install --silent' 2>&1 || true
    # Run
    compose exec -T stress-ts bash -c \
        "cd tests/e2e/cmd_stress_ts && npx tsx main.ts \
            --startup-nodes '${STARTUP_NODES}' \
            --password '${PASSWD}' \
            --workers ${WORKERS} \
            --duration ${DURATION} \
            --progress-interval ${PROGRESS_INTERVAL} \
            --key-count ${KEY_COUNT} \
            --repeat ${REPEAT} \
            --cmd-timeout ${CMD_TIMEOUT} \
            --tls-insecure '${TLS_ENABLED}'" \
        2>&1 | tee "${SCRIPT_DIR}/cmd-stress-ts.log"
    local rc=${PIPESTATUS[0]}
    [ "${rc}" -eq 0 ] || { echo "FAIL: TypeScript stress failed (rc=${rc})"; exit 1; }
    echo "  TypeScript stress PASS"
}

# ── Remove ──
do_remove() {
    echo "=== Remove cluster ==="
    control_eloqctl_cmd stop "${CLUSTER}" --all --force >/dev/null 2>&1 || true
    control_eloqctl_cmd remove "${CLUSTER}" --force >/dev/null 2>&1 || true
    compose_down
    echo "  removed"
}

step launch do_launch
step py-stress do_py_stress
step go-stress do_go_stress
step ts-stress do_ts_stress
step remove do_remove

echo "ALL PASS"
