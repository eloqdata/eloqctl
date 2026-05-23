#!/bin/bash
# E2E multi-language command stress test for eloqkv cluster.
#
# Each language SDK runs inside its own Docker container (same subnet as cluster).
#   - Python  → stress-client   (redis-py)
#   - Go      → stress-go       (go-redis/v9)
#   - TS      → stress-ts       (ioredis)
#
# Env overrides:
#   STEPS=launch,eloqctl-mutate,py-stress,go-stress,ts-stress,remove
#   DURATION_SECONDS=300   WORKERS=8   INFLIGHT=4   KEY_COUNT=256   REPEAT=10
#   CMD_TIMEOUT=5         PROGRESS_INTERVAL=5
#   TLS_ENABLED=1         SKIP_DEPS=1
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../" && pwd)"
source "${REPO_ROOT}/tests/docker_env.sh"

CLUSTER="test-e2e"
TOPO="${SCRIPT_DIR}/topology.generated.yaml"
CONTROL_TOPO="${CONTROL_REPO_ROOT}/tests/e2e/topology.generated.yaml"
STEPS="${STEPS:-launch,eloqctl-mutate,py-stress,go-stress,ts-stress,remove}"

DURATION="${DURATION_SECONDS:-300}"
WORKERS="${WORKERS:-16}"
INFLIGHT="${INFLIGHT:-4}"
REPEAT="${REPEAT:-10}"
KEY_COUNT="${KEY_COUNT:-256}"
CMD_TIMEOUT="${CMD_TIMEOUT:-5}"
PROGRESS_INTERVAL="${PROGRESS_INTERVAL:-5}"
TLS_ENABLED="${TLS_ENABLED:-1}"
SKIP_DEPS="${SKIP_DEPS:-1}"
INFO_ONLY_WORKERS="${INFO_ONLY_WORKERS:-64}"
INFO_ONLY_INFLIGHT="${INFO_ONLY_INFLIGHT:-16}"
INFO_ONLY_REPEAT="${INFO_ONLY_REPEAT:-50}"
INFO_ONLY_DURATION="${INFO_ONLY_DURATION_SECONDS:-300}"

PASSWD="testpass"
N1="172.28.10.11"
N2="172.28.10.12"
STARTUP_NODES="${N1}:6379,${N2}:6379"
MASTER=""  # discovered dynamically
REPLICA=""

# Accumulated failures across steps
FAILED_STEPS=""

TLS_FLAG=""
[ "${TLS_ENABLED}" = "1" ] && TLS_FLAG="--tls"

control_eloqctl_cmd() {
    control_exec env ELOQCTL_HOME="${CONTROL_ELOQCTL_HOME}" "${CONTROL_ELOQCTL}" "$@"
}

run_control_eloqctl_with_progress() {
    local timeout_seconds="$1"
    local log_file="$2"
    shift 2
    run_with_progress "${timeout_seconds}" "${log_file}" \
        docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" exec -T -u eloq \
            "${CONTROL_NODE_SERVICE}" env HOME=/home/eloq ELOQCTL_HOME="${CONTROL_ELOQCTL_HOME}" \
            "${CONTROL_ELOQCTL}" "$@" \
        || { dump_failure_diagnostics "${log_file}"; return 1; }
}

wait_cluster_ready() {
    run_with_progress 240 "${SCRIPT_DIR}/cmd-stress-status.log" \
        docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" exec -T -u eloq \
            "${CONTROL_NODE_SERVICE}" env HOME=/home/eloq ELOQCTL_HOME="${CONTROL_ELOQCTL_HOME}" \
            "${CONTROL_ELOQCTL}" status "${CLUSTER}" --wait 180 >/dev/null 2>&1 \
        || { echo "FAIL: cluster not healthy"; return 1; }
}

cleanup() {
    if [ "${KEEP_LOGS:-0}" != "1" ]; then
        rm -f "${SCRIPT_DIR}/cmd-stress-"*.log "${SCRIPT_DIR}/launch-cmd-stress.log" "${TOPO}"
    fi
    if [ -n "${FAILED_STEPS}" ]; then
        echo ""
        echo "FAILED STEPS: ${FAILED_STEPS}"
        exit 1
    fi
}
trap cleanup EXIT

step() {
    local name="$1"; shift
    if [[ ",${STEPS}," == *",${name},"* ]]; then
        echo "=== step: ${name} ==="
        if "$@"; then
            echo "  ${name} PASS"
        else
            echo "  ${name} FAIL"
            FAILED_STEPS="${FAILED_STEPS} ${name}"
        fi
    else
        echo "[skip] ${name}"
    fi
}

# ── Master discovery (works independently of launch) ──
discover_master() {
    echo "  discovering cluster topology ..."
    local nodes_info
    nodes_info=$(compose exec -T stress-client python3 -c "
import ssl
TLS={'ssl':True,'ssl_cert_reqs':ssl.CERT_NONE,'ssl_check_hostname':False}
from redis import Redis
r=Redis(host='${N1}',port=6379,password='${PASSWD}',socket_timeout=5,**TLS)
print(r.execute_command('CLUSTER','NODES').decode())
r.close()
" 2>/dev/null) || { echo "FAIL: cannot connect to cluster for discovery"; return 1; }

    MASTER=""
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
        return 1
    fi
    echo "  master=${MASTER} replica=${REPLICA}"
    sleep 5
    return 0
}

# ── Launch ──
do_launch() {
    echo "=== Launch cluster ==="
    local launch_args=()
    if [ "${SKIP_DEPS}" = "1" ]; then
        launch_args+=(--skip-deps)
    fi
    render_topology_for_control "${SCRIPT_DIR}/topology.yaml" "${TOPO}"
    start_docker_env
    control_eloqctl_cmd stop "${CLUSTER}" --all --force >/dev/null 2>&1 || true
    control_eloqctl_cmd remove "${CLUSTER}" --force >/dev/null 2>&1 || true
    run_with_progress 420 "${SCRIPT_DIR}/launch-cmd-stress.log" \
        docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" exec -T -u eloq \
            "${CONTROL_NODE_SERVICE}" env HOME=/home/eloq ELOQCTL_HOME="${CONTROL_ELOQCTL_HOME}" \
            "${CONTROL_ELOQCTL}" launch "${launch_args[@]}" "${CONTROL_TOPO}" \
        || { dump_failure_diagnostics "${SCRIPT_DIR}/launch-cmd-stress.log"; return 1; }
    run_with_progress 240 "${SCRIPT_DIR}/launch-cmd-stress.log" \
        docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" exec -T -u eloq \
            "${CONTROL_NODE_SERVICE}" env HOME=/home/eloq ELOQCTL_HOME="${CONTROL_ELOQCTL_HOME}" \
            "${CONTROL_ELOQCTL}" status "${CLUSTER}" --wait 180 >/dev/null 2>&1 \
        || { echo "FAIL: cluster not healthy after launch"; return 1; }
    echo "  cluster ready"
    discover_master || return 1
}

do_eloqctl_mutate() {
    local original_master original_replica
    local original_master_host original_master_port
    local original_replica_host original_replica_port
    local current_master_host current_master_port

    discover_master || return 1
    original_master="${MASTER}"
    original_replica="${REPLICA}"
    if [ -z "${original_replica}" ]; then
        echo "FAIL: no replica discovered for failover test"
        return 1
    fi

    original_master_host="${original_master%:*}"
    original_master_port="${original_master##*:}"
    original_replica_host="${original_replica%:*}"
    original_replica_port="${original_replica##*:}"

    echo "=== eloqctl mutation check ==="

    echo "  failover ${original_master} -> ${original_replica}"
    run_control_eloqctl_with_progress 240 "${SCRIPT_DIR}/cmd-stress-failover-1.log" \
        failover "${CLUSTER}" \
        --old-leader-host "${original_master_host}" --old-leader-port "${original_master_port}" \
        --new-leader-host "${original_replica_host}" --new-leader-port "${original_replica_port}" \
        --password "${PASSWD}" \
        || return 1
    wait_cluster_ready || return 1
    discover_master || return 1
    if [ "${MASTER}" != "${original_replica}" ]; then
        echo "FAIL: failover did not move master to ${original_replica}; current master=${MASTER}"
        return 1
    fi

    current_master_host="${MASTER%:*}"
    current_master_port="${MASTER##*:}"
    echo "  failover ${MASTER} -> ${original_master}"
    run_control_eloqctl_with_progress 240 "${SCRIPT_DIR}/cmd-stress-failover-2.log" \
        failover "${CLUSTER}" \
        --old-leader-host "${current_master_host}" --old-leader-port "${current_master_port}" \
        --new-leader-host "${original_master_host}" --new-leader-port "${original_master_port}" \
        --password "${PASSWD}" \
        || return 1
    wait_cluster_ready || return 1
    discover_master || return 1
    if [ "${MASTER}" != "${original_master}" ]; then
        echo "FAIL: failback did not restore master to ${original_master}; current master=${MASTER}"
        return 1
    fi

    echo "  eloqctl mutations verified"
}

# ── Python stress (inside stress-client container) ──
do_py_stress() {
    discover_master || return 1
    echo "=== Python command stress (redis-py) ==="
    local py_startup_args=(--startup-node "${MASTER}")
    if [ -n "${REPLICA}" ]; then
        py_startup_args+=(--startup-node "${REPLICA}")
    fi
    compose exec -T stress-client python3 -u tests/e2e/cmd_stress_py/main.py \
        "${py_startup_args[@]}" \
        --password "${PASSWD}" --cmd-timeout "${CMD_TIMEOUT}" \
        --progress-interval "${PROGRESS_INTERVAL}" --key-count "${KEY_COUNT}" \
        --workers "${WORKERS}" --inflight "${INFLIGHT}" --duration "${DURATION}" --repeat "${REPEAT}" \
        ${TLS_FLAG} 2>&1 | tee "${SCRIPT_DIR}/cmd-stress-py.log"
    local py_status=${PIPESTATUS[0]}
    [ ${py_status} -eq 0 ] || return ${py_status}

    echo "=== Python INFO burst (redis-py) ==="
    compose exec -T stress-client python3 -u tests/e2e/cmd_stress_py/main.py \
        "${py_startup_args[@]}" \
        --password "${PASSWD}" --cmd-timeout "${CMD_TIMEOUT}" \
        --progress-interval "${PROGRESS_INTERVAL}" --key-count "${KEY_COUNT}" \
        --workers "${INFO_ONLY_WORKERS}" --inflight "${INFO_ONLY_INFLIGHT}" \
        --duration "${INFO_ONLY_DURATION}" --repeat "${INFO_ONLY_REPEAT}" \
        --command-set info-only \
        ${TLS_FLAG} 2>&1 | tee "${SCRIPT_DIR}/cmd-stress-py-info.log"
    return ${PIPESTATUS[0]}
}

# ── Go stress (inside stress-go container) ──
do_go_stress() {
    echo "=== Go command stress (go-redis/v9) ==="
    echo "  installing Go deps ..."
    compose exec -T stress-go bash -c \
        'cd tests/e2e/cmd_stress_go && go mod download' 2>&1 || true
    compose exec -T stress-go bash -c \
        "cd tests/e2e/cmd_stress_go && go run . \
            --startup-nodes '${STARTUP_NODES}' \
            --password '${PASSWD}' \
            --workers ${WORKERS} \
            --inflight ${INFLIGHT} \
            --duration ${DURATION}s \
            --repeat ${REPEAT} \
            --progress-interval ${PROGRESS_INTERVAL}s \
            --key-count ${KEY_COUNT} \
            --cmd-timeout ${CMD_TIMEOUT}s \
            $([ "${TLS_ENABLED}" = "1" ] && echo '--tls-insecure')" \
        2>&1 | tee "${SCRIPT_DIR}/cmd-stress-go.log"
    local go_status=${PIPESTATUS[0]}
    [ ${go_status} -eq 0 ] || return ${go_status}

    echo "=== Go INFO burst (go-redis/v9) ==="
    compose exec -T stress-go bash -c \
        "cd tests/e2e/cmd_stress_go && go run . \
            --startup-nodes '${STARTUP_NODES}' \
            --password '${PASSWD}' \
            --workers ${INFO_ONLY_WORKERS} \
            --inflight ${INFO_ONLY_INFLIGHT} \
            --duration ${INFO_ONLY_DURATION}s \
            --repeat ${INFO_ONLY_REPEAT} \
            --progress-interval ${PROGRESS_INTERVAL}s \
            --key-count ${KEY_COUNT} \
            --cmd-timeout ${CMD_TIMEOUT}s \
            --command-set info-only \
            $([ "${TLS_ENABLED}" = "1" ] && echo '--tls-insecure')" \
        2>&1 | tee "${SCRIPT_DIR}/cmd-stress-go-info.log"
    return ${PIPESTATUS[0]}
}

# ── TypeScript stress (inside stress-ts container) ──
do_ts_stress() {
    echo "=== TypeScript command stress (ioredis) ==="
    echo "  installing npm deps ..."
    compose exec -T stress-ts bash -c \
        'cd tests/e2e/cmd_stress_ts && npm install --silent' 2>&1 || true
    compose exec -T stress-ts bash -c \
        "cd tests/e2e/cmd_stress_ts && npx tsx main.ts \
            --startup-nodes '${STARTUP_NODES}' \
            --password '${PASSWD}' \
            --workers ${WORKERS} \
            --inflight ${INFLIGHT} \
            --duration ${DURATION} \
            --repeat ${REPEAT} \
            --progress-interval ${PROGRESS_INTERVAL} \
            --key-count ${KEY_COUNT} \
            --cmd-timeout ${CMD_TIMEOUT} \
            --read-from-replicas 'false' \
            --tls-insecure '${TLS_ENABLED}'" \
        2>&1 | tee "${SCRIPT_DIR}/cmd-stress-ts.log"
    local ts_status=${PIPESTATUS[0]}
    [ ${ts_status} -eq 0 ] || return ${ts_status}

    echo "=== TypeScript INFO burst (ioredis) ==="
    compose exec -T stress-ts bash -c \
        "cd tests/e2e/cmd_stress_ts && npx tsx main.ts \
            --startup-nodes '${STARTUP_NODES}' \
            --password '${PASSWD}' \
            --workers ${INFO_ONLY_WORKERS} \
            --inflight ${INFO_ONLY_INFLIGHT} \
            --duration ${INFO_ONLY_DURATION} \
            --repeat ${INFO_ONLY_REPEAT} \
            --progress-interval ${PROGRESS_INTERVAL} \
            --key-count ${KEY_COUNT} \
            --cmd-timeout ${CMD_TIMEOUT} \
            --command-set info-only \
            --read-from-replicas 'false' \
            --tls-insecure '${TLS_ENABLED}'" \
        2>&1 | tee "${SCRIPT_DIR}/cmd-stress-ts-info.log"
    return ${PIPESTATUS[0]}
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
step eloqctl-mutate do_eloqctl_mutate
step py-stress do_py_stress
step go-stress do_go_stress
step ts-stress do_ts_stress
step remove do_remove

if [ -z "${FAILED_STEPS}" ]; then
    echo "ALL PASS"
fi
