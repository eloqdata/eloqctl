#!/bin/bash
# E2E multi-language command stress test for eloqkv cluster.
#
# Each language SDK runs inside its own Docker container (same subnet as cluster).
#   - Python  → stress-python   (redis-py)
#   - Go      → stress-go       (go-redis/v9)
#   - TS      → stress-ts       (ioredis)
#   - RESP    → resp-compat     (tair-opensource/resp-compatibility)
#
# Env overrides:
#   STEPS=launch,cluster-update,monitor-update,eloqctl-mutate,py-stress,go-stress,ts-stress,resp-compat,remove
#   DURATION_SECONDS=300   WORKERS=8   INFLIGHT=4   KEY_COUNT=256   REPEAT=10
#   CMD_TIMEOUT=5         INFO_CMD_TIMEOUT=10   PROGRESS_INTERVAL=5
#   TLS_ENABLED=1         SKIP_DEPS=1
#   RESP_COMPAT_VERSION=7.0.0
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../" && pwd)"
source "${REPO_ROOT}/tests/docker_env.sh"

CLUSTER="test-e2e"
TOPO="${SCRIPT_DIR}/topology.generated.yaml"
CONTROL_TOPO="${CONTROL_REPO_ROOT}/tests/e2e/topology.generated.yaml"
STEPS="${STEPS:-launch,cluster-update,monitor-update,eloqctl-mutate,py-stress,go-stress,ts-stress,resp-compat,remove}"

DURATION="${DURATION_SECONDS:-300}"
WORKERS="${WORKERS:-16}"
INFLIGHT="${INFLIGHT:-4}"
REPEAT="${REPEAT:-10}"
KEY_COUNT="${KEY_COUNT:-256}"
CMD_TIMEOUT="${CMD_TIMEOUT:-5}"
INFO_CMD_TIMEOUT="${INFO_CMD_TIMEOUT:-10}"
PROGRESS_INTERVAL="${PROGRESS_INTERVAL:-5}"
TLS_ENABLED="${TLS_ENABLED:-1}"
SKIP_DEPS="${SKIP_DEPS:-1}"
LAUNCH_TIMEOUT_SECONDS="${LAUNCH_TIMEOUT_SECONDS:-900}"
STATUS_WAIT_TIMEOUT_SECONDS="${STATUS_WAIT_TIMEOUT_SECONDS:-240}"
INFO_ONLY_WORKERS="${INFO_ONLY_WORKERS:-64}"
INFO_ONLY_INFLIGHT="${INFO_ONLY_INFLIGHT:-16}"
INFO_ONLY_REPEAT="${INFO_ONLY_REPEAT:-50}"
INFO_ONLY_DURATION="${INFO_ONLY_DURATION_SECONDS:-300}"
TS_INFO_ONLY_WORKERS="${TS_INFO_ONLY_WORKERS:-16}"
TS_INFO_ONLY_INFLIGHT="${TS_INFO_ONLY_INFLIGHT:-8}"
TS_INFO_ONLY_REPEAT="${TS_INFO_ONLY_REPEAT:-50}"
TS_INFO_ONLY_DURATION="${TS_INFO_ONLY_DURATION_SECONDS:-30}"
GRAFANA_UPDATE_URL="${GRAFANA_UPDATE_URL:-https://dl.grafana.com/grafana/release/13.0.1+security-01/grafana_13.0.1+security-01_25720641773_linux_amd64.tar.gz}"
ELOQKV_UPDATE_VERSION="${ELOQKV_UPDATE_VERSION:-${ELOQKV_VERSION:-1.3.1}}"
GRAFANA_HTTP_URL="${GRAFANA_HTTP_URL:-http://172.28.10.14:3301}"
ALERTMANAGER_HTTP_URL="${ALERTMANAGER_HTTP_URL:-http://172.28.10.14:9093}"
ALERTMANAGER_WEBHOOK_ADAPTER_HTTP_URL="${ALERTMANAGER_WEBHOOK_ADAPTER_HTTP_URL:-http://172.28.10.14:8080}"
RESP_COMPAT_VERSION="${RESP_COMPAT_VERSION:-7.0.0}"

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
    control_ssh_cmd "$@"
}

control_ssh_exec_string() {
    local remote_cmd
    printf -v remote_cmd '%q ' \
        env HOME=/home/eloq ELOQCTL_HOME="${CONTROL_ELOQCTL_HOME}" "${CONTROL_ELOQCTL}" "$@"
    printf 'ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o PasswordAuthentication=no -o BatchMode=yes -o ConnectTimeout=3 -i %q eloq@127.0.0.1 -p 2224 %q' \
        "${ELOQCTL_DOCKER_SSH_KEY}" \
        "bash -lc $(printf '%q' "${remote_cmd}")"
}

control_ssh_exec_string_with_timeout() {
    local timeout_seconds="$1"
    shift
    local remote_cmd
    printf -v remote_cmd '%q ' \
        timeout --kill-after=10s "${timeout_seconds}s" \
        env HOME=/home/eloq ELOQCTL_HOME="${CONTROL_ELOQCTL_HOME}" "${CONTROL_ELOQCTL}" "$@"
    printf 'ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o PasswordAuthentication=no -o BatchMode=yes -o ConnectTimeout=3 -i %q eloq@127.0.0.1 -p 2224 %q' \
        "${ELOQCTL_DOCKER_SSH_KEY}" \
        "bash -lc $(printf '%q' "${remote_cmd}")"
}

run_control_eloqctl_with_progress() {
    local timeout_seconds="$1"
    local log_file="$2"
    shift 2
    local subcommand="$1"
    local control_log_file="${CONTROL_ELOQCTL_HOME}/logs/last-${subcommand}.log"
    local observer_timeout=$((timeout_seconds + 60))
    run_with_progress "${observer_timeout}" "${log_file}" --eloq-log "${control_log_file}" \
        bash -lc "$(control_ssh_exec_string_with_timeout "${timeout_seconds}" "$@")" \
        || { dump_failure_diagnostics "${log_file}"; return 1; }
}

wait_cluster_ready() {
    local observer_timeout=$((STATUS_WAIT_TIMEOUT_SECONDS + 30))
    run_with_progress "${observer_timeout}" "${SCRIPT_DIR}/cmd-stress-status.log" --eloq-log "${CONTROL_ELOQCTL_HOME}/logs/last-status.log" \
        bash -lc "$(control_ssh_exec_string_with_timeout "${STATUS_WAIT_TIMEOUT_SECONDS}" status "${CLUSTER}" --wait 180)" >/dev/null 2>&1 \
        || { echo "FAIL: cluster not healthy"; return 1; }
}

wait_monitor_ready() {
    local url="${1:-${GRAFANA_HTTP_URL}}"
    for _ in $(seq 1 60); do
        if control_exec curl -fsS "${url}/api/health" >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    echo "FAIL: monitor endpoint not healthy at ${url}"
    return 1
}

wait_alertmanager_ready() {
    local url="${1:-${ALERTMANAGER_HTTP_URL}}"
    for _ in $(seq 1 60); do
        if control_exec curl -fsS "${url}/-/healthy" >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    echo "FAIL: alertmanager endpoint not healthy at ${url}"
    return 1
}

wait_alertmanager_webhook_adapter_ready() {
    local url="${1:-${ALERTMANAGER_WEBHOOK_ADAPTER_HTTP_URL}}"
    for _ in $(seq 1 60); do
        if control_exec curl -fsS "${url}" >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    echo "FAIL: alertmanager-webhook-adapter endpoint not healthy at ${url}"
    return 1
}

assert_cluster_registered() {
    control_eloqctl_cmd list | grep -Eq "[[:space:]]${CLUSTER}[[:space:]]" \
        || { echo "FAIL: eloqctl list does not contain cluster ${CLUSTER}"; return 1; }
}

refresh_monitor_status() {
    run_control_eloqctl_with_progress 180 "${SCRIPT_DIR}/monitor-status.log" \
        monitor status --cluster "${CLUSTER}" >/dev/null \
        || return 1
}

cluster_has_monitor_service() {
    local service="$1"
    grep -Eq "\\|[[:space:]]*${service}[[:space:]]*\\|" "${SCRIPT_DIR}/monitor-status.log"
}

assert_monitor_version() {
    local expected_major="$1"
    local url="${2:-${GRAFANA_HTTP_URL}}"
    local health
    health=$(control_exec curl -fsS "${url}/api/health") \
        || { echo "FAIL: failed to query monitor health from ${url}"; return 1; }
    echo "${health}" | grep -Eq "\"version\"[[:space:]]*:[[:space:]]*\"${expected_major}\\." \
        || {
            echo "FAIL: expected Grafana ${expected_major}.x, got: ${health}"
            return 1
        }
}

assert_export_contains() {
    local expected="$1"
    local export_file="/tmp/${CLUSTER}-export.yaml"
    control_eloqctl_cmd export "${CLUSTER}" --output "${export_file}" >/dev/null
    control_exec grep -F "${expected}" "${export_file}" >/dev/null \
        || { echo "FAIL: export does not contain expected text: ${expected}"; return 1; }
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
    nodes_info=$(docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" exec -T stress-python python3 -c "
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
    start_docker_env
    (cd "${REPO_ROOT}" && bash tests/install_control_eloqctl.sh)
    render_topology_for_control "${SCRIPT_DIR}/topology.yaml" "${TOPO}"
    prefetch_control_download_cache "${SCRIPT_DIR}/topology.yaml" "${GRAFANA_UPDATE_URL}"
    sync_control_download_cache
    control_exec test -f "${CONTROL_TOPO}" \
        || { echo "FAIL: control topology not found at ${CONTROL_TOPO}"; return 1; }
    control_eloqctl_cmd stop "${CLUSTER}" --all --force >/dev/null 2>&1 || true
    control_eloqctl_cmd remove "${CLUSTER}" --force >/dev/null 2>&1 || true
    local observer_timeout=$((LAUNCH_TIMEOUT_SECONDS + 60))
    run_with_progress "${observer_timeout}" "${SCRIPT_DIR}/launch-cmd-stress.log" --eloq-log "${CONTROL_ELOQCTL_HOME}/logs/last-launch.log" \
        bash -lc "$(control_ssh_exec_string_with_timeout "${LAUNCH_TIMEOUT_SECONDS}" launch "${launch_args[@]}" "${CONTROL_TOPO}")" \
        || { dump_failure_diagnostics "${SCRIPT_DIR}/launch-cmd-stress.log"; return 1; }
    run_with_progress "${STATUS_WAIT_TIMEOUT_SECONDS}" "${SCRIPT_DIR}/launch-cmd-stress.log" --eloq-log "${CONTROL_ELOQCTL_HOME}/logs/last-status.log" \
        bash -lc "$(control_ssh_exec_string_with_timeout "${STATUS_WAIT_TIMEOUT_SECONDS}" status "${CLUSTER}" --wait 180)" >/dev/null 2>&1 \
        || { echo "FAIL: cluster not healthy after launch"; return 1; }
    assert_cluster_registered || return 1
    refresh_monitor_status || { echo "FAIL: monitor status failed after launch"; return 1; }
    if cluster_has_monitor_service "grafana"; then
        wait_monitor_ready || return 1
    fi
    if cluster_has_monitor_service "alertmanager"; then
        wait_alertmanager_ready || return 1
    fi
    if cluster_has_monitor_service "alertmanager_webhook_adapter"; then
        wait_alertmanager_webhook_adapter_ready || return 1
    fi
    if cluster_has_monitor_service "grafana"; then
        assert_monitor_version 9 || return 1
    fi
    echo "  cluster ready"
    discover_master || return 1
}

do_monitor_update() {
    echo "=== Monitor Grafana update check ==="
    assert_export_contains "grafana-9.3.6.linux-amd64.tar.gz" || return 1
    assert_cluster_registered || return 1
    run_control_eloqctl_with_progress 420 "${SCRIPT_DIR}/cmd-stress-monitor-update.log" \
        monitor update --cluster "${CLUSTER}" --component grafana --url "${GRAFANA_UPDATE_URL}" \
        || return 1
    refresh_monitor_status || return 1
    if ! cluster_has_monitor_service "grafana"; then
        echo "FAIL: monitor status does not report grafana after update"
        return 1
    fi
    wait_monitor_ready || return 1
    assert_monitor_version 13 || return 1
    wait_cluster_ready || return 1
    assert_export_contains "${GRAFANA_UPDATE_URL}" || return 1
    echo "  grafana update verified"
}

do_cluster_update() {
    echo "=== EloqKV rolling update check (${ELOQKV_VERSION} -> ${ELOQKV_UPDATE_VERSION}) ==="
    assert_cluster_registered || return 1
    run_control_eloqctl_with_progress 900 "${SCRIPT_DIR}/cmd-stress-cluster-update.log" \
        update "${CLUSTER}" "${ELOQKV_UPDATE_VERSION}" --password "${PASSWD}" \
        || return 1
    wait_cluster_ready || return 1
    assert_export_contains "version: ${ELOQKV_UPDATE_VERSION}" || return 1
    refresh_monitor_status || return 1
    if cluster_has_monitor_service "grafana"; then
        wait_monitor_ready || return 1
    fi
    if cluster_has_monitor_service "alertmanager"; then
        wait_alertmanager_ready || return 1
    fi
    if cluster_has_monitor_service "alertmanager_webhook_adapter"; then
        wait_alertmanager_webhook_adapter_ready || return 1
    fi
    discover_master || return 1
    echo "  rolling update verified (${ELOQKV_VERSION} -> ${ELOQKV_UPDATE_VERSION})"
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

# ── Python stress (inside stress-python container) ──
do_py_stress() {
    discover_master || return 1
    echo "=== Python command stress (redis-py) ==="
    local py_startup_args=(--startup-node "${MASTER}")
    if [ -n "${REPLICA}" ]; then
        py_startup_args+=(--startup-node "${REPLICA}")
    fi
    docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" exec -T stress-python python3 -u tests/e2e/cmd_stress_py/main.py \
        "${py_startup_args[@]}" \
        --password "${PASSWD}" --cmd-timeout "${CMD_TIMEOUT}" \
        --progress-interval "${PROGRESS_INTERVAL}" --key-count "${KEY_COUNT}" \
        --workers "${WORKERS}" --inflight "${INFLIGHT}" --duration "${DURATION}" --repeat "${REPEAT}" \
        ${TLS_FLAG} 2>&1 | tee "${SCRIPT_DIR}/cmd-stress-py.log"
    local py_status=${PIPESTATUS[0]}
    [ ${py_status} -eq 0 ] || return ${py_status}

    echo "=== Python INFO burst (redis-py) ==="
    docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" exec -T stress-python python3 -u tests/e2e/cmd_stress_py/main.py \
        "${py_startup_args[@]}" \
        --password "${PASSWD}" --cmd-timeout "${INFO_CMD_TIMEOUT}" \
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
    docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" exec -T stress-go bash -c \
        'cd tests/e2e/cmd_stress_go && go mod download' 2>&1 || true
    docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" exec -T stress-go bash -c \
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
    docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" exec -T stress-go bash -c \
        "cd tests/e2e/cmd_stress_go && go run . \
            --startup-nodes '${STARTUP_NODES}' \
            --password '${PASSWD}' \
            --workers ${INFO_ONLY_WORKERS} \
            --inflight ${INFO_ONLY_INFLIGHT} \
            --duration ${INFO_ONLY_DURATION}s \
            --repeat ${INFO_ONLY_REPEAT} \
            --progress-interval ${PROGRESS_INTERVAL}s \
            --key-count ${KEY_COUNT} \
            --cmd-timeout ${INFO_CMD_TIMEOUT}s \
            --command-set info-only \
            $([ "${TLS_ENABLED}" = "1" ] && echo '--tls-insecure')" \
        2>&1 | tee "${SCRIPT_DIR}/cmd-stress-go-info.log"
    return ${PIPESTATUS[0]}
}

# ── TypeScript stress (inside stress-ts container) ──
do_ts_stress() {
    echo "=== TypeScript command stress (ioredis) ==="
    echo "  installing npm deps ..."
    docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" exec -T stress-ts bash -c \
        'cd tests/e2e/cmd_stress_ts && npm install --silent' 2>&1 || true
    docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" exec -T stress-ts bash -c \
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
    docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" exec -T stress-ts bash -c \
        "cd tests/e2e/cmd_stress_ts && npx tsx main.ts \
            --startup-nodes '${STARTUP_NODES}' \
            --password '${PASSWD}' \
            --workers ${TS_INFO_ONLY_WORKERS} \
            --inflight ${TS_INFO_ONLY_INFLIGHT} \
            --duration ${TS_INFO_ONLY_DURATION} \
            --repeat ${TS_INFO_ONLY_REPEAT} \
            --progress-interval ${PROGRESS_INTERVAL} \
            --key-count ${KEY_COUNT} \
            --cmd-timeout ${INFO_CMD_TIMEOUT} \
            --command-set info-only \
            --read-from-replicas 'false' \
            --tls-insecure '${TLS_ENABLED}'" \
        2>&1 | tee "${SCRIPT_DIR}/cmd-stress-ts-info.log"
    return ${PIPESTATUS[0]}
}

# ── RESP compatibility test (standalone + cluster against Redis 7.0) ──
do_resp_compat() {
    discover_master || return 1
    echo "=== RESP compatibility test (Redis ${RESP_COMPAT_VERSION}) ==="
    local master_host="${MASTER%:*}"
    local master_port="${MASTER##*:}"
    local compose_file="${DOCKER_E2E_DIR}/docker-compose.yaml"
    local standalone_log="${SCRIPT_DIR}/resp-compat-standalone.log"
    local cluster_log="${SCRIPT_DIR}/resp-compat-cluster.log"
    local summary_log="${SCRIPT_DIR}/resp-compat-summary.log"
    local cts="/tmp/cts_filtered.json"
    local script="/opt/resp-compatibility/resp_compatibility.py"

    # Generate summary report header
    {
        echo "╔══════════════════════════════════════════════╗"
        echo "║  RESP Compatibility Report (Redis ${RESP_COMPAT_VERSION})"
        printf "║  EloqKV: %-37s║\n" "eloqctl cluster ${CLUSTER}"
        echo "╚══════════════════════════════════════════════╝"
    } > "${summary_log}"

    # Filter out commands known to hang on EloqKV
    docker compose -f "${compose_file}" exec -T resp-compat python3 -c "
import json
with open('/opt/resp-compatibility/cts.json') as f:
    tests = json.load(f)
skip = {'script flush with SYNC', 'script flush with ASYNC'}
for t in tests:
    if t['name'] in skip:
        t['skipped'] = True
with open('${cts}','w') as f:
    json.dump(tests, f)
"

    echo "--- standalone mode ---"
    docker compose -f "${compose_file}" exec -T resp-compat bash -c \
        "python3 -u ${script} --host ${master_host} --port ${master_port} --password ${PASSWD} --testfile ${cts} --specific-version ${RESP_COMPAT_VERSION} --show-failed >/tmp/standalone.log 2>&1"
    docker compose -f "${compose_file}" cp resp-compat:/tmp/standalone.log "${standalone_log}"
    cat "${standalone_log}"
    local standalone_status=${PIPESTATUS[0]}

    # Extract standalone summary and failed tests
    {
        echo ""
        echo "─── Standalone Mode ───"
        grep "^Summary:" "${standalone_log}" || echo "Summary: N/A"
        echo ""
        echo "Failed tests:"
        awk '/^FailedTest/,/^$/' "${standalone_log}" | grep "^FailedTest" | sort || echo "  (none)"
    } >> "${summary_log}"

    # Wait for cluster to recover after standalone test
    echo "  waiting for cluster recovery..."
    for _ in $(seq 1 30); do
        if docker compose -f "${compose_file}" exec -T stress-python python3 -c "
import ssl; from redis import Redis
r=Redis(host='172.28.10.11',port=6379,password='${PASSWD}',socket_timeout=5,ssl=True,ssl_cert_reqs=ssl.CERT_NONE,ssl_check_hostname=False)
info=r.execute_command('CLUSTER','INFO').decode()
print('OK' if 'cluster_state:ok' in info else info.split('cluster_state:')[1].split('\\\\n')[0] if 'cluster_state:' in info else 'UNKNOWN')
r.close()
" 2>/dev/null | grep -q "OK"; then
            echo "  cluster recovered"
            break
        fi
        sleep 4
    done

    echo "--- cluster mode ---"
    docker compose -f "${compose_file}" exec -T resp-compat bash -c \
        "python3 -u ${script} --host ${master_host} --port ${master_port} --password ${PASSWD} --testfile ${cts} --specific-version ${RESP_COMPAT_VERSION} --show-failed --cluster >/tmp/cluster.log 2>&1"
    docker compose -f "${compose_file}" cp resp-compat:/tmp/cluster.log "${cluster_log}"
    cat "${cluster_log}"
    local cluster_status=${PIPESTATUS[0]}

    # Extract cluster summary and failed tests
    {
        echo ""
        echo "─── Cluster Mode ───"
        grep "^Summary:" "${cluster_log}" || echo "Summary: N/A"
        echo ""
        echo "Failed tests:"
        awk '/^FailedTest/,/^$/' "${cluster_log}" | grep "^FailedTest" | sort || echo "  (none)"
    } >> "${summary_log}"

    if [ ${standalone_status} -ne 0 ] || [ ${cluster_status} -ne 0 ]; then
        echo "FAIL: resp-compat standalone=${standalone_status} cluster=${cluster_status}"
        return 1
    fi
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
step cluster-update do_cluster_update
step monitor-update do_monitor_update
step eloqctl-mutate do_eloqctl_mutate
step py-stress do_py_stress
step go-stress do_go_stress
step ts-stress do_ts_stress
step resp-compat do_resp_compat
step remove do_remove

if [ -z "${FAILED_STEPS}" ]; then
    echo "ALL PASS"
fi
