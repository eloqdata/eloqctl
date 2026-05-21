#!/bin/bash
# Standalone stress test: launch a cluster, run 30K concurrent connections, verify health.
#   STEPS=launch,stress,remove   # comma-separated steps (default: all)
#   CONNECTIONS=30000            # number of concurrent connections
#   CMD_TIMEOUT=5                # per-command timeout in seconds
#   SKIP_CLEANUP=1               # keep containers after script exits
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../" && pwd)"
DOCKER_E2E_DIR="${REPO_ROOT}/tests/docker_ha"
source "${REPO_ROOT}/tests/docker_env.sh"
CLUSTER="test-stress"
TOPO="${SCRIPT_DIR}/topology.generated.yaml"
STEPS="${STEPS:-launch,stress,remove}"
CONNECTIONS="${CONNECTIONS:-30000}"
CMD_TIMEOUT="${CMD_TIMEOUT:-5}"
PASSWD="testpass"
MASTER_HOST="172.28.10.11"
MASTER_PORT="6379"
SSH_KEY="${ELOQCTL_DOCKER_SSH_KEY}"

cleanup() {
    rc=$?
    if [ "${KEEP_LOGS:-0}" != "1" ]; then
        rm -f "${SCRIPT_DIR}/stress-"*.log "${SCRIPT_DIR}/launch-stress.log" "${TOPO}"
    fi
    exit "${rc}"
}
trap cleanup EXIT

step() {
    local name="$1"; shift
    if [[ ",${STEPS}," == *",${name},"* ]]; then
        "$@"
    else
        echo "[skip] ${name}"
    fi
}

do_launch() {
    echo "=== Launch cluster ==="
    render_topology "${SCRIPT_DIR}/topology.yaml" "${TOPO}"
    start_docker_env
    "${ELOQCTL}" stop "${CLUSTER}" --all --force >/dev/null 2>&1 || true
    "${ELOQCTL}" remove "${CLUSTER}" --force >/dev/null 2>&1 || true
    run_with_progress 300 "${SCRIPT_DIR}/launch-stress.log" "${ELOQCTL}" launch "${TOPO}" || {
        echo "FAIL: launch failed"; dump_failure_diagnostics "${SCRIPT_DIR}/launch-stress.log"; exit 1
    }
    run_with_progress 180 "${SCRIPT_DIR}/launch-stress.log" "${ELOQCTL}" status "${CLUSTER}" --wait 120 >/dev/null 2>&1 || {
        echo "FAIL: cluster not healthy after launch"; exit 1
    }
    echo "  cluster ready"
}

do_stress() {
    echo "=== Stress test: ${CONNECTIONS} connections ==="
    echo "  installing redis-py..."
    for port in 2221 2222 2223; do
        ssh_cmd "${port}" "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3-pip >/dev/null 2>&1 && pip3 install --quiet redis >/dev/null 2>&1" &
    done
    wait
    scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no \
        -o PasswordAuthentication=no -o BatchMode=yes -o ConnectTimeout=10 \
        -i "${SSH_KEY}" -P 2221 \
        "${SCRIPT_DIR}/stress.py" "eloq@127.0.0.1:/home/eloq/${CLUSTER}/stress.py" \
        >/dev/null 2>&1 || { echo "FAIL: cannot upload stress script"; exit 1; }
    ssh_cmd 2221 "python3 /home/eloq/${CLUSTER}/stress.py --host ${MASTER_HOST} --port ${MASTER_PORT} --password ${PASSWD} --connections ${CONNECTIONS} --cmd-timeout ${CMD_TIMEOUT}" 2>&1 || {
        echo "FAIL: stress test failed"; exit 1
    }
    echo "  verifying cluster health..."
    "${ELOQCTL}" status "${CLUSTER}" --wait 30 >/dev/null 2>&1 || { echo "FAIL: unhealthy after stress"; exit 1; }
    echo "  OK"
}

do_remove() {
    echo "=== Remove cluster ==="
    "${ELOQCTL}" stop "${CLUSTER}" --all --force >/dev/null 2>&1 || true
    "${ELOQCTL}" remove "${CLUSTER}" --force >/dev/null 2>&1 || true
    compose_down
    echo "  removed"
}

step launch do_launch
step stress do_stress
step remove do_remove

echo "PASS"
