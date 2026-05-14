#!/bin/bash
# End-to-end test: deploy EloqKV HA into Ubuntu Docker containers via SSH.

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DOCKER_E2E_DIR="${SCRIPT_DIR}"
source "${REPO_ROOT}/tests/docker_env.sh"
CLUSTER="test-docker-ha"
TOPO="${SCRIPT_DIR}/topology.generated.yaml"
LAUNCH_TIMEOUT_SECONDS="${LAUNCH_TIMEOUT_SECONDS:-120}"
STATUS_TIMEOUT_SECONDS="${STATUS_TIMEOUT_SECONDS:-120}"

cleanup() {
    rc=$?
    timeout --kill-after=5s "${CLEANUP_TIMEOUT_SECONDS}s" "${ELOQCTL}" stop "${CLUSTER}" --all --force >/dev/null 2>&1 || true
    timeout --kill-after=5s "${CLEANUP_TIMEOUT_SECONDS}s" "${ELOQCTL}" remove "${CLUSTER}" --force >/dev/null 2>&1 || true
    compose_down
    if [ "${KEEP_E2E_LOGS:-0}" != "1" ]; then
        rm -f "${SCRIPT_DIR}/launch.log" "${SCRIPT_DIR}/status.log" "${TOPO}" \
              "${SCRIPT_DIR}/versions.log" "${SCRIPT_DIR}/list.log" \
              "${SCRIPT_DIR}/export.log" "${SCRIPT_DIR}/connect.log" \
              "${SCRIPT_DIR}/exported.yaml"
    fi
    exit "${rc}"
}
trap cleanup EXIT

render_topology "${SCRIPT_DIR}/topology.yaml" "${TOPO}"

start_docker_env

echo "[4/5] Launch EloqKV HA cluster"
"${ELOQCTL}" stop "${CLUSTER}" --all --force >/dev/null 2>&1 || true
"${ELOQCTL}" remove "${CLUSTER}" --force >/dev/null 2>&1 || true
set +e
run_with_progress "${LAUNCH_TIMEOUT_SECONDS}" "${SCRIPT_DIR}/launch.log" "${ELOQCTL}" launch "${TOPO}"
launch_rc=$?
set -e
if [ ${launch_rc} -ne 0 ]; then
    echo "FAIL: launch exited ${launch_rc}"
    dump_failure_diagnostics "${SCRIPT_DIR}/launch.log"
    exit 1
fi
if grep -q "FAIL" "${SCRIPT_DIR}/launch.log"; then
    echo "FAIL in launch:"
    dump_failure_diagnostics "${SCRIPT_DIR}/launch.log"
    exit 1
fi

echo "[5/9] Verify cluster status"
run_with_progress "${STATUS_TIMEOUT_SECONDS}" "${SCRIPT_DIR}/status.log" "${ELOQCTL}" status "${CLUSTER}" --wait 90 || {
    echo "FAIL: status --wait failed"
    dump_failure_diagnostics "${SCRIPT_DIR}/status.log"
    exit 1
}

echo "[6/9] Check available versions"
"${ELOQCTL}" versions > "${SCRIPT_DIR}/versions.log" 2>&1 || {
    echo "FAIL: versions"
    cat "${SCRIPT_DIR}/versions.log"
    exit 1
}
grep -q "1." "${SCRIPT_DIR}/versions.log" || {
    echo "FAIL: versions output does not list any version numbers"
    cat "${SCRIPT_DIR}/versions.log"
    exit 1
}
echo "  OK"

echo "[7/9] List registered clusters"
"${ELOQCTL}" list > "${SCRIPT_DIR}/list.log" 2>&1 || {
    echo "FAIL: list"
    cat "${SCRIPT_DIR}/list.log"
    exit 1
}
grep -q "${CLUSTER}" "${SCRIPT_DIR}/list.log" || {
    echo "FAIL: list does not contain ${CLUSTER}"
    cat "${SCRIPT_DIR}/list.log"
    exit 1
}
echo "  OK"

echo "[8/9] Export cluster topology"
"${ELOQCTL}" export "${CLUSTER}" --output "${SCRIPT_DIR}/exported.yaml" > "${SCRIPT_DIR}/export.log" 2>&1 || {
    echo "FAIL: export"
    cat "${SCRIPT_DIR}/export.log"
    exit 1
}
grep -q "cluster_name.*${CLUSTER}" "${SCRIPT_DIR}/exported.yaml" || {
    echo "FAIL: exported YAML missing cluster_name"
    cat "${SCRIPT_DIR}/exported.yaml"
    exit 1
}
echo "  OK"

echo "[9/9] Connect to cluster"
CLIENT=$("${ELOQCTL}" -q connect "${CLUSTER}" 2>"${SCRIPT_DIR}/connect.log") || {
    echo "FAIL: connect"
    cat "${SCRIPT_DIR}/connect.log"
    exit 1
}
echo "  OK"

echo ""
echo "PASS: Docker HA EloqKV cluster deployed and healthy, all commands verified"
