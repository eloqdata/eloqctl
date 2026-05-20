#!/bin/bash
# End-to-end test: deploy EloqKV once, run all scenarios sequentially.
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../" && pwd)"
DOCKER_E2E_DIR="${REPO_ROOT}/tests/docker_ha"
source "${REPO_ROOT}/tests/docker_env.sh"
CLUSTER="test-e2e"
TOPO="${SCRIPT_DIR}/topology.generated.yaml"
LAUNCH_TIMEOUT_SECONDS="${LAUNCH_TIMEOUT_SECONDS:-120}"
STATUS_TIMEOUT_SECONDS="${STATUS_TIMEOUT_SECONDS:-120}"

cleanup() {
    rc=$?
    timeout --kill-after=5s "${CLEANUP_TIMEOUT_SECONDS}s" "${ELOQCTL}" stop "${CLUSTER}" --all --force >/dev/null 2>&1 || true
    timeout --kill-after=5s "${CLEANUP_TIMEOUT_SECONDS}s" "${ELOQCTL}" remove "${CLUSTER}" --force >/dev/null 2>&1 || true
    compose_down
    if [ "${KEEP_E2E_LOGS:-0}" != "1" ]; then
        rm -f "${SCRIPT_DIR}/"*.log "${TOPO}" "${SCRIPT_DIR}/exported.yaml"
    fi
    if [ ${rc} -ne 0 ]; then
        echo "FAIL: e2e suite failed"
    fi
    exit "${rc}"
}
trap cleanup EXIT

render_topology "${SCRIPT_DIR}/topology.yaml" "${TOPO}"
start_docker_env

# ---- [1] Launch cluster ----
echo "[1/6] Launch cluster"
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
echo "  OK"

# ---- [2] Verify status ----
echo "[2/6] Verify cluster status"
run_with_progress "${STATUS_TIMEOUT_SECONDS}" "${SCRIPT_DIR}/status.log" "${ELOQCTL}" status "${CLUSTER}" --wait 90 || {
    echo "FAIL: status --wait failed"
    dump_failure_diagnostics "${SCRIPT_DIR}/status.log"
    exit 1
}
echo "  OK"

# ---- [3] Read-only commands ----
echo "[3/6] Test read-only commands"
"${ELOQCTL}" versions > "${SCRIPT_DIR}/versions.log" 2>&1 || { echo "FAIL: versions"; exit 1; }
grep -q "1." "${SCRIPT_DIR}/versions.log" || { echo "FAIL: versions"; exit 1; }

"${ELOQCTL}" list > "${SCRIPT_DIR}/list.log" 2>&1 || { echo "FAIL: list"; exit 1; }
grep -q "${CLUSTER}" "${SCRIPT_DIR}/list.log" || { echo "FAIL: list"; exit 1; }

"${ELOQCTL}" export "${CLUSTER}" --output "${SCRIPT_DIR}/exported.yaml" > "${SCRIPT_DIR}/export.log" 2>&1 || { echo "FAIL: export"; exit 1; }
grep -q "cluster_name" "${SCRIPT_DIR}/exported.yaml" || { echo "FAIL: export"; exit 1; }

CLIENT=$("${ELOQCTL}" -q connect "${CLUSTER}" 2>"${SCRIPT_DIR}/connect.log")
echo "  OK"

# ---- [4] Rolling update via apply ----
echo "[4/6] Rolling update (apply checkpoint_interval change)"
"${ELOQCTL}" status "${CLUSTER}" >/dev/null 2>&1
# Generate a modified topology with different checkpoint_interval from the rendered version
sed 's/checkpoint_interval: 120/checkpoint_interval: 130/' "${TOPO}" > "${SCRIPT_DIR}/topology-v2.yaml"
"${ELOQCTL}" plan "${SCRIPT_DIR}/topology-v2.yaml" > "${SCRIPT_DIR}/plan.log" 2>&1 || { echo "FAIL: plan"; cat "${SCRIPT_DIR}/plan.log"; exit 1; }
"${ELOQCTL}" apply "${SCRIPT_DIR}/topology-v2.yaml" > "${SCRIPT_DIR}/apply.log" 2>&1 || { echo "FAIL: apply"; exit 1; }
run_with_progress "${STATUS_TIMEOUT_SECONDS}" "${SCRIPT_DIR}/post-apply.log" "${ELOQCTL}" status "${CLUSTER}" --wait 60 || {
    echo "FAIL: status after apply"
    dump_failure_diagnostics "${SCRIPT_DIR}/post-apply.log"
    exit 1
}
rm -f "${SCRIPT_DIR}/topology-v2.yaml"
echo "  OK"

# ---- [5] Scale add + remove ----
echo "[5/6] Scale add standby"
"${ELOQCTL}" status "${CLUSTER}" >/dev/null 2>&1
"${ELOQCTL}" scale "${CLUSTER}" --add-nodes "172.28.10.11:6390" --ng-id 0 --is-candidate true > "${SCRIPT_DIR}/scale-add.log" 2>&1 || {
    echo "FAIL: scale add"
    dump_failure_diagnostics "${SCRIPT_DIR}/scale-add.log"
    exit 1
}
run_with_progress "${STATUS_TIMEOUT_SECONDS}" "${SCRIPT_DIR}/post-scale-add.log" "${ELOQCTL}" status "${CLUSTER}" --wait 60 || {
    echo "FAIL: status after scale add"
    exit 1
}
echo "  OK"

echo "[6/6] Scale remove old standby"
"${ELOQCTL}" scale "${CLUSTER}" --remove-nodes "172.28.10.12:6379" > "${SCRIPT_DIR}/scale-remove.log" 2>&1 || {
    echo "FAIL: scale remove"
    dump_failure_diagnostics "${SCRIPT_DIR}/scale-remove.log"
    exit 1
}
run_with_progress "${STATUS_TIMEOUT_SECONDS}" "${SCRIPT_DIR}/post-scale-rm.log" "${ELOQCTL}" status "${CLUSTER}" --wait 60 || {
    echo "FAIL: status after scale remove"
    exit 1
}
echo "  OK"

# ---- [7] Stop + start cycle ----
echo "[7/14] Stop cluster and validate topology"
"${ELOQCTL}" stop "${CLUSTER}" --all --force > "${SCRIPT_DIR}/stop.log" 2>&1 || { echo "FAIL: stop"; exit 1; }
sleep 3
"${ELOQCTL}" check "${TOPO}" > "${SCRIPT_DIR}/check.log" 2>&1 && echo "  OK" || { echo "FAIL: check valid topology"; cat "${SCRIPT_DIR}/check.log"; exit 1; }

echo "[8/14] Restart cluster"
"${ELOQCTL}" start "${CLUSTER}" > "${SCRIPT_DIR}/start.log" 2>&1 || { echo "FAIL: start"; exit 1; }
run_with_progress "${STATUS_TIMEOUT_SECONDS}" "${SCRIPT_DIR}/post-start.log" "${ELOQCTL}" status "${CLUSTER}" --wait 60 || {
    echo "FAIL: status after restart"
    exit 1
}
echo "  OK"

# ---- [9] Failover ----
echo "[9/14] Failover standby to master"
"${ELOQCTL}" failover "${CLUSTER}" \
    --old-leader-host 172.28.10.11 --old-leader-port 6379 \
    --new-leader-host 172.28.10.11 --new-leader-port 6390 \
    > "${SCRIPT_DIR}/failover.log" 2>&1 || {
    echo "FAIL: failover"
    cat "${SCRIPT_DIR}/failover.log"
    exit 1
}
run_with_progress "${STATUS_TIMEOUT_SECONDS}" "${SCRIPT_DIR}/post-failover.log" "${ELOQCTL}" status "${CLUSTER}" --wait 60 || {
    echo "FAIL: status after failover"
    exit 1
}
echo "  OK"

# ---- [10] Monitor + log-service status ----
echo "[10/14] Monitor and log-service status"
"${ELOQCTL}" monitor status "${CLUSTER}" > "${SCRIPT_DIR}/monitor-status.log" 2>&1 && echo "  monitor OK" || echo "  monitor N/A (skipped)"
"${ELOQCTL}" log-service status "${CLUSTER}" > "${SCRIPT_DIR}/log-status.log" 2>&1 && echo "  log-srv OK" || echo "  log-srv N/A (skipped)"
echo "  OK"

# ---- [11] Exec custom command ----
echo "[11/14] Exec custom remote command"
"${ELOQCTL}" exec "uname -a" "${TOPO}" > "${SCRIPT_DIR}/exec.log" 2>&1 || { echo "FAIL: exec"; cat "${SCRIPT_DIR}/exec.log"; exit 1; }
echo "  OK"

# ---- [12] Schema upgrade ----
echo "[12/14] Schema upgrade"
"${ELOQCTL}" upgrade > "${SCRIPT_DIR}/upgrade.log" 2>&1 || { echo "FAIL: upgrade"; exit 1; }
echo "  OK"

# ---- [11] Remove cluster ----
echo "[13/14] Remove cluster"
"${ELOQCTL}" stop "${CLUSTER}" --all --force >/dev/null 2>&1 || true
"${ELOQCTL}" remove "${CLUSTER}" --force > "${SCRIPT_DIR}/remove.log" 2>&1 || { echo "FAIL: remove"; exit 1; }
"${ELOQCTL}" list > "${SCRIPT_DIR}/post-remove-list.log" 2>&1
grep -q "${CLUSTER}" "${SCRIPT_DIR}/post-remove-list.log" && { echo "FAIL: cluster still present after remove"; exit 1; }
echo "  OK"

echo ""
echo "PASS: all E2E tests completed"
