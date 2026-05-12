#!/bin/bash
# End-to-end test: zero-downtime rolling upgrade via `eloqctl apply`.
#
# apply → detects config change → calls update-conf --restart → RollingUpgrade
#
# Prerequisites: SSH key at ~/.ssh/id_rsa, sshd on localhost
# Usage: cargo build -p cluster_mgr && bash tests/test_rolling_upgrade_host.sh

set -eo pipefail
ELOQCTL="${PWD}/target/debug/cluster_mgr"
TOPO="${PWD}/tests/rolling_upgrade_standby_localhost.yaml"
TOPO_V2="/tmp/rolling_upgrade_v2.yaml"
CLUSTER="test-rolling-standby"
export ELOQCTL_HOME="${HOME}/.eloqctl"
mkdir -p "${ELOQCTL_HOME}"

cleanup() {
    "${ELOQCTL}" stop "${CLUSTER}" --all 2>/dev/null || true
    "${ELOQCTL}" remove "${CLUSTER}" --force 2>/dev/null || true
    [ -n "${WRITER_PID:-}" ] && kill "$WRITER_PID" 2>/dev/null || true
    [ -n "${WRITE_LOG:-}" ] && rm -f "${WRITE_LOG}"
    [ -n "${ERROR_LOG:-}" ] && rm -f "${ERROR_LOG}"
    rm -f /tmp/rolling_launch.log "${TOPO_V2}"
}
trap cleanup EXIT

echo "[1/5] Launch cluster (checkpoint_interval=120)"
rm -rf "${HOME}/${CLUSTER}" "${ELOQCTL_HOME}/db/cluster_mgr_state.db" 2>/dev/null || true
set +e
"${ELOQCTL}" launch "${TOPO}" -s > /tmp/rolling_launch.log 2>&1
LAUNCH_RC=$?
set -e
[ ${LAUNCH_RC} -ne 0 ] && { echo "FAIL: launch exited ${LAUNCH_RC}"; tail -20 /tmp/rolling_launch.log; exit 1; }
grep -q "FAIL" /tmp/rolling_launch.log && { echo "FAIL in launch:"; grep FAIL /tmp/rolling_launch.log; exit 1; }
echo "  OK"

echo "[2/5] Wait ready"
CLIENT="$("${ELOQCTL}" -q connect "${CLUSTER}")"
for i in $(seq 1 60); do
    "${CLIENT}" set _t v >/dev/null 2>&1 && { echo "  ready (${i}s)"; break; }
    [ $i -ge 60 ] && { echo "FAIL: not ready after 60s"; exit 1; }
    sleep 1
done
"${CLIENT}" cluster slots

echo "[3/5] Create modified YAML (checkpoint_interval=130)"
sed 's/checkpoint_interval: 120/checkpoint_interval: 130/' "${TOPO}" > "${TOPO_V2}"
echo "  diff:"
diff "${TOPO}" "${TOPO_V2}" || true

echo "[4/5] Start writes + apply (triggers RollingUpgrade)..."
WRITE_LOG=$(mktemp); ERROR_LOG=$(mktemp)
(while true; do
    SEQ=$((SEQ+1))
    OUT=$("${CLIENT}" set rolling_k "${SEQ}" 2>&1) || echo "FAIL ${SEQ}" >> "${ERROR_LOG}"
    echo "${SEQ}" >> "${WRITE_LOG}"
    sleep 0.05
done) & WRITER_PID=$!
sleep 2

start_ts=$(date +%s)
"${ELOQCTL}" apply "${TOPO_V2}" 2>&1
elapsed=$(($(date +%s) - start_ts))
echo "  apply done (${elapsed}s)"

sleep 2; kill "$WRITER_PID" 2>/dev/null || true; wait "$WRITER_PID" 2>/dev/null || true

echo "[5/5] Results"
echo "  writes=$(wc -l < "${WRITE_LOG}") errors=$(wc -l < "${ERROR_LOG}")"
if [ "$(wc -l < "${ERROR_LOG}" 2>/dev/null || echo 0)" -gt 0 ]; then
    echo "FAIL: write errors"
    head -10 "${ERROR_LOG}"
    exit 1
fi

"${CLIENT}" cluster slots
"${CLIENT}" set final_k ok >/dev/null 2>&1
VAL=$("${CLIENT}" get rolling_k 2>/dev/null || echo "N/A")
echo "  last rolling key = ${VAL}"

echo "PASS"
