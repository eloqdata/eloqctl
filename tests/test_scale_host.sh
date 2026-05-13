#!/bin/bash
# End-to-end test: `eloqctl scale` — add and remove replica nodes.
set -eo pipefail

ELOQCTL="${PWD}/target/debug/cluster_mgr"
TOPO="${PWD}/tests/scale_standby_localhost.yaml"
CLUSTER="test-scale-standby"
export ELOQCTL_HOME="${HOME}/.eloqctl"
mkdir -p "${ELOQCTL_HOME}"

cleanup() {
    "${ELOQCTL}" stop "${CLUSTER}" --all --force 2>/dev/null || true
    "${ELOQCTL}" remove "${CLUSTER}" --force 2>/dev/null || true
    pkill -9 -f "eloqkv.*${CLUSTER}" 2>/dev/null || true
    rm -f /tmp/scale_launch.log
}
trap cleanup EXIT

echo "[1/5] Launch cluster (1 master + 1 standby)"
rm -rf "${HOME}/${CLUSTER}" "${ELOQCTL_HOME}/db/cluster_mgr_state.db"* 2>/dev/null || true
rm -rf "${ELOQCTL_HOME}/db/" 2>/dev/null || true
set +e
"${ELOQCTL}" launch "${TOPO}" -s > /tmp/scale_launch.log 2>&1
LAUNCH_RC=$?
set -e
[ ${LAUNCH_RC} -ne 0 ] && { echo "FAIL: launch exited ${LAUNCH_RC}"; tail -20 /tmp/scale_launch.log; exit 1; }
grep -q "FAIL" /tmp/scale_launch.log && { echo "FAIL in launch:"; grep FAIL /tmp/scale_launch.log; exit 1; }
echo "  OK"

echo "[2/5] Wait ready"
for i in $(seq 1 60); do
    redis-cli -p 6379 set _t v >/dev/null 2>&1 && { echo "  ready (${i}s)"; break; }
    [ $i -ge 60 ] && { echo "FAIL: not ready after 60s"; exit 1; }
    sleep 1
done
redis-cli -p 6379 cluster slots

echo "[3/5] Scale add: new replica at 127.0.0.1:6399"
"${ELOQCTL}" scale "${CLUSTER}" \
    --add-nodes 127.0.0.1:6399 \
    --ng-id 0 \
    --is-candidate true 2>&1
sleep 2
redis-cli -p 6379 set scale_add ok 2>&1 | grep -v OK && { echo "FAIL: write after add failed"; exit 1; }
echo "  write after add: OK"

echo "[4/5] Scale remove: old standby at 127.0.0.1:6389"
"${ELOQCTL}" scale "${CLUSTER}" \
    --remove-nodes 127.0.0.1:6389 2>&1
sleep 2
redis-cli -p 6379 set scale_rm ok 2>&1 | grep -v OK && { echo "FAIL: write after remove failed"; exit 1; }
echo "  write after remove: OK"

echo "[5/5] Final state"
redis-cli -p 6379 get scale_add 2>/dev/null
redis-cli -p 6379 get scale_rm 2>/dev/null

echo ""
echo "PASS: scale add and remove completed, cluster healthy"
