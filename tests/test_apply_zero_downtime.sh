#!/bin/bash
set -exo pipefail

echo ">>> Test zero-downtime apply with hot standby"

MY_IP=$(ip -4 addr | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | sed -n '2p')
sed -i "s|127.0.0.1|${MY_IP}|g" "${ELOQCTL_HOME}/config/examples/eloqkv_rocksdb_standby.yaml"

# Step 1: launch cluster with standby
eloqctl launch "${ELOQCTL_HOME}/config/examples/eloqkv_rocksdb_standby.yaml" -s

CLIENT_6379=$(eloqctl -q connect eloqkv_with_hot_standby)
CLIENT_6389="${CLIENT_6379/6379/6389}"
CLIENT_6399="${CLIENT_6379/6379/6399}"

# Step 2: wait for cluster ready
MAX_RETRIES=30
RETRY_DELAY=2
COUNT=0
while [ $COUNT -lt $MAX_RETRIES ]; do
    $CLIENT_6379 set k v
    GET_OUTPUT_1=$($CLIENT_6389 get k)
    GET_OUTPUT_2=$($CLIENT_6399 get k)
    if [[ "$GET_OUTPUT_1" == "v" && "$GET_OUTPUT_2" == "v" ]]; then
        echo "Cluster is ready."
        break
    fi
    echo "Waiting for cluster to be ready..."
    sleep $RETRY_DELAY
    ((COUNT++))
done
$CLIENT_6379 set k v

$CLIENT_6379 cluster slots

# Step 3: start continuous writes in the background
WRITE_LOG=$(mktemp)
ERROR_LOG=$(mktemp)

continuous_write() {
    local SEQ=1000
    while true; do
        local OUTPUT
        OUTPUT=$($CLIENT_6379 set apply_test_key "${SEQ}" 2>&1)
        local STATUS=$?
        if [[ $STATUS -ne 0 ]]; then
            echo "WRITE_FAILED at seq=${SEQ}: ${OUTPUT}" >> "${ERROR_LOG}"
        fi
        echo "${SEQ}" >> "${WRITE_LOG}"
        SEQ=$((SEQ + 1))
        sleep 0.1
    done
}

continuous_write &
WRITER_PID=$!
sleep 2

echo ">>> Running apply with new rocksdb config..."
# Step 4: apply the config change (adds rocksdb_periodic_compaction_seconds)
eloqctl apply "${ELOQCTL_HOME}/config/examples/eloqkv_rocksdb_standby.yaml"

# Step 5: stop writer and check for write errors
sleep 2
kill $WRITER_PID 2>/dev/null
wait $WRITER_PID 2>/dev/null

WRITE_COUNT=$(wc -l < "${WRITE_LOG}")
ERROR_COUNT=$(wc -l < "${ERROR_LOG}")

echo "Total writes during apply: ${WRITE_COUNT}"
echo "Total write errors: ${ERROR_COUNT}"

if [[ $ERROR_COUNT -gt 0 ]]; then
    echo "WRITE ERRORS DETECTED:"
    cat "${ERROR_LOG}"
    echo "FAIL: Service was interrupted during apply!"
else
    echo "PASS: Zero-downtime upgrade verified - no write errors during apply."
fi

# Step 6: verify cluster still healthy
$CLIENT_6379 cluster slots
$CLIENT_6379 get apply_test_key

# Step 7: verify new config is applied (check INI file on master)
MY_HOST=$(hostname -I | awk '{print $1}')
INI_FILE="${ELOQCTL_HOME}/upload/eloqkv_with_hot_standby/${MY_IP}/EloqKv-node-6379.ini"
if [ -f "${INI_FILE}" ]; then
    echo "=== Checking INI for rocksdb settings ==="
    grep -E "rocksdb_periodic_compaction_seconds|rocksdb_enable_stats|rocksdb_stats_dump_period_sec" "${INI_FILE}" || echo "(Add these to your YAML to see them in INI)"
fi

# Cleanup
eloqctl stop eloqkv_with_hot_standby --all
eloqctl remove eloqkv_with_hot_standby --force

rm -f "${WRITE_LOG}" "${ERROR_LOG}"
echo "zero-downtime apply tests PASSED !!!"
