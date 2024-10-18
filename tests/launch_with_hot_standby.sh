#!/bin/bash
set -exo pipefail

echo ">>> Test Launch command"

MY_IP=$(ip -4 addr | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | sed -n '2p')
sed -i "s|127.0.0.1|${MY_IP}|g" "${ELOQCTL_HOME}/config/examples/eloqkv_rocksdb_standby_multi_node.yaml"

eloqctl launch "${ELOQCTL_HOME}/config/examples/eloqkv_rocksdb_standby_multi_node.yaml" -s

CLIENT=$(eloqctl -q connect eloqkv_with_hot_standby)

wait_for_cluster_ready() {
    local MAX_RETRIES=30
    local RETRY_DELAY=1
    local COUNT=0
    local PING_OUTPUT

    while [ $COUNT -lt $MAX_RETRIES ]; do
        PING_OUTPUT=$($CLIENT ping 2>&1 || true)
        if [[ "$PING_OUTPUT" == "PONG" ]]; then
            echo "Cluster is ready."
            return 0
        else
            echo "Waiting for cluster to be ready... ($PING_OUTPUT)"
            sleep $RETRY_DELAY
            ((COUNT++))
        fi
    done
    echo "Cluster is not ready after $MAX_RETRIES retries."
    exit 1
}
wait_for_cluster_ready

# Function to run a command and check for errors
run_command() {
    local OUTPUT
    OUTPUT=$("$@")
    local STATUS=$?

    # Convert OUTPUT to lowercase
    echo "$OUTPUT"
    local OUTPUT_LOWER=${OUTPUT,,}

    if [[ $STATUS -ne 0 ]] || ! [[ $OUTPUT_LOWER =~ ^[0-9]+$ ]]; then
        echo "Error executing command: $*"
        echo "Output: $OUTPUT"
        exit 1
    fi
}

echo $CLIENT

# test if the leader set in config is still the leader
run_command $CLIENT incr mycounter
run_command $CLIENT get mycounter
run_command $CLIENT incr mycounter
run_command $CLIENT get mycounter

eloqctl restart eloqkv_with_hot_standby
wait_for_cluster_ready

# test if the leader set in config is still the leader
run_command $CLIENT incr mycounter
run_command $CLIENT get mycounter
run_command $CLIENT incr mycounter
run_command $CLIENT get mycounter

eloqctl stop eloqkv_with_hot_standby --all
eloqctl inspect eloqkv_with_hot_standby

eloqctl list
# eloqctl remove eloqsql-cluster
eloqctl remove eloqkv_with_hot_standby

echo "Launch tests PASSED !!!"
