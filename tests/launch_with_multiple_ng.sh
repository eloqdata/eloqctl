#!/bin/bash
set -exo pipefail

echo ">>> Test multiple node group"

sed -i "s|enable_data_store=false|enable_data_store=true|" ${ELOQCTL_HOME}/config/EloqKv.ini

MY_IP=$(ip -4 addr | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | sed -n '2p')
sed -i "s|127.0.0.1|${MY_IP}|g" "${ELOQCTL_HOME}/config/examples/eloqkv_rocksdb_multiple_ng.yaml"

eloqctl launch "${ELOQCTL_HOME}/config/examples/eloqkv_rocksdb_multiple_ng.yaml" -s
CLIENT_6379=$(eloqctl -q connect eloqkv_with_hot_standby)
CLIENT_6389="${CLIENT_6379/6379/6389}"
CLIENT_6399="${CLIENT_6379/6379/6399}"

wait_for_cluster_ready() {
    set +ex

    local MAX_RETRIES=30
    local RETRY_DELAY=2
    local COUNT=0

    while [ $COUNT -lt $MAX_RETRIES ]; do
        $CLIENT_6379 set k v
        GET_OUTPUT_1=$($CLIENT_6389 get k)
        GET_OUTPUT_2=$($CLIENT_6399 get k)
        
        if [[ "$GET_OUTPUT_1" == "v" && "$GET_OUTPUT_2" == "v" ]]; then
            $CLIENT_6379 set k q
            set -ex
            echo "Cluster is ready."
            return 0
        else
            echo "Waiting for cluster to be ready..."
            sleep $RETRY_DELAY
            ((COUNT++))
        fi
    done
    set -ex
    echo "Cluster is not ready after $MAX_RETRIES retries."
    exit 1
}

wait_for_cluster_ready
$CLIENT_6379 cluster slots


# Function to run a command and check for errors
run_counter_command() {
    local EXPECTED_RESULT="${@: -1}"       # Get the last argument as expected result
    local CMD=( "${@:1:$#-1}" )            # All arguments except the last one form the command
    local OUTPUT
    OUTPUT=$("${CMD[@]}")
    local STATUS=$?

    # Convert OUTPUT to lowercase
    echo "$OUTPUT"
    local OUTPUT_LOWER=${OUTPUT,,}

    if [[ $STATUS -ne 0 ]] || ! [[ $OUTPUT_LOWER =~ ^[0-9]+$ ]]; then
        echo "Error executing command: ${CMD[*]}"
        echo "Output: $OUTPUT"
        exit 1
    fi

    # Compare OUTPUT with EXPECTED_RESULT
    if [[ "$OUTPUT" != "$EXPECTED_RESULT" ]]; then
        echo "Output does not match expected result."
        echo "Expected: $EXPECTED_RESULT"
        echo "Actual: $OUTPUT"
        exit 1
    fi
}

# test if the leader set in config is still the leader
run_counter_command $CLIENT_6379 incr mycounter 1
run_counter_command $CLIENT_6379 get mycounter 1
run_counter_command $CLIENT_6379 incr mycounter 2
run_counter_command $CLIENT_6379 get mycounter 2

eloqctl restart eloqkv_with_hot_standby
wait_for_cluster_ready
$CLIENT_6379 cluster slots


# test if the leader set in config is still the leader
run_counter_command $CLIENT_6379 incr mycounter 3
run_counter_command $CLIENT_6379 get mycounter 3
run_counter_command $CLIENT_6379 incr mycounter 4
run_counter_command $CLIENT_6379 get mycounter 4

# eloqctl stop eloqkv_with_hot_standby --all
# eloqctl inspect eloqkv_with_hot_standby

# eloqctl list
# eloqctl remove eloqkv_with_hot_standby

echo "multiple node group tests PASSED !!!"
