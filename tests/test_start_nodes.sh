#!/bin/bash
set -exo pipefail

echo ">>> Test Start --nodes command"

MY_IP=$(ip -4 addr | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | sed -n '2p')
sed -i "s|127.0.0.1|${MY_IP}|g" "${ELOQCTL_HOME}/config/examples/eloqkv_rocksdb_standby.yaml"

eloqctl launch "${ELOQCTL_HOME}/config/examples/eloqkv_rocksdb_standby.yaml" -s
CLIENT_6379=$(eloqctl -q connect eloqkv_with_hot_standby)
CLIENT_6389="${CLIENT_6379/6379/6389}"
CLIENT_6399="${CLIENT_6379/6379/6399}"

check_pid () {
    set +x  # Disable printing commands
    # Loop over each provided port number
    for port in "$@"; do
        # Loop until a PID is found for the specific port
        while true; do
            pid=$(ps uxwe -u "$USER" | grep "/home/mono/eloqkv_with_hot_standby/EloqKV/bin/eloqkv" | grep "$port" | grep -v "grep" | awk '{print $2}')

            if [[ -n "$pid" ]]; then
                break
            else
                sleep 1
            fi
        done
    done
    set -x  # Enable printing commands again

    echo "All processes are running."
}

wait_for_cluster_ready() {
    set +ex

    local MAX_RETRIES=30
    local RETRY_DELAY=2
    local COUNT=0
    local GET_OUTPUT

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
            echo "Waiting for cluster to be ready... ($GET_OUTPUT)"
            sleep $RETRY_DELAY
            ((COUNT++))
        fi
    done
    set -ex
    echo "Cluster is not ready after $MAX_RETRIES retries."
    exit 1
}

wait_for_cluster_ready
check_pid 6379 6389 6399
$CLIENT_6379 cluster slots

# Function to run a command and check for errors
run_counter_command() {
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

# test if the leader set in config is still the leader
run_counter_command $CLIENT_6379 incr mycounter
run_counter_command $CLIENT_6379 get mycounter
run_counter_command $CLIENT_6379 incr mycounter
run_counter_command $CLIENT_6379 get mycounter

echo "Kill a standby node..."
ps uxwe -u $USER | grep /home/mono/eloqkv_with_hot_standby/EloqKV/bin/eloqkv | grep 6389 | grep -v grep |  awk '{print $2}' | xargs -r kill -9 
sleep 1
eloqctl start eloqkv_with_hot_standby --nodes 127.0.0.1:6389
check_pid 6379 6389 6399
wait_for_cluster_ready
$CLIENT_6389 -e cluster slots

# test if the leader set in config is still the leader
run_counter_command $CLIENT_6379 incr mycounter
run_counter_command $CLIENT_6379 get mycounter
run_counter_command $CLIENT_6379 incr mycounter
run_counter_command $CLIENT_6379 get mycounter

# eloqctl restart eloqkv_with_hot_standby
# wait_for_cluster_ready
# check_pid 6379 6389 6399
# $CLIENT_6379 -e cluster slots

echo "Kill a leader node..."
ps uxwe -u $USER | grep /home/mono/eloqkv_with_hot_standby/EloqKV/bin/eloqkv | grep 6379 | grep -v grep |  awk '{print $2}' | xargs -r kill -9 
sleep 1
eloqctl start eloqkv_with_hot_standby --nodes 127.0.0.1:6379
check_pid 6379 6389 6399
sleep 10
$CLIENT_6379 -e cluster slots
# Q? leader not change to original one (6379)
wait_for_cluster_ready

# test if the leader set in config is still the leader
run_counter_command $CLIENT_6379 incr mycounter
run_counter_command $CLIENT_6379 get mycounter
run_counter_command $CLIENT_6379 incr mycounter
run_counter_command $CLIENT_6379 get mycounter

eloqctl stop eloqkv_with_hot_standby --all
eloqctl inspect eloqkv_with_hot_standby

eloqctl list
eloqctl remove eloqkv_with_hot_standby

echo "Start --nodes tests PASSED !!!"
