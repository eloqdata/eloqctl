#!/bin/bash
set -exo pipefail

echo ">>> Test Launch command"

MY_IP=$(ip -4 addr | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | sed -n '2p')
sed -i "s|127.0.0.1|${MY_IP}|g" "${ELOQCTL_HOME}/config/examples/eloqkv_rocksdb_standby_with_voter.yaml"

eloqctl launch "${ELOQCTL_HOME}/config/examples/eloqkv_rocksdb_standby_with_voter.yaml" -s
CLIENT=$(eloqctl -q connect eloqkv_with_hot_standby_and_voter)

# Function to run a command and check for errors
run_command() {
    local OUTPUT
    OUTPUT=$("$@")
    local STATUS=$?

    # Convert OUTPUT to lowercase
    echo "$OUTPUT"
    local OUTPUT_LOWER=${OUTPUT,,}

    # if [[ $STATUS -ne 0 ]] || [[ $OUTPUT_LOWER == *"error"* ]] || [[ $OUTPUT_LOWER == *"fail"* ]] || [[ $OUTPUT_LOWER == *"moved"* ]]; then
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

eloqctl restart eloqkv_with_hot_standby_and_voter
# test if the leader set in config is still the leader
run_command $CLIENT incr mycounter
run_command $CLIENT get mycounter
run_command $CLIENT incr mycounter
run_command $CLIENT get mycounter

eloqctl stop eloqkv_with_hot_standby_and_voter --all
eloqctl inspect eloqkv_with_hot_standby_and_voter

eloqctl list
# eloqctl remove eloqsql-cluster
eloqctl remove eloqkv_with_hot_standby_and_voter

echo "Launch tests PASSED !!!"
