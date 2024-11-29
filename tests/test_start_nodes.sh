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
    set +ex # Disable printing commands and exit on error(grep may return error)
    # Loop over each provided port number
    for port in "$@"; do
        # Loop until a PID is found for the specific port
        while true; do
            pid=$(ps uxwe -u "$USER" | grep "/home/$USER/eloqkv_with_hot_standby/EloqKV/bin/eloqkv" | grep "$port" | grep -v "grep" | awk '{print $2}')

            if [[ -n "$pid" ]]; then
                break
            else
                sleep 1
            fi
        done
    done
    set -ex

    echo "All processes are running."
}

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
check_pid 6379 6389 6399
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

echo "Kill a standby node..."
ps uxwe -u $USER | grep /home/$USER/eloqkv_with_hot_standby/EloqKV/bin/eloqkv | grep 6389 | grep -v grep |  awk '{print $2}' | xargs -r kill -9 
sleep 1
eloqctl start eloqkv_with_hot_standby --nodes 127.0.0.1:6389
check_pid 6379 6389 6399
wait_for_cluster_ready
$CLIENT_6389 -e cluster slots

# test if the leader set in config is still the leader
run_counter_command $CLIENT_6379 incr mycounter 3
run_counter_command $CLIENT_6379 get mycounter 3
run_counter_command $CLIENT_6379 incr mycounter 4
run_counter_command $CLIENT_6379 get mycounter 4



echo "Kill a leader node..."
ps uxwe -u $USER | grep /home/$USER/eloqkv_with_hot_standby/EloqKV/bin/eloqkv | grep 6379 | grep -v grep |  awk '{print $2}' | xargs -r kill -9 
sleep 1
eloqctl start eloqkv_with_hot_standby --nodes 127.0.0.1:6379
check_pid 6379 6389 6399
# currently leader not change to original one (6379), lack of regain leader logic for now, so we only check 6379 could read here

test_read() {
    set +ex

    local MAX_RETRIES=30
    local RETRY_DELAY=2
    local COUNT
    local GET_OUTPUT

    # Iterate over each port passed as an argument
    for PORT in "$@"; do
        echo "Testing client on port: $PORT"
        
        COUNT=0
        while [ $COUNT -lt $MAX_RETRIES ]; do
            # Use the specified port to get the value of 'mycounter'
            case "$PORT" in
                6379)
                    GET_OUTPUT=$($CLIENT_6379 get mycounter)
                    ;;
                6389)
                    GET_OUTPUT=$($CLIENT_6389 get mycounter)
                    ;;
                6399)
                    GET_OUTPUT=$($CLIENT_6399 get mycounter)
                    ;;
                *)
                    echo "Unknown client for port $PORT"
                    exit 1
                    ;;
            esac

            # Check if the output is a valid number
            if [[ "$GET_OUTPUT" == "4" ]]; then
                set -ex
                echo "$PORT is able to read: $GET_OUTPUT."
                break
            else
                echo "Waiting for $PORT to be ready... ($GET_OUTPUT)"
                sleep $RETRY_DELAY
                ((COUNT++))

                if [ $COUNT -ge $MAX_RETRIES ]; then
                    set -ex
                    echo "$PORT is not able to read after $MAX_RETRIES retries."
                    exit 1
                fi
            fi
        done

    done

    echo "All specified Redis clients are able to read."
    return 0
}

test_read 6379 6389 6399
$CLIENT_6379 -e cluster slots

eloqctl stop eloqkv_with_hot_standby --all
eloqctl inspect eloqkv_with_hot_standby

eloqctl list
eloqctl remove eloqkv_with_hot_standby

echo "Start --nodes tests PASSED !!!"
