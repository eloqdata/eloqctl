#!/bin/bash
set -exo pipefail
source "$(dirname "$0")/util.sh"
CLUSTER_NAME='eloqkv_with_hot_standby_and_voter'

echo ">>> Test Multiple Node Scaling (Log and TX nodes on multiple targets)"

## Define static IPs for test environment
TX_IP_1="192.168.122.24"
TX_LEADER_PORT_1=6349

TX_IP_2="192.168.122.25"
TX_LEADER_PORT_2=6359

LOG_IP="192.168.122.24"
LOG_LEADER_PORT=9400

TARGET_IP_1="192.168.122.27"  # First target node
NEW_TX_PORT_1=6379

TARGET_IP_2="192.168.122.28"  # Second target node
NEW_TX_PORT_2=6389

## Use the standby-with-voter-with-log example
CONFIG="/home/eloq/workspace/monograph_waiter/tests/eloqkv_rocksdb_standby_with_voter_with_log_server.yaml"
# Launch the cluster in standalone mode
eloqctl launch "$CONFIG" -s

## Define Redis CLI commands for cluster nodes
CLI_LEADER_1="redis-cli -h ${TX_IP_1} -p ${TX_LEADER_PORT_1}"
CLI_LEADER_2="redis-cli -h ${TX_IP_2} -p ${TX_LEADER_PORT_2}"
CLI_NEW_TX_1="redis-cli -h ${TARGET_IP_1} -p ${NEW_TX_PORT_1}"
CLI_NEW_TX_2="redis-cli -h ${TARGET_IP_2} -p ${NEW_TX_PORT_2}"

# Helper: check cluster status
check_cluster_status() {
    eloqctl status "$CLUSTER_NAME"
}


# 1. Validate initial cluster
echo ">>> STEP 1: Validate initial cluster" 
wait_for_cluster_ready "${TX_IP_1}:${TX_LEADER_PORT_1}"
check_cluster_status

# Initialize test data
run_command "$CLI_LEADER_1 set multi_test init_value" "OK"
run_command "$CLI_LEADER_1 get multi_test" "init_value"

# 2. Add new log nodes to both target nodes simultaneously
NEW_LOG_PORT_1=9700
NEW_LOG_PORT_2=9800
echo ">>> STEP 2: Adding new log nodes to multiple targets" 
# Create data directories on the target nodes
# ssh ${TARGET_IP_1} "mkdir -p /home/\${USER}/eloqkv_with_hot_standby_and_voter/wal_eloqkv/${NEW_LOG_PORT_1}"
# ssh ${TARGET_IP_2} "mkdir -p /home/\${USER}/eloqkv_with_hot_standby_and_voter/wal_eloqkv/${NEW_LOG_PORT_2}"

# Add both log nodes at once
eloqctl scale-log "$CLUSTER_NAME" --add-nodes ${TARGET_IP_1}:${NEW_LOG_PORT_1},${TARGET_IP_2}:${NEW_LOG_PORT_2} --log-ng-id 0

# Wait for cluster to stabilize
wait_for_cluster_ready "${TX_IP_1}:${TX_LEADER_PORT_1}"
check_cluster_status

# Verify data accessibility after adding log nodes
run_command "$CLI_LEADER_1 incr log_counter" "1"
run_command "$CLI_LEADER_1 get log_counter" "1"

# 3. Now add TX nodes to both target nodes
echo ">>> STEP 3: Adding new TX nodes to multiple targets" 
eloqctl scale "$CLUSTER_NAME" --add-nodes ${TARGET_IP_1}:${NEW_TX_PORT_1},${TARGET_IP_2}:${NEW_TX_PORT_2} --is-candidate true,true --ng-id 0

# Wait for cluster to stabilize
wait_for_cluster_ready "${TX_IP_1}:${TX_LEADER_PORT_1}"
wait_for_cluster_ready "${TARGET_IP_1}:${NEW_TX_PORT_1}"
wait_for_cluster_ready "${TARGET_IP_2}:${NEW_TX_PORT_2}"
check_cluster_status

# Verify data accessibility from all TX nodes
run_command "$CLI_LEADER_1 incr tx_counter" "1"
run_command "$CLI_LEADER_1 get tx_counter" "1"
run_command "$CLI_NEW_TX_1 ping" "PONG"
run_command "$CLI_NEW_TX_2 ping" "PONG"
run_command "$CLI_LEADER_1 incr tx_counter" "2"
run_command "$CLI_NEW_TX_1 ping" "PONG"
run_command "$CLI_NEW_TX_2 ping" "PONG"
run_command "$CLI_LEADER_1 get tx_counter" "2"

# 4. Test data replication across all nodes
echo ">>> STEP 4: Testing data replication across all nodes"
run_command "$CLI_LEADER_1 set repl_test main_value" "OK"
run_command "$CLI_NEW_TX_1 ping" "PONG"
run_command "$CLI_NEW_TX_2 ping" "PONG"

run_command "$CLI_LEADER_1 set repl_test node1_value" "OK"
run_command "$CLI_NEW_TX_1 ping" "PONG"
run_command "$CLI_NEW_TX_2 ping" "PONG"

# 5. Test data replication across all nodes
echo ">>> STEP 5: Testing data replication across all nodes after stopping and starting the cluster"
eloqctl stop "$CLUSTER_NAME" -a
sleep 1
eloqctl start "$CLUSTER_NAME"
run_command "$CLI_LEADER_1 set repl_test main_value" "OK"
run_command "$CLI_NEW_TX_1 ping" "PONG"
run_command "$CLI_NEW_TX_2 ping" "PONG"

run_command "$CLI_LEADER_2 set repl_test node2_value" "OK"
run_command "$CLI_NEW_TX_1 ping" "PONG"
run_command "$CLI_NEW_TX_2 ping" "PONG"

# 6. Remove the TX nodes
echo ">>> STEP 6: Removing TX nodes from multiple targets" 
eloqctl scale "$CLUSTER_NAME" --remove-nodes ${TX_IP_1}:${TX_LEADER_PORT_1},${TARGET_IP_2}:${NEW_TX_PORT_2}

# Wait until TX processes exit
set +e
timeout=30
while [ $timeout -gt 0 ]; do
    nodes_running=0
    ssh ${TX_IP_1} "ps ux | grep '/home/\${USER}/eloqkv_with_hot_standby_and_voter/EloqKV/bin/eloqkv' | grep ${TX_LEADER_PORT_1} | grep -v grep" &> /dev/null
    if [ $? -eq 0 ]; then nodes_running=$((nodes_running+1)); fi
    
    ssh ${TARGET_IP_2} "ps ux | grep '/home/\${USER}/eloqkv_with_hot_standby_and_voter/EloqKV/bin/eloqkv' | grep ${NEW_TX_PORT_2} | grep -v grep" &> /dev/null
    if [ $? -eq 0 ]; then nodes_running=$((nodes_running+1)); fi
    
    if [ $nodes_running -eq 0 ]; then break; fi
    sleep 1
    timeout=$((timeout-1))
done
set -e

run_command "$CLI_NEW_TX_1 ping" "PONG"
run_command "$CLI_LEADER_2 get repl_test" "node2_value"
run_command "$CLI_LEADER_2 set repl_test node22_value" "OK"


# 7. Remove the log nodes
echo ">>> STEP 7: Removing log nodes from multiple targets" 
eloqctl scale-log "$CLUSTER_NAME" --remove-nodes ${LOG_IP}:${LOG_LEADER_PORT},${TARGET_IP_2}:${NEW_LOG_PORT_2}

# Wait until log processes exit
set +e
timeout=30
while [ $timeout -gt 0 ]; do
    nodes_running=0
    ssh ${LOG_IP} "ps ux | grep '/home/\${USER}/eloqkv_with_hot_standby_and_voter/EloqKV/bin/eloqkv' | grep ${LOG_LEADER_PORT} | grep -v grep" &> /dev/null
    if [ $? -eq 0 ]; then nodes_running=$((nodes_running+1)); fi
    
    ssh ${TARGET_IP_2} "ps ux | grep '/home/\${USER}/eloqkv_with_hot_standby_and_voter/EloqKV/bin/eloqkv' | grep ${NEW_LOG_PORT_2} | grep -v grep" &> /dev/null
    if [ $? -eq 0 ]; then nodes_running=$((nodes_running+1)); fi
    
    if [ $nodes_running -eq 0 ]; then break; fi
    sleep 1
    timeout=$((timeout-1))
done
set -e

# Verify data is still accessible after removing all added nodes
run_command "$CLI_NEW_TX_1 ping" "PONG"
run_command "$CLI_LEADER_2 get multi_test" "init_value"
run_command "$CLI_LEADER_2 get tx_counter" "2"
run_command "$CLI_LEADER_2 get log_counter" "1"
run_command "$CLI_LEADER_2 get repl_test" "node22_value"

# 8. Cleanup
eloqctl stop "$CLUSTER_NAME" --all
eloqctl list
eloqctl remove "$CLUSTER_NAME"

echo "Multiple Node Scale (Log+TX) tests PASSED !!!" 


# TODO(zx): add test for scale/scale-log on the existing hosts
