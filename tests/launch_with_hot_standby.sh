#!/bin/bash
set -exo pipefail

echo ">>> Test Launch command"

MY_IP=$(ip -4 addr | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | sed -n '2p')
sed -i "s|127.0.0.1|${MY_IP}|g" ${ELOQCTL_HOME}/config/examples/eloqkv_rocksdb_standby.yaml

eloqctl launch ${ELOQCTL_HOME}/config/examples/eloqkv_rocksdb_standby.yaml
CLIENT=$(eloqctl -q connect eloqkv_with_hot_standby)
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eloqctl stop eloqkv_with_hot_standby --all
eloqctl inspect eloqkv_with_hot_standby

eloqctl list
# eloqctl remove eloqsql-cluster
eloqctl remove eloqkv_with_hot_standby

echo "Launch tests PASSED !!!"
