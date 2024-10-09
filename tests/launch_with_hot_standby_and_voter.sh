#!/bin/bash
set -exo pipefail

echo ">>> Test Launch command"

MY_IP=$(ip -4 addr | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | sed -n '2p')
# sed -i "s|127.0.0.1|${MY_IP}|g" ${ELOQCTL_HOME}/config/examples/eloqsql_cassandra.yaml
sed -i "s|127.0.0.1|${MY_IP}|g" ${ELOQCTL_HOME}/config/examples/eloqkv_rocksdb_standby_with_voter.yaml

# TODO(ZX) temporarily disable eloqsql test
# eloqctl launch ${ELOQCTL_HOME}/config/examples/eloqsql_cassandra.yaml
# CLIENT=$(eloqctl -q connect eloqsql-cluster)
# eval "${CLIENT} --execute 'SHOW DATABASES'"
# eloqctl stop eloqsql-cluster --all

eloqctl launch ${ELOQCTL_HOME}/config/examples/eloqkv_rocksdb_standby_with_voter.yaml
CLIENT=$(eloqctl -q connect eloqkv_with_hot_standby_and_voter)
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eloqctl stop eloqkv_with_hot_standby_and_voter --all
eloqctl inspect eloqkv_with_hot_standby_and_voter

eloqctl list
# eloqctl remove eloqsql-cluster
eloqctl remove eloqkv_with_hot_standby_and_voter

echo "Launch tests PASSED !!!"
