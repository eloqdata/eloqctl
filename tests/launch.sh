#!/bin/bash
set -exo pipefail

echo ">>> Test Launch command"

eloqctl launch ${ELOQCTL_HOME}/config/examples/eloqsql_cassandra.yaml
CLIENT=$(eloqctl -q connect eloqsql-cluster)
eloqctl status eloqsql-cluster --wait 30
eval "${CLIENT} --execute 'SHOW DATABASES'"
eloqctl monitor eloqsql-cluster stop
eloqctl stop eloqsql-cluster --all

sleep 15
eloqctl launch ${ELOQCTL_HOME}/config/examples/eloqkv_rocksdb.yaml --skip-deps
CLIENT=$(eloqctl -q connect eloqkv-cluster)
eloqctl status eloqkv-cluster --wait 30
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eloqctl monitor eloqkv-cluster stop
eloqctl stop eloqkv-cluster --all

eloqctl list
eloqctl remove eloqsql-cluster
eloqctl remove eloqkv-cluster
eloqctl list

MY_IP=$(ip -4 addr | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | sed -n '2p')
sed -i "s|127.0.0.1|${MY_IP}|g" ${ELOQCTL_HOME}/config/examples/eloqsql_cassandra.yaml
sed -i "s|127.0.0.1|${MY_IP}|g" ${ELOQCTL_HOME}/config/examples/eloqkv_cassandra.yaml

sleep 15
eloqctl launch ${ELOQCTL_HOME}/config/examples/eloqsql_cassandra.yaml --skip-deps
CLIENT=$(eloqctl -q connect eloqsql-cluster)
eloqctl status eloqsql-cluster --wait 30
eval "${CLIENT} --execute 'SHOW DATABASES'"
eloqctl monitor eloqsql-cluster stop
eloqctl stop eloqsql-cluster --all

sleep 15
eloqctl launch ${ELOQCTL_HOME}/config/examples/eloqkv_cassandra.yaml --skip-deps
CLIENT=$(eloqctl -q connect eloqkv-cluster)
eloqctl status eloqkv-cluster --wait 30
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eloqctl monitor eloqkv-cluster stop
eloqctl stop eloqkv-cluster --all
eloqctl inspect eloqkv-cluster

eloqctl list
eloqctl remove eloqsql-cluster
eloqctl remove eloqkv-cluster

echo "Launch tests PASSED !!!"
