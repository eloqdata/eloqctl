#!/bin/bash
set -exo pipefail

echo ">>> Test Launch command"

cluster_mgr launch ${CLUSTER_MGR_HOME}/config/examples/eloqsql_cassandra.yaml
CLIENT=$(cluster_mgr -q connect eloqsql-cluster)
cluster_mgr status eloqsql-cluster --wait 5
eval "${CLIENT} --execute 'SHOW DATABASES'"
cluster_mgr monitor eloqsql-cluster stop
cluster_mgr stop eloqsql-cluster --all

sleep 15
cluster_mgr launch ${CLUSTER_MGR_HOME}/config/examples/eloqkv_rocksdb.yaml --skip-deps
CLIENT=$(cluster_mgr -q connect eloqkv-cluster)
cluster_mgr status eloqkv-cluster --wait 5
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
cluster_mgr monitor eloqkv-cluster stop
cluster_mgr stop eloqkv-cluster --all

cluster_mgr list
cluster_mgr remove eloqsql-cluster
cluster_mgr remove eloqkv-cluster
cluster_mgr list

MY_IP=$(ip -4 addr | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | sed -n '2p')
sed -i "s|127.0.0.1|${MY_IP}|g" ${CLUSTER_MGR_HOME}/config/examples/eloqsql_cassandra.yaml
sed -i "s|127.0.0.1|${MY_IP}|g" ${CLUSTER_MGR_HOME}/config/examples/eloqkv_cassandra.yaml

sleep 15
cluster_mgr launch ${CLUSTER_MGR_HOME}/config/examples/eloqsql_cassandra.yaml --skip-deps
CLIENT=$(cluster_mgr -q connect eloqsql-cluster)
cluster_mgr status eloqsql-cluster --wait 5
eval "${CLIENT} --execute 'SHOW DATABASES'"
cluster_mgr monitor eloqsql-cluster stop
cluster_mgr stop eloqsql-cluster --all

sleep 15
cluster_mgr launch ${CLUSTER_MGR_HOME}/config/examples/eloqkv_cassandra.yaml --skip-deps
CLIENT=$(cluster_mgr -q connect eloqkv-cluster)
cluster_mgr status eloqkv-cluster --wait 5
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
cluster_mgr monitor eloqkv-cluster stop
cluster_mgr stop eloqkv-cluster --all
cluster_mgr inspect eloqkv-cluster

cluster_mgr list
cluster_mgr remove eloqsql-cluster
cluster_mgr remove eloqkv-cluster

echo "Launch tests PASSED !!!"
