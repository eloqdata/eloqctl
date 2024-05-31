#!/bin/bash
set -exo pipefail

echo ">>> Test Demo command"

# test eloq-sql
cluster_mgr demo eloq-sql --version nightly
CLIENT=$(cluster_mgr connect demo-sql-cassandra)
cluster_mgr status demo-sql-cassandra --wait 5
eval "${CLIENT} --execute 'SHOW DATABASES'"
cluster_mgr monitor demo-sql-cassandra stop
cluster_mgr stop demo-sql-cassandra
cluster_mgr update-conf demo-sql-cassandra
cluster_mgr start demo-sql-cassandra
cluster_mgr status demo-sql-cassandra --wait 5
eval "${CLIENT} --execute 'SHOW DATABASES'"
cluster_mgr stop demo-sql-cassandra --all

# test eloq-kv
sleep 15
cluster_mgr demo eloq-kv --version nightly
CLIENT=$(cluster_mgr connect demo-kv-cassandra)
cluster_mgr status demo-kv-cassandra --wait 5
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
cluster_mgr monitor demo-kv-cassandra stop 
cluster_mgr list
cluster_mgr stop demo-kv-cassandra --all

cluster_mgr remove demo-sql-cassandra
cluster_mgr remove demo-kv-cassandra

sleep 15
cluster_mgr demo eloq-kv --store rocks
CLIENT=$(cluster_mgr connect demo-kv-rocksdb)
cluster_mgr status demo-kv-rocksdb --wait 5
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
cluster_mgr monitor demo-kv-rocksdb stop 
cluster_mgr list
cluster_mgr stop demo-kv-rocksdb --all
cluster_mgr remove demo-kv-rocksdb

echo ">>> Test Launch command"

sleep 15
cluster_mgr launch ${CLUSTER_MGR_HOME}/config/examples/eloqsql_cassandra.yaml
CLIENT=$(cluster_mgr connect eloqsql-cluster)
cluster_mgr status eloqsql-cluster --wait 5
eval "${CLIENT} --execute 'SHOW DATABASES'"
cluster_mgr monitor eloqsql-cluster stop 
cluster_mgr stop eloqsql-cluster --all
cluster_mgr inspect eloqsql-cluster

sleep 15
cluster_mgr launch ${CLUSTER_MGR_HOME}/config/examples/eloqkv_rocksdb.yaml
CLIENT=$(cluster_mgr connect eloqkv-cluster)
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
cluster_mgr list

MY_IP=$(ip -4 addr | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | sed -n '2p')
sed -i "s|127.0.0.1|${MY_IP}|g" ${CLUSTER_MGR_HOME}/config/examples/eloqsql_cassandra.yaml
sed -i "s|127.0.0.1|${MY_IP}|g" ${CLUSTER_MGR_HOME}/config/examples/eloqkv_cassandra.yaml

sleep 15
cluster_mgr launch ${CLUSTER_MGR_HOME}/config/examples/eloqsql_cassandra.yaml
CLIENT=$(cluster_mgr connect eloqsql-cluster)
cluster_mgr status eloqsql-cluster --wait 5
eval "${CLIENT} --execute 'SHOW DATABASES'"
cluster_mgr monitor eloqsql-cluster stop 
cluster_mgr stop eloqsql-cluster --all
cluster_mgr inspect eloqsql-cluster

sleep 15
cluster_mgr launch ${CLUSTER_MGR_HOME}/config/examples/eloqkv_cassandra.yaml
CLIENT=$(cluster_mgr connect eloqkv-cluster)
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

echo "Basic tests PASSED !!!"
