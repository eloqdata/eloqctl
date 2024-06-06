#!/bin/bash
set -exo pipefail

echo ">>> Test Demo command"

# test eloq-sql
cluster_mgr demo eloq-sql --version nightly
CLIENT=$(cluster_mgr -q connect demo-sql-cassandra)
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
CLIENT=$(cluster_mgr -q connect demo-kv-cassandra)
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
cluster_mgr demo eloq-kv --store rocksdb
CLIENT=$(cluster_mgr -q connect demo-kv-rocksdb)
cluster_mgr status demo-kv-rocksdb --wait 5
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
cluster_mgr monitor demo-kv-rocksdb stop 
cluster_mgr list
cluster_mgr stop demo-kv-rocksdb --all
cluster_mgr remove demo-kv-rocksdb

echo "Demo tests PASSED !!!"
