#!/bin/bash
set -exo pipefail

echo ">>> Test Update command"

cluster_mgr demo eloq-kv --store rocksdb --skip-deps --union-wal --no-monitor
cluster_mgr status demo-kv-rocksdb --wait 30
cluster_mgr update demo-kv-rocksdb nightly
cluster_mgr status demo-kv-rocksdb --wait 30
cluster_mgr remove demo-kv-rocksdb

cluster_mgr demo eloq-kv --skip-deps
cluster_mgr status demo-kv-cassandra --wait 30
cluster_mgr update demo-kv-cassandra latest
cluster_mgr status demo-kv-cassandra --wait 30
cluster_mgr update demo-kv-cassandra --cass-mirror "https://dlcdn.apache.org"
cluster_mgr update demo-kv-cassandra --cassandra 4.1.5
cluster_mgr status demo-kv-cassandra --wait 30
cluster_mgr remove demo-kv-cassandra

sleep 15
cluster_mgr demo eloq-sql --skip-deps
cluster_mgr status demo-sql-cassandra --wait 30
cluster_mgr update demo-sql-cassandra nightly
cluster_mgr status demo-sql-cassandra --wait 30
cluster_mgr remove demo-sql-cassandra
