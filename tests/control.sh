#!/bin/bash
set -exo pipefail

echo ">>> Test Start/Stop command"

eloqctl demo eloq-sql --skip-deps
eloqctl status demo-sql-cassandra --wait 30
eloqctl stop demo-sql-cassandra
eloqctl start demo-sql-cassandra
eloqctl status demo-sql-cassandra --wait 30
eloqctl stop demo-sql-cassandra --log
eloqctl start demo-sql-cassandra
eloqctl status demo-sql-cassandra --wait 30
eloqctl stop demo-sql-cassandra --store
eloqctl start demo-sql-cassandra
eloqctl status demo-sql-cassandra --wait 30
eloqctl stop demo-sql-cassandra --log --store
eloqctl start demo-sql-cassandra
eloqctl status demo-sql-cassandra --wait 30
eloqctl stop demo-sql-cassandra --tx false --monitor
eloqctl status demo-sql-cassandra --wait 30
eloqctl stop demo-sql-cassandra --all --force
eloqctl remove demo-sql-cassandra

eloqctl demo eloq-kv --skip-deps
eloqctl status demo-kv-cassandra --wait 30
eloqctl stop demo-kv-cassandra
eloqctl start demo-kv-cassandra
eloqctl status demo-kv-cassandra --wait 30
eloqctl stop demo-kv-cassandra --log
eloqctl start demo-kv-cassandra
eloqctl status demo-kv-cassandra --wait 30
eloqctl stop demo-kv-cassandra --store
eloqctl start demo-kv-cassandra
eloqctl status demo-kv-cassandra --wait 30
eloqctl stop demo-kv-cassandra --log --store
eloqctl start demo-kv-cassandra
eloqctl status demo-kv-cassandra --wait 30
eloqctl stop demo-kv-cassandra --all --force
eloqctl start demo-kv-cassandra
eloqctl status demo-kv-cassandra --wait 30
eloqctl remove demo-kv-cassandra

eloqctl demo eloq-kv --store rocksdb --no-monitor --skip-deps
eloqctl status demo-kv-rocksdb --wait 30
eloqctl stop demo-kv-rocksdb
eloqctl start demo-kv-rocksdb
eloqctl status demo-kv-rocksdb --wait 30
eloqctl stop demo-kv-rocksdb --tx false
eloqctl status demo-kv-rocksdb --wait 30
eloqctl stop demo-kv-rocksdb --all
eloqctl start demo-kv-rocksdb
eloqctl status demo-kv-rocksdb --wait 30
eloqctl remove demo-kv-rocksdb

echo "Control tests PASSED !!!"
