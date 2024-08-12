#!/bin/bash
set -exo pipefail

echo ">>> Test Demo command"

eloqctl --version

# test eloq-sql
eloqctl demo eloq-sql
CLIENT=$(eloqctl -q connect demo-sql-cassandra)
eloqctl status demo-sql-cassandra --wait 30
eval "${CLIENT} --execute 'SHOW DATABASES'"
eloqctl monitor demo-sql-cassandra stop
eloqctl stop demo-sql-cassandra
eloqctl update-conf demo-sql-cassandra
eloqctl start demo-sql-cassandra
eloqctl status demo-sql-cassandra --wait 30
eval "${CLIENT} --execute 'SHOW DATABASES'"
eloqctl stop demo-sql-cassandra --all
eloqctl remove demo-sql-cassandra

sleep 15
eloqctl demo eloq-sql --skip-deps --no-monitor
eloqctl status demo-sql-cassandra --wait 30
eloqctl remove demo-sql-cassandra

sleep 15
eloqctl demo eloq-sql --skip-deps --joint-wal
eloqctl status demo-sql-cassandra --wait 30
eloqctl remove demo-sql-cassandra

sleep 15
eloqctl demo eloq-sql --skip-deps --joint-wal --no-monitor
eloqctl status demo-sql-cassandra --wait 30
eloqctl remove demo-sql-cassandra

# test eloq-kv
sleep 15
eloqctl demo eloq-kv --skip-deps
CLIENT=$(eloqctl -q connect demo-kv-cassandra)
eloqctl status demo-kv-cassandra --wait 30
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eloqctl remove demo-kv-cassandra

sleep 15
eloqctl demo eloq-kv --store rocksdb --skip-deps
CLIENT=$(eloqctl -q connect demo-kv-rocksdb)
eloqctl status demo-kv-rocksdb --wait 30
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eloqctl monitor demo-kv-rocksdb stop
eloqctl list
eloqctl stop demo-kv-rocksdb --all
eloqctl remove demo-kv-rocksdb

sleep 15
eloqctl demo eloq-kv --skip-deps --no-monitor
eloqctl status demo-kv-cassandra --wait 30
eloqctl remove demo-kv-cassandra

sleep 15
eloqctl demo eloq-kv --skip-deps --joint-wal
eloqctl status demo-kv-cassandra --wait 30
eloqctl remove demo-kv-cassandra

sleep 15
eloqctl demo eloq-kv --skip-deps --joint-wal --no-monitor
eloqctl status demo-kv-cassandra --wait 30
eloqctl remove demo-kv-cassandra

echo "Demo tests PASSED !!!"
