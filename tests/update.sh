#!/bin/bash
set -exo pipefail

echo ">>> Test Update command"

eloqctl demo eloq-kv --store rocksdb --skip-deps --joint-wal --no-monitor
eloqctl status demo-kv-rocksdb --wait 30
eloqctl update demo-kv-rocksdb latest
eloqctl status demo-kv-rocksdb --wait 30
eloqctl remove demo-kv-rocksdb

eloqctl demo eloq-kv --skip-deps
eloqctl status demo-kv-cassandra --wait 30
eloqctl update demo-kv-cassandra latest
eloqctl status demo-kv-cassandra --wait 30
eloqctl update demo-kv-cassandra --cassandra 4.1.5
eloqctl status demo-kv-cassandra --wait 30
eloqctl remove demo-kv-cassandra

eloqctl demo eloq-sql --skip-deps
eloqctl status demo-sql-cassandra --wait 30
eloqctl update demo-sql-cassandra latest --cassandra 4.1.5
eloqctl status demo-sql-cassandra --wait 30
eloqctl remove demo-sql-cassandra

eloqctl demo eloq-sql --version 0.4.4 --skip-deps --joint-wal --no-monitor
eloqctl status demo-sql-cassandra --wait 30
eloqctl stop --all demo-sql-cassandra
eloqctl update demo-sql-cassandra 0.4.6
eloqctl status demo-sql-cassandra --wait 30
eloqctl remove demo-sql-cassandra

echo "Update tests PASSED !!!"
