#!/bin/bash
set -exo pipefail

echo ">>> Test Update command"

eloqctl demo eloq-kv --store rocksdb --skip-deps --joint-wal --no-monitor
eloqctl update demo-kv-rocksdb latest
eloqctl remove demo-kv-rocksdb

eloqctl demo eloq-kv --skip-deps
eloqctl update demo-kv-cassandra latest
eloqctl update demo-kv-cassandra --cassandra 4.1.3
eloqctl remove demo-kv-cassandra

eloqctl demo eloq-sql --skip-deps
eloqctl update demo-sql-cassandra latest --cassandra 4.1.3
eloqctl remove demo-sql-cassandra

eloqctl demo eloq-sql --version 0.4.4 --skip-deps --joint-wal --no-monitor
eloqctl stop --all demo-sql-cassandra
eloqctl update demo-sql-cassandra 0.4.6
eloqctl remove demo-sql-cassandra

echo "Update tests PASSED !!!"
