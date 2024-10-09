#!/bin/bash
set -exo pipefail

echo ">>> Test Demo command"

eloqctl --version

# TODO(ZX) temporarily disable eloqsql test
# # test eloq-sql
# eloqctl demo eloq-sql --skip-deps
# CLIENT=$(eloqctl -q connect demo-sql-cassandra)
# eval "${CLIENT} --execute 'SHOW DATABASES'"
# eloqctl monitor stop demo-sql-cassandra
# eloqctl stop demo-sql-cassandra
# eloqctl update-conf demo-sql-cassandra
# eloqctl start demo-sql-cassandra
# eval "${CLIENT} --execute 'SHOW DATABASES'"
# eloqctl stop demo-sql-cassandra --all
# eloqctl remove demo-sql-cassandra

# eloqctl demo eloq-sql --skip-deps --no-monitor
# eloqctl remove demo-sql-cassandra

# eloqctl demo eloq-sql --skip-deps --joint-wal
# eloqctl remove demo-sql-cassandra

# eloqctl demo eloq-sql --skip-deps --joint-wal --no-monitor
# eloqctl remove demo-sql-cassandra

# test eloq-kv

eloqctl demo eloq-kv --skip-deps
CLIENT=$(eloqctl -q connect demo-kv-cassandra)
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eloqctl remove demo-kv-cassandra

eloqctl demo eloq-kv --store rocksdb --skip-deps
CLIENT=$(eloqctl -q connect demo-kv-rocksdb)
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eval ${CLIENT} incr mycounter
eval ${CLIENT} get mycounter
eloqctl monitor stop demo-kv-rocksdb
eloqctl list
eloqctl stop demo-kv-rocksdb --all
eloqctl remove demo-kv-rocksdb

eloqctl demo eloq-kv --skip-deps --no-monitor
eloqctl remove demo-kv-cassandra

eloqctl demo eloq-kv --skip-deps --joint-wal
eloqctl remove demo-kv-cassandra

eloqctl demo eloq-kv --skip-deps --joint-wal --no-monitor
eloqctl remove demo-kv-cassandra

echo "Demo tests PASSED !!!"
