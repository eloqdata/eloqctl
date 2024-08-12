#!/bin/bash
set -exo pipefail

echo ">>> Test external cassandra"
CASSANDRA_HOST=$1
source /etc/os-release
KEYSPACE_NAME="waiter_${ID}${VERSION_ID%.*}"
cqlsh ${CASSANDRA_HOST} -e "DROP KEYSPACE IF EXISTS ${KEYSPACE_NAME}"

sed -i "s|monograph_keyspace_name=eloqsql|monograph_keyspace_name=${KEYSPACE_NAME}|" ${ELOQCTL_HOME}/config/EloqSql.ini
sed -i "s|#cass_keyspace=eloqkv|cass_keyspace=${KEYSPACE_NAME}|" ${ELOQCTL_HOME}/config/EloqKv.ini
sed -i "s|enable_data_store=none|enable_data_store=all|" ${ELOQCTL_HOME}/config/EloqKv.ini

eloqctl demo eloq-sql --skip-deps --unlimited --ext-cass ${CASSANDRA_HOST}
CLIENT=$(eloqctl -q connect demo-sql-cassandra)
eloqctl status demo-sql-cassandra --wait 30
eval "${CLIENT} --execute 'SHOW DATABASES'"
eloqctl monitor demo-sql-cassandra stop
eloqctl stop demo-sql-cassandra --all
eloqctl remove demo-sql-cassandra

sleep 15
eloqctl demo eloq-kv --skip-deps --unlimited --ext-cass ${CASSANDRA_HOST}
CLIENT=$(eloqctl -q connect demo-kv-cassandra)
eloqctl status demo-kv-cassandra --wait 30
eval ${CLIENT} incr mycounter
eloqctl restart demo-kv-cassandra
eloqctl status demo-kv-cassandra --wait 30
eval ${CLIENT} incr mycounter
eloqctl remove demo-kv-cassandra

echo "External cassandra/scylla tests PASSED !!!"
