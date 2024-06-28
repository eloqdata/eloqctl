#!/bin/bash
set -exo pipefail

echo ">>> Test external cassandra"
CASSANDRA_HOST=$1
source /etc/os-release
KEYSPACE_NAME="waiter_${ID}${VERSION_ID%.*}"
cqlsh ${CASSANDRA_HOST} -e "DROP KEYSPACE IF EXISTS ${KEYSPACE_NAME}"

sed -i "s|monograph_keyspace_name=eloqsql|monograph_keyspace_name=${KEYSPACE_NAME}|" ${CLUSTER_MGR_HOME}/config/eloqsql.ini
sed -i "s|#cass_keyspace=eloqkv|cass_keyspace=${KEYSPACE_NAME}|" ${CLUSTER_MGR_HOME}/config/eloqkv.ini
sed -i "s|enable_data_store=none|enable_data_store=all|" ${CLUSTER_MGR_HOME}/config/eloqkv.ini

cluster_mgr demo eloq-sql --skip-deps --unlimited --ext-cass ${CASSANDRA_HOST}
CLIENT=$(cluster_mgr -q connect demo-sql-cassandra)
cluster_mgr status demo-sql-cassandra --wait 5
eval "${CLIENT} --execute 'SHOW DATABASES'"
cluster_mgr monitor demo-sql-cassandra stop
cluster_mgr stop demo-sql-cassandra --all
cluster_mgr remove demo-sql-cassandra

sleep 15
cluster_mgr demo eloq-kv --skip-deps --unlimited --ext-cass ${CASSANDRA_HOST}
CLIENT=$(cluster_mgr -q connect demo-kv-cassandra)
cluster_mgr status demo-kv-cassandra --wait 5
eval ${CLIENT} incr mycounter
cluster_mgr restart demo-kv-cassandra
cluster_mgr status demo-kv-cassandra --wait 5
eval ${CLIENT} incr mycounter
cluster_mgr remove demo-kv-cassandra

echo "External cassandra/scylla tests PASSED !!!"
