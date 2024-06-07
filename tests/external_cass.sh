#!/bin/bash
set -exo pipefail

CASSANDRA_ADDR=$1
source /etc/os-release
OS_ID=${ID}$(echo ${VERSION_ID}| tr -d '.')
echo ">>> Test external cassandra"
sed -i "s|monograph_keyspace_name=eloqsql|monograph_keyspace_name=waiter_${OS_ID}|" ${CLUSTER_MGR_HOME}/config/eloqsql.ini
sed -i "s|#cass_keyspace=eloqkv|cass_keyspace=waiter_${OS_ID}|" ${CLUSTER_MGR_HOME}/config/eloqkv.ini
sed -i "s|enable_data_store=none|enable_data_store=all|" ${CLUSTER_MGR_HOME}/config/eloqkv.ini

cluster_mgr demo eloq-sql --skip-deps --ext-cass ${CASSANDRA_ADDR}
CLIENT=$(cluster_mgr -q connect demo-sql-cassandra)
cluster_mgr status demo-sql-cassandra --wait 5
eval "${CLIENT} --execute 'SHOW DATABASES'"
cluster_mgr monitor demo-sql-cassandra stop
cluster_mgr stop demo-sql-cassandra --all
cluster_mgr remove demo-sql-cassandra

sleep 15
cluster_mgr demo eloq-kv --skip-deps --ext-cass ${CASSANDRA_ADDR}
CLIENT=$(cluster_mgr -q connect demo-kv-cassandra)
cluster_mgr status demo-kv-cassandra --wait 5
eval ${CLIENT} incr mycounter
cluster_mgr monitor demo-kv-cassandra stop
cluster_mgr stop demo-kv-cassandra --all
cluster_mgr remove demo-kv-cassandra

echo "External cassandra/scylla tests PASSED !!!"
