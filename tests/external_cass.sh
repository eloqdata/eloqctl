#!/bin/bash
set -exo pipefail

CASSANDRA_ADDR=$1
source /etc/os-release
OS_ID=${ID}$(echo ${VERSION_ID}| tr -d '.')
echo ">>> Test external cassandra"
sed -i "s|monograph_keyspace_name=eloqsql|monograph_keyspace_name=waiter_${OS_ID}|" ${CLUSTER_MGR_HOME}/config/my_template.cnf
sed -i "s|#cass_keyspace=eloqkv|cass_keyspace=waiter_${OS_ID}|" ${CLUSTER_MGR_HOME}/config/redis_template.ini
sed -i "s|enable_data_store=none|enable_data_store=all|" ${CLUSTER_MGR_HOME}/config/redis_template.ini

cluster_mgr demo --product eloq-sql --skip-deps --ext-cass ${CASSANDRA_ADDR}
CLIENT=$(cluster_mgr connect --cluster demo-sql-cassandra)
cluster_mgr status --cluster demo-sql-cassandra --wait 5
eval "${CLIENT} --execute 'SHOW DATABASES'"
cluster_mgr monitor --command stop --cluster demo-sql-cassandra
cluster_mgr stop --cluster demo-sql-cassandra --all
cluster_mgr remove --cluster demo-sql-cassandra

sleep 15
cluster_mgr demo --product eloq-kv --skip-deps --ext-cass ${CASSANDRA_ADDR}
CLIENT=$(cluster_mgr connect --cluster demo-kv-cassandra)
cluster_mgr status --cluster demo-kv-cassandra --wait 5
eval ${CLIENT} incr mycounter
cluster_mgr monitor --command stop --cluster demo-kv-cassandra
cluster_mgr stop --cluster demo-kv-cassandra --all
cluster_mgr remove --cluster demo-kv-cassandra

echo "External cassandra/scylla tests PASSED !!!"
