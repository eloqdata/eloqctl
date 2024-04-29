#!/bin/bash
set -exo pipefail

# prepare environment
source /etc/os-release
if [[ "$ID" == "centos" ]]; then
    sudo yum install -y epel-release && sudo yum update -y && sudo yum install -y sudo openssh-server
    sudo ssh-keygen -t rsa -f /etc/ssh/ssh_host_rsa_key -N ''
    sudo ssh-keygen -t rsa -f /etc/ssh/ssh_host_dsa_key -N ''
    sudo ssh-keygen -t rsa -f /etc/ssh/ssh_host_ed25519_key -N ''
    sudo ssh-keygen -t rsa -f /etc/ssh/ssh_host_ecdsa_key -N ''
    sudo /usr/sbin/sshd
elif [[ "$ID" == "ubuntu" ]]; then
    sudo apt update && DEBIAN_FRONTEND=noninteractive sudo apt install -y sudo openssh-server
    sudo service ssh start
fi
sudo chown -R mono ${PWD}
sudo chown -R mono ${HOME}

export CLUSTER_MGR_HOME="${HOME}/.eloqwaiter"
bash waiter_src/concourse/install.sh
# curl --proto '=https' --tlsv1.2 -sSf https://www.eloqdata.com/download/mono-waiter/install.sh | sh
export PATH="$PATH:$CLUSTER_MGR_HOME"
BASE_PATH=${PATH}

# test eloq-sql
cluster_mgr demo --product eloq-sql
export PATH="${BASE_PATH}:${CLUSTER_MGR_HOME}/demo-sql/monograph-tx-service-release/install/bin"
cluster_mgr status --cluster demo-sql --wait 5
mariadb -S /tmp/mysql3316.sock --execute "SHOW DATABASES"
mariadb -S /tmp/mysql3316.sock --execute "CREATE DATABASE test"
mariadb -S /tmp/mysql3316.sock --execute "CREATE TABLE test.t1(id INT PRIMARY KEY, c VARCHAR(10))"
mariadb -S /tmp/mysql3316.sock --execute "INSERT INTO test.t1 VALUES(1,'a'),(2,'b'),(3,'c')"
mariadb -S /tmp/mysql3316.sock --execute "SELECT * FROM test.t1"
cluster_mgr monitor --command stop --cluster demo-sql
cluster_mgr stop --cluster demo-sql
cluster_mgr update-conf --cluster demo-sql
cluster_mgr start --cluster demo-sql
cluster_mgr status --cluster demo-sql --wait 5
mariadb -S /tmp/mysql3316.sock --execute "SELECT * FROM test.t1"
cluster_mgr stop --cluster demo-sql --all

# test eloq-kv
cluster_mgr demo --product eloq-kv
export PATH="${BASE_PATH}:${CLUSTER_MGR_HOME}/demo-kv/monograph_redis"
cluster_mgr status --cluster demo-kv --wait 5
redis_cli -server 127.0.0.1:6389 incr mycounter
redis_cli -server 127.0.0.1:6389 get mycounter
redis_cli -server 127.0.0.1:6389 incr mycounter
redis_cli -server 127.0.0.1:6389 get mycounter
cluster_mgr monitor --command stop --cluster demo-kv
cluster_mgr list
cluster_mgr stop --cluster demo-kv --all

cat ${HOME}/.ssh/id_rsa.pub >>${HOME}/.ssh/authorized_keys

cluster_mgr launch --topology-file ${CLUSTER_MGR_HOME}/config/deployment_sql.yaml
export PATH="${BASE_PATH}:${HOME}/eloq/eloqsql-cluster/monograph-tx-service-release/install/bin"
cluster_mgr status --cluster eloqsql-cluster --wait 0
mariadb -S /tmp/mysql3316.sock --execute "SHOW DATABASES"
mariadb -S /tmp/mysql3316.sock --execute "CREATE DATABASE test"
mariadb -S /tmp/mysql3316.sock --execute "CREATE TABLE test.t1(id INT PRIMARY KEY, c VARCHAR(10))"
mariadb -S /tmp/mysql3316.sock --execute "INSERT INTO test.t1 VALUES(1,'a'),(2,'b'),(3,'c')"
mariadb -S /tmp/mysql3316.sock --execute "SELECT * FROM test.t1"
cluster_mgr monitor --command stop --cluster eloqsql-cluster
cluster_mgr stop --cluster eloqsql-cluster --all
cluster_mgr inspect --cluster eloqsql-cluster

cluster_mgr launch --topology-file ${CLUSTER_MGR_HOME}/config/deployment_kv.yaml
export PATH="${BASE_PATH}:${HOME}/eloq/eloqkv-cluster/monograph_redis"
cluster_mgr status --cluster eloqkv-cluster --wait 5
redis_cli -server 127.0.0.1:6389 incr mycounter
redis_cli -server 127.0.0.1:6389 get mycounter
redis_cli -server 127.0.0.1:6389 incr mycounter
redis_cli -server 127.0.0.1:6389 get mycounter
cluster_mgr monitor --command stop --cluster eloqkv-cluster
cluster_mgr stop --cluster eloqkv-cluster --all
cluster_mgr inspect --cluster eloqkv-cluster

cluster_mgr list
cluster_mgr remove --cluster demo-sql
cluster_mgr remove --cluster demo-kv
cluster_mgr remove --cluster eloqsql-cluster
cluster_mgr remove --cluster eloqkv-cluster
cluster_mgr list
