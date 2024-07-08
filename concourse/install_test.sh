#!/bin/bash
set -exo pipefail

# prepare environment
source /etc/os-release
if [ "$ID" == "centos" ] || [ "$ID" == "rocky" ] || [ "$ID" == "rhel" ]; then
  sudo /usr/sbin/sshd
elif [ "$ID" == "ubuntu" ]; then
  sudo service ssh start
fi
export CLUSTER_MGR_HOME="${HOME}/.eloqwaiter"
bash waiter_src/concourse/install.sh
export PATH="$PATH:$CLUSTER_MGR_HOME"

cd ${CLUSTER_MGR_HOME}
cat version
bash tests/demo.sh
sleep 15
bash tests/launch.sh
sleep 15
bash tests/update.sh
sleep 15
wget https://downloads.datastax.com/enterprise/cqlsh-astra.tar.gz
tar -xzvf cqlsh-astra.tar.gz
export PATH=$PATH:${PWD}/cqlsh-astra/bin
bash tests/external_cass.sh 172.31.5.203
