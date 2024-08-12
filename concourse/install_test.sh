#!/bin/bash
set -exo pipefail

# prepare environment
source /etc/os-release
if [ "$ID" == "centos" ] || [ "$ID" == "rocky" ] || [ "$ID" == "rhel" ]; then
  sudo /usr/sbin/sshd
elif [ "$ID" == "ubuntu" ]; then
  sudo service ssh start
fi
export ELOQCTL_HOME="${HOME}/.eloqctl"
bash waiter_src/concourse/install.sh
export PATH="$PATH:$ELOQCTL_HOME/bin"

cd $ELOQCTL_HOME
cat version
bash tests/demo.sh
sleep 15
bash tests/launch.sh
sleep 15
bash tests/update.sh

if [[ ! "$(python3 --version)" =~ "Python 3.12" ]]; then
  sleep 15
  wget https://downloads.datastax.com/enterprise/cqlsh-astra.tar.gz
  tar -xzvf cqlsh-astra.tar.gz
  export PATH=$PATH:${PWD}/cqlsh-astra/bin
  bash tests/external_cass.sh 172.31.5.203
fi
