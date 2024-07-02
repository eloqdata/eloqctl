#!/bin/bash
set -exo pipefail

# prepare environment
source /etc/os-release
if [ "$ID" == "centos" ] || [ "$ID" == "rocky" ]; then
  if [ "$VERSION_ID" == "7" ]; then
    sudo yum install -y epel-release && sudo yum update -y
    sudo yum install -y sudo openssh-server iproute redis-tools
  else
    # sudo dnf install -y epel-release && sudo dnf update -y
    sudo dnf install -y sudo openssh-server iproute redis
  fi
  sudo ssh-keygen -t rsa -f /etc/ssh/ssh_host_rsa_key -N ''
  sudo ssh-keygen -t rsa -f /etc/ssh/ssh_host_dsa_key -N ''
  sudo ssh-keygen -t rsa -f /etc/ssh/ssh_host_ed25519_key -N ''
  sudo ssh-keygen -t rsa -f /etc/ssh/ssh_host_ecdsa_key -N ''
  sudo /usr/sbin/sshd
  if [ -f "/run/nologin" ]; then
    sudo rm /run/nologin
  fi
elif [ "$ID" == "ubuntu" ]; then
  sudo apt update && DEBIAN_FRONTEND=noninteractive sudo apt install -y sudo openssh-server iproute2 redis-tools
  sudo service ssh start
fi
sudo chown -R mono ${PWD}
sudo chown -R mono ${HOME}

export CLUSTER_MGR_HOME="${HOME}/.eloqwaiter"
bash waiter_src/concourse/install.sh
# curl --proto '=https' --tlsv1.2 -sSf https://www.eloqdata.com/download/mono-waiter/install.sh | sh
export PATH="$PATH:$CLUSTER_MGR_HOME"
cat ${HOME}/.ssh/id_rsa.pub >>${HOME}/.ssh/authorized_keys
cat ${CLUSTER_MGR_HOME}/version

bash ${CLUSTER_MGR_HOME}/tests/demo.sh
sleep 15
bash ${CLUSTER_MGR_HOME}/tests/launch.sh

sleep 15
wget https://downloads.datastax.com/enterprise/cqlsh-astra.tar.gz
tar -xzvf cqlsh-astra.tar.gz
export PATH=$PATH:$HOME/cqlsh-astra/bin
bash ${CLUSTER_MGR_HOME}/tests/external_cass.sh 172.31.5.203

sleep 15
bash ${CLUSTER_MGR_HOME}/tests/update.sh
