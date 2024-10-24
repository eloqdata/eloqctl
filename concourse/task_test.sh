#!/bin/bash
set -exo pipefail

# prepare environment
source /etc/os-release
if [ "$ID" == "centos" ] || [ "$ID" == "rocky" ] || [ "$ID" == "rhel" ]; then
    sudo /usr/sbin/sshd
elif [ "$ID" == "ubuntu" ]; then
    sudo service ssh start
fi

# Variables
KNOWN_HOSTS_FILE="$HOME/.ssh/known_hosts"

# Ensure the .ssh directory exists
mkdir -p "$HOME/.ssh"

# Get the private IP address (assumes the first IP is the private IP)
PRIVATE_IP=$(hostname -I | awk '{print $1}')

# Check if PRIVATE_IP is empty
if [[ -z "$PRIVATE_IP" ]]; then
    echo "Error: Unable to retrieve private IP address."
    exit 1
fi

echo "Private IP Address: $PRIVATE_IP"

# Remove existing entries for 127.0.0.1 and the private IP
ssh-keygen -R 127.0.0.1 2>/dev/null || true
ssh-keygen -R "$PRIVATE_IP" 2>/dev/null || true

# Fetch and add host keys to known_hosts
{
    ssh-keyscan -H 127.0.0.1
    ssh-keyscan -H "$PRIVATE_IP"
} >> "$KNOWN_HOSTS_FILE" 2>/dev/null

# Set correct permissions for known_hosts
chmod 600 "$KNOWN_HOSTS_FILE"

echo "Successfully added 127.0.0.1 and $PRIVATE_IP to $KNOWN_HOSTS_FILE"

export ELOQCTL_HOME="${HOME}/.eloqctl"
export PATH="$PATH:$ELOQCTL_HOME/bin"

bash waiter_src/concourse/install.sh

# use the tests from the latest commit of the branch set in pipeline, rather than the tests in pre-built tarball, in case the content of the tests is modified and you want to verify the modification
cd waiter_src
sudo chown -R $USER .

bash tests/launch.sh
bash tests/demo.sh
bash tests/update.sh
bash tests/control.sh

bash tests/launch_with_hot_standby.sh
bash tests/launch_with_hot_standby_and_voter.sh
bash tests/test_start_nodes.sh


if [[ ! "$(python3 --version)" =~ "Python 3.12" ]]; then
    curl -L -O https://downloads.datastax.com/enterprise/cqlsh-astra.tar.gz
    tar -xzvf cqlsh-astra.tar.gz
    export PATH=$PATH:${PWD}/cqlsh-astra/bin
    bash tests/external_cass.sh 172.31.5.203
fi
