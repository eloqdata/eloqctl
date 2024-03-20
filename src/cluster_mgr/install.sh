#!/bin/sh

repo='https://d143xau9fe26d8.cloudfront.net'
if [ -n "$MONO_MIRRORS" ]; then
    repo=$MONO_MIRRORS
fi

case $(uname -s) in
    Linux|linux) os=linux ;;
    Darwin|darwin) os=darwin ;;
    *) os= ;;
esac

if [ -z "$os" ]; then
    echo "OS $(uname -s) not supported." >&2
    exit 1
fi

case $(uname -m) in
    amd64|x86_64) arch=amd64 ;;
    arm64|aarch64) arch=arm64 ;;
    *) arch= ;;
esac

if [ -z "$arch" ]; then
    echo "Architecture  $(uname -m) not supported." >&2
    exit 1
fi

LINUX_ID=$(cat /etc/os-release | awk -F'=' '{ if ($1 == "ID") {print $2} }' | tr -d '"')
VERSION_ID=$(cat /etc/os-release | awk -F'=' '{ if ($1 == "VERSION_ID") {print $2} }' | tr -d '".')

if [ -z "$CLUSTER_MGR_HOME" ]; then
    CLUSTER_MGR_HOME=${HOME}/.eloqwaiter
fi
bin_dir=$CLUSTER_MGR_HOME
mkdir -p "$bin_dir"

install_binary() {
    curl "$repo/eloq-waiter/${LINUX_ID}${VERSION_ID}/nightly/waiter-cluster-mgr.tar.gz?$(date "+%Y%m%d%H%M%S")" -o "/tmp/eloqwaiter.tar.gz" || return 1
    tar -zxf "/tmp/eloqwaiter.tar.gz" -C "$CLUSTER_MGR_HOME" --strip-components 1 --overwrite || return 1
    rm "/tmp/eloqwaiter.tar.gz"
    return 0
}

if ! install_binary; then
    echo "Failed to download and/or extract cluster-mgr archive."
    exit 1
fi

chmod 755 "$bin_dir/cluster_mgr"

# "$bin_dir/cluster_mgr" mirror set $repo

ssh-keygen -t ed25519 -f $CLUSTER_MGR_HOME/ed25519 -q -N ""
if [ ! -d "${HOME}/.ssh" ]; then
    mkdir ${HOME}/.ssh
    chmod 700 ${HOME}/.ssh
fi
if [ ! -f "${HOME}/.ssh/authorized_keys" ]; then
    touch ${HOME}/.ssh/authorized_keys
    chmod 600 ${HOME}/.ssh/authorized_keys
fi
cat $CLUSTER_MGR_HOME/ed25519.pub >> ${HOME}/.ssh/authorized_keys

bold=$(tput bold 2>/dev/null)
sgr0=$(tput sgr0 2>/dev/null)

# Refrence: https://stackoverflow.com/questions/14637979/how-to-permanently-set-path-on-linux-unix
shell=$(echo $SHELL | awk 'BEGIN {FS="/";} { print $NF }')
# echo "Detected shell: ${bold}$shell${sgr0}"
if [ -f "${HOME}/.${shell}_profile" ]; then
    PROFILE=${HOME}/.${shell}_profile
elif [ -f "${HOME}/.${shell}_login" ]; then
    PROFILE=${HOME}/.${shell}_login
elif [ -f "${HOME}/.${shell}rc" ]; then
    PROFILE=${HOME}/.${shell}rc
else
    PROFILE=${HOME}/.profile
fi
# echo "Shell profile:  ${bold}$PROFILE${sgr0}"

case :$PATH: in
    *:$bin_dir:*)
        echo "PATH already contains $bin_dir" ;;
    *)
        printf '\nexport PATH=%s:$PATH\nexport CLUSTER_MGR_HOME=%s\n' "$bin_dir" "$CLUSTER_MGR_HOME" >> "$PROFILE"
        echo "$PROFILE has been modified to add cluster_mgr to PATH"
        ;;
esac

# echo "Installed path: ${bold}$bin_dir/cluster_mgr${sgr0}"
echo "==============================================="
echo "To use it, open a new terminal or execute:"
echo "${bold}source ${PROFILE}${sgr0}"
echo "==============================================="
