#!/bin/sh

source /etc/os-release
repo='https://download.eloqdata.com'
if [ -n "$MONO_MIRRORS" ]; then
    repo=$MONO_MIRRORS
fi

case $(uname -m) in
amd64 | x86_64) ARCH=amd64 ;;
arm64 | aarch64) ARCH=arm64 ;;
*) ARCH= $(uname -m) ;;
esac

if [[ "$ID" == "centos" ]] || [[ "$ID" == "rocky" ]]; then
    OS_ID="rhel${VERSION_ID%.*}"
else
    OS_ID="${ID}${VERSION_ID%.*}"
fi

if [ -z "$CLUSTER_MGR_HOME" ]; then
    CLUSTER_MGR_HOME=${HOME}/.eloqwaiter
fi
bin_dir=$CLUSTER_MGR_HOME
mkdir -p "$bin_dir"

install_binary() {
    curl "$repo/waiter/waiter-${OS_ID}-${ARCH}.tar.gz?$(date "+%Y%m%d%H%M%S")" -o "/tmp/eloqwaiter.tar.gz" || return 1
    tar -zxf "/tmp/eloqwaiter.tar.gz" -C "$CLUSTER_MGR_HOME" --strip-components 1 --overwrite || return 1
    rm "/tmp/eloqwaiter.tar.gz"
    return 0
}

if ! install_binary; then
    echo "Failed to download and/or extract eloqwaiter archive."
    exit 1
fi

chmod 755 "$bin_dir/cluster_mgr"

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
    echo "PATH already contains $bin_dir"
    ;;
*)
    printf '\nexport PATH=%s:$PATH\nexport CLUSTER_MGR_HOME=%s\n' "$bin_dir" "$CLUSTER_MGR_HOME" >>"$PROFILE"
    echo "$PROFILE has been modified to add cluster_mgr to PATH"
    ;;
esac

# echo "Installed path: ${bold}$bin_dir/cluster_mgr${sgr0}"
echo "==============================================="
echo "To use it, open a new terminal or execute:"
echo "${bold}source ${PROFILE}${sgr0}"
echo "==============================================="
