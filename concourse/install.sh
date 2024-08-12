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

if [ -z "$ELOQCTL_HOME" ]; then
    ELOQCTL_HOME=${HOME}/.eloqctl
fi
bin_dir=${ELOQCTL_HOME}/bin
mkdir -p "$bin_dir"

install_binary() {
    curl "$repo/eloqctl/eloqctl-${OS_ID}-${ARCH}.tar.gz?$(date "+%Y%m%d%H%M%S")" -o "/tmp/eloqctl.tar.gz" || return 1
    tar -zxf "/tmp/eloqctl.tar.gz" -C $ELOQCTL_HOME --strip-components 1 --overwrite || return 1
    return 0
}

if ! install_binary; then
    echo "Failed to download and/or extract eloqctl archive."
    exit 1
fi

chmod 755 "$bin_dir/cluster_mgr"

bold=$(tput bold 2>/dev/null)
sgr0=$(tput sgr0 2>/dev/null)

# Refrence: https://stackoverflow.com/questions/14637979/how-to-permanently-set-path-on-linux-unix
shell=$(echo $SHELL | awk 'BEGIN {FS="/";} { print $NF }')
if [ -f "${HOME}/.${shell}_profile" ]; then
    PROFILE=${HOME}/.${shell}_profile
elif [ -f "${HOME}/.${shell}_login" ]; then
    PROFILE=${HOME}/.${shell}_login
elif [ -f "${HOME}/.${shell}rc" ]; then
    PROFILE=${HOME}/.${shell}rc
else
    PROFILE=${HOME}/.profile
fi

case :$PATH: in
*:$bin_dir:*)
    echo "PATH already contains $bin_dir"
    ;;
*)
    printf '\nexport PATH=%s:$PATH\nexport ELOQCTL_HOME=%s\n' "$bin_dir" "$ELOQCTL_HOME" >>"$PROFILE"
    echo "$PROFILE has been modified to add eloqctl to PATH"
    ;;
esac

echo "==============================================="
echo "To use it, open a new terminal or execute:"
echo "${bold}source ${PROFILE}${sgr0}"
echo "==============================================="
