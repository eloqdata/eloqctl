#!/bin/bash

WORK_DIR=$(pwd)
cd monograph_waiter
cargo make --no-workspace --makefile Makefile.toml rest_api_pkg
tar -czvf eloqctl.tar.gz eloqctl

source /etc/os-release
if [[ "$ID" == "centos" ]] || [[ "$ID" == "rocky" ]]; then
    OS_ID="rhel${VERSION_ID%.*}"
else
    OS_ID="${ID}${VERSION_ID%.*}"
fi

case $(uname -m) in
amd64 | x86_64) ARCH=amd64 ;;
arm64 | aarch64) ARCH=arm64 ;;
*) ARCH= $(uname -m) ;;
esac

aws s3 cp eloqctl.tar.gz s3://eloq-release/eloqctl/eloqctl-${OS_ID}-${ARCH}.tar.gz
