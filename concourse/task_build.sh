#!/bin/bash
set -exo pipefail

cargo install cargo-make
cd monograph_waiter

# Determine OS version
source /etc/os-release
if [[ "$ID" == "centos" ]] || [[ "$ID" == "rocky" ]]; then
    OS_ID="rhel${VERSION_ID%.*}"
else
    OS_ID="${ID}${VERSION_ID%.*}"
fi

# Determine architecture
case $(uname -m) in
amd64 | x86_64) ARCH=amd64 ;;
arm64 | aarch64) ARCH=arm64 ;;
*) ARCH=$(uname -m) ;;
esac

# Handle tagged versions
if [ -n "${TAGGED}" ]; then
    TAG=$(git tag --points-at HEAD --sort=-version:refname | head -n 1)
    if [ -z "${TAG}" ]; then
        echo "No tag found for HEAD. Exiting."
        exit 1
    fi
else
    TAG="main"
fi

git checkout "${TAG}"
TX_TARBALL="eloqctl-${TAG}-${OS_ID}-${ARCH}.tar.gz"

# Build
cargo make --no-workspace --makefile Makefile.toml rest_api_pkg
tar -czvf eloqctl.tar.gz eloqctl

# Upload to S3
aws s3 cp eloqctl.tar.gz s3://eloq-release/eloqctl/${TX_TARBALL}
