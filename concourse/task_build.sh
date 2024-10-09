#!/bin/bash
set -exo pipefail

# Install cargo-make
cargo install cargo-make

# Build rest_api_pkg
cd monograph_waiter
cargo make --no-workspace --makefile Makefile.toml rest_api_pkg
tar -czvf eloqctl.tar.gz eloqctl

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
    TAGGED=$(git tag --points-at HEAD --sort=-version:refname | head -n 1)
    if [ -z "${TAGGED}" ]; then
        echo "No tag found for HEAD. Exiting."
        exit 1
    fi
    scripts/git-checkout.sh "${TAGGED}"
fi

# Set tarball name
OUT_NAME=${OUT_NAME:-"default"}
if [ -n "${TAGGED}" ]; then
    TX_TARBALL="eloqctl-${TAGGED}-${OS_ID}-${ARCH}.tar.gz"
else
    TX_TARBALL="eloqctl-${OUT_NAME}-${OS_ID}-${ARCH}.tar.gz"
fi

# Upload to S3
aws s3 cp eloqctl.tar.gz s3://eloq-release/eloqctl/${TX_TARBALL}
