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
    TAG=$(git tag | sort -V | tail -n 1)
    if [ -z "${TAG}" ]; then
        echo "No tag found for HEAD. Exiting."
        exit 1
    fi
else
    TAG="main"
fi
TX_TARBALL="eloqctl-${TAG}-${OS_ID}-${ARCH}.tar.gz"

# Build
if [[ "$TAG" != "main" ]]; then
    echo "Checking out to $TAG..."
    git checkout "${TAG}"
else
    echo "TAG is 'main', no checkout performed."
fi
cargo make --no-workspace --makefile Makefile.toml rest_api_pkg
tar -czvf ../output/"${TX_TARBALL}" eloqctl

# Upload to S3
aws s3 cp ../output/"${TX_TARBALL}" s3://eloq-release/eloqctl/${ARCH}/${TAG}/${TX_TARBALL}
