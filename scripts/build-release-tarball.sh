#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
TARGET_DIR=${CARGO_TARGET_DIR:-"${WORKSPACE_DIR}/target"}
PKG_DIR=${ELOQCTL_PKG_DIR:-"${WORKSPACE_DIR}/eloqctl"}
OUTPUT_DIR=${OUTPUT_DIR:-"${WORKSPACE_DIR}/dist"}
RELEASE_TAG=${RELEASE_TAG:-$(git -C "${WORKSPACE_DIR}" describe --tags --always)}
TARGET_ARCH=${TARGET_ARCH:-}
TARGET_OS_ID=${TARGET_OS_ID:-}

if [[ -z "${TARGET_ARCH}" ]]; then
    case "$(uname -m)" in
    amd64 | x86_64) TARGET_ARCH="amd64" ;;
    arm64 | aarch64) TARGET_ARCH="arm64" ;;
    *) TARGET_ARCH="$(uname -m)" ;;
    esac
fi

if [[ -z "${TARGET_OS_ID}" ]]; then
    # shellcheck disable=SC1091
    source /etc/os-release
    if [[ "${ID}" == "centos" || "${ID}" == "rocky" ]]; then
        TARGET_OS_ID="rhel${VERSION_ID%.*}"
    else
        TARGET_OS_ID="${ID}${VERSION_ID%.*}"
    fi
fi

TARBALL="eloqctl-${RELEASE_TAG}-${TARGET_OS_ID}-${TARGET_ARCH}.tar.gz"

rm -rf "${PKG_DIR}" "${OUTPUT_DIR}"
mkdir -p "${PKG_DIR}/bin" "${OUTPUT_DIR}"

pushd "${WORKSPACE_DIR}" >/dev/null

cargo build --package cluster_mgr --release
cargo build --package rest_api --release

"${WORKSPACE_DIR}/version.sh" >"${PKG_DIR}/version" 2>/dev/null || true
cp -r "${WORKSPACE_DIR}/src/cluster_mgr/config" "${PKG_DIR}/"
cp -r "${WORKSPACE_DIR}/tests" "${PKG_DIR}/"
cp "${TARGET_DIR}/release/cluster_mgr" "${PKG_DIR}/bin/"
cp "${TARGET_DIR}/release/rest_api" "${PKG_DIR}/bin/"
ln -sfn cluster_mgr "${PKG_DIR}/bin/eloqctl"

tar -czf "${OUTPUT_DIR}/${TARBALL}" -C "${PKG_DIR}" .

popd >/dev/null

echo "${OUTPUT_DIR}/${TARBALL}"
