#!/bin/sh

set -eu

if [ -z "${1:-}" ]; then
    TAG="latest"
else
    TAG="$1"
fi

REPO_SLUG="${ELOQCTL_REPO:-monographdb/eloq_waiter}"
RELEASES_URL="${ELOQCTL_RELEASES_URL:-https://github.com/${REPO_SLUG}/releases}"
LATEST_API_URL="https://api.github.com/repos/${REPO_SLUG}/releases/latest"

if [ -f /etc/os-release ]; then
    . /etc/os-release
else
    echo "/etc/os-release not found"
    exit 1
fi

case "$(uname -m)" in
amd64 | x86_64) ARCH=amd64 ;;
arm64 | aarch64) ARCH=arm64 ;;
*) ARCH="$(uname -m)" ;;
esac

if [ "${ID}" = "centos" ] || [ "${ID}" = "rocky" ]; then
    OS_ID="rhel${VERSION_ID%.*}"
else
    OS_ID="${ID}${VERSION_ID%.*}"
fi

if [ -z "${ELOQCTL_HOME:-}" ]; then
    ELOQCTL_HOME="${HOME}/.eloqctl"
fi

BIN_DIR="${ELOQCTL_HOME}/bin"
TMP_TARBALL="${TMPDIR:-/tmp}/eloqctl.tar.gz"
mkdir -p "${BIN_DIR}"

resolve_latest_tag() {
    curl -fsSL "${LATEST_API_URL}" \
        | sed -n 's/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' \
        | head -n 1
}

install_binary() {
    echo "TAG: ${TAG}"

    RELEASE_TAG="${TAG}"
    if [ "${TAG}" = "latest" ]; then
        RELEASE_TAG="$(resolve_latest_tag)"
        if [ -z "${RELEASE_TAG}" ]; then
            echo "Failed to resolve latest release tag."
            return 1
        fi
    fi

    TARBALL="eloqctl-${RELEASE_TAG}-${OS_ID}-${ARCH}.tar.gz"
    if [ "${TAG}" = "latest" ]; then
        DOWNLOAD_URL="${RELEASES_URL}/latest/download/${TARBALL}"
    else
        DOWNLOAD_URL="${RELEASES_URL}/download/${RELEASE_TAG}/${TARBALL}"
    fi

    curl -fsSL "${DOWNLOAD_URL}?$(date "+%Y%m%d%H%M%S")" -o "${TMP_TARBALL}" || return 1
    tar -zxf "${TMP_TARBALL}" -C "${ELOQCTL_HOME}" --strip-components 1 --overwrite || return 1
    return 0
}

if ! install_binary; then
    echo "Failed to download and/or extract eloqctl archive."
    exit 1
fi

chmod 755 "${BIN_DIR}/cluster_mgr"

bold=$(tput bold 2>/dev/null || true)
sgr0=$(tput sgr0 2>/dev/null || true)

shell=$(echo "${SHELL:-/bin/sh}" | awk 'BEGIN {FS="/"} {print $NF}')
if [ -f "${HOME}/.${shell}_profile" ]; then
    PROFILE="${HOME}/.${shell}_profile"
elif [ -f "${HOME}/.${shell}_login" ]; then
    PROFILE="${HOME}/.${shell}_login"
elif [ -f "${HOME}/.${shell}rc" ]; then
    PROFILE="${HOME}/.${shell}rc"
else
    PROFILE="${HOME}/.profile"
fi

case ":$PATH:" in
*:"$BIN_DIR":*)
    echo "PATH already contains ${BIN_DIR}"
    ;;
*)
    printf '\nexport PATH=%s:$PATH\nexport ELOQCTL_HOME=%s\n' "${BIN_DIR}" "${ELOQCTL_HOME}" >>"${PROFILE}"
    echo "${PROFILE} has been modified to add eloqctl to PATH"
    ;;
esac

echo "==============================================="
echo "To use it, open a new terminal or execute:"
echo "${bold}source ${PROFILE}${sgr0}"
echo "==============================================="
