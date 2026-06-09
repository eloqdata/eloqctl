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
BIN_PATH="${BIN_DIR}/cluster_mgr"
STATE_DB_PATH="${ELOQCTL_HOME}/db/cluster_mgr_state.db"
TMP_TARBALL="$(mktemp "${TMPDIR:-/tmp}/eloqctl.XXXXXX.tar.gz")"
trap 'rm -f "${TMP_TARBALL}"' EXIT
mkdir -p "${BIN_DIR}"

HAD_EXISTING_INSTALL=false
if [ -x "${BIN_PATH}" ] || [ -f "${STATE_DB_PATH}" ]; then
    HAD_EXISTING_INSTALL=true
fi

resolve_latest_tag() {
    latest_tag="$(
        curl -fsSL \
            -H "Accept: application/vnd.github+json" \
            -H "X-GitHub-Api-Version: 2022-11-28" \
            -H "User-Agent: eloqctl-install-script" \
            "${LATEST_API_URL}" 2>/dev/null \
            | sed -n 's/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' \
            | head -n 1
    )"

    if [ -n "${latest_tag}" ]; then
        echo "${latest_tag}"
        return 0
    fi

    # Fallback for API rate limits or temporary API failures:
    # follow /releases/latest redirect and parse the /tag/<tag> URL.
    redirected_url="$(curl -fsSIL -o /dev/null -w '%{url_effective}' "${RELEASES_URL}/latest" 2>/dev/null || true)"
    printf '%s' "${redirected_url}" | sed -n 's|.*/tag/\([^/?#]*\).*|\1|p'
}

install_binary() {
    RELEASE_TAG="${TAG}"
    if [ "${TAG}" = "latest" ]; then
        echo "Resolving latest version..."
        RELEASE_TAG="$(resolve_latest_tag)"
        if [ -z "${RELEASE_TAG}" ]; then
            echo "Failed to resolve latest release tag."
            return 1
        fi
        echo "Installing eloqctl ${RELEASE_TAG}"
    else
        echo "Installing eloqctl ${TAG}"
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

if [ "${HAD_EXISTING_INSTALL}" = true ]; then
    echo "Running local state upgrade..."
    if ! ELOQCTL_HOME="${ELOQCTL_HOME}" "${BIN_PATH}" upgrade; then
        echo "WARNING: eloqctl was installed, but local state upgrade failed." >&2
        echo "Run this manually before using existing clusters:" >&2
        echo "  ELOQCTL_HOME=${ELOQCTL_HOME} ${BIN_PATH} upgrade" >&2
    fi
fi

print_completion_help() {
    echo "Shell completion is available but is not enabled automatically."
    case "${shell}" in
    bash)
        echo "Enable it with:"
        echo "  eloqctl completion bash > \"${ELOQCTL_HOME}/eloqctl.bash\""
        echo "  echo 'source \"${ELOQCTL_HOME}/eloqctl.bash\"' >> \"${HOME}/.bashrc\""
        echo "  source \"${HOME}/.bashrc\""
        ;;
    zsh)
        echo "Enable it with:"
        echo "  eloqctl completion zsh > \"${ELOQCTL_HOME}/_eloqctl\""
        echo "  echo 'fpath=(\"${ELOQCTL_HOME}\" \$fpath)' >> \"${HOME}/.zshrc\""
        echo "  echo 'autoload -Uz compinit && compinit' >> \"${HOME}/.zshrc\""
        echo "  source \"${HOME}/.zshrc\""
        ;;
    fish)
        echo "Enable it with:"
        echo "  mkdir -p \"${HOME}/.config/fish/completions\""
        echo "  eloqctl completion fish > \"${HOME}/.config/fish/completions/eloqctl.fish\""
        ;;
    *)
        echo "Current shell '${shell}' has no install hint yet."
        echo "You can still generate a script with: eloqctl completion <bash|zsh|fish>"
        ;;
    esac
}

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
    PATH_EXPORT_LINE="export PATH=${BIN_DIR}:\$PATH"
    HOME_EXPORT_LINE="export ELOQCTL_HOME=${ELOQCTL_HOME}"

    path_line_exists=false
    home_line_exists=false
    if [ -f "${PROFILE}" ]; then
        if grep -Fqx "${PATH_EXPORT_LINE}" "${PROFILE}"; then
            path_line_exists=true
        fi
        if grep -Fqx "${HOME_EXPORT_LINE}" "${PROFILE}"; then
            home_line_exists=true
        fi
    fi

    if [ "${path_line_exists}" = false ] || [ "${home_line_exists}" = false ]; then
        {
            printf '\n'
            if [ "${path_line_exists}" = false ]; then
                printf '%s\n' "${PATH_EXPORT_LINE}"
            fi
            if [ "${home_line_exists}" = false ]; then
                printf '%s\n' "${HOME_EXPORT_LINE}"
            fi
        } >>"${PROFILE}"
        echo "${PROFILE} has been modified to add eloqctl to PATH"
    else
        echo "${PROFILE} already contains eloqctl settings"
    fi
    ;;
esac

echo "==============================================="
echo "To use it, open a new terminal or execute:"
echo "${bold}source ${PROFILE}${sgr0}"
echo
print_completion_help
echo "==============================================="
