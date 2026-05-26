#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${REPO_ROOT:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
DOCKER_E2E_DIR="${DOCKER_E2E_DIR:-${REPO_ROOT}/tests/docker_ha}"

source "${REPO_ROOT}/tests/docker_env.sh"

BINARY_PATH="${1:-${REPO_ROOT}/target/debug/cluster_mgr}"
CONFIG_DIR="${REPO_ROOT}/src/cluster_mgr/config"
SSH_KEY_PATH="${DOCKER_E2E_DIR}/id_ed25519"
INSTALL_ROOT="${CONTROL_ELOQCTL_HOME}"
INSTALL_BIN="${INSTALL_ROOT}/bin/cluster_mgr"

if [ ! -x "${BINARY_PATH}" ]; then
    echo "missing executable binary: ${BINARY_PATH}" >&2
    echo "build it first with: cargo build -p cluster_mgr --bin cluster_mgr" >&2
    exit 1
fi

if [ ! -d "${CONFIG_DIR}" ]; then
    echo "missing config directory: ${CONFIG_DIR}" >&2
    exit 1
fi

if [ ! -f "${SSH_KEY_PATH}" ]; then
    echo "missing ssh key: ${SSH_KEY_PATH}" >&2
    exit 1
fi

compose ps "${CONTROL_NODE_SERVICE}" >/dev/null

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

cp "${BINARY_PATH}" "${TMP_DIR}/cluster_mgr"
cp -R "${CONFIG_DIR}" "${TMP_DIR}/config"
cp "${SSH_KEY_PATH}" "${TMP_DIR}/id_ed25519"

compose exec -T "${CONTROL_NODE_SERVICE}" bash -lc "
    set -euo pipefail
    install -d -m 700 -o eloq -g eloq /home/eloq/.ssh
    install -d -m 755 -o eloq -g eloq '${INSTALL_ROOT}'
    install -d -m 755 -o eloq -g eloq '${INSTALL_ROOT}/bin'
    install -d -m 755 -o eloq -g eloq '${INSTALL_ROOT}/db'
    install -d -m 755 -o eloq -g eloq '${INSTALL_ROOT}/download'
    install -d -m 755 -o eloq -g eloq '${INSTALL_ROOT}/logs'
    install -d -m 755 -o eloq -g eloq '${INSTALL_ROOT}/upload'
    rm -f '${INSTALL_BIN}'
    rm -rf '${INSTALL_ROOT}/config'
"

compose cp "${TMP_DIR}/cluster_mgr" "${CONTROL_NODE_SERVICE}:${INSTALL_BIN}"
compose cp "${TMP_DIR}/config" "${CONTROL_NODE_SERVICE}:${INSTALL_ROOT}/config"
compose cp "${TMP_DIR}/id_ed25519" "${CONTROL_NODE_SERVICE}:${ELOQCTL_CONTROL_SSH_KEY}"

compose exec -T "${CONTROL_NODE_SERVICE}" bash -lc "
    set -euo pipefail
    chown -R eloq:eloq '${INSTALL_ROOT}' /home/eloq/.ssh
    chmod 755 '${INSTALL_BIN}'
    chmod 600 '${ELOQCTL_CONTROL_SSH_KEY}'
    printf '%s\n' '#!/bin/bash' \
        'export HOME=/home/eloq' \
        'export ELOQCTL_HOME=/home/eloq/.eloqctl' \
        'exec /home/eloq/.eloqctl/bin/cluster_mgr "\$@"' \
        > /usr/local/bin/eloqctl
    chmod 755 /usr/local/bin/eloqctl
"

echo "installed eloqctl into ${CONTROL_NODE_SERVICE}"
echo "login with: ssh -i \"${SSH_KEY_PATH}\" -p 2224 eloq@127.0.0.1"
echo "run with: eloqctl --help"
