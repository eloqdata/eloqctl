#!/bin/bash

set -eo pipefail

DOCKER_E2E_DIR="${DOCKER_E2E_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/docker_ha" && pwd)}"
REPO_ROOT="${REPO_ROOT:-$(cd "${DOCKER_E2E_DIR}/../.." && pwd)}"
export ELOQCTL_HOME="${ELOQCTL_HOME:-${HOME}/.eloqctl}"
ELOQCTL="${ELOQCTL:-${ELOQCTL_HOME}/bin/cluster_mgr}"
ELOQCTL_DOCKER_SSH_KEY="${ELOQCTL_DOCKER_SSH_KEY:-${DOCKER_E2E_DIR}/id_ed25519}"
ELOQCTL_CONTROL_SSH_KEY="${ELOQCTL_CONTROL_SSH_KEY:-/home/eloq/.ssh/id_ed25519}"
CONTROL_NODE_SERVICE="${CONTROL_NODE_SERVICE:-eloq-node-4}"
CONTROL_REPO_ROOT="${CONTROL_REPO_ROOT:-/workspace/eloq_waiter}"
CONTROL_ELOQCTL_HOME="${CONTROL_ELOQCTL_HOME:-/home/eloq/.eloqctl}"
CONTROL_ELOQCTL="${CONTROL_ELOQCTL:-${CONTROL_ELOQCTL_HOME}/bin/cluster_mgr}"
HOST_ELOQCTL_HOME="${HOST_ELOQCTL_HOME:-${REPO_ROOT}/.tmp-eloqctl}"
export ELOQCTL_DOCKER_SSH_KEY
CLEANUP_TIMEOUT_SECONDS="${CLEANUP_TIMEOUT_SECONDS:-20}"
ELOQKV_VERSION="${ELOQKV_VERSION:-1.2.2}"
MINIO_ALIAS="${MINIO_ALIAS:-e2e-minio}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://127.0.0.1:19000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-storeeloqservice}"

COMPOSE_BASE="${DOCKER_E2E_DIR}/docker-compose.yaml"
COMPOSE_OVERRIDE=""
if [ -n "${COMPOSE_OVERRIDE_FILE:-}" ] && [ -f "${COMPOSE_OVERRIDE_FILE}" ]; then
    COMPOSE_OVERRIDE="${COMPOSE_OVERRIDE_FILE}"
fi

compose() {
    local args=(-f "${COMPOSE_BASE}")
    [ -n "${COMPOSE_OVERRIDE}" ] && args+=(-f "${COMPOSE_OVERRIDE}")
    if docker compose version >/dev/null 2>&1; then
        docker compose "${args[@]}" "$@"
    else
        docker-compose "${args[@]}" "$@"
    fi
}

compose_down() {
    if docker compose version >/dev/null 2>&1; then
        timeout --kill-after=5s "${CLEANUP_TIMEOUT_SECONDS}s" docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" down -v >/dev/null 2>&1 || true
    else
        timeout --kill-after=5s "${CLEANUP_TIMEOUT_SECONDS}s" docker-compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" down -v >/dev/null 2>&1 || true
    fi
}

ssh_cmd() {
    ssh -o UserKnownHostsFile=/dev/null \
        -o StrictHostKeyChecking=no \
        -o PasswordAuthentication=no \
        -o BatchMode=yes \
        -o ConnectTimeout=3 \
        -i "${ELOQCTL_DOCKER_SSH_KEY}" \
        eloq@127.0.0.1 \
        -p "$1" \
        "${@:2}"
}

ensure_dev_eloqctl() {
    if [ ! -d "${ELOQCTL_HOME}/config" ] || [ ! -x "${ELOQCTL}" ]; then
        "${REPO_ROOT}/scripts/install-dev.sh"
    fi
}

ensure_ssh_key() {
    if [ ! -f "${ELOQCTL_DOCKER_SSH_KEY}" ]; then
        ssh-keygen -t ed25519 -N '' -f "${ELOQCTL_DOCKER_SSH_KEY}" >/dev/null
    fi
    cp "${ELOQCTL_DOCKER_SSH_KEY}.pub" "${DOCKER_E2E_DIR}/authorized_keys"
}

render_topology() {
    local source_topology="$1"
    local rendered_topology="$2"
    sed -e "s|\${ELOQCTL_DOCKER_SSH_KEY}|${ELOQCTL_DOCKER_SSH_KEY}|g" \
        -e "s|\${ELOQKV_VERSION}|${ELOQKV_VERSION}|g" \
        "${source_topology}" > "${rendered_topology}"
}

render_topology_for_control() {
    local source_topology="$1"
    local rendered_topology="$2"
    sed -e "s|\${ELOQCTL_DOCKER_SSH_KEY}|${ELOQCTL_CONTROL_SSH_KEY}|g" \
        -e "s|\${ELOQKV_VERSION}|${ELOQKV_VERSION}|g" \
        "${source_topology}" > "${rendered_topology}"
}

control_exec() {
    compose exec -T -u eloq "${CONTROL_NODE_SERVICE}" env HOME=/home/eloq "$@"
}

prepare_control_node() {
    compose exec -T "${CONTROL_NODE_SERVICE}" bash -lc "
        set -eu
        install -d -m 700 -o eloq -g eloq /home/eloq/.ssh
        install -m 600 -o eloq -g eloq '${CONTROL_REPO_ROOT}/tests/docker_ha/id_ed25519' '${ELOQCTL_CONTROL_SSH_KEY}'
        install -d -m 755 -o eloq -g eloq '${CONTROL_ELOQCTL_HOME}'
        install -d -m 755 -o eloq -g eloq '${CONTROL_ELOQCTL_HOME}/bin'
        cp '${CONTROL_REPO_ROOT}/target/debug/cluster_mgr' '${CONTROL_ELOQCTL}'
        chmod 755 '${CONTROL_ELOQCTL}'
        rm -f /usr/local/bin/eloqctl
        printf '%s\n' '#!/bin/bash' \
            'export HOME=/home/eloq' \
            'export ELOQCTL_HOME=/home/eloq/.eloqctl' \
            'exec /home/eloq/.eloqctl/bin/cluster_mgr "\$@"' \
            > /usr/local/bin/eloqctl
        chmod 755 /usr/local/bin/eloqctl
        rm -f '${CONTROL_ELOQCTL_HOME}/config'
        ln -s '${CONTROL_REPO_ROOT}/src/cluster_mgr/config' '${CONTROL_ELOQCTL_HOME}/config'
        cat > /etc/profile.d/eloqctl.sh <<'EOF'
export ELOQCTL_HOME='${CONTROL_ELOQCTL_HOME}'
EOF
        grep -qxF 'export ELOQCTL_HOME=${ELOQCTL_HOME:-/home/eloq/.eloqctl}' /home/eloq/.bashrc || \
            printf '\nexport ELOQCTL_HOME=${ELOQCTL_HOME:-/home/eloq/.eloqctl}\n' >> /home/eloq/.bashrc
        chown -h eloq:eloq '${CONTROL_ELOQCTL_HOME}/config'
        chown -R eloq:eloq '${CONTROL_ELOQCTL_HOME}'
    "
}

start_docker_env() {
    ensure_dev_eloqctl
    ensure_ssh_key

    compose_down

    echo "[docker] Build Ubuntu SSH containers"
    COMPOSE_PROGRESS=plain compose build

    echo "[docker] Start Docker HA network"
    compose up -d >/dev/null

    echo "[docker] Wait for MinIO"
    for _ in $(seq 1 60); do
        if curl -fsS "${MINIO_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    curl -fsS "${MINIO_ENDPOINT}/minio/health/live" >/dev/null 2>&1 || {
        echo "FAIL: MinIO is not ready at ${MINIO_ENDPOINT}"
        compose ps || true
        compose logs --no-color --tail=80 minio || true
        exit 1
    }

    echo "[docker] Ensure MinIO bucket ${MINIO_BUCKET}"
    docker run --rm --network host minio/mc:RELEASE.2025-05-21T01-59-54Z \
        alias set "${MINIO_ALIAS}" "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null
    docker run --rm --network host minio/mc:RELEASE.2025-05-21T01-59-54Z \
        mb --ignore-existing "${MINIO_ALIAS}/${MINIO_BUCKET}" >/dev/null

    echo "[docker] Wait for SSH"
    for host in 2221 2222 2223 2224; do
        for _ in $(seq 1 60); do
            if ssh_cmd "${host}" true >/dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        ssh_cmd "${host}" true >/dev/null || {
            echo "FAIL: SSH is not ready on 127.0.0.1:${host}"
            compose ps || true
            compose logs --no-color --tail=80 || true
            exit 1
        }
    done

    echo "[docker] Prepare control node ${CONTROL_NODE_SERVICE}"
    prepare_control_node
}

dump_failure_diagnostics() {
    local log_file="$1"
    echo "---- ${log_file} ----"
    if [ -f "${log_file}" ]; then
        tail -80 "${log_file}" || true
    else
        echo "missing"
    fi
    echo "---- eloqctl command logs ----"
    if [ -d "${ELOQCTL_HOME}/logs" ]; then
        ls -lt "${ELOQCTL_HOME}/logs" || true
        for file in "${ELOQCTL_HOME}"/logs/last-*.log; do
            [ -f "${file}" ] || continue
            echo "---- ${file} ----"
            tail -80 "${file}" || true
        done
    fi
    echo "---- docker status ----"
    compose ps || true
    compose logs --no-color --tail=80 || true
}

run_with_progress() {
    local timeout_seconds="$1"
    local log_file="$2"
    shift 2

    : > "${log_file}"
    timeout --kill-after=10s "${timeout_seconds}s" "$@" > "${log_file}" 2>&1 &
    local cmd_pid=$!
    local elapsed=0
    local last_cmd_lines=0
    local last_eloq_lines=0
    while kill -0 "${cmd_pid}" >/dev/null 2>&1; do
        sleep 5
        elapsed=$((elapsed + 5))
        echo "  ... still running after ${elapsed}s: $*"
        if [ -s "${log_file}" ]; then
            local cmd_lines
            cmd_lines=$(wc -l < "${log_file}" 2>/dev/null || echo 0)
            if [ "${cmd_lines}" -gt "${last_cmd_lines}" ]; then
                echo "  ---- new command output ----"
                sed -n "$((last_cmd_lines + 1)),$((cmd_lines))p" "${log_file}" || true
                last_cmd_lines="${cmd_lines}"
            fi
        fi
        if [ -f "${ELOQCTL_HOME}/logs/last-launch.log" ]; then
            local eloq_lines
            eloq_lines=$(wc -l < "${ELOQCTL_HOME}/logs/last-launch.log" 2>/dev/null || echo 0)
            if [ "${eloq_lines}" -gt "${last_eloq_lines}" ]; then
                echo "  ---- new eloqctl log ----"
                sed -n "$((last_eloq_lines + 1)),$((eloq_lines))p" "${ELOQCTL_HOME}/logs/last-launch.log" || true
                last_eloq_lines="${eloq_lines}"
            fi
        fi
    done
    wait "${cmd_pid}"
}
