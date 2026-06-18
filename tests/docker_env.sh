#!/bin/bash

_docker_env_script_dir() {
    if [ -n "${BASH_SOURCE[0]:-}" ]; then
        cd "$(dirname "${BASH_SOURCE[0]}")" && pwd
        return
    fi
    if [ -n "${ZSH_VERSION:-}" ]; then
        cd "$(dirname "${(%):-%x}")" && pwd
        return
    fi
    pwd
}

DOCKER_ENV_SCRIPT_DIR="${DOCKER_ENV_SCRIPT_DIR:-$(_docker_env_script_dir)}"
DOCKER_E2E_DIR="${DOCKER_E2E_DIR:-$(cd "${DOCKER_ENV_SCRIPT_DIR}/docker_ha" && pwd)}"
REPO_ROOT="${REPO_ROOT:-$(cd "${DOCKER_E2E_DIR}/../.." && pwd)}"
export ELOQCTL_HOME="${ELOQCTL_HOME:-${HOME}/.eloqctl}"
ELOQCTL="${ELOQCTL:-${ELOQCTL_HOME}/bin/eloqctl}"
ELOQCTL_DOCKER_SSH_KEY="${ELOQCTL_DOCKER_SSH_KEY:-${DOCKER_E2E_DIR}/id_ed25519}"
ELOQCTL_CONTROL_SSH_KEY="${ELOQCTL_CONTROL_SSH_KEY:-/home/eloq/.ssh/id_ed25519}"
CONTROL_NODE_SERVICE="${CONTROL_NODE_SERVICE:-eloq-control}"
CONTROL_REPO_ROOT="${CONTROL_REPO_ROOT:-/workspace/eloq_waiter}"
CONTROL_ELOQCTL_HOME="${CONTROL_ELOQCTL_HOME:-/home/eloq/.eloqctl}"
CONTROL_ELOQCTL="${CONTROL_ELOQCTL:-${CONTROL_ELOQCTL_HOME}/bin/eloqctl}"
export ELOQCTL_DOCKER_SSH_KEY
CLEANUP_TIMEOUT_SECONDS="${CLEANUP_TIMEOUT_SECONDS:-20}"
FORCE_DOCKER_BUILD="${FORCE_DOCKER_BUILD:-0}"
ELOQKV_VERSION="${ELOQKV_VERSION:-1.3.1}"
ELOQCTL_FEISHU_ROBOT_URL="${ELOQCTL_FEISHU_ROBOT_URL:-}"
MINIO_ALIAS="${MINIO_ALIAS:-e2e-minio}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://127.0.0.1:19000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-storeeloqservice}"
HOST_DOWNLOAD_CACHE_ROOT="${HOST_DOWNLOAD_CACHE_ROOT:-${REPO_ROOT}/.tmp-e2e-download-cache}"
LOCAL_PREFETCH_CONTROL_CACHE="${LOCAL_PREFETCH_CONTROL_CACHE:-1}"
if [ "${CI:-}" = "true" ] || [ "${GITHUB_ACTIONS:-}" = "true" ]; then
    LOCAL_PREFETCH_CONTROL_CACHE="0"
fi

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

docker_image_exists() {
    docker image inspect "$1" >/dev/null 2>&1
}

ensure_docker_images() {
    if [ "${FORCE_DOCKER_BUILD}" = "1" ]; then
        echo "[docker] Build Ubuntu SSH and stress containers (forced)"
        COMPOSE_PROGRESS=plain compose build
        return
    fi

    local required_images=(
        "eloqctl-e2e-ubuntu:24.04"
        "eloqctl-e2e-stress:latest"
        "eloqctl-e2e-stress-go:latest"
        "eloqctl-e2e-stress-ts:latest"
        "eloqctl-e2e-resp-compat:latest"
    )
    local image
    for image in "${required_images[@]}"; do
        if ! docker_image_exists "${image}"; then
            echo "[docker] Build Ubuntu SSH and stress containers"
            COMPOSE_PROGRESS=plain compose build
            return
        fi
    done

    echo "[docker] Reuse existing Ubuntu SSH and stress images"
}

compose_down() {
    if docker compose version >/dev/null 2>&1; then
        timeout --kill-after=5s "${CLEANUP_TIMEOUT_SECONDS}s" docker compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" down -v --remove-orphans >/dev/null 2>&1 || true
    else
        timeout --kill-after=5s "${CLEANUP_TIMEOUT_SECONDS}s" docker-compose -f "${DOCKER_E2E_DIR}/docker-compose.yaml" down -v --remove-orphans >/dev/null 2>&1 || true
    fi
}

clear_minio_data() {
    if ! docker ps --format '{{.Names}}' | grep -qx 'eloqctl-e2e-minio'; then
        return 0
    fi

    if ! curl -fsS "${MINIO_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
        echo "[docker] Skip MinIO cleanup because ${MINIO_ENDPOINT} is not ready"
        return 0
    fi

    echo "[docker] Clear MinIO bucket ${MINIO_BUCKET}"
    docker run --rm --network host --entrypoint /bin/sh \
        minio/mc:RELEASE.2025-05-21T01-59-54Z -lc "
            set -euo pipefail
            mc alias set '${MINIO_ALIAS}' '${MINIO_ENDPOINT}' \
                '${MINIO_ROOT_USER}' '${MINIO_ROOT_PASSWORD}' >/dev/null
            mc rm --recursive --force '${MINIO_ALIAS}/${MINIO_BUCKET}' >/dev/null 2>&1 || true
        "
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

control_ssh_cmd() {
    local remote_cmd
    printf -v remote_cmd '%q ' \
        env HOME=/home/eloq ELOQCTL_HOME="${CONTROL_ELOQCTL_HOME}" "${CONTROL_ELOQCTL}" "$@"
    ssh_cmd 2224 "bash -lc $(printf '%q' "${remote_cmd}")"
}

ensure_ssh_key() {
    if [ ! -f "${ELOQCTL_DOCKER_SSH_KEY}" ]; then
        ssh-keygen -t ed25519 -N '' -f "${ELOQCTL_DOCKER_SSH_KEY}" >/dev/null
    fi
    cp "${ELOQCTL_DOCKER_SSH_KEY}.pub" "${DOCKER_E2E_DIR}/authorized_keys"
}

reset_control_runtime_state_in_container() {
    compose exec -T "${CONTROL_NODE_SERVICE}" bash -lc "
        set -eu
        rm -rf '${CONTROL_ELOQCTL_HOME}'
        install -d -m 755 -o eloq -g eloq \
            '${CONTROL_ELOQCTL_HOME}' \
            '${CONTROL_ELOQCTL_HOME}/bin' \
            '${CONTROL_ELOQCTL_HOME}/db' \
            '${CONTROL_ELOQCTL_HOME}/download' \
            '${CONTROL_ELOQCTL_HOME}/logs' \
            '${CONTROL_ELOQCTL_HOME}/upload'
    "
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
        -e "s|\${ELOQCTL_FEISHU_ROBOT_URL}|${ELOQCTL_FEISHU_ROBOT_URL}|g" \
        "${source_topology}" > "${rendered_topology}"
}

e2e_eloqkv_store_for_topology() {
    local topology_path="$1"
    if rg -q 'backend:\s*!eloqstore' "${topology_path}"; then
        local provider
        provider="$(sed -n 's/.*eloq_store_cloud_provider:[[:space:]]*"\([^"]*\)".*/\1/p' "${topology_path}" | head -n1)"
        case "${provider:-aws}" in
            gcs)
                echo "eloqstore_gcs"
                ;;
            *)
                echo "eloqstore_s3"
                ;;
        esac
        return 0
    fi

    echo "rocks_s3"
}

e2e_eloqkv_url() {
    local version="${1:-${ELOQKV_VERSION}}"
    local topology_path="${2:-}"
    local store="rocks_s3"
    if [ -n "${topology_path}" ] && [ -f "${topology_path}" ]; then
        store="$(e2e_eloqkv_store_for_topology "${topology_path}")"
    fi
    printf '%s\n' \
        "https://github.com/eloqdata/eloqkv/releases/download/${version}/eloqkv-${version}-${store}-ubuntu24-amd64.tar.gz"
}

_download_cache_target_for_url() {
    local url="$1"
    local filename="${url##*/}"
    if [[ "${url}" == https://github.com/eloqdata/eloqkv/releases/download/* ]]; then
        local store
        store="$(printf '%s\n' "${filename}" | sed -E 's/^eloqkv-[^-]+-([^-]+)-.*$/\1/')"
        printf '%s\n' "${HOST_DOWNLOAD_CACHE_ROOT}/eloqkv/${store}/${filename}"
        return
    fi
    printf '%s\n' "${HOST_DOWNLOAD_CACHE_ROOT}/${filename}"
}

prefetch_url_to_host_cache() {
    local url="$1"
    local target
    target="$(_download_cache_target_for_url "${url}")"
    local part="${target}.partial"
    mkdir -p "$(dirname "${target}")"

    local expected_len=""
    expected_len="$(curl -fsSLI -o /dev/null -w '%{content_length_download}' "${url}" 2>/dev/null || true)"
    if [ -f "${target}" ] && [ -n "${expected_len}" ] && [ "${expected_len}" != "-1" ]; then
        local actual_len
        actual_len="$(wc -c < "${target}" | tr -d '[:space:]')"
        if [ "${actual_len}" = "${expected_len}" ]; then
            return 0
        fi
        mv "${target}" "${part}"
    elif [ -f "${target}" ] && [ ! -f "${part}" ]; then
        mv "${target}" "${part}"
    fi

    if ! curl -fL --retry 8 --retry-delay 3 --retry-all-errors --continue-at - \
        -o "${part}" "${url}"; then
        echo "[docker] Resume failed, restarting download from scratch"
        rm -f "${part}"
        curl -fL --retry 8 --retry-delay 3 --retry-all-errors \
            -o "${part}" "${url}"
    fi
    mv "${part}" "${target}"
}

prefetch_control_download_cache() {
    local topology_path="$1"
    shift || true
    if [ "${LOCAL_PREFETCH_CONTROL_CACHE}" != "1" ]; then
        echo "[docker] Skip host prefetch for control-node download cache"
        return 0
    fi

    local urls=()
    urls+=("$(e2e_eloqkv_url "${ELOQKV_VERSION}" "${topology_path}")")
    while IFS= read -r url; do
        [ -n "${url}" ] && urls+=("${url}")
    done < <(sed -n 's/.*: "\(https\?:[^"]*\)".*/\1/p' "${topology_path}")
    while [ "$#" -gt 0 ]; do
        [ -n "${1}" ] && urls+=("${1}")
        shift
    done

    echo "[docker] Prefetch release packages on host"
    mkdir -p "${HOST_DOWNLOAD_CACHE_ROOT}"

    local url
    for url in "${urls[@]}"; do
        prefetch_url_to_host_cache "${url}"
    done
}

sync_control_download_cache() {
    if [ "${LOCAL_PREFETCH_CONTROL_CACHE}" != "1" ]; then
        return 0
    fi
    if [ ! -d "${HOST_DOWNLOAD_CACHE_ROOT}" ]; then
        return 0
    fi

    echo "[docker] Sync host download cache into control node"
    tar -C "${HOST_DOWNLOAD_CACHE_ROOT}" -cf - . | \
        compose exec -T "${CONTROL_NODE_SERVICE}" bash -lc "
            set -euo pipefail
            install -d -m 755 -o eloq -g eloq '${CONTROL_ELOQCTL_HOME}/download'
            tar -C '${CONTROL_ELOQCTL_HOME}/download' -xf -
            chown -R eloq:eloq '${CONTROL_ELOQCTL_HOME}/download'
        "
}

control_exec() {
    compose exec -T -u eloq "${CONTROL_NODE_SERVICE}" env HOME=/home/eloq "$@"
}

prepare_control_node() {
    compose exec -T "${CONTROL_NODE_SERVICE}" bash -lc "
        set -eu
        install -d -m 700 -o eloq -g eloq /home/eloq/.ssh
        install -d -m 755 -o eloq -g eloq '${CONTROL_ELOQCTL_HOME}'
        install -m 600 -o eloq -g eloq '${CONTROL_REPO_ROOT}/tests/docker_ha/id_ed25519' '${ELOQCTL_CONTROL_SSH_KEY}'
        cat > /etc/profile.d/eloqctl.sh <<'EOF'
export ELOQCTL_HOME='${CONTROL_ELOQCTL_HOME}'
EOF
        grep -qxF 'export ELOQCTL_HOME=${ELOQCTL_HOME:-/home/eloq/.eloqctl}' /home/eloq/.bashrc || \
            printf '\nexport ELOQCTL_HOME=${ELOQCTL_HOME:-/home/eloq/.eloqctl}\n' >> /home/eloq/.bashrc
        chown -R eloq:eloq '${CONTROL_ELOQCTL_HOME}' /home/eloq/.ssh
    "
}

start_docker_env() {
    ensure_ssh_key

    compose_down

    ensure_docker_images

    echo "[docker] Start Docker HA network"
    compose up -d --remove-orphans >/dev/null

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
    echo "[docker] Reset control-node runtime state"
    reset_control_runtime_state_in_container
}

dump_failure_diagnostics() {
    local log_file="$1"
    echo "---- ${log_file} ----"
    if [ -f "${log_file}" ]; then
        tail -80 "${log_file}" || true
    else
        echo "missing"
    fi
    echo "---- host eloqctl command logs ----"
    if [ -d "${ELOQCTL_HOME}/logs" ]; then
        ls -lt "${ELOQCTL_HOME}/logs" || true
        for file in "${ELOQCTL_HOME}"/logs/last-*.log; do
            [ -f "${file}" ] || continue
            echo "---- ${file} ----"
            tail -80 "${file}" || true
        done
    fi
    echo "---- control eloqctl command logs ----"
    control_exec bash -lc '
        set +e
        if [ -d "'"${CONTROL_ELOQCTL_HOME}"'/logs" ]; then
            ls -lt "'"${CONTROL_ELOQCTL_HOME}"'/logs"
            for file in "'"${CONTROL_ELOQCTL_HOME}"'/logs"/last-*.log; do
                [ -f "${file}" ] || continue
                echo "---- ${file} ----"
                tail -80 "${file}" || true
            done
        else
            echo "missing"
        fi
    ' || true
    echo "---- docker status ----"
    compose ps || true
    compose logs --no-color --tail=80 || true
}

run_with_progress() {
    local timeout_seconds="$1"
    local log_file="$2"
    shift 2
    local control_log_file=""
    if [ "${1:-}" = "--eloq-log" ]; then
        control_log_file="$2"
        shift 2
    fi

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
        if [ -n "${control_log_file}" ]; then
            local eloq_lines
            eloq_lines=$(control_exec bash -lc "wc -l < '${control_log_file}' 2>/dev/null || echo 0" 2>/dev/null | tr -d '\r' || echo 0)
            if [ "${eloq_lines}" -gt "${last_eloq_lines}" ]; then
                echo "  ---- new control eloqctl log (${control_log_file}) ----"
                control_exec bash -lc "sed -n '$((last_eloq_lines + 1)),$((eloq_lines))p' '${control_log_file}' || true" || true
                last_eloq_lines="${eloq_lines}"
            fi
        fi
    done
    wait "${cmd_pid}"
}
