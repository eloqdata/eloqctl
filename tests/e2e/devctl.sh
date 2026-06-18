#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source "${REPO_ROOT}/tests/docker_env.sh"

CONTROL_TOPO="/home/eloq/topology.generated.yaml"
DEFAULT_CLUSTER="test-e2e"

topology_template() {
    echo "${E2E_TOPOLOGY_TEMPLATE:-tests/e2e/topology.yaml}"
}

usage() {
    cat <<'EOF'
Usage: tests/e2e/devctl.sh <command> [args]

Commands:
  build                     Build eloqctl locally
  env-up                    Start the Docker E2E environment
  env-down                  Stop and remove the Docker E2E environment
  install-control           Copy the current eloqctl build into the control node
  control-shell             SSH into the control node
  render-topology           Render the selected topology inside the control node
  prefetch-control-cache    Prefetch release packages on host and sync into control node
  launch                    Launch the default E2E cluster from the control node
  status                    Show cluster and monitor status
  grafana-update [url]      Upgrade Grafana with monitor update
  cluster-update [version]  Run rolling eloqctl update on the default cluster
  export-topology           Export cluster topology from the control node
  stress [steps]            Run cmd_stress_test.sh via devctl; default steps are unchanged
  traffic-start             Start manual cluster-client stress traffic in stress-python
  traffic-pause             Pause manual stress traffic with SIGSTOP
  traffic-resume            Resume manual stress traffic with SIGCONT
  traffic-stop              Stop manual stress traffic and clean pid file
  traffic-status            Show manual stress traffic process status and recent log
  backup                    Run backup start, list, and remove on the default cluster
  full                      build -> env-up -> install-control -> render-topology -> launch

Environment:
  CLUSTER_NAME              Default: test-e2e
  ELOQKV_VERSION            Override the EloqKV version during topology rendering
  E2E_TOPOLOGY_TEMPLATE     Default: tests/e2e/topology.yaml
EOF
}

cluster_name() {
    echo "${CLUSTER_NAME:-${DEFAULT_CLUSTER}}"
}

build_eloqctl() {
    (cd "${REPO_ROOT}" && cargo build -p cluster_mgr --bin eloqctl)
}

install_control() {
    (cd "${REPO_ROOT}" && bash tests/install_control_eloqctl.sh)
}

render_control_topology() {
    local template
    local version
    local feishu_url
    template="$(topology_template)"
    version="${ELOQKV_VERSION:-1.3.1}"
    feishu_url="${ELOQCTL_FEISHU_ROBOT_URL:-}"
    control_exec env \
        ELOQKV_VERSION="${version}" \
        ELOQCTL_FEISHU_ROBOT_URL="${feishu_url}" \
        bash -lc "
        cd '${CONTROL_REPO_ROOT}'
        source tests/docker_env.sh
        render_topology_for_control '${template}' '${CONTROL_TOPO}'
        ls -l '${CONTROL_TOPO}'
    "
}

prefetch_control_cache() {
    local template
    template="${REPO_ROOT}/$(topology_template)"
    prefetch_control_download_cache "${template}" \
        "https://dl.grafana.com/grafana/release/13.0.1+security-01/grafana_13.0.1+security-01_25720641773_linux_amd64.tar.gz"
    sync_control_download_cache
}

launch_cluster() {
    local cluster
    cluster="$(cluster_name)"
    prefetch_control_cache
    control_exec bash -lc "
        eloqctl stop '${cluster}' --all --force >/dev/null 2>&1 || true
        eloqctl remove '${cluster}' --force >/dev/null 2>&1 || true
        eloqctl launch --skip-deps '${CONTROL_TOPO}'
    "
}

show_status() {
    local cluster
    cluster="$(cluster_name)"
    control_exec bash -lc "
        eloqctl status '${cluster}' --wait 180
        eloqctl monitor status --cluster '${cluster}'
    "
}

grafana_update() {
    local cluster url
    cluster="$(cluster_name)"
    url="${1:-https://dl.grafana.com/grafana/release/13.0.1+security-01/grafana_13.0.1+security-01_25720641773_linux_amd64.tar.gz}"
    control_exec bash -lc "
        eloqctl monitor update --cluster '${cluster}' --component grafana --url '${url}'
    "
}

cluster_update() {
    local cluster version
    cluster="$(cluster_name)"
    version="${1:-${ELOQKV_VERSION:-1.3.1}}"
    control_exec bash -lc "
        eloqctl update '${cluster}' '${version}' --password testpass
    "
}

export_topology() {
    local cluster
    cluster="$(cluster_name)"
    control_exec bash -lc "
        eloqctl export '${cluster}' --output /home/eloq/${cluster}-export.yaml
        ls -l /home/eloq/${cluster}-export.yaml
    "
}

stress_run() {
    local steps_arg="${1:-}"
    if [ -n "${steps_arg}" ]; then
        (cd "${REPO_ROOT}" && STEPS="${steps_arg}" bash tests/e2e/cmd_stress_test.sh)
    else
        (cd "${REPO_ROOT}" && bash tests/e2e/cmd_stress_test.sh)
    fi
}

traffic_pid_file() {
    echo "/tmp/eloq-cluster-stress.pid"
}

traffic_log_file() {
    echo "/tmp/eloq-cluster-stress.log"
}

traffic_start() {
    local pid_file log_file
    pid_file="$(traffic_pid_file)"
    log_file="$(traffic_log_file)"
    compose exec -T stress-python bash -lc "
        set -euo pipefail
        if [ -f '${pid_file}' ]; then
            pid=\"\$(cat '${pid_file}')\"
            stat=\"\$(ps -o stat= -p \"\${pid}\" 2>/dev/null | tr -d ' ' || true)\"
            if [ -n \"\${stat}\" ] && [ \"\${stat#Z}\" = \"\${stat}\" ]; then
                echo stress already running with pid \"\${pid}\" state=\"\${stat}\"
                exit 0
            fi
            rm -f '${pid_file}'
        fi
        nohup python3 -u tests/e2e/cmd_stress_py/main.py \
          --startup-node 172.28.10.11:6379 \
          --startup-node 172.28.10.12:6379 \
          --password testpass \
          --tls \
          --read-from-replicas \
          --client-mode cluster-only \
          --workers 16 \
          --inflight 50 \
          --repeat 10 \
          --key-count 256 \
          --cmd-timeout 5 \
          --progress-interval 5 \
          --duration 0 \
          >'${log_file}' 2>&1 &
        echo \$! > '${pid_file}'
        echo started stress pid \$(cat '${pid_file}')
    "
}

traffic_pause() {
    local pid_file
    pid_file="$(traffic_pid_file)"
    compose exec -T stress-python bash -lc "
        set -euo pipefail
        test -f '${pid_file}'
        kill -STOP \"\$(cat '${pid_file}')\"
        echo paused stress pid \"\$(cat '${pid_file}')\"
    "
}

traffic_resume() {
    local pid_file
    pid_file="$(traffic_pid_file)"
    compose exec -T stress-python bash -lc "
        set -euo pipefail
        test -f '${pid_file}'
        kill -CONT \"\$(cat '${pid_file}')\"
        echo resumed stress pid \"\$(cat '${pid_file}')\"
    "
}

traffic_stop() {
    local pid_file
    pid_file="$(traffic_pid_file)"
    compose exec -T stress-python bash -lc "
        set +e
        set -uo pipefail
        if [ -f '${pid_file}' ]; then
            pid=\"\$(cat '${pid_file}')\"
            stat=\"\$(ps -o stat= -p \"\${pid}\" 2>/dev/null | tr -d ' ' || true)\"
            if [ -z \"\${stat}\" ]; then
                rm -f '${pid_file}'
                echo stress pid \"\${pid}\" already gone
                exit 0
            fi
            if [ \"\${stat#Z}\" != \"\${stat}\" ]; then
                rm -f '${pid_file}'
                echo \"stress pid \${pid} is zombie; cleaned stale pid file\"
                exit 0
            fi
            kill -TERM \"\${pid}\" 2>/dev/null || true
            for _ in 1 2 3 4 5 6 7 8 9 10; do
                stat=\"\$(ps -o stat= -p \"\${pid}\" 2>/dev/null | tr -d ' ' || true)\"
                if [ -z \"\${stat}\" ]; then
                    break
                fi
                if [ \"\${stat#Z}\" != \"\${stat}\" ]; then
                    rm -f '${pid_file}'
                    echo \"stress pid \${pid} became zombie after SIGTERM; cleaned stale pid file\"
                    exit 0
                fi
                sleep 1
            done
            if kill -0 \"\${pid}\" 2>/dev/null; then
                kill -KILL \"\${pid}\" 2>/dev/null || true
                sleep 1
            fi
            rm -f '${pid_file}'
            echo stopped stress pid \"\${pid}\"
        else
            echo stress not running
        fi
    "
}

traffic_status() {
    local pid_file log_file
    pid_file="$(traffic_pid_file)"
    log_file="$(traffic_log_file)"
    compose exec -T stress-python bash -lc "
        set +e
        set -uo pipefail
        if [ -f '${pid_file}' ]; then
            pid=\"\$(cat '${pid_file}')\"
            ps -o pid,ppid,stat,etime,cmd -p \"\${pid}\"
            stat=\"\$(ps -o stat= -p \"\${pid}\" 2>/dev/null | tr -d ' ' || true)\"
            if [ -n \"\${stat}\" ] && [ \"\${stat#Z}\" != \"\${stat}\" ]; then
                echo 'note: stress process is zombie; pid file can be cleaned with traffic-stop'
            fi
        else
            echo stress pid file missing
        fi
        echo '--- log tail ---'
        tail -20 '${log_file}' 2>/dev/null || true
    "
}

run_backup() {
    local cluster backup_path
    cluster="$(cluster_name)"
    backup_path="/home/eloq/backups"

    echo "=== Backup: create backup directory ==="
    control_exec bash -lc "
        mkdir -p '${backup_path}'
    "

    echo "=== Backup: start snapshot ==="
    control_exec bash -lc "
        eloqctl backup '${cluster}' start --path '${backup_path}' --password testpass
    "

    echo "=== Backup: list snapshots ==="
    control_exec bash -lc "
        eloqctl backup '${cluster}' list
    "

    echo "=== Backup: remove snapshots older than 1 second ==="
    control_exec bash -lc "
        eloqctl backup '${cluster}' remove --until '1s' --force
    "

    echo "=== Backup: list snapshots after cleanup ==="
    control_exec bash -lc "
        eloqctl backup '${cluster}' list
    "

    echo "=== Backup: e2e test completed ==="
}

control_shell() {
    exec ssh -i "${DOCKER_E2E_DIR}/id_ed25519" -p 2224 eloq@127.0.0.1
}

cmd="${1:-}"
shift || true

case "${cmd}" in
    build)
        build_eloqctl
        ;;
    env-up)
        start_docker_env
        ;;
    env-down)
        clear_minio_data
        compose_down
        ;;
    install-control)
        install_control
        ;;
    control-shell)
        control_shell
        ;;
    render-topology)
        render_control_topology
        ;;
    prefetch-control-cache)
        prefetch_control_cache
        ;;
    launch)
        launch_cluster
        ;;
    status)
        show_status
        ;;
    grafana-update)
        grafana_update "${1:-}"
        ;;
    cluster-update)
        cluster_update "${1:-}"
        ;;
    export-topology)
        export_topology
        ;;
    stress)
        stress_run "${1:-}"
        ;;
    traffic-start)
        traffic_start
        ;;
    traffic-pause)
        traffic_pause
        ;;
    traffic-resume)
        traffic_resume
        ;;
    traffic-stop)
        traffic_stop
        ;;
    traffic-status)
        traffic_status
        ;;
    backup)
        run_backup
        ;;
    full)
        build_eloqctl
        start_docker_env
        install_control
        render_control_topology
        launch_cluster
        ;;
    ""|-h|--help|help)
        usage
        ;;
    *)
        echo "unknown command: ${cmd}" >&2
        usage >&2
        exit 1
        ;;
esac
