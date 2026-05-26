#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source "${REPO_ROOT}/tests/docker_env.sh"

CONTROL_TOPO_TEMPLATE="tests/e2e/topology.yaml"
CONTROL_TOPO="/home/eloq/topology.generated.yaml"
DEFAULT_CLUSTER="test-e2e"

usage() {
    cat <<'EOF'
Usage: tests/e2e/devctl.sh <command> [args]

Commands:
  build                     Build eloqctl locally
  env-up                    Start the Docker E2E environment
  env-down                  Stop and remove the Docker E2E environment
  install-control           Copy the current eloqctl build into the control node
  control-shell             SSH into the control node
  render-topology           Render topology.yaml inside the control node
  launch                    Launch the default E2E cluster from the control node
  status                    Show cluster and monitor status
  grafana-update [url]      Upgrade Grafana with monitor update
  export-topology           Export cluster topology from the control node
  stress [steps]            Run cmd_stress_test.sh via devctl; default steps are unchanged
  full                      build -> env-up -> install-control -> render-topology -> launch

Environment:
  CLUSTER_NAME              Default: test-e2e
  ELOQKV_VERSION            Override the EloqKV version during topology rendering
EOF
}

cluster_name() {
    echo "${CLUSTER_NAME:-${DEFAULT_CLUSTER}}"
}

build_eloqctl() {
    (cd "${REPO_ROOT}" && cargo build -p cluster_mgr)
}

install_control() {
    (cd "${REPO_ROOT}" && bash tests/install_control_eloqctl.sh)
}

render_control_topology() {
    control_exec bash -lc "
        cd '${CONTROL_REPO_ROOT}'
        source tests/docker_env.sh
        render_topology_for_control '${CONTROL_TOPO_TEMPLATE}' '${CONTROL_TOPO}'
        ls -l '${CONTROL_TOPO}'
    "
}

launch_cluster() {
    local cluster
    cluster="$(cluster_name)"
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
    launch)
        launch_cluster
        ;;
    status)
        show_status
        ;;
    grafana-update)
        grafana_update "${1:-}"
        ;;
    export-topology)
        export_topology
        ;;
    stress)
        stress_run "${1:-}"
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
