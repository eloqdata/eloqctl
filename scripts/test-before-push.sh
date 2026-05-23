#!/bin/bash

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

echo "[1/4] Install dev eloqctl"
"${REPO_ROOT}/scripts/install-dev.sh"

echo "[2/4] Check formatting"
cargo fmt --all -- --check

echo "[3/4] Check and lint"
cargo check -p cluster_mgr
cargo clippy --all-targets --all-features -- -D warnings

echo "[4/4] Done"
echo "PASS: pre-push checks completed (full E2E runs in GitHub Actions)"
