#!/bin/bash

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

echo "[1/5] Install dev eloqctl"
"${REPO_ROOT}/scripts/install-dev.sh"

echo "[2/5] Check formatting"
cargo fmt --all -- --check

echo "[3/5] Check and lint"
cargo check -p cluster_mgr
cargo clippy --all-targets --all-features -- -D warnings

echo "[4/5] Run Docker E2E suite"
bash tests/e2e/test.sh

echo "[5/5] Done"
echo "PASS: pre-push test suite completed"
