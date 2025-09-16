#!/bin/bash
set -exo pipefail

# Ensure cargo is installed (supports CentOS7/Rocky and Ubuntu 20/22/24)
if ! command -v cargo >/dev/null 2>&1; then
	echo "cargo not found, installing via rustup..."
	# Determine OS family and install prerequisites
	if [ -f /etc/os-release ]; then
		source /etc/os-release
	fi
	# Ensure we have a downloader (curl or wget). Try to install only if neither is available.
	if ! command -v curl >/dev/null 2>&1 && ! command -v wget >/dev/null 2>&1; then
		if command -v sudo >/dev/null 2>&1; then SUDO="sudo -n"; else SUDO=""; fi
		if command -v apt-get >/dev/null 2>&1; then
			$SUDO apt-get update || true
			DEBIAN_FRONTEND=noninteractive $SUDO apt-get install -y curl || true
		elif command -v yum >/dev/null 2>&1; then
			$SUDO yum install -y curl || true
		elif command -v dnf >/dev/null 2>&1; then
			$SUDO dnf install -y curl || true
		fi
	fi

	# Install rustup (non-interactive) and setup environment using curl or wget
	if command -v curl >/dev/null 2>&1; then
		curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
	elif command -v wget >/dev/null 2>&1; then
		wget -qO- https://sh.rustup.rs | sh -s -- -y --profile minimal
	else
		echo "Neither curl nor wget available, and installation not permitted. Please ensure curl/wget is available."
		exit 1
	fi
	# shellcheck disable=SC1090
	[ -f "$HOME/.cargo/env" ] && source "$HOME/.cargo/env"
fi

# Make sure rustup env is loaded if present
# shellcheck disable=SC1090
[ -f "$HOME/.cargo/env" ] && source "$HOME/.cargo/env"

cargo install cargo-make
cd monograph_waiter

# Determine OS version
source /etc/os-release
if [[ "$ID" == "centos" ]] || [[ "$ID" == "rocky" ]]; then
    OS_ID="rhel${VERSION_ID%.*}"
else
    OS_ID="${ID}${VERSION_ID%.*}"
fi

# Determine architecture
case $(uname -m) in
amd64 | x86_64) ARCH=amd64 ;;
arm64 | aarch64) ARCH=arm64 ;;
*) ARCH=$(uname -m) ;;
esac

# Handle tagged versions
if [ -n "${TAGGED}" ]; then
    TAG=$(git tag | sort -V | tail -n 1)
    if [ -z "${TAG}" ]; then
        echo "No tag found for HEAD. Exiting."
        exit 1
    fi
else
    TAG="main"
fi
TX_TARBALL="eloqctl-${TAG}-${OS_ID}-${ARCH}.tar.gz"

# Build
if [[ "$TAG" != "main" ]]; then
    echo "Checking out to $TAG..."
    git checkout "${TAG}"
else
    echo "TAG is 'main', no checkout performed."
fi

# Ensure cargo environment and a writable target dir (avoid workspace permission issues)
export RUSTUP_HOME="${HOME}/.rustup"
export CARGO_HOME="${HOME}/.cargo"
export PATH="${CARGO_HOME}/bin:${PATH}"
export CARGO_TARGET_DIR="${HOME}/.cargo-target"
mkdir -p "${CARGO_TARGET_DIR}"

# Disable AWS CLI pager to avoid interactive prompts in non-TTY CI environments
export AWS_PAGER=""

# Package dir in a writable location
export ELOQCTL_PKG_DIR="${HOME}/eloqctl"
mkdir -p "${ELOQCTL_PKG_DIR}"

cargo make --no-workspace --makefile Makefile.toml rest_api_pkg
# Ensure output dir is writable and under HOME to avoid permission issues
OUTPUT_DIR="${HOME}/output"
mkdir -p "${OUTPUT_DIR}"
tar -czvf "${OUTPUT_DIR}/${TX_TARBALL}" -C "${ELOQCTL_PKG_DIR}" .

# Upload to S3
aws s3 cp "${OUTPUT_DIR}/${TX_TARBALL}" s3://eloq-release/eloqctl/${ARCH}/${TAG}/${TX_TARBALL}
if [ -n "${CLOUDFRONT_DIST}" ]; then
    aws cloudfront create-invalidation --distribution-id ${CLOUDFRONT_DIST} --paths "/eloqctl/${ARCH}/${TAG}/${TX_TARBALL}"
fi
