# MonographDB Waiter

## Introduction

Monograph Waiter is a development and management tool for MonographDB that includes two modules.

1. devtools is a productivity tool for developers to simplify the cost of compilation, development, and debugging and to
   improve development efficiency
2. cluster_mgr_cli is a command line tool for cluster installation and deployment and management, which is designed to
   make it easier to install and manage MonographDB clusters in non-Kubernetes environments.

> NOTE: Currently only fully tested on Ubuntu and sudo user privilege is required to run ``monograph_waiter``.

## Install

```shell
curl -fsSL https://raw.githubusercontent.com/monographdb/eloq_waiter/main/install.sh | sh
```

Install a specific release tag:

```shell
curl -fsSL https://raw.githubusercontent.com/monographdb/eloq_waiter/main/install.sh | sh -s -- v1.0.4
```

## Quick Start

Start a full EloqKV cluster from a topology file. `eloqctl launch` will start every service declared in the YAML, including tx nodes, `log_service`, storage, and `monitor` if it is configured:

```shell
eloqctl launch "$ELOQCTL_HOME/config/examples/eloqkv_cassandra.yaml" -s
```

Check the cluster status and get a client command:

```shell
eloqctl status eloqkv-cluster
CLIENT=$(eloqctl -q connect eloqkv-cluster)
eval "$CLIENT" ping
```

Stop the entire cluster, including tx, log, storage, and monitor services:

```shell
eloqctl stop eloqkv-cluster --all
```

If you do not want to use `launch`, the equivalent step-by-step flow is:

```shell
eloqctl run-deps /path/to/deployment.yaml
eloqctl deploy /path/to/deployment.yaml
eloqctl log-service start eloqkv-cluster
eloqctl install eloqkv-cluster
eloqctl start eloqkv-cluster
eloqctl monitor start eloqkv-cluster
eloqctl status eloqkv-cluster
CLIENT=$(eloqctl -q connect eloqkv-cluster)
eval "$CLIENT" ping
```

For EloqKV with a standalone `log_service`, start `log-service` before `install`, because bootstrap depends on the log service already being available.

## Build

- If you do not have Rust installed, please follow the command below to install it.

> NOTE: For Ubuntu, you need to install compile-time dependencies for rust. run sudo apt install build-essential
> pkg-config libssl-dev

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

- Install cargo make

```shell
cargo install --force cargo-make
```

- Compile and generate release files

```shell
cargo build --release
# Compile the packages separately with cargo make.
cargo make --no-workspace  --makefile Makefile.toml  pkg_eloqctl
cargo make --no-workspace  --makefile Makefile.toml  rest_api_pkg

# Fast local iteration (compile only cluster_mgr in dev profile)
cargo make --no-workspace --makefile Makefile.toml dev_fast

# Or use cargo aliases from .cargo/config.toml
cargo cm-check
cargo cm-build

# Install local git hooks for commit/push quality gates
cargo make --no-workspace --makefile Makefile.toml install_hooks

# Run the full local quality suite on demand
cargo make --no-workspace --makefile Makefile.toml quality
```

## Quality Gates

This repository includes both local and CI quality checks:

1. Local git hooks live in `.githooks/`.
2. `pre-commit` runs `cargo fmt --all -- --check` and `cargo cm-check`.
3. `pre-push` runs `cargo test --tests -- --test-threads=1` and `cargo clippy --all-targets --all-features -- -D warnings` as blocking gates.
4. `commit-msg` enforces Conventional Commit messages.
5. CI runs format check, strict clippy, and single-threaded tests as blocking gates on pull requests and key branches.

## Versioning And Release

This repository uses SemVer with automated version management.

1. PRs should use Conventional Commit titles, for example:
   - `feat: add s3 endpoint validation`
   - `fix: handle missing grafana config`
2. On each push to `main`, Release Please analyzes commits and opens or updates a release PR.
3. Merging the release PR creates a new `vX.Y.Z` tag automatically.
4. The tag triggers the GitHub Actions release workflow to build and publish release artifacts.

Rust crate versions are inherited from `workspace.package.version` in `Cargo.toml`, so the release PR updates version in one place.

## Features

### Devtools

1. management compile and run dependencies
2. playground
3. autocomplete and command history support

### ClusterMgr

1. Installation and deployment the MonographDB cluster, including the underlying storage it depends on (if required)
2. Manage cluster start, stop, status check, and commands are idempotent.
3. Support batch execution of custom commands.

### REST API

The HTTP API with the same functionality as ClusterMgr. [REST API](./doc/cluster_mgr_rest.md)

## How to use

Please look at the documentation for more information about the design, implementation, and
commands.  [devtools](./doc/devtools_cmd.md) or [cluster_mgr](./doc/cluster_mgr.md)
