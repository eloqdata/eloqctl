# Eloq Waiter

Eloq Waiter contains tools for developing and operating EloqData components outside Kubernetes.

The active cluster manager product target is **EloqKV**. Legacy EloqSQL, MonoSQL, Codis, and MySQL exporter deployment paths have been removed.

## Install

Install the latest release:

```sh
curl -fsSL https://raw.githubusercontent.com/monographdb/eloq_waiter/main/install.sh | sh
```

Install a specific release tag:

```sh
curl -fsSL https://raw.githubusercontent.com/monographdb/eloq_waiter/main/install.sh | sh -s -- v1.6.7
```

For local development, build and install the current checkout:

```sh
scripts/install-dev.sh
```

This installs `${ELOQCTL_HOME:-$HOME/.eloqctl}/bin/cluster_mgr` and links `${ELOQCTL_HOME:-$HOME/.eloqctl}/config` to `src/cluster_mgr/config` in this repository.

When the installer detects an existing `eloqctl` state directory, it also runs
`eloqctl upgrade` automatically to migrate local cluster metadata.

## Quick Start

Launch an EloqKV cluster from a topology file:

```sh
eloqctl launch /path/to/topology.yaml
```

Check live status by cluster name. `status` does not require the YAML file; it uses the local cluster index to find the saved topology and then probes the real hosts:

```sh
eloqctl status eloqkv-cluster --wait 60
```

Get a client command:

```sh
CLIENT=$(eloqctl -q connect eloqkv-cluster)
eval "$CLIENT" ping
```

Preview and apply supported declarative changes:

```sh
eloqctl plan /path/to/topology.yaml
eloqctl apply /path/to/topology.yaml
```

Export the saved launch-compatible topology:

```sh
eloqctl export eloqkv-cluster --output eloqkv-cluster.yaml
```

Stop and remove a cluster through `eloqctl`:

```sh
eloqctl stop eloqkv-cluster --all --force
eloqctl remove eloqkv-cluster --force
```

## State Model

`eloqctl` separates desired and observed state:

1. Desired topology is stored as YAML under `${ELOQCTL_HOME:-$HOME/.eloqctl}/clusters/<cluster>/topology.yaml` and can be exported with `eloqctl export`.
2. SQLite stores only local operational metadata such as the cluster index, locks, operation history, and backup metadata.
3. Runtime health is always observed live from the hosts and EloqKV endpoints. SQLite task history is not treated as proof that a service is running.

## Build

Install Rust and the system build dependencies first. On Ubuntu:

```sh
sudo apt install build-essential pkg-config libssl-dev
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Build the cluster manager:

```sh
cargo build -p cluster_mgr
cargo build -p cluster_mgr --release
```

Useful local commands:

```sh
cargo fmt --all -- --check
cargo check -p cluster_mgr
scripts/install-dev.sh
scripts/test-before-push.sh
scripts/install-git-hooks.sh
```

## Quality Gates

The push gate is `scripts/test-before-push.sh`. It performs:

1. Local dev install of `eloqctl`.
2. Rust formatting check.
3. `cargo check -p cluster_mgr`.
4. `cargo clippy --all-targets --all-features -- -D warnings`.
5. Docker HA EloqKV E2E.
6. Docker rolling update E2E.
7. Docker scale E2E.

The gate uses the Rust nightly toolchain specified in `rust-toolchain.toml`. Ensure the clippy component is installed:

```sh
rustup component add clippy
```

Install the pre-push hook with:

```sh
scripts/install-git-hooks.sh
```

The Docker E2E tests keep `eloqctl` on the host and use Ubuntu containers only as SSH-accessible target nodes. Runtime dependencies are installed by `eloqctl run-deps`/`launch`, not baked into the test image.

## Documentation

1. Cluster manager commands: `doc/cluster_mgr.md`
2. Declarative reconcile model: `doc/declarative_reconcile.md`
3. Idempotency guarantees: `doc/idempotency.md`
4. Backup and dump tools: `doc/backup_and_dump_tools.md`
5. Docker E2E tests: `tests/README.md`
6. Developer helper commands: `doc/devtools_cmd.md`
