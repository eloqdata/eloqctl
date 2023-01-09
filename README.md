# MonographDB Waiter

## Introduction

Monograph Waiter is a development and management tool for MonographDB that includes two modules.

1. devtools is a productivity tool for developers to simplify the cost of compilation, development, and debugging and to
   improve development efficiency
2. cluster_mgr_cli is a command line tool for cluster installation and deployment and management, which is designed to
   make it easier to install and manage MonographDB clusters in non-Kubernetes environments.

> NOTE: Currently only fully tested on Ubuntu and sudo user privilege is required to run ``monograph_waiter``.

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
# Compile the two packages separately with cargo make.
cargo make --no-workspace  --makefile Makefile.toml  cluster_mgr_pkg/devtools_pkg
```

## Features

### Devtools

1. management compile and run dependencies
2. playground
3. autocomplete and command history support

### ClusterMgr

1. Installation and deployment the MonographDB cluster, including the underlying storage it depends on (if required)
2. Manage cluster start, stop, status check, and commands are idempotent.
3. Support batch execution of custom commands.

## How to use

Please look at the documentation for more information about the design, implementation, and
commands.  [devtools](./doc/devtools_cmd.md) or [cluster_mgr](./doc/cluster_mgr.md)
