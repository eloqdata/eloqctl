#! /bin/bash

cargo make --no-workspace --makefile Makefile.toml pkg_eloqctl && cp -r /home/mono/workspace/monograph_waiter/eloqctl/* /home/mono/.eloqctl

eloqctl remove eloqkv_standby
unproxy
eloqctl launch /home/mono/workspace/monograph_waiter/src/cluster_mgr/config/examples/eloqkv_rocksdb_standby.yaml -s

# fix bug
cp /home/mono/workspace/monograph_redis_bin/Debug/bin/eloqkv /home/mono/eloqkv_standby/EloqKV/bin/eloqkv

# format
cargo clippy --all-targets --all-features -- -D warnings
