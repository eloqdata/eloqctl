use once_cell::sync::Lazy;
use std::collections::HashMap;

pub static MONO_WATER_CONF: &str = "MONO_WATER_CONF_DIR";
pub static PROTOBUF_TAR_FILE_NAME: &str = "protobuf-bin.tar.gz";
pub static CASSANDRA_TAR_FILE_NAME: &str = "cassandra-bin.tar.gz";
// Build and runtime dependencies. For now, it only supports Linux and macOS
pub static DEPS: Lazy<HashMap<&'static str, Vec<&'static str>>> = Lazy::new(|| {
    let mut dep_mapping = HashMap::new();
    dep_mapping.insert(
        "macos",
        vec![
            "git",
            "cmake",
            "ninja",
            "libuv",
            "glog",
            "openssl@1.1",
            "openjdk@11",
            "gnu-getopt",
            "coreutils",
            "gflags",
            "leveldb",
            "gperftools",
            "bison",
        ],
    );
    dep_mapping.insert(
        "linux",
        vec![
            "git",
            "g++",
            "make",
            "libssl-dev",
            "libgflags-dev",
            "libgoogle-glog-dev",
            "libprotobuf-dev",
            "libprotoc-dev",
            "protobuf-compiler",
            "libleveldb-dev",
            "libsnappy-dev",
        ],
    );
    dep_mapping
});

pub static SUPPORT_CMD_LIST: Lazy<Vec<&'static str>> = Lazy::new(|| {
    vec![
        "check_deps",
        "install_deps",
        "setup_workspace",
        "ln_source",
        "gen_mysql_cnf",
        "build",
        "playground",
        "stop_all",
        "start_all",
    ]
});
