#!/bin/bash
function log_start() {
  log_dir=${LOG_INSTALL_DIR}/logs/lg${GROUP_ID}/ln${NODE_ID}
  mkdir -p ${STORAGE_DIR} && mkdir -p ${log_dir}
  export GLOG_log_dir=${log_dir}
  export GLOG_max_log_size=1024
  if [ "${VERSION}" = "debug" ]; then
    export ASAN_OPTIONS=${ASAN_OPTS}:log_path=${log_dir}/asan
  else
    export LD_PRELOAD=${LOG_INSTALL_DIR}/lib/libmimalloc.so.2
  fi
  export LD_LIBRARY_PATH=${LOG_INSTALL_DIR}/lib:${LD_LIBRARY_PATH}
  log_start_cmd="${LOG_INSTALL_DIR}/bin/launch_sv -conf=${GROUP_MEMBERS} -raft_max_parallel_append_entries_rpc_num=64 \
    -raft_enable_append_entries_cache=true -raft_max_append_entries_cache_size=256 \
    -start_log_group_id=${GROUP_ID} -node_id=${NODE_ID} -storage_path=${STORAGE_DIR} -bthread_concurrency=6 > /dev/null 2>&1 &"
  echo "$log_start_cmd"
  eval "$log_start_cmd"
}
log_start
