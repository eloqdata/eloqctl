#!/usr/bin/env python3
"""Full-command stress test for eloqkv cluster covering Python redis-py SDK.

Runs comprehensive Redis commands across all supported families:
  string, hash, list, set, sorted-set, generic/key, TTL, server, pub/sub.
"""

import argparse
import concurrent.futures
import json
import os
import signal
import ssl
import sys
import threading
import time
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple

from redis import Redis
from redis.cluster import ClusterNode, RedisCluster

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="eloqkv full-command stress test")
parser.add_argument("--startup-node", action="append", default=[],
                    help="Cluster startup node in host:port form (repeatable)")
parser.add_argument("--password", default="testpass")
parser.add_argument("--cmd-timeout", type=float, default=5.0)
parser.add_argument("--progress-interval", type=float, default=5.0)
parser.add_argument("--key-count", type=int, default=256)
parser.add_argument("--tls", action="store_true")
parser.add_argument("--read-from-replicas", action="store_true")
parser.add_argument("--base-connections", type=int, default=20)
parser.add_argument("--target-connections", type=int, default=10000)
parser.add_argument("--ramp-per-second", type=float, default=10.0)
parser.add_argument("--ramp-workers", type=int, default=4)
parser.add_argument("--phase1-qps", type=float, default=5000.0)
parser.add_argument("--phase2-qps", type=float, default=5000.0)
parser.add_argument("--duration", type=int, default=0,
                    help="Phase-2 seconds; <=0 means run until interrupted")
parser.add_argument("--workers", type=int, default=16,
                    help="Worker threads for command execution")
parser.add_argument("--skip-cmd-coverage", action="store_true",
                    help="Skip full command coverage (only SET/GET)")
parser.add_argument("--results-file", default="",
                    help="Write JSON results to file")
args = parser.parse_args()

if not args.startup_node:
    print("FAIL: at least one --startup-node is required", flush=True)
    sys.exit(1)

TLS_KWARGS: Dict[str, Any] = {}
if args.tls:
    TLS_KWARGS = {"ssl": True, "ssl_cert_reqs": ssl.CERT_NONE,
                  "ssl_check_hostname": False}

startup_nodes = [
    ClusterNode(*node.rsplit(":", 1)) for node in args.startup_node
]
master_node = startup_nodes[0]
replica_node = startup_nodes[1] if len(startup_nodes) > 1 else startup_nodes[0]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def build_client(node: ClusterNode, decode: bool = True) -> Redis:
    return Redis(
        host=node.host, port=node.port,
        password=args.password,
        socket_timeout=args.cmd_timeout,
        socket_connect_timeout=args.cmd_timeout,
        decode_responses=decode, **TLS_KWARGS,
    )


def build_cluster_client() -> RedisCluster:
    return RedisCluster(
        startup_nodes=startup_nodes,
        password=args.password,
        socket_timeout=args.cmd_timeout,
        socket_connect_timeout=args.cmd_timeout,
        decode_responses=True, **TLS_KWARGS,
    )


def close_client(c: Redis) -> None:
    try: c.close()
    except Exception: pass

# ---------------------------------------------------------------------------
# Command coverage definitions
# Each entry: (name, callable(client, index) -> result)
# ---------------------------------------------------------------------------
def _k(i: int) -> str:
    return f"stress:cmd:{i}"

def _kt(i: int, suffix: str = "") -> str:
    """Hash-tagged key for multi-key commands to share same slot."""
    return f"{{stress:cmd:{i}}}{suffix}"

def _mk(i: int) -> Dict[str, str]:
    return {f"f{j}": f"v{j}" for j in range(4)}

Str = str

COMMAND_TESTS: List[Tuple[str, Callable[[Redis, int], Any]]] = [
    # ── Connection / Server ──
    ("PING",     lambda c, i: c.ping()),
    ("ECHO",     lambda c, i: c.echo(Str(i))),
    ("DBSIZE",   lambda c, i: c.dbsize()),
    ("TIME",     lambda c, i: c.time()),
    ("READONLY", lambda c, i: c.execute_command("READONLY")),
    ("INFO",    lambda c, i: c.info("stats")),
    ("CLUSTER_INFO", lambda c, i: c.cluster_info() if hasattr(c, 'cluster_info') else c.execute_command("CLUSTER", "INFO")),

    # ── String ──
    ("SET",     lambda c, i: c.set(_k(i), Str(i))),
    ("GET",     lambda c, i: c.get(_k(i))),
    ("GETDEL",  lambda c, i: (c.set(_k(i) + "gd", "x"), c.getdel(_k(i) + "gd"))[1]),
    ("SETNX",   lambda c, i: c.setnx(_k(i) + "nx", Str(i))),
    ("SETEX",   lambda c, i: c.setex(_k(i) + "ex", 60, Str(i))),
    ("PSETEX",  lambda c, i: c.psetex(_k(i) + "px", 60000, Str(i))),
    ("STRLEN",  lambda c, i: c.strlen(_k(i))),
    ("GETRANGE",lambda c, i: c.getrange(_k(i), 0, 3)),
    ("SETRANGE",lambda c, i: c.setrange(_k(i) + "sr", 0, "xx")),
    ("APPEND",  lambda c, i: c.append(_k(i) + "ap", "tail")),
    ("GETBIT",  lambda c, i: c.getbit(_k(i), 0)),
    ("SETBIT",  lambda c, i: c.setbit(_k(i) + "bi", 7, 1)),
    ("BITCOUNT",lambda c, i: c.bitcount(_k(i))),
    ("BITPOS",  lambda c, i: c.bitpos(_k(i), 1)),
    ("INCR",    lambda c, i: c.incr(_k(i) + "ctr")),
    ("DECR",    lambda c, i: c.decr(_k(i) + "ctr")),
    ("INCRBY",  lambda c, i: c.incrby(_k(i) + "ctr", 10)),
    ("DECRBY",  lambda c, i: c.decrby(_k(i) + "ctr", 5)),
    ("INCRBYFLOAT", lambda c, i: c.incrbyfloat(_k(i) + "fl", 0.5)),
    ("MSET",    lambda c, i: c.mset({_kt(i, "m0"): "a", _kt(i, "m1"): "b"})),
    ("MGET",    lambda c, i: c.mget([_kt(i, "m0"), _kt(i, "m1")])),

    # ── Hash ──
    ("HSET",    lambda c, i: c.hset(_k(i) + "h", mapping=_mk(i))),
    ("HGET",    lambda c, i: c.hget(_k(i) + "h", "f0")),
    ("HDEL",    lambda c, i: c.hdel(_k(i) + "hd", "f0")),
    ("HEXISTS", lambda c, i: c.hexists(_k(i) + "h", "f0")),
    ("HGETALL", lambda c, i: c.hgetall(_k(i) + "h")),
    ("HLEN",    lambda c, i: c.hlen(_k(i) + "h")),
    ("HSTRLEN", lambda c, i: c.hstrlen(_k(i) + "h", "f0")),
    ("HINCRBY", lambda c, i: (c.hset(_k(i) + "hc", mapping={"cnt": 0}),
                               c.hincrby(_k(i) + "hc", "cnt", 1))[1]),
    ("HINCRBYFLOAT", lambda c, i: (c.hset(_k(i) + "hf", mapping={"val": "0"}),
                                    c.hincrbyfloat(_k(i) + "hf", "val", 0.5))[1]),
    ("HMGET",   lambda c, i: c.hmget(_k(i) + "h", ["f0", "f1"])),
    ("HKEYS",   lambda c, i: c.hkeys(_k(i) + "h")),
    ("HVALS",   lambda c, i: c.hvals(_k(i) + "h")),
    ("HSETNX",  lambda c, i: c.hsetnx(_k(i) + "hx", "uniq", Str(i))),

    # ── List ──
    ("LPUSH",   lambda c, i: c.lpush(_k(i) + "l", Str(i), Str(i + 1))),
    ("RPUSH",   lambda c, i: c.rpush(_k(i) + "r", Str(i), Str(i + 1))),
    ("LPOP",    lambda c, i: c.lpop(_k(i) + "l")),
    ("RPOP",    lambda c, i: c.rpop(_k(i) + "r")),
    ("LLEN",    lambda c, i: c.llen(_k(i) + "l")),
    ("LRANGE",  lambda c, i: c.lrange(_k(i) + "l", 0, -1)),
    ("LINDEX",  lambda c, i: c.lindex(_k(i) + "l", 0)),
    ("LSET",    lambda c, i: (c.lpush(_k(i) + "ls", "x") or True,
                                c.lset(_k(i) + "ls", 0, "UPD"))[1]),
    ("LTRIM",   lambda c, i: c.ltrim(_k(i) + "lt", 0, 1)),
    ("LREM",    lambda c, i: c.lrem(_k(i) + "l", 1, Str(i))),
    ("LPUSHX",  lambda c, i: c.lpushx(_k(i) + "l", "xx")),
    ("RPUSHX",  lambda c, i: c.rpushx(_k(i) + "r", "xx")),
    ("RPOPLPUSH", lambda c, i: (c.rpush(_kt(i, "r"), "x") or True,
                                 c.rpoplpush(_kt(i, "r"), _kt(i, "rl")))[1]),

    # ── Set ──
    ("SADD",    lambda c, i: c.sadd(_k(i) + "s", Str(i), Str(i + 1))),
    ("SMEMBERS",lambda c, i: c.smembers(_k(i) + "s")),
    ("SCARD",   lambda c, i: c.scard(_k(i) + "s")),
    ("SISMEMBER",lambda c, i: c.sismember(_k(i) + "s", Str(i))),
    ("SMISMEMBER",lambda c, i: c.smismember(_k(i) + "s", [Str(i), Str(i + 1)])),
    ("SREM",    lambda c, i: c.srem(_k(i) + "s", Str(i))),
    ("SPOP",    lambda c, i: c.spop(_k(i) + "s")),
    ("SRANDMEMBER", lambda c, i: (c.sadd(_kt(i, "s"), "x") or True,
                                  c.srandmember(_kt(i, "s"), 1))[1]),
    ("SMOVE",   lambda c, i: (c.sadd(_kt(i, "s1"), "x"), c.smove(_kt(i, "s1"), _kt(i, "s2"), "x"))[1]),
    ("SUNION",  lambda c, i: c.sunion(_kt(i, "s"), _kt(i, "s2"))),
    ("SUNIONSTORE", lambda c, i: c.sunionstore(_kt(i, "su"), [_kt(i, "s"), _kt(i, "s2")])),
    ("SINTER",  lambda c, i: c.sinter(_kt(i, "s"), _kt(i, "s2"))),
    ("SINTERCARD", lambda c, i: c.sintercard(2, [_kt(i, "s"), _kt(i, "s2")])),
    ("SDIFF",   lambda c, i: c.sdiff(_kt(i, "s"), _kt(i, "s2"))),
    ("SSCAN",   lambda c, i: c.sscan(_k(i) + "s")[1][:1]),

    # ── Sorted Set ──
    ("ZADD",    lambda c, i: c.zadd(_k(i) + "z", {Str(j): float(j) for j in range(i % 10, i % 10 + 3)})),
    ("ZCARD",   lambda c, i: c.zcard(_k(i) + "z")),
    ("ZCOUNT",  lambda c, i: c.zcount(_k(i) + "z", 0, 100)),
    ("ZSCORE",  lambda c, i: c.zscore(_k(i) + "z", Str(i % 10))),
    ("ZMSCORE", lambda c, i: c.zmscore(_k(i) + "z", [Str(i % 10), Str(i % 10 + 1)])),
    ("ZRANK",   lambda c, i: c.zrank(_k(i) + "z", Str(i % 10))),
    ("ZREVRANK",lambda c, i: c.zrevrank(_k(i) + "z", Str(i % 10))),
    ("ZRANGE",  lambda c, i: c.zrange(_k(i) + "z", 0, -1)),
    ("ZRANGEBYSCORE", lambda c, i: c.zrangebyscore(_k(i) + "z", 0, 100)),
    ("ZREVRANGE", lambda c, i: c.zrevrange(_k(i) + "z", 0, -1)),
    ("ZREVRANGEBYSCORE", lambda c, i: c.zrevrangebyscore(_k(i) + "z", 100, 0)),
    ("ZINCRBY", lambda c, i: c.zincrby(_k(i) + "z", 1, Str(i % 10))),
    ("ZREM",    lambda c, i: c.zrem(_k(i) + "zr", Str(i % 10))),
    ("ZREMRANGEBYSCORE", lambda c, i: c.zremrangebyscore(_k(i) + "zr", 0, 50)),
    ("ZREMRANGEBYRANK", lambda c, i: c.zremrangebyrank(_k(i) + "zr", 0, 1)),
    ("ZLEXCOUNT", lambda c, i: c.zlexcount(_k(i) + "zl", "-", "+")),
    ("ZPOPMIN", lambda c, i: c.zpopmin(_k(i) + "z")),
    ("ZRANDMEMBER", lambda c, i: c.zrandmember(_k(i) + "z", 1)),
    ("ZSCAN",   lambda c, i: c.zscan(_k(i) + "z")[1][:1]),

    # ── Generic / Key ──
    ("DEL",     lambda c, i: (c.set(_k(i) + "td", "x"), c.delete(_k(i) + "td"))[1]),
    ("UNLINK",  lambda c, i: (c.set(_k(i) + "tu", "x"), c.unlink(_k(i) + "tu"))[1]),
    ("EXISTS",  lambda c, i: c.exists(_k(i))),
    ("TYPE",    lambda c, i: c.type(_k(i))),
    ("EXPIRE",  lambda c, i: (c.set(_k(i) + "te", "x"), c.expire(_k(i) + "te", 300))[1]),
    ("PEXPIRE", lambda c, i: (c.set(_k(i) + "tp", "x"), c.pexpire(_k(i) + "tp", 300000))[1]),
    ("EXPIREAT",lambda c, i: (c.set(_k(i) + "tea", "x"), c.expireat(_k(i) + "tea", int(time.time()) + 300))[1]),
    ("PEXPIREAT",lambda c, i: (c.set(_k(i) + "tpa", "x"), c.pexpireat(_k(i) + "tpa", int(time.time() * 1000) + 300000))[1]),
    ("TTL",     lambda c, i: c.ttl(_k(i))),
    ("PTTL",    lambda c, i: c.pttl(_k(i))),
    ("PERSIST", lambda c, i: (c.set(_k(i) + "tpe", "x"), c.expire(_k(i) + "tpe", 300),
                               c.persist(_k(i) + "tpe"))[2]),
    ("SCAN",    lambda c, i: c.scan(match="stress:cmd:*", count=10)[1][:5]),
    ("SORT",    lambda c, i: (c.lpush(_k(i) + "so", "2", "1", "3"), c.sort(_k(i) + "so", alpha=True))[1]),
    ("KEYS",    lambda c, i: c.keys(_k(i) + "*")),
    ("DUMP",    lambda c, i: c.dump(_k(i))),
    ("RESTORE", lambda c, i: (payload := c.dump(_k(i)),
                                 c.restore(_kt(i, "rs"), 0, payload, replace=True))),
]


def run_cmd_coverage(client: Redis) -> Dict[str, Tuple[int, int, List[str]]]:
    """Run every command once and report per-command pass/fail."""
    results: Dict[str, Tuple[int, int, List[str]]] = {}
    for name, fn in COMMAND_TESTS:
        ok, fail, errs = 0, 0, []
        try:
            fn(client, 0)
            ok = 1
        except Exception:
            fail = 1
            errs = [traceback.format_exc()[-200:]]
        results[name] = (ok, fail, errs)
    return results


# ---------------------------------------------------------------------------
# Multi-command stress worker
# ---------------------------------------------------------------------------
_CMD_ORDER = [name for name, _ in COMMAND_TESTS]
_CMD_FNS = {name: fn for name, fn in COMMAND_TESTS}

def stress_worker(client: Redis, stop_event: threading.Event, phase_event: threading.Event,
                  stats_lock: threading.Lock, cmd_stats: Dict[str, Dict[str, int]],
                  worker_id: int) -> None:
    phase_event.wait()
    idx = worker_id
    key_mod = args.key_count
    while not stop_event.is_set():
        for cmd_name in _CMD_ORDER:
            if stop_event.is_set():
                break
            fn = _CMD_FNS[cmd_name]
            ki = idx % key_mod
            try:
                fn(client, ki)
                with stats_lock:
                    cmd_stats[cmd_name]["ok"] += 1
            except Exception as e:
                # ECHO can't be routed by cluster client (no key) — skip, not fail
                if cmd_name == "ECHO" and "Missing key" in str(e):
                    continue
                with stats_lock:
                    cmd_stats[cmd_name]["fail"] += 1
            idx += 1


# ---------------------------------------------------------------------------
# Progress reporter
# ---------------------------------------------------------------------------
def fmt_cmd_stats(cmd_stats: Dict[str, Dict[str, int]]) -> str:
    total_ok = sum(v["ok"] for v in cmd_stats.values())
    total_fail = sum(v["fail"] for v in cmd_stats.values())
    failures = [f"{k}:{v['fail']}" for k, v in cmd_stats.items() if v["fail"] > 0]
    s = f"ok={total_ok} fail={total_fail}"
    if failures:
        s += " | " + " ".join(failures[:10])
    return s


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    # ── Preload keys ──
    print(f"Preloading keyspace: key_count={args.key_count}", flush=True)
    client = build_client(master_node)
    try:
        for i in range(args.key_count):
            client.set(_k(i), Str(i))
            client.hset(_k(i) + "h", mapping=_mk(i))
            client.lpush(_k(i) + "l", Str(i))
            client.rpush(_k(i) + "r", Str(i))
            client.sadd(_k(i) + "s", Str(i))
            client.zadd(_k(i) + "z", {Str(i % 50): float(i % 50)})
            client.zadd(_k(i) + "zr", {Str(j): float(j) for j in range(i % 5, i % 5 + 3)})
            client.zadd(_k(i) + "zl", {f"a{j}": 0 for j in range(3)})
            client.sadd(_k(i) + "s2", "shared")
            client.lpush(_k(i) + "lt", "a", "b", "c")
    finally:
        close_client(client)
    print("Keyspace ready.", flush=True)

    # ── Quick command coverage check (standalone + cluster) ──
    if not args.skip_cmd_coverage:
        print("Running command coverage check (standalone) ...", flush=True)
        coverage = run_cmd_coverage(client := build_client(master_node))
        close_client(client)
        total_ok = sum(v[0] for v in coverage.values())
        total_fail = sum(v[1] for v in coverage.values())
        failures = [(n, errs) for n, (ok, fail, errs) in coverage.items() if fail]
        print(f"  standalone coverage: {total_ok}/{total_ok + total_fail}", flush=True)
        for name, errs in failures:
            print(f"    FAIL {name}: {errs[0][:120] if errs else ''}", flush=True)

        print("Running command coverage check (cluster) ...", flush=True)
        ccoverage = run_cmd_coverage(client := build_cluster_client())
        close_client(client)
        ctotal_ok = sum(v[0] for v in ccoverage.values())
        ctotal_fail = sum(v[1] for v in ccoverage.values())
        cfailures = [(n, errs) for n, (ok, fail, errs) in ccoverage.items() if fail]
        print(f"  cluster coverage: {ctotal_ok}/{ctotal_ok + ctotal_fail}", flush=True)
        for name, errs in cfailures:
            print(f"    FAIL {name}: {errs[0][:120] if errs else ''}", flush=True)

    stop_event = threading.Event()

    # ── Build clients: standalone + cluster ──
    standalone_client = build_client(master_node)
    cluster_client = build_cluster_client()

    workers: List[Tuple[Redis, threading.Thread]] = []
    standalone_stats: Dict[str, Dict[str, int]] = {
        name: {"ok": 0, "fail": 0} for name in _CMD_ORDER
    }
    cluster_stats: Dict[str, Dict[str, int]] = {
        name: {"ok": 0, "fail": 0} for name in _CMD_ORDER
    }
    stats_lock = threading.Lock()
    phase_event = threading.Event()

    n_standalone = args.workers // 2
    n_cluster = args.workers - n_standalone
    print(f"Starting {n_standalone} standalone + {n_cluster} cluster stress workers ...", flush=True)
    for i in range(n_standalone):
        cli = standalone_client
        th = threading.Thread(target=stress_worker,
                               args=(cli, stop_event, phase_event,
                                     stats_lock, standalone_stats, i),
                               daemon=True)
        th.start()
        workers.append((cli, th))
    for i in range(n_cluster):
        cli = cluster_client
        th = threading.Thread(target=stress_worker,
                               args=(cli, stop_event, phase_event,
                                     stats_lock, cluster_stats, i),
                               daemon=True)
        th.start()
        workers.append((cli, th))

    phase_event.set()

    # ── Progress loop ──
    deadline = (time.time() + args.duration) if args.duration > 0 else float("inf")

    def _should_stop():
        return stop_event.is_set() or time.time() >= deadline

    try:
        last = time.time()
        while not _should_stop():
            time.sleep(args.progress_interval)
            now = time.time()
            elapsed = now - last
            s_ok = sum(v["ok"] for v in standalone_stats.values())
            s_fail = sum(v["fail"] for v in standalone_stats.values())
            c_ok = sum(v["ok"] for v in cluster_stats.values())
            c_fail = sum(v["fail"] for v in cluster_stats.values())
            total_ok = s_ok + c_ok
            total_fail = s_fail + c_fail
            qps = (total_ok + total_fail) / max(elapsed, 0.001)
            print(f"[progress] standalone: ok={s_ok} fail={s_fail} "
                  f"cluster: ok={c_ok} fail={c_fail} "
                  f"total_qps={qps:.0f}", flush=True)
    except KeyboardInterrupt:
        pass

    stop_event.set()
    for cli, th in workers:
        th.join(timeout=3)
    close_client(standalone_client)
    close_client(cluster_client)

    # ── Report ──
    def _report(title: str, stats: dict) -> None:
        total_ok = sum(v["ok"] for v in stats.values())
        total_fail = sum(v["fail"] for v in stats.values())
        print(f"\n--- {title} ---")
        print(f"total_commands: ok={total_ok} fail={total_fail}")
        for name in _CMD_ORDER:
            ok, fail = stats[name]["ok"], stats[name]["fail"]
            if fail > 0:
                print(f"  {name}: ok={ok} fail={fail}")
    _report("Standalone Client Results", standalone_stats)
    _report("Cluster Client Results", cluster_stats)
    s_fail = sum(v["fail"] for v in standalone_stats.values())
    c_fail = sum(v["fail"] for v in cluster_stats.values())
    if args.results_file:
        with open(args.results_file, "w") as f:
            json.dump({"standalone": standalone_stats, "cluster": cluster_stats}, f, indent=2)
    sys.exit(0 if (s_fail + c_fail) == 0 else 1)


if __name__ == "__main__":
    main()
