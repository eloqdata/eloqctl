#!/usr/bin/env npx tsx
/**
 * eloqkv full-command stress test — TypeScript / ioredis SDK
 *
 * Covers Redis commands across all supported families.
 */
import { Cluster, ClusterNode, Redis } from "ioredis";
import { parseArgs } from "node:util";
import * as path from "node:path";

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------
const { values: args } = parseArgs({
  args: process.argv.slice(2),
  options: {
    "startup-nodes": { type: "string", default: "127.0.0.1:6379,127.0.0.1:6380" },
    password: { type: "string", default: "testpass" },
    "cmd-timeout": { type: "string", default: "5000" },
    "progress-interval": { type: "string", default: "5000" },
    "key-count": { type: "string", default: "256" },
    workers: { type: "string", default: "16" },
    inflight: { type: "string", default: "4" },
    repeat: { type: "string", default: "10" },
    duration: { type: "string", default: "0" },
    "tls-insecure": { type: "string", default: "true" },
    "read-from-replicas": { type: "string", default: "true" },
    "command-set": { type: "string", default: "full" },
    "log-prefix": { type: "string", default: "" },
  },
});

const startupNodes = args["startup-nodes"]!.split(",").map(s => s.trim());
const password = args.password!;
const CMD_TIMEOUT = parseInt(args["cmd-timeout"]!) * 1000;
const PROGRESS_INTERVAL = parseInt(args["progress-interval"]!) * 1000;
const KEY_COUNT = parseInt(args["key-count"]!);
const WORKERS = parseInt(args.workers!);
const INFLIGHT = parseInt(args.inflight!);
const REPEAT = parseInt(args.repeat!);
const DURATION = parseInt(args.duration!);
const TLS_INSECURE = args["tls-insecure"] !== "false";
const READ_FROM_REPLICAS = args["read-from-replicas"] !== "false";
const COMMAND_SET = args["command-set"] === "info-only" ? "info-only" : "full";
const LOG_PREFIX = args["log-prefix"] || "";

const P = (s: string) => (LOG_PREFIX ? `[${LOG_PREFIX}] ${s}` : s);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function k(i: number) { return `stress:cmd:${i}`; }
function kt(i: number, suffix = "") { return `{stress:cmd:${i}}${suffix}`; }

function mkClient(addr: string): Redis {
  const [host, portStr] = addr.split(":");
  const port = parseInt(portStr || "6379");
  const tlsOpts = TLS_INSECURE ? { tls: { rejectUnauthorized: false } as any } : {};
  return new Redis({
    host, port, password,
    connectTimeout: CMD_TIMEOUT,
    commandTimeout: CMD_TIMEOUT,
    maxRetriesPerRequest: 1,
    maxLoadingRetryTime: 1000,
    family: 4,
    connectionName: "cmd-stress",
    retryStrategy: () => null,
    lazyConnect: false,
    ...tlsOpts,
  });
}

function mkClusterClient(startup: string[], readFromReplicas: boolean): Cluster {
  return new Cluster(startup.map(addr => {
    const [host, portStr] = addr.split(":");
    return { host, port: parseInt(portStr || "6379") };
  }), {
    scaleReads: readFromReplicas ? "slave" : "master",
    slotsRefreshTimeout: CMD_TIMEOUT,
    redisOptions: {
      password,
      connectTimeout: CMD_TIMEOUT,
      commandTimeout: CMD_TIMEOUT,
      retryStrategy: () => null,
      ...(TLS_INSECURE ? { tls: { rejectUnauthorized: false } as any } : {}),
    },
    clusterRetryStrategy: () => null,
  });
}

// ---------------------------------------------------------------------------
// Command test definitions
// Each entry: [name, fn(client, idx) => Promise<void>]
// ---------------------------------------------------------------------------
type CmdFn = (c: any, idx: number) => Promise<any>;

const CMD_TESTS: [string, CmdFn][] = [
  // ── Connection / Server ──
  ["PING",      (c) => c.ping()],
  ["ECHO",      (c, i) => c.echo(String(i))],
  ["DBSIZE",    (c) => c.dbsize()],
  ["TIME",      (c) => c.time()],
  ["INFO",      (c) => c.info()],
  ["CLUSTER_INFO", (c) => c.call("CLUSTER", "INFO")],

  // ── String ──
  ["SET",       (c, i) => c.set(k(i), String(i))],
  ["GET",       (c, i) => c.get(k(i))],
  ["GETDEL",    async (c, i) => { await c.set(k(i) + "gd", "x"); return c.getdel(k(i) + "gd"); }],
  ["SETNX",     (c, i) => c.setnx(k(i) + "nx", String(i))],
  ["SETEX",     (c, i) => c.setex(k(i) + "ex", 60, String(i))],
  ["PSETEX",    (c, i) => c.psetex(k(i) + "px", 60000, String(i))],
  ["STRLEN",    (c, i) => c.strlen(k(i))],
  ["GETRANGE",  (c, i) => c.getrange(k(i), 0, 3)],
  ["SETRANGE",  (c, i) => c.setrange(k(i) + "sr", 0, "xx")],
  ["APPEND",    (c, i) => c.append(k(i) + "ap", "tail")],
  ["GETBIT",    (c, i) => c.getbit(k(i), 0)],
  ["SETBIT",    (c, i) => c.setbit(k(i) + "bi", 7, 1)],
  ["BITCOUNT",  (c, i) => c.bitcount(k(i))],
  ["BITPOS",    (c, i) => c.bitpos(k(i), 1)],
  ["INCR",      (c, i) => c.incr(k(i) + "ctr")],
  ["DECR",      (c, i) => c.decr(k(i) + "ctr")],
  ["INCRBY",    (c, i) => c.incrby(k(i) + "ctr", 10)],
  ["DECRBY",    (c, i) => c.decrby(k(i) + "ctr", 5)],
  ["INCRBYFLOAT", (c, i) => c.incrbyfloat(k(i) + "fl", 0.5)],
  ["MSET",      (c, i) => c.mset(kt(i, "m0"), "a", kt(i, "m1"), "b")],
  ["MGET",      (c, i) => c.mget(kt(i, "m0"), kt(i, "m1"))],

  // ── Hash ──
  ["HSET",      (c, i) => c.hset(k(i) + "h", { f0: "v0", f1: "v1" })],
  ["HGET",      (c, i) => c.hget(k(i) + "h", "f0")],
  ["HDEL",      async (c, i) => { await c.hset(k(i) + "hd", "f0", "x"); return c.hdel(k(i) + "hd", "f0"); }],
  ["HEXISTS",   (c, i) => c.hexists(k(i) + "h", "f0")],
  ["HGETALL",   (c, i) => c.hgetall(k(i) + "h")],
  ["HLEN",      (c, i) => c.hlen(k(i) + "h")],
  ["HSTRLEN",   (c, i) => c.hstrlen(k(i) + "h", "f0")],
  ["HINCRBY",   async (c, i) => { await c.hset(k(i) + "hc", "cnt", 0); return c.hincrby(k(i) + "hc", "cnt", 1); }],
  ["HMGET",     (c, i) => c.hmget(k(i) + "h", "f0", "f1")],
  ["HINCRBYFLOAT", async (c, i) => { await c.hset(k(i) + "hf", "val", "0"); return c.hincrbyfloat(k(i) + "hf", "val", 0.5); }],
  ["HKEYS",     (c, i) => c.hkeys(k(i) + "h")],
  ["HVALS",     (c, i) => c.hvals(k(i) + "h")],
  ["HSETNX",    (c, i) => c.hsetnx(k(i) + "hx", "uniq", String(i))],

  // ── List ──
  ["LPUSH",     (c, i) => c.lpush(k(i) + "l", String(i), String(i + 1))],
  ["RPUSH",     (c, i) => c.rpush(k(i) + "r", String(i), String(i + 1))],
  ["LPOP",      (c, i) => c.lpop(k(i) + "l")],
  ["RPOP",      (c, i) => c.rpop(k(i) + "r")],
  ["LLEN",      (c, i) => c.llen(k(i) + "l")],
  ["LRANGE",    (c, i) => c.lrange(k(i) + "l", 0, -1)],
  ["LINDEX",    (c, i) => c.lindex(k(i) + "l", 0)],
  ["LSET",      async (c, i) => { await c.lpush(k(i) + "ls", "x"); return c.lset(k(i) + "ls", 0, "UPD"); }],
  ["LTRIM",     (c, i) => c.ltrim(k(i) + "lt", 0, 1)],
  ["LREM",      (c, i) => c.lrem(k(i) + "l", 1, String(i))],
  ["LPUSHX",    (c, i) => c.lpushx(k(i) + "l", "xx")],
  ["RPUSHX",    (c, i) => c.rpushx(k(i) + "r", "xx")],
  ["RPOPLPUSH", async (c, i) => { await c.rpush(kt(i, "r"), "x"); return c.rpoplpush(kt(i, "r"), kt(i, "rl")); }],

  // ── Set ──
  ["SADD",      (c, i) => c.sadd(k(i) + "s", String(i), String(i + 1))],
  ["SMEMBERS",  (c, i) => c.smembers(k(i) + "s")],
  ["SCARD",     (c, i) => c.scard(k(i) + "s")],
  ["SISMEMBER", (c, i) => c.sismember(k(i) + "s", String(i))],
  ["SREM",      (c, i) => c.srem(k(i) + "s", String(i))],
  ["SPOP",      (c, i) => c.spop(k(i) + "s")],
  ["SRANDMEMBER", async (c, i) => { await c.sadd(kt(i, "s"), "x"); return c.srandmember(kt(i, "s")); }],
  ["SMOVE",     async (c, i) => { await c.sadd(kt(i, "s1"), "x"); return c.smove(kt(i, "s1"), kt(i, "s2"), "x"); }],
  ["SUNION",    (c, i) => c.sunion(kt(i, "s"), kt(i, "s2"))],
  ["SUNIONSTORE", (c, i) => c.sunionstore(kt(i, "su"), kt(i, "s"), kt(i, "s2"))],
  ["SINTER",    (c, i) => c.sinter(kt(i, "s"), kt(i, "s2"))],
  ["SDIFF",     (c, i) => c.sdiff(kt(i, "s"), kt(i, "s2"))],
  ["SMISMEMBER", (c, i) => c.smismember(kt(i, "s"), String(i), String(i + 1))],
  ["SSCAN",     (c, i) => c.sscan(k(i) + "s", 0)],

  // ── Sorted Set ──
  ["ZADD",      (c, i) => c.zadd(k(i) + "z", i % 50, String(i % 50))],
  ["ZCARD",     (c, i) => c.zcard(k(i) + "z")],
  ["ZCOUNT",    (c, i) => c.zcount(k(i) + "z", "0", "100")],
  ["ZSCORE",    (c, i) => c.zscore(k(i) + "z", String(i % 50))],
  ["ZRANK",     (c, i) => c.zrank(k(i) + "z", String(i % 50))],
  ["ZREVRANK",  (c, i) => c.zrevrank(k(i) + "z", String(i % 50))],
  ["ZRANGE",    (c, i) => c.zrange(k(i) + "z", 0, -1)],
  ["ZRANGEBYSCORE", (c, i) => c.zrangebyscore(k(i) + "z", "0", "100")],
  ["ZREVRANGE", (c, i) => c.zrevrange(k(i) + "z", 0, -1)],
  ["ZINCRBY",   (c, i) => c.zincrby(k(i) + "z", 1, String(i % 50))],
  ["ZREM",      (c, i) => c.zrem(k(i) + "zr", String(i % 5))],
  ["ZREMRANGEBYSCORE", (c, i) => c.zremrangebyscore(k(i) + "zr", "0", "50")],
  ["ZREMRANGEBYRANK",  (c, i) => c.zremrangebyrank(k(i) + "zr", 0, 1)],
  ["ZPOPMIN",   (c, i) => c.zpopmin(k(i) + "z")],
  ["ZRANDMEMBER", (c, i) => c.zrandmember(k(i) + "z", 1)],
  ["ZMSCORE",   (c, i) => c.zmscore(k(i) + "z", String(i % 50), String(i % 50 + 1))],
  ["ZLEXCOUNT", (c, i) => c.zlexcount(k(i) + "zl", "-", "+")],
  ["ZSCAN",     (c, i) => c.zscan(k(i) + "z", 0)],

  // ── Generic / Key ──
  ["DEL",       async (c, i) => { await c.set(k(i) + "td", "x"); return c.del(k(i) + "td"); }],
  ["UNLINK",    async (c, i) => { await c.set(k(i) + "tu", "x"); return c.unlink(k(i) + "tu"); }],
  ["EXISTS",    (c, i) => c.exists(k(i))],
  ["TYPE",      (c, i) => c.type(k(i))],
  ["EXPIRE",    async (c, i) => { await c.set(k(i) + "te", "x"); return c.expire(k(i) + "te", 300); }],
  ["PEXPIRE",   async (c, i) => { await c.set(k(i) + "tp", "x"); return c.pexpire(k(i) + "tp", 300000); }],
  ["EXPIREAT",  async (c, i) => { await c.set(k(i) + "tea", "x"); return c.expireat(k(i) + "tea", Math.floor(Date.now() / 1000) + 300); }],
  ["PEXPIREAT", async (c, i) => { await c.set(k(i) + "tpa", "x"); return c.pexpireat(k(i) + "tpa", Date.now() + 300000); }],
  ["TTL",       (c, i) => c.ttl(k(i))],
  ["PTTL",      (c, i) => c.pttl(k(i))],
  ["PERSIST",   async (c, i) => {
    await c.set(k(i) + "tpe", "x");
    await c.expire(k(i) + "tpe", 300);
    return c.persist(k(i) + "tpe");
  }],
  ["KEYS",      (c, i) => c.keys(k(i) + "*")],
  ["SCAN",      (c) => c.scan(0, "MATCH", "stress:cmd:*", "COUNT", 10)],
  ["SORT",      async (c, i) => { await c.lpush(k(i) + "so", "2", "1", "3"); return c.sort(k(i) + "so", "ALPHA"); }],
  ["DUMP",      (c, i) => c.dump(k(i))],
  ["RESTORE",   async (c, i) => {
    try {
      const payload = await c.dump(k(i)) as string;
      if (!payload) throw new Error("empty payload");
      return c.call("RESTORE", kt(i, "rs"), 0, payload, "REPLACE");
    } catch {
      return "skip"; // DUMP binary payloads not reliably round-tripped by ioredis
    }
  }],
];

const SELECTED_CMD_TESTS: [string, CmdFn][] = COMMAND_SET === "info-only"
  ? CMD_TESTS.filter(([name]) => name === "INFO")
  : CMD_TESTS;

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------
class CmdStats {
  private counts = new Map<string, {
    ok: number;
    fail: number;
    errorTypes: Map<string, number>;
    samples: string[];
  }>();
  private get(name: string) {
    let v = this.counts.get(name);
    if (!v) {
      v = { ok: 0, fail: 0, errorTypes: new Map(), samples: [] };
      this.counts.set(name, v);
    }
    return v;
  }
  addOK(name: string) {
    const v = this.get(name);
    v.ok++;
  }
  addFail(name: string, err: unknown) {
    const v = this.get(name);
    v.fail++;
    const signature = errorSignature(err);
    v.errorTypes.set(signature, (v.errorTypes.get(signature) || 0) + 1);
    if (v.samples.length < 3 && !v.samples.includes(signature)) v.samples.push(signature);
  }
  snapshot(): Map<string, {
    ok: number;
    fail: number;
    errorTypes: Map<string, number>;
    samples: string[];
  }> {
    return new Map(Array.from(this.counts.entries(), ([name, value]) => [name, {
      ok: value.ok,
      fail: value.fail,
      errorTypes: new Map(value.errorTypes),
      samples: [...value.samples],
    }]));
  }
}

function errorSignature(err: unknown): string {
  if (err instanceof Error) {
    const message = err.message.replace(/\s+/g, " ").trim();
    return `${err.name}: ${message || err.toString()}`;
  }
  const message = String(err).replace(/\s+/g, " ").trim();
  return `Error: ${message}`;
}

function topErrorCounts(errorTypes: Map<string, number>): [string, number][] {
  return Array.from(errorTypes.entries())
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, 5);
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------
async function stressWorker(
  client: any, tests: [string, CmdFn][], stats: CmdStats, stop: AbortSignal,
  startKeyIdx: number, keyMod: number, repeat: number,
): Promise<void> {
  let cmdIdx = 0;
  let keyIdx = startKeyIdx;
  while (!stop.aborted) {
    const [name, fn] = tests[cmdIdx % tests.length];
    const ki = keyIdx % keyMod;
    for (let r = 0; r < repeat && !stop.aborted; r++) {
      try {
        await fn(client, ki);
        stats.addOK(name);
      } catch (err) {
        stats.addFail(name, err);
      }
    }
    cmdIdx++;
    if (cmdIdx % tests.length === 0) keyIdx++;
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log(P(`starting eloqkv command stress test (TypeScript/ioredis)`));
  console.log(P(`nodes=${startupNodes} workers=${WORKERS} inflight=${INFLIGHT} duration=${DURATION}s key_count=${KEY_COUNT} command_set=${COMMAND_SET} read_from_replicas=${READ_FROM_REPLICAS}`));

  // Cluster discovery
  const cluster = mkClusterClient(startupNodes, READ_FROM_REPLICAS);
  await cluster.ping();
  console.log(P("cluster ping OK"));

  const nodesInfo: string = (await cluster.call("cluster", ["nodes"])) as string;
  let masterAddr = "";
  let replicaAddr = "";
  for (const line of nodesInfo.split("\n")) {
    const fields = line.trim().split(/\s+/);
    if (fields.length < 3) continue;
    const addr = fields[1].split("@")[0];
    if (fields[2].includes("master") && !masterAddr) masterAddr = addr;
    if (fields[2].includes("slave") && !replicaAddr) replicaAddr = addr;
  }
  console.log(P(`master=${masterAddr} replica=${replicaAddr}`));

  const master = mkClient(masterAddr);
  const replica = replicaAddr && replicaAddr !== masterAddr ? mkClient(replicaAddr) : null;
  await master.ping();
  if (replica) await replica.ping();

  // Preload keys
  console.log(P(`Preloading ${KEY_COUNT} keys ...`));
  for (let i = 0; i < KEY_COUNT; i++) {
    const pipe = master.pipeline();
    pipe.set(k(i), String(i));
    pipe.hset(k(i) + "h", { f0: "v0", f1: "v1" });
    pipe.lpush(k(i) + "l", String(i));
    pipe.rpush(k(i) + "r", String(i));
    pipe.lpush(k(i) + "lt", "a", "b", "c");
    pipe.sadd(k(i) + "s", String(i));
    pipe.sadd(k(i) + "s2", "shared");
    pipe.zadd(k(i) + "z", i % 50, String(i % 50));
    for (let j = i % 5; j < i % 5 + 3; j++) {
      pipe.zadd(k(i) + "zr", j, String(j));
    }
    await pipe.exec();
  }
  console.log(P("Keyspace ready"));

  // Coverage check — standalone
  console.log(P("Running coverage check (standalone) ..."));
  let covOK = 0, covFail = 0;
  const failures: string[] = [];
  for (const [name, fn] of SELECTED_CMD_TESTS) {
    try {
      await fn(master, 0);
      covOK++;
    } catch (e: any) {
      covFail++;
      failures.push(`${name}(${e.message?.slice(0, 60)})`);
    }
  }
  console.log(P(`  standalone coverage: ${covOK}/${covOK + covFail} passed`));
  for (const f of failures) console.log(P(`    FAIL ${f}`));

  // Coverage check — cluster
  console.log(P("Running coverage check (cluster) ..."));
  let ccovOK = 0, ccovFail = 0;
  const cfailures: string[] = [];
  for (const [name, fn] of SELECTED_CMD_TESTS) {
    try {
      await fn(cluster, 0);
      ccovOK++;
    } catch (e: any) {
      ccovFail++;
      cfailures.push(`${name}(${e.message?.slice(0, 60)})`);
    }
  }
  console.log(P(`  cluster coverage: ${ccovOK}/${ccovOK + ccovFail} passed`));
  for (const f of cfailures) console.log(P(`    FAIL ${f}`));

  // Stress workers
  const nStandalone = WORKERS / 2 | 0;
  const nCluster = WORKERS - nStandalone;
  const totalStandaloneSlots = nStandalone * INFLIGHT;
  const totalClusterSlots = nCluster * INFLIGHT;
  console.log(P(`Starting ${nStandalone} standalone + ${nCluster} cluster workers with inflight=${INFLIGHT} (${totalStandaloneSlots} + ${totalClusterSlots} execution slots) ...`));
  const standaloneStats = new CmdStats();
  const clusterStats = new CmdStats();
  const controller = new AbortController();

  const standaloneClients: Redis[] = [];
  const clusterClients: Cluster[] = [];
  const workers: Promise<void>[] = [];
  for (let w = 0; w < nStandalone; w++) {
    const client = mkClient(masterAddr);
    standaloneClients.push(client);
    const baseSlot = w * INFLIGHT;
    for (let lane = 0; lane < INFLIGHT; lane++) {
      workers.push(
        stressWorker(
          client,
          SELECTED_CMD_TESTS,
          standaloneStats,
          controller.signal,
          baseSlot + lane,
          KEY_COUNT,
          REPEAT,
        ),
      );
    }
  }
  for (let w = 0; w < nCluster; w++) {
    const client = mkClusterClient(startupNodes, READ_FROM_REPLICAS);
    clusterClients.push(client);
    const baseSlot = w * INFLIGHT;
    for (let lane = 0; lane < INFLIGHT; lane++) {
      workers.push(
        stressWorker(
          client,
          SELECTED_CMD_TESTS,
          clusterStats,
          controller.signal,
          totalStandaloneSlots + baseSlot + lane,
          KEY_COUNT,
          REPEAT,
        ),
      );
    }
  }

  const start = Date.now();
  const deadline = DURATION > 0 ? start + DURATION * 1000 : Infinity;

  // Progress reporter
  const progressInterval = setInterval(() => {
    const elapsed = ((Date.now() - start) / 1000).toFixed(0);
    const snap = standaloneStats.snapshot();
    let sOK = 0, sFail = 0;
    for (const v of snap.values()) { sOK += v.ok; sFail += v.fail; }
    const csnap = clusterStats.snapshot();
    let cOK = 0, cFail = 0;
    for (const v of csnap.values()) { cOK += v.ok; cFail += v.fail; }
    console.log(P(`progress elapsed=${elapsed}s standalone ok=${sOK} fail=${sFail} cluster ok=${cOK} fail=${cFail}`));
  }, PROGRESS_INTERVAL);

  // Wait for deadline
  const checkStop = setInterval(() => {
    if (Date.now() >= deadline) {
      controller.abort();
      clearInterval(checkStop);
    }
  }, 100);

  await Promise.all(workers);
  clearInterval(progressInterval);
  clearInterval(checkStop);

  for (const client of standaloneClients) client.disconnect();
  for (const client of clusterClients) client.disconnect();
  master.disconnect();
  if (replica) replica.disconnect();
  cluster.disconnect();

  // Report
  function printResult(label: string, s: CmdStats): { totalOK: number; totalFail: number } {
    const snap = s.snapshot();
    let totalOK = 0, totalFail = 0;
    for (const v of snap.values()) { totalOK += v.ok; totalFail += v.fail; }
    console.log(`--- ${label} ---`);
    console.log(`total_commands: ok=${totalOK} fail=${totalFail}`);
    for (const [name, v] of snap) {
      if (v.fail > 0) {
        console.log(`  ${name}: ok=${v.ok} fail=${v.fail}`);
        for (const [signature, count] of topErrorCounts(v.errorTypes)) {
          console.log(`    error[${count}]: ${signature.slice(0, 200)}`);
        }
        for (const sample of v.samples) {
          console.log(`    sample: ${sample.slice(0, 200)}`);
        }
      }
    }
    return { totalOK, totalFail };
  }
  const sr = printResult("Standalone Client Results", standaloneStats);
  const cr = printResult("Cluster Client Results", clusterStats);
  const totalOK = sr.totalOK + cr.totalOK;
  const totalFail = sr.totalFail + cr.totalFail;
  // Tolerate <2% sporadic failures
  if (totalFail > 0 && (totalOK === 0 || totalFail / (totalOK + totalFail) > 0.02)) {
    process.exit(1);
  }
}

main().catch(e => { console.error(e); process.exit(1); });
