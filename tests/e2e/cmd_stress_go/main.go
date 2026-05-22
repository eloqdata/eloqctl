package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// cmdTestCase defines a single command test.
type cmdTestCase struct {
	Name string
	Fn   func(ctx context.Context, c redis.UniversalClient, keyIdx int) error
}

// ---------------------------------------------------------------------------
// Command definitions covering eloqkv's supported Redis commands
// ---------------------------------------------------------------------------
func k(i int) string    { return fmt.Sprintf("stress:cmd:%d", i) }
func kt(i int, suffix string) string { return fmt.Sprintf("{stress:cmd:%d}%s", i, suffix) }
func mk(i int) map[string]interface{} {
	return map[string]interface{}{"f0": "v0", "f1": "v1", "f2": "v2", "f3": "v3"}
}

func cmdTests(replicaAddr string) []cmdTestCase {
	return []cmdTestCase{
		// ── Connection / Server ──
		{"PING", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.Ping(ctx).Err()
		}},
		{"ECHO", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.Echo(ctx, fmt.Sprintf("%d", i)).Result()
			return err
		}},
		{"DBSIZE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.DBSize(ctx).Result()
			return err
		}},
		{"TIME", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.Time(ctx).Result()
			return err
		}},
		{"INFO", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.Info(ctx, "stats").Err()
		}},
		{"CLUSTER_INFO", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.Do(ctx, "CLUSTER", "INFO").Err()
		}},

		// ── String ──
		{"SET", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.Set(ctx, k(i), i, 0).Err()
		}},
		{"GET", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.Get(ctx, k(i)).Err()
		}},
		{"GETDEL", func(ctx context.Context, c redis.UniversalClient, i int) error {
			key := k(i) + "gd"
			c.Set(ctx, key, "x", 0)
			return c.GetDel(ctx, key).Err()
		}},
		{"SETNX", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.SetNX(ctx, k(i)+"nx", i, 0).Result()
			return err
		}},
		{"SETEX", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.SetEx(ctx, k(i)+"ex", i, 60*time.Second).Err()
		}},
		{"PSETEX", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.Do(ctx, "PSETEX", k(i)+"px", int64(60000), fmt.Sprintf("%d", i)).Err()
		}},
		{"STRLEN", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.StrLen(ctx, k(i)).Result()
			return err
		}},
		{"GETRANGE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.GetRange(ctx, k(i), 0, 3).Result()
			return err
		}},
		{"SETRANGE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.SetRange(ctx, k(i)+"sr", 0, "xx").Result()
			return err
		}},
		{"APPEND", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.Append(ctx, k(i)+"ap", "tail").Result()
			return err
		}},
		{"GETBIT", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.GetBit(ctx, k(i), 0).Result()
			return err
		}},
		{"SETBIT", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.SetBit(ctx, k(i)+"bi", 7, 1).Result()
			return err
		}},
		{"BITCOUNT", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.BitCount(ctx, k(i), nil).Result()
			return err
		}},
		{"BITPOS", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.BitPos(ctx, k(i), 1).Result()
			return err
		}},
		{"INCR", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.Incr(ctx, k(i)+"ctr").Result()
			return err
		}},
		{"DECR", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.Decr(ctx, k(i)+"ctr").Result()
			return err
		}},
		{"INCRBY", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.IncrBy(ctx, k(i)+"ctr", 10).Result()
			return err
		}},
		{"DECRBY", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.DecrBy(ctx, k(i)+"ctr", 5).Result()
			return err
		}},
		{"INCRBYFLOAT", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.IncrByFloat(ctx, k(i)+"fl", 0.5).Result()
			return err
		}},
		{"MSET", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.MSet(ctx, kt(i, "m0"), "a", kt(i, "m1"), "b").Err()
		}},
		{"MGET", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.MGet(ctx, kt(i, "m0"), kt(i, "m1")).Err()
		}},

		// ── Hash ──
		{"HSET", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.HSet(ctx, k(i)+"h", mk(i)).Result()
			return err
		}},
		{"HGET", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.HGet(ctx, k(i)+"h", "f0").Err()
		}},
		{"HDEL", func(ctx context.Context, c redis.UniversalClient, i int) error {
			c.HSet(ctx, k(i)+"hd", "f0", "x")
			_, err := c.HDel(ctx, k(i)+"hd", "f0").Result()
			return err
		}},
		{"HEXISTS", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.HExists(ctx, k(i)+"h", "f0").Result()
			return err
		}},
		{"HGETALL", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.HGetAll(ctx, k(i)+"h").Err()
		}},
		{"HLEN", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.HLen(ctx, k(i)+"h").Result()
			return err
		}},
		{"HSTRLEN", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.Do(ctx, "HSTRLEN", k(i)+"h", "f0").Err()
		}},
		{"HINCRBY", func(ctx context.Context, c redis.UniversalClient, i int) error {
			c.HSet(ctx, k(i)+"hc", "cnt", 0)
			_, err := c.HIncrBy(ctx, k(i)+"hc", "cnt", 1).Result()
			return err
		}},
		{"HINCRBYFLOAT", func(ctx context.Context, c redis.UniversalClient, i int) error {
			c.HSet(ctx, k(i)+"hf", "val", "0")
			_, err := c.HIncrByFloat(ctx, k(i)+"hf", "val", 0.5).Result()
			return err
		}},
		{"HMGET", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.HMGet(ctx, k(i)+"h", "f0", "f1").Err()
		}},
		{"HKEYS", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.HKeys(ctx, k(i)+"h").Err()
		}},
		{"HVALS", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.HVals(ctx, k(i)+"h").Err()
		}},
		{"HSETNX", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.HSetNX(ctx, k(i)+"hx", "uniq", i).Result()
			return err
		}},

		// ── List ──
		{"LPUSH", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.LPush(ctx, k(i)+"l", i, i+1).Result()
			return err
		}},
		{"RPUSH", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.RPush(ctx, k(i)+"r", i, i+1).Result()
			return err
		}},
		{"LPOP", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.LPop(ctx, k(i)+"l").Err()
		}},
		{"RPOP", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.RPop(ctx, k(i)+"r").Err()
		}},
		{"LLEN", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.LLen(ctx, k(i)+"l").Result()
			return err
		}},
		{"LRANGE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.LRange(ctx, k(i)+"l", 0, -1).Err()
		}},
		{"LINDEX", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.LIndex(ctx, k(i)+"l", 0).Err()
		}},
		{"LSET", func(ctx context.Context, c redis.UniversalClient, i int) error {
			c.LPush(ctx, k(i)+"ls", "x")
			return c.LSet(ctx, k(i)+"ls", 0, "UPD").Err()
		}},
		{"LTRIM", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.LTrim(ctx, k(i)+"lt", 0, 1).Err()
		}},
		{"LREM", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.LRem(ctx, k(i)+"l", 1, fmt.Sprintf("%d", i)).Result()
			return err
		}},
		{"LPUSHX", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.LPushX(ctx, k(i)+"l", "xx").Result()
			return err
		}},
		{"RPUSHX", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.RPushX(ctx, k(i)+"r", "xx").Result()
			return err
		}},
		{"RPOPLPUSH", func(ctx context.Context, c redis.UniversalClient, i int) error {
			c.RPush(ctx, kt(i, "r"), "x")
			return c.RPopLPush(ctx, kt(i, "r"), kt(i, "rl")).Err()
		}},

		// ── Set ──
		{"SADD", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.SAdd(ctx, k(i)+"s", i, i+1).Result()
			return err
		}},
		{"SMEMBERS", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.SMembers(ctx, k(i)+"s").Err()
		}},
		{"SCARD", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.SCard(ctx, k(i)+"s").Result()
			return err
		}},
		{"SISMEMBER", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.SIsMember(ctx, k(i)+"s", i).Result()
			return err
		}},
		{"SREM", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.SRem(ctx, k(i)+"s", fmt.Sprintf("%d", i)).Result()
			return err
		}},
		{"SPOP", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.SPop(ctx, k(i)+"s").Err()
		}},
		{"SRANDMEMBER", func(ctx context.Context, c redis.UniversalClient, i int) error {
			c.SAdd(ctx, kt(i, "s"), "x")
			return c.SRandMember(ctx, kt(i, "s")).Err()
		}},
		{"SMOVE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.SMove(ctx, kt(i, "s"), kt(i, "s2"), "shared").Result()
			return err
		}},
		{"SUNION", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.SUnion(ctx, kt(i, "s"), kt(i, "s2")).Err()
		}},
		{"SUNIONSTORE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.SUnionStore(ctx, kt(i, "su"), kt(i, "s"), kt(i, "s2")).Result()
			return err
		}},
		{"SINTER", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.SInter(ctx, kt(i, "s"), kt(i, "s2")).Err()
		}},
		{"SDIFF", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.SDiff(ctx, kt(i, "s"), kt(i, "s2")).Err()
		}},
		{"SMISMEMBER", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.SMIsMember(ctx, kt(i, "s"), fmt.Sprintf("%d", i), fmt.Sprintf("%d", i+1)).Err()
		}},
		{"SINTERCARD", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.SInterCard(ctx, 2, kt(i, "s"), kt(i, "s2")).Result()
			return err
		}},
		{"SSCAN", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.SScan(ctx, k(i)+"s", 0, "", 10).Err()
		}},

		// ── Sorted Set ──
		{"ZADD", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.ZAdd(ctx, k(i)+"z", redis.Z{Score: float64(i), Member: fmt.Sprintf("%d", i)}).Result()
			return err
		}},
		{"ZCARD", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.ZCard(ctx, k(i)+"z").Result()
			return err
		}},
		{"ZCOUNT", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.ZCount(ctx, k(i)+"z", "0", "100").Result()
			return err
		}},
		{"ZSCORE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.ZScore(ctx, k(i)+"z", fmt.Sprintf("%d", i%50)).Err()
		}},
		{"ZRANK", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.ZRank(ctx, k(i)+"z", fmt.Sprintf("%d", i%50)).Result()
			return err
		}},
		{"ZREVRANK", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.ZRevRank(ctx, k(i)+"z", fmt.Sprintf("%d", i%50)).Result()
			return err
		}},
		{"ZRANGE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.ZRange(ctx, k(i)+"z", 0, -1).Err()
		}},
		{"ZRANGEBYSCORE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.ZRangeByScore(ctx, k(i)+"z", &redis.ZRangeBy{Min: "0", Max: "100"}).Err()
		}},
		{"ZREVRANGE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.ZRevRange(ctx, k(i)+"z", 0, -1).Err()
		}},
		{"ZREVRANGEBYSCORE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.ZRevRangeByScore(ctx, k(i)+"z", &redis.ZRangeBy{Min: "0", Max: "100"}).Err()
		}},
		{"ZINCRBY", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.ZIncrBy(ctx, k(i)+"z", 1, fmt.Sprintf("%d", i%50)).Result()
			return err
		}},
		{"ZREM", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.ZRem(ctx, k(i)+"zr", fmt.Sprintf("%d", i%5)).Result()
			return err
		}},
		{"ZREMRANGEBYSCORE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.ZRemRangeByScore(ctx, k(i)+"zr", "0", "50").Result()
			return err
		}},
		{"ZREMRANGEBYRANK", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.ZRemRangeByRank(ctx, k(i)+"zr", 0, 1).Result()
			return err
		}},
		{"ZPOPMIN", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.ZPopMin(ctx, k(i)+"z", 1).Err()
		}},
		{"ZRANDMEMBER", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.ZRandMember(ctx, k(i)+"z", 1).Err()
		}},
		{"ZMSCORE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.ZMScore(ctx, k(i)+"z", fmt.Sprintf("%d", i%50), fmt.Sprintf("%d", i%50+1)).Err()
		}},
		{"ZLEXCOUNT", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.ZLexCount(ctx, k(i)+"zl", "-", "+").Result()
			return err
		}},
		{"ZSCAN", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.ZScan(ctx, k(i)+"z", 0, "", 10).Err()
		}},

		// ── Generic / Key ──
		{"DEL", func(ctx context.Context, c redis.UniversalClient, i int) error {
			c.Set(ctx, k(i)+"td", "x", 0)
			_, err := c.Del(ctx, k(i)+"td").Result()
			return err
		}},
		{"UNLINK", func(ctx context.Context, c redis.UniversalClient, i int) error {
			c.Set(ctx, k(i)+"tu", "x", 0)
			_, err := c.Unlink(ctx, k(i)+"tu").Result()
			return err
		}},
		{"EXISTS", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.Exists(ctx, k(i)).Result()
			return err
		}},
		{"TYPE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.Type(ctx, k(i)).Result()
			return err
		}},
		{"EXPIRE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			c.Set(ctx, k(i)+"te", "x", 0)
			_, err := c.Expire(ctx, k(i)+"te", 300*time.Second).Result()
			return err
		}},
		{"PEXPIRE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			c.Set(ctx, k(i)+"tp", "x", 0)
			_, err := c.PExpire(ctx, k(i)+"tp", 300000*time.Millisecond).Result()
			return err
		}},
		{"EXPIREAT", func(ctx context.Context, c redis.UniversalClient, i int) error {
			c.Set(ctx, k(i)+"tea", "x", 0)
			_, err := c.ExpireAt(ctx, k(i)+"tea", time.Now().Add(300*time.Second)).Result()
			return err
		}},
		{"PEXPIREAT", func(ctx context.Context, c redis.UniversalClient, i int) error {
			c.Set(ctx, k(i)+"tpa", "x", 0)
			_, err := c.PExpireAt(ctx, k(i)+"tpa", time.Now().Add(300000*time.Millisecond)).Result()
			return err
		}},
		{"TTL", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.TTL(ctx, k(i)).Result()
			return err
		}},
		{"PTTL", func(ctx context.Context, c redis.UniversalClient, i int) error {
			_, err := c.PTTL(ctx, k(i)).Result()
			return err
		}},
		{"PERSIST", func(ctx context.Context, c redis.UniversalClient, i int) error {
			key := k(i) + "tpe"
			c.Set(ctx, key, "x", 0)
			c.Expire(ctx, key, 300*time.Second)
			_, err := c.Persist(ctx, key).Result()
			return err
		}},
		{"KEYS", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.Keys(ctx, k(i)+"*").Err()
		}},
		{"SCAN", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.Scan(ctx, 0, "stress:cmd:*", 10).Err()
		}},
		{"SORT", func(ctx context.Context, c redis.UniversalClient, i int) error {
			c.LPush(ctx, k(i)+"so", "2", "1", "3")
			return c.Sort(ctx, k(i)+"so", &redis.Sort{Alpha: true}).Err()
		}},
		{"DUMP", func(ctx context.Context, c redis.UniversalClient, i int) error {
			return c.Dump(ctx, k(i)).Err()
		}},
		{"RESTORE", func(ctx context.Context, c redis.UniversalClient, i int) error {
			payload, err := c.Dump(ctx, k(i)).Bytes()
			if err != nil {
				return err
			}
			return c.RestoreReplace(ctx, kt(i, "rs"), 0, string(payload)).Err()
		}},
	}
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------
type cmdStats struct {
	mu     sync.Mutex
	counts map[string]struct{ ok, fail int64 }
}

func newCmdStats() *cmdStats {
	return &cmdStats{counts: map[string]struct{ ok, fail int64 }{}}
}

func (s *cmdStats) addOK(name string) {
	s.mu.Lock()
	c := s.counts[name]
	c.ok++
	s.counts[name] = c
	s.mu.Unlock()
}

func (s *cmdStats) addFail(name string) {
	s.mu.Lock()
	c := s.counts[name]
	c.fail++
	s.counts[name] = c
	s.mu.Unlock()
}

func (s *cmdStats) snapshot() map[string]struct{ ok, fail int64 } {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]struct{ ok, fail int64 }{}
	for k, v := range s.counts {
		out[k] = v
	}
	return out
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------
func stressWorker(ctx context.Context, client redis.UniversalClient, tests []cmdTestCase,
	stats *cmdStats, wg *sync.WaitGroup, startKeyIdx, keyMod int) {
	defer wg.Done()
	cmdIdx := 0
	keyIdx := startKeyIdx
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		tc := tests[cmdIdx%len(tests)]
		ki := keyIdx % keyMod
		err := tc.Fn(ctx, client, ki)
		if err != nil {
			stats.addFail(tc.Name)
		} else {
			stats.addOK(tc.Name)
		}
		cmdIdx++
		if cmdIdx%len(tests) == 0 {
			keyIdx++
		}
	}
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
func main() {
	var startupNodes string
	var password string
	var workers int
	var duration time.Duration
	var progressInterval time.Duration
	var keyCount int
	var insecureTLS bool
	var cmdTimeout time.Duration
	var logPrefix string

	flag.StringVar(&startupNodes, "startup-nodes", "127.0.0.1:6379,127.0.0.1:6380",
		"comma-separated startup nodes (host:port)")
	flag.StringVar(&password, "password", "testpass", "redis password")
	flag.IntVar(&workers, "workers", 16, "number of concurrent stress workers")
	flag.DurationVar(&duration, "duration", 60*time.Second, "test duration")
	flag.DurationVar(&progressInterval, "progress-interval", 5*time.Second, "progress print interval")
	flag.IntVar(&keyCount, "key-count", 256, "key space size")
	flag.BoolVar(&insecureTLS, "tls-insecure", true, "skip TLS verification")
	flag.DurationVar(&cmdTimeout, "cmd-timeout", 5*time.Second, "per-command timeout")
	flag.StringVar(&logPrefix, "log-prefix", "", "optional prefix in log lines")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	addrs := strings.Split(startupNodes, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}
	if len(addrs) < 1 {
		logger.Fatalf("need at least 1 startup node, got %q", startupNodes)
	}

	logger.Printf("%sstarting eloqkv command stress test", pref(logPrefix))
	logger.Printf("%snodes=%s workers=%d duration=%s key_count=%d",
		pref(logPrefix), strings.Join(addrs, ","), workers, duration, keyCount)

	// Cluster client for discovery
	clusterOpts := &redis.ClusterOptions{
		Addrs:    addrs,
		Password: password,
		DialTimeout:  cmdTimeout,
		ReadTimeout:  cmdTimeout,
		WriteTimeout: cmdTimeout,
		ReadOnly: true,
	}
	if insecureTLS {
		clusterOpts.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}
	clusterClient := redis.NewClusterClient(clusterOpts)
	defer clusterClient.Close()

	if err := clusterClient.Ping(ctx).Err(); err != nil {
		logger.Fatalf("cluster ping failed: %v", err)
	}
	logger.Printf("%scluster ping OK", pref(logPrefix))

	// Discover master/replica
	nodes, err := clusterClient.ClusterNodes(ctx).Result()
	if err != nil {
		logger.Fatalf("cluster nodes failed: %v", err)
	}
	var masterAddr, replicaAddr string
	for _, line := range strings.Split(nodes, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		addr := strings.Split(fields[1], "@")[0]
		if strings.Contains(fields[2], "master") && masterAddr == "" {
			masterAddr = addr
		}
		if strings.Contains(fields[2], "slave") && replicaAddr == "" {
			replicaAddr = addr
		}
	}
	if masterAddr == "" {
		logger.Fatalf("no master found in cluster nodes")
	}

	logger.Printf("%smaster=%s replica=%s", pref(logPrefix), masterAddr, replicaAddr)

	clientOpts := func() *redis.Options {
		return &redis.Options{
			Password:      password,
			DialTimeout:   cmdTimeout,
			ReadTimeout:   cmdTimeout,
			WriteTimeout:  cmdTimeout,
			PoolSize:      workers * 2,
			MinIdleConns:  workers,
		}
	}

	masterOpts := clientOpts()
	masterOpts.Addr = masterAddr
	if insecureTLS {
		masterOpts.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}
	masterClient := redis.NewClient(masterOpts)
	defer masterClient.Close()

	var replicaClient *redis.Client
	if replicaAddr != "" && replicaAddr != masterAddr {
		replicaOpts := clientOpts()
		replicaOpts.Addr = replicaAddr
		if insecureTLS {
			replicaOpts.TLSConfig = &tls.Config{InsecureSkipVerify: true}
		}
		replicaClient = redis.NewClient(replicaOpts)
		defer replicaClient.Close()
	}

	// Preload keys
	logger.Printf("%sPreloading %d keys ...", pref(logPrefix), keyCount)
	for i := 0; i < keyCount; i++ {
		if err := masterClient.Set(ctx, k(i), i, 0).Err(); err != nil {
			logger.Fatalf("preload key %d failed: %v", i, err)
		}
		masterClient.HSet(ctx, k(i)+"h", mk(i))
		masterClient.LPush(ctx, k(i)+"l", i)
		masterClient.RPush(ctx, k(i)+"r", i)
		masterClient.LPush(ctx, k(i)+"lt", "a", "b", "c")
		masterClient.SAdd(ctx, k(i)+"s", i)
		masterClient.SAdd(ctx, k(i)+"s2", "shared")
		masterClient.ZAdd(ctx, k(i)+"z", redis.Z{Score: float64(i % 50), Member: fmt.Sprintf("%d", i%50)})
		for j := i % 5; j < i%5+3; j++ {
			masterClient.ZAdd(ctx, k(i)+"zr", redis.Z{Score: float64(j), Member: fmt.Sprintf("%d", j)})
		}
	}
	logger.Printf("%sKeyspace ready", pref(logPrefix))

	tests := cmdTests(replicaAddr)

	// Quick coverage check — standalone
	logger.Printf("%sRunning coverage check (standalone) ...", pref(logPrefix))
	standaloneCovOK, standaloneCovFail := 0, 0
	var standaloneFailedCmds []string
	for _, tc := range tests {
		err := tc.Fn(ctx, masterClient, 0)
		if err != nil {
			standaloneCovFail++
			standaloneFailedCmds = append(standaloneFailedCmds, fmt.Sprintf("%s(%v)", tc.Name, err))
		} else {
			standaloneCovOK++
		}
	}
	logger.Printf("%s  standalone coverage: %d/%d passed", pref(logPrefix), standaloneCovOK, standaloneCovOK+standaloneCovFail)
	for _, fc := range standaloneFailedCmds {
		logger.Printf("%s    FAIL %s", pref(logPrefix), fc)
	}

	// Quick coverage check — cluster
	logger.Printf("%sRunning coverage check (cluster) ...", pref(logPrefix))
	clusterCovOK, clusterCovFail := 0, 0
	var clusterFailedCmds []string
	for _, tc := range tests {
		err := tc.Fn(ctx, clusterClient, 0)
		if err != nil {
			clusterCovFail++
			clusterFailedCmds = append(clusterFailedCmds, fmt.Sprintf("%s(%v)", tc.Name, err))
		} else {
			clusterCovOK++
		}
	}
	logger.Printf("%s  cluster coverage: %d/%d passed", pref(logPrefix), clusterCovOK, clusterCovOK+clusterCovFail)
	for _, fc := range clusterFailedCmds {
		logger.Printf("%s    FAIL %s", pref(logPrefix), fc)
	}

	// Start stress workers
	standaloneStats := newCmdStats()
	clusterStats := newCmdStats()
	nStandalone := workers / 2
	nCluster := workers - nStandalone
	logger.Printf("%sStarting %d standalone + %d cluster stress workers ...",
		pref(logPrefix), nStandalone, nCluster)
	testCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	var wg sync.WaitGroup
	for w := 0; w < nStandalone; w++ {
		wg.Add(1)
		go stressWorker(testCtx, masterClient, tests, standaloneStats, &wg, w, keyCount)
	}
	for w := 0; w < nCluster; w++ {
		wg.Add(1)
		go stressWorker(testCtx, clusterClient, tests, clusterStats, &wg, w+nStandalone, keyCount)
	}

	// Progress reporter
	go func() {
		ticker := time.NewTicker(progressInterval)
		defer ticker.Stop()
		start := time.Now()
		for {
			select {
			case <-testCtx.Done():
				return
			case <-ticker.C:
				snap := standaloneStats.snapshot()
				var sOK, sFail int64
				for _, v := range snap {
					sOK += v.ok
					sFail += v.fail
				}
				csnap := clusterStats.snapshot()
				var cOK, cFail int64
				for _, v := range csnap {
					cOK += v.ok
					cFail += v.fail
				}
				logger.Printf("%sprogress elapsed=%s standalone ok=%d fail=%d cluster ok=%d fail=%d",
					pref(logPrefix),
					time.Since(start).Truncate(time.Second),
					sOK, sFail, cOK, cFail)
			}
		}
	}()

	wg.Wait()

	// Report
	printResult := func(label string, s *cmdStats) int64 {
		snap := s.snapshot()
		var totalOK, totalFail int64
		for _, v := range snap {
			totalOK += v.ok
			totalFail += v.fail
		}
		logger.Printf("--- %s ---", label)
		logger.Printf("total_commands: ok=%d fail=%d", totalOK, totalFail)
		for name, v := range snap {
			if v.fail > 0 {
				logger.Printf("  %s: ok=%d fail=%d", name, v.ok, v.fail)
			}
		}
		return totalFail
	}
	sFail := printResult("Standalone Client Results", standaloneStats)
	cFail := printResult("Cluster Client Results", clusterStats)

	if sFail+cFail > 0 {
		os.Exit(1)
	}
	logger.Printf("PASS")
}

func pref(s string) string {
	if s == "" {
		return ""
	}
	return "[" + s + "] "
}
