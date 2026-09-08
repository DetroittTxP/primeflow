package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

func bg() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}

// ------------------------------------------------------------- throttle ---

// RedisThrottle is a shared login brute-force guard. A counter per key expires
// after `window`; on crossing `maxFailures` a lock key is set for `lockFor`.
type RedisThrottle struct {
	c           *redis.Client
	maxFailures int
	lockFor     time.Duration
	window      time.Duration
}

func (t *RedisThrottle) countKey(k string) string { return "pf:lt:" + k }
func (t *RedisThrottle) lockKey(k string) string  { return "pf:ltlock:" + k }

func (t *RedisThrottle) Locked(key string) (bool, time.Duration) {
	ctx, cancel := bg()
	defer cancel()
	d, err := t.c.PTTL(ctx, t.lockKey(key)).Result()
	if err != nil || d <= 0 {
		return false, 0
	}
	return true, d
}

func (t *RedisThrottle) Fail(key string) {
	ctx, cancel := bg()
	defer cancel()
	n, err := t.c.Incr(ctx, t.countKey(key)).Result()
	if err != nil {
		return
	}
	if n == 1 {
		t.c.Expire(ctx, t.countKey(key), t.window)
	}
	if int(n) >= t.maxFailures {
		t.c.Set(ctx, t.lockKey(key), "1", t.lockFor)
		t.c.Del(ctx, t.countKey(key))
	}
}

func (t *RedisThrottle) Reset(key string) {
	ctx, cancel := bg()
	defer cancel()
	t.c.Del(ctx, t.countKey(key), t.lockKey(key))
}

// ------------------------------------------------------------- limiter ---

// tokenBucketLua atomically refills and consumes one token.
// KEYS[1] = bucket key
// ARGV[1] = rate (tokens/sec), ARGV[2] = burst, ARGV[3] = now (ms)
// returns {allowed(0/1), retry_ms}
var tokenBucketLua = redis.NewScript(`
local key   = KEYS[1]
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now   = tonumber(ARGV[3])

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then tokens = burst; ts = now end

tokens = math.min(burst, tokens + (now - ts) / 1000.0 * rate)
ts = now

local allowed = 0
local retry = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry = math.ceil((1 - tokens) / rate * 1000.0)
end

redis.call('HSET', key, 'tokens', tokens, 'ts', ts)
redis.call('PEXPIRE', key, math.ceil(burst / rate * 1000.0) + 1000)
return {allowed, retry}
`)

// RedisLimiter is a shared per-key token bucket (burst == perMinute).
type RedisLimiter struct{ c *redis.Client }

func (l *RedisLimiter) key(k string) string { return "pf:rl:" + k }

func (l *RedisLimiter) Allow(key string, perMinute int) (bool, time.Duration) {
	if perMinute <= 0 {
		return true, 0
	}
	ctx, cancel := bg()
	defer cancel()
	rate := float64(perMinute) / 60.0
	res, err := tokenBucketLua.Run(ctx, l.c, []string{l.key(key)},
		rate, float64(perMinute), time.Now().UnixMilli()).Result()
	if err != nil {
		return true, 0 // fail open: a Redis hiccup must not lock out every caller
	}
	arr, ok := res.([]any)
	if !ok || len(arr) != 2 {
		return true, 0
	}
	allowed, _ := arr[0].(int64)
	retryMs, _ := arr[1].(int64)
	if allowed == 1 {
		return true, 0
	}
	return false, time.Duration(retryMs) * time.Millisecond
}

func (l *RedisLimiter) Forget(key string) {
	ctx, cancel := bg()
	defer cancel()
	l.c.Del(ctx, l.key(key))
}
