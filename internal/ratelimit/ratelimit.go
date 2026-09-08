// Package ratelimit provides the login brute-force throttle and the per-API-key
// rate limiter, in two interchangeable flavours:
//
//   - in-process (the default; per server replica), and
//   - Redis-backed (shared across replicas), selected when a Redis URL is set.
//
// The interfaces match the method sets internal/authn.Throttle and
// internal/apiauth.RateLimiter already expose, so those in-process types satisfy
// them directly and remain the fallback.
package ratelimit

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// Throttle is a per-key brute-force guard for the login endpoint.
type Throttle interface {
	// Locked reports whether key is currently locked out, and for how long.
	Locked(key string) (bool, time.Duration)
	// Fail records a failed attempt; a key is locked once it hits the ceiling.
	Fail(key string)
	// Reset clears a key's failure count and any lock (after a success).
	Reset(key string)
}

// Limiter is a per-key request-rate limiter (token bucket).
type Limiter interface {
	// Allow consumes one token for key at perMinute tokens/min; returns whether
	// the call is allowed and, if not, how long until a token frees up.
	Allow(key string, perMinute int) (bool, time.Duration)
	// Forget drops a key's state (on rotation or deletion).
	Forget(key string)
}

// Config picks and builds the pair.
type Config struct {
	// RedisURL, when set, selects the shared Redis-backed implementations.
	RedisURL string
	// RedisPassword is optional (also read from the URL if present).
	RedisPassword string
	// ThrottleMaxFailures / ThrottleLockFor tune the login guard (defaults
	// 5 / 15m when zero).
	ThrottleMaxFailures int
	ThrottleLockFor     time.Duration
	Logger              *slog.Logger
}

// New returns a (Throttle, Limiter) pair plus a close func. When RedisURL is
// empty, or Redis cannot be reached, it returns the supplied in-process
// fallbacks and a no-op close.
func New(ctx context.Context, cfg Config, memThrottle Throttle, memLimiter Limiter) (Throttle, Limiter, func() error) {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	noop := func() error { return nil }
	if cfg.RedisURL == "" {
		return memThrottle, memLimiter, noop
	}

	var opt *redis.Options
	if u, err := redis.ParseURL(cfg.RedisURL); err == nil {
		opt = u
	} else {
		opt = &redis.Options{Addr: cfg.RedisURL, Password: cfg.RedisPassword}
	}
	client := redis.NewClient(opt)
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		log.Warn("rate-limit redis unavailable; using in-process limits", "err", err)
		_ = client.Close()
		return memThrottle, memLimiter, noop
	}

	maxF := cfg.ThrottleMaxFailures
	if maxF <= 0 {
		maxF = 5
	}
	lockFor := cfg.ThrottleLockFor
	if lockFor <= 0 {
		lockFor = 15 * time.Minute
	}
	log.Info("rate limiting is cluster-wide (redis-backed)")
	return &RedisThrottle{c: client, maxFailures: maxF, lockFor: lockFor, window: lockFor},
		&RedisLimiter{c: client},
		client.Close
}
