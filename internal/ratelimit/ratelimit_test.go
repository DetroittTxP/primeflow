package ratelimit

import (
	"context"
	"os"
	"testing"
	"time"
)

// fakeThrottle / fakeLimiter are trivial stand-ins so the fallback path can be
// asserted without importing internal/authn or internal/apiauth here.
type fakeThrottle struct{ failed int }

func (f *fakeThrottle) Locked(string) (bool, time.Duration) { return false, 0 }
func (f *fakeThrottle) Fail(string)                         { f.failed++ }
func (f *fakeThrottle) Reset(string)                        {}

type fakeLimiter struct{ forgot int }

func (f *fakeLimiter) Allow(string, int) (bool, time.Duration) { return true, 0 }
func (f *fakeLimiter) Forget(string)                           { f.forgot++ }

func TestNewFallsBackWithoutRedis(t *testing.T) {
	ft, fl := &fakeThrottle{}, &fakeLimiter{}
	tr, li, closeFn := New(context.Background(), Config{RedisURL: ""}, ft, fl)
	defer closeFn()
	if tr != Throttle(ft) || li != Limiter(fl) {
		t.Fatal("with no RedisURL, New must return the supplied in-process impls")
	}
}

func TestNewFallsBackWhenRedisUnreachable(t *testing.T) {
	ft, fl := &fakeThrottle{}, &fakeLimiter{}
	// A port nothing listens on.
	tr, li, closeFn := New(context.Background(),
		Config{RedisURL: "redis://127.0.0.1:6399"}, ft, fl)
	defer closeFn()
	if tr != Throttle(ft) || li != Limiter(fl) {
		t.Fatal("an unreachable Redis must fall back to the in-process impls")
	}
}

func TestRedisImplsSharedAcrossInstances(t *testing.T) {
	url := os.Getenv("PRIMEFLOW_TEST_REDIS_URL")
	if url == "" {
		t.Skip("set PRIMEFLOW_TEST_REDIS_URL to run the Redis rate-limit test")
	}
	ctx := context.Background()

	// Two independent limiters, one Redis: the bucket must be shared.
	_, a, closeA := New(ctx, Config{RedisURL: url}, &fakeThrottle{}, &fakeLimiter{})
	defer closeA()
	_, b, closeB := New(ctx, Config{RedisURL: url}, &fakeThrottle{}, &fakeLimiter{})
	defer closeB()
	if _, isFake := a.(*fakeLimiter); isFake {
		t.Fatal("expected the Redis limiter, got the fallback")
	}

	key := "rt-test-" + time.Now().Format("150405.000")
	a.Forget(key)
	allowed := 0
	for i := 0; i < 8; i++ {
		who := a
		if i%2 == 1 {
			who = b
		}
		if ok, _ := who.Allow(key, 5); ok {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("shared bucket of 5 allowed %d calls across two instances", allowed)
	}
	a.Forget(key)

	// Two throttles, one Redis: the lock trips once, seen by both.
	_, _, _ = closeA, closeB, ctx
	tr1, _, c1 := New(ctx, Config{RedisURL: url, ThrottleMaxFailures: 3, ThrottleLockFor: time.Minute}, &fakeThrottle{}, &fakeLimiter{})
	defer c1()
	tr2, _, c2 := New(ctx, Config{RedisURL: url, ThrottleMaxFailures: 3, ThrottleLockFor: time.Minute}, &fakeThrottle{}, &fakeLimiter{})
	defer c2()
	lk := "lt-test-" + time.Now().Format("150405.000")
	tr1.Reset(lk)
	tr1.Fail(lk)
	tr1.Fail(lk)
	tr2.Fail(lk) // third failure, via the other instance
	if locked, d := tr2.Locked(lk); !locked || d <= 0 {
		t.Fatalf("lock not visible across instances: locked=%v d=%v", locked, d)
	}
	tr1.Reset(lk)
	if locked, _ := tr2.Locked(lk); locked {
		t.Fatal("Reset on one instance should clear the shared lock")
	}
}
