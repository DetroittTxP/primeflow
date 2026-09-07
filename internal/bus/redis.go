package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisBus implements Bus on Redis pub/sub.
//
// Pub/sub, not streams: these messages are hints with no replay value. A worker
// that misses one picks the work up on its next poll, so paying for stream
// persistence would buy nothing.
type RedisBus struct {
	client *redis.Client
	log    *slog.Logger
}

// NewRedis dials Redis. addr accepts either "host:port" or a full redis:// URL.
func NewRedis(ctx context.Context, addr, password string, db int, log *slog.Logger) (*RedisBus, error) {
	var opt *redis.Options
	if u, err := redis.ParseURL(addr); err == nil {
		opt = u
	} else {
		opt = &redis.Options{Addr: addr, Password: password, DB: db}
	}
	c := redis.NewClient(opt)
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.Ping(pingCtx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	if log == nil {
		log = slog.Default()
	}
	return &RedisBus{client: c, log: log}, nil
}

// Publish sends a payload to a topic. Errors are returned but callers normally
// log and continue: the bus is an optimisation.
func (b *RedisBus) Publish(ctx context.Context, topic string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return b.client.Publish(ctx, topic, raw).Err()
}

// Subscribe consumes the given topics until ctx is cancelled, reconnecting on
// its own if the connection drops.
func (b *RedisBus) Subscribe(ctx context.Context, topics []string, h Handler) error {
	sub := b.client.Subscribe(ctx, topics...)
	go func() {
		defer sub.Close()
		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-ch:
				if !ok {
					return
				}
				h(Message{Topic: m.Channel, Payload: json.RawMessage(m.Payload)})
			}
		}
	}()
	return nil
}

// Close releases the connection.
func (b *RedisBus) Close() error { return b.client.Close() }

// ---------------------------------------------------------------------------

// InMemoryBus is the fallback when no Redis or NATS is configured. It is also
// what the all-in-one binary and the tests use, and it makes the dependency on
// an external broker genuinely optional.
type InMemoryBus struct {
	mu   sync.RWMutex
	subs map[string][]Handler
}

// NewInMemory returns a process-local bus.
func NewInMemory() *InMemoryBus {
	return &InMemoryBus{subs: map[string][]Handler{}}
}

// Publish delivers synchronously to every local subscriber.
func (b *InMemoryBus) Publish(_ context.Context, topic string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	b.mu.RLock()
	hs := append([]Handler(nil), b.subs[topic]...)
	b.mu.RUnlock()
	for _, h := range hs {
		go h(Message{Topic: topic, Payload: raw})
	}
	return nil
}

// Subscribe registers a handler for each topic.
func (b *InMemoryBus) Subscribe(ctx context.Context, topics []string, h Handler) error {
	b.mu.Lock()
	for _, t := range topics {
		b.subs[t] = append(b.subs[t], h)
	}
	b.mu.Unlock()
	go func() {
		<-ctx.Done()
		b.mu.Lock()
		for _, t := range topics {
			b.subs[t] = nil
		}
		b.mu.Unlock()
	}()
	return nil
}

// Close is a no-op.
func (b *InMemoryBus) Close() error { return nil }

var (
	_ Bus = (*RedisBus)(nil)
	_ Bus = (*InMemoryBus)(nil)
)
