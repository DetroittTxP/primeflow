package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

// NATSBus implements Bus on core NATS pub/sub.
//
// Core subjects, not JetStream: these messages are wake-up hints with no replay
// value, exactly like the Redis implementation. A worker that misses one still
// converges on its next poll, so stream persistence would buy nothing and cost
// operational weight.
type NATSBus struct {
	conn *nats.Conn
	log  *slog.Logger
}

// NewNATS dials a NATS server. url accepts the usual "nats://host:4222" form (or
// a comma-separated list for a cluster).
func NewNATS(ctx context.Context, url string, log *slog.Logger) (*NATSBus, error) {
	if log == nil {
		log = slog.Default()
	}
	deadline := 5 * time.Second
	if dl, ok := ctx.Deadline(); ok {
		if d := time.Until(dl); d > 0 && d < deadline {
			deadline = d
		}
	}
	conn, err := nats.Connect(url,
		nats.Name("primeflow"),
		nats.Timeout(deadline),
		nats.RetryOnFailedConnect(false),
		nats.MaxReconnects(-1), // reconnect forever once first connected
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, e error) {
			if e != nil {
				log.Warn("nats disconnected", "err", e)
			}
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Info("nats reconnected", "url", c.ConnectedUrl())
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	return &NATSBus{conn: conn, log: log}, nil
}

// Publish sends a payload to a subject. Errors are returned but callers normally
// log and continue: the bus is an optimisation.
func (b *NATSBus) Publish(_ context.Context, topic string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return b.conn.Publish(topic, raw)
}

// Subscribe consumes the given subjects until ctx is cancelled. nats.go handles
// reconnection transparently; the subscriptions survive it.
func (b *NATSBus) Subscribe(ctx context.Context, topics []string, h Handler) error {
	subs := make([]*nats.Subscription, 0, len(topics))
	for _, t := range topics {
		sub, err := b.conn.Subscribe(t, func(m *nats.Msg) {
			h(Message{Topic: m.Subject, Payload: json.RawMessage(m.Data)})
		})
		if err != nil {
			for _, s := range subs {
				_ = s.Unsubscribe()
			}
			return fmt.Errorf("nats subscribe %q: %w", t, err)
		}
		subs = append(subs, sub)
	}
	go func() {
		<-ctx.Done()
		for _, s := range subs {
			_ = s.Unsubscribe()
		}
	}()
	return nil
}

// Close drains and releases the connection.
func (b *NATSBus) Close() error {
	if b.conn == nil {
		return nil
	}
	return b.conn.Drain()
}

var _ Bus = (*NATSBus)(nil)
