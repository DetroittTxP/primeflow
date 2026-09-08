package remote

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/primex/primeflow/internal/bus"
)

// Bus receives wake-ups from the server's worker stream over the same HTTPS the
// rest of the worker API uses, because a site that can only reach port 443
// cannot reach NATS or Redis.
//
// It keeps the contract the rest of the codebase relies on: every message is a
// hint, never a fact the system depends on. The stream is opened in the
// background and reconnected with backoff, a failure to connect is logged and
// nothing else, and a worker that never receives a single message still runs
// every flow — just at its poll interval instead of instantly. Nothing is
// replayed after a reconnect, because the queue in Postgres is still the truth
// and the next poll reads it.
type Bus struct {
	url    string
	token  string
	worker string
	log    *slog.Logger
	hc     *http.Client
	cancel context.CancelFunc
}

var _ bus.Bus = (*Bus)(nil)

// NewBus builds the client. It does not dial: Subscribe does, in the
// background, so a server that is down at start-up delays nothing.
func NewBus(cfg Config) (*Bus, error) {
	if cfg.BaseURL == "" || cfg.Token == "" || cfg.WorkerID == "" {
		return nil, errors.New("remote: BaseURL, Token and WorkerID are required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	base := strings.TrimSuffix(strings.TrimRight(cfg.BaseURL, "/"), "/api/v1")
	return &Bus{
		url: base + "/api/v1/worker/stream", token: cfg.Token, worker: cfg.WorkerID,
		log: cfg.Logger,
		// No client timeout: the stream is meant to stay open. Liveness comes
		// from the server's 20-second comment pings and the read deadline below.
		hc: &http.Client{},
	}, nil
}

// Publish is a no-op. A remote worker has nothing to announce: the server
// records its transitions and publishes on its behalf, which is also what stops
// a site from writing into a channel other sites read.
func (b *Bus) Publish(context.Context, string, any) error { return nil }

// Subscribe starts reading the stream and delivers work and control notices to
// h. It returns immediately; the connection lives until ctx is cancelled or
// Close is called.
func (b *Bus) Subscribe(ctx context.Context, topics []string, h Handler) error {
	want := map[string]bool{}
	for _, t := range topics {
		want[t] = true
	}
	ctx, cancel := context.WithCancel(ctx)
	b.cancel = cancel
	go b.run(ctx, want, h)
	return nil
}

// Handler mirrors bus.Handler so this file reads without an import alias.
type Handler = bus.Handler

func (b *Bus) Close() error {
	if b.cancel != nil {
		b.cancel()
	}
	return nil
}

func (b *Bus) run(ctx context.Context, want map[string]bool, h Handler) {
	var attempt int
	for {
		if ctx.Err() != nil {
			return
		}
		if err := b.stream(ctx, want, h); err != nil && ctx.Err() == nil {
			attempt++
			d := backoff(attempt)
			b.log.Debug("wake-up stream dropped; polling covers the gap",
				"err", err, "retry_in", d)
			select {
			case <-ctx.Done():
				return
			case <-time.After(d):
			}
			continue
		}
		attempt = 0
	}
}

// stream holds one connection open, returning when it ends for any reason.
func (b *Bus) stream(ctx context.Context, want map[string]bool, h Handler) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("X-API-Key", b.token)
	req.Header.Set("X-PrimeFlow-Worker-ID", b.worker)

	resp, err := b.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("stream: %d: %s", resp.StatusCode, errText(raw))
	}
	b.log.Info("wake-up stream connected", "url", b.url)

	// One SSE frame is "event: <name>", "data: <json>", blank line. Comment
	// lines (": ping") keep the connection warm and are ignored.
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	var event string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			event = ""
		case strings.HasPrefix(line, ":"):
			// comment / keep-alive
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			topic := ""
			switch event {
			case "work":
				topic = bus.TopicWork
			case "control":
				topic = bus.TopicControl
			default:
				continue
			}
			if !want[topic] {
				continue
			}
			h(bus.Message{Topic: topic, Payload: json.RawMessage(payload)})
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return errors.New("stream closed by server")
}
