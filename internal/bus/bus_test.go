package bus

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// collector is a Handler that records the payloads it receives.
type collector struct {
	mu   sync.Mutex
	msgs []Message
}

func (c *collector) handle(m Message) {
	c.mu.Lock()
	c.msgs = append(c.msgs, m)
	c.mu.Unlock()
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.msgs)
}

// waitFor polls until pred is true or the deadline passes.
func waitFor(t *testing.T, d time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

func TestInMemoryBusRoundTrip(t *testing.T) {
	b := NewInMemory()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var got collector
	if err := b.Subscribe(ctx, []string{TopicWork, TopicControl}, got.handle); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := b.Publish(ctx, TopicWork, map[string]string{"queue": "vcd"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := b.Publish(ctx, TopicControl, ControlMessage{Action: "cancel", FlowRunID: "r1"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// A topic nobody subscribed to must not reach the handler.
	if err := b.Publish(ctx, TopicEvents, map[string]int{"n": 1}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitFor(t, time.Second, func() bool { return got.count() == 2 })

	var work struct {
		Queue string `json:"queue"`
	}
	var ctrl ControlMessage
	for _, m := range got.msgs {
		switch m.Topic {
		case TopicWork:
			_ = json.Unmarshal(m.Payload, &work)
		case TopicControl:
			_ = json.Unmarshal(m.Payload, &ctrl)
		default:
			t.Fatalf("unexpected topic %q", m.Topic)
		}
	}
	if work.Queue != "vcd" || ctrl.FlowRunID != "r1" || ctrl.Action != "cancel" {
		t.Fatalf("payloads not delivered intact: work=%+v ctrl=%+v", work, ctrl)
	}
}

func TestInMemoryBusUnsubscribesOnContextCancel(t *testing.T) {
	b := NewInMemory()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())

	var got collector
	_ = b.Subscribe(ctx, []string{TopicWork}, got.handle)
	cancel()
	// Give the cleanup goroutine a moment to drop the handler.
	waitFor(t, time.Second, func() bool {
		b.mu.RLock()
		defer b.mu.RUnlock()
		return len(b.subs[TopicWork]) == 0
	})

	_ = b.Publish(context.Background(), TopicWork, map[string]string{"queue": "x"})
	time.Sleep(50 * time.Millisecond)
	if got.count() != 0 {
		t.Fatalf("handler fired after context cancel: %d messages", got.count())
	}
}
