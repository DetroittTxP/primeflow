// Package bus carries low-latency notifications between the server and its
// workers.
//
// The bus is deliberately not a queue. Postgres holds the queue and decides who
// runs what; the bus only says "something changed, look now" so a worker does
// not have to wait for its poll interval, and so the UI can stream live. If the
// bus is down or a message is dropped, every consumer still converges on the
// next poll — correctness never depends on delivery.
package bus

import (
	"context"
	"encoding/json"
)

// Topic names. Keep them few and coarse; consumers filter in memory.
const (
	// TopicWork announces that new work is available on a queue. Payload is
	// the queue name.
	TopicWork = "primeflow.work"
	// TopicEvents fans out core.Event values for automations and the UI feed.
	TopicEvents = "primeflow.events"
	// TopicControl carries out-of-band instructions to workers, currently
	// only cancellation. Payload is a ControlMessage.
	TopicControl = "primeflow.control"
)

// ControlMessage instructs a worker to act on a run it is holding.
type ControlMessage struct {
	Action    string `json:"action"` // "cancel"
	FlowRunID string `json:"flow_run_id"`
}

// Message is one delivery.
type Message struct {
	Topic   string
	Payload json.RawMessage
}

// Handler consumes messages. It must not block for long.
type Handler func(Message)

// Bus is publish/subscribe with at-most-once semantics.
type Bus interface {
	Publish(ctx context.Context, topic string, payload any) error
	Subscribe(ctx context.Context, topics []string, h Handler) error
	Close() error
}
