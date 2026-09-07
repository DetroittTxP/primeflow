package bus

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

// TestNATSBusRoundTrip needs a reachable NATS server. Point PRIMEFLOW_TEST_NATS_URL
// at one (e.g. nats://localhost:4222) to run it.
func TestNATSBusRoundTrip(t *testing.T) {
	url := os.Getenv("PRIMEFLOW_TEST_NATS_URL")
	if url == "" {
		t.Skip("set PRIMEFLOW_TEST_NATS_URL to run the NATS bus test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b, err := NewNATS(ctx, url, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer b.Close()

	var got collector
	if err := b.Subscribe(ctx, []string{TopicWork}, got.handle); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// Core NATS drops messages published before the subscription is wired; a
	// short pause makes the test deterministic.
	time.Sleep(100 * time.Millisecond)

	if err := b.Publish(ctx, TopicWork, map[string]string{"queue": "metering"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool { return got.count() == 1 })

	var work struct {
		Queue string `json:"queue"`
	}
	_ = json.Unmarshal(got.msgs[0].Payload, &work)
	if work.Queue != "metering" {
		t.Fatalf("payload not intact: %+v", work)
	}
}
