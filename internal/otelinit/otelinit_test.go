package otelinit

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
)

func TestEnabledFromEnv(t *testing.T) {
	t.Setenv("PRIMEFLOW_OTEL_ENABLED", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	if Enabled() {
		t.Fatal("should be disabled with nothing set")
	}
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
	if !Enabled() {
		t.Fatal("an OTLP endpoint should enable tracing")
	}
	t.Setenv("PRIMEFLOW_OTEL_ENABLED", "false")
	if Enabled() {
		t.Fatal("explicit false must win over a set endpoint")
	}
}

func TestSetupNoopWhenDisabled(t *testing.T) {
	t.Setenv("PRIMEFLOW_OTEL_ENABLED", "false")
	shutdown, err := Setup(context.Background(), "primeflow", "test")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("noop shutdown returned %v", err)
	}
	// Propagators are always installed so inbound trace context is preserved.
	if otel.GetTextMapPropagator() == nil {
		t.Fatal("expected a text-map propagator to be set")
	}
	// A span from the (no-op) global tracer must not record.
	_, span := Tracer("t").Start(context.Background(), "s")
	if span.IsRecording() {
		t.Fatal("tracing disabled but span is recording")
	}
	span.End()
}
