// Package otelinit wires OpenTelemetry tracing on or off from the environment.
//
// It is deliberately all-or-nothing and zero-config: if no OTLP endpoint is set
// it installs a no-op tracer provider (the OTEL default) and returns a no-op
// shutdown, so every span the rest of the code starts costs nothing. Point
// OTEL_EXPORTER_OTLP_ENDPOINT (or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) at a
// collector to turn it on. PRIMEFLOW_OTEL_ENABLED=false forces it off even if an
// endpoint is present.
package otelinit

import (
	"context"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// ShutdownFunc flushes and stops the tracer provider. It is always safe to call,
// even when tracing was never enabled.
type ShutdownFunc func(context.Context) error

func noop(context.Context) error { return nil }

// Enabled reports whether the environment asks for tracing.
func Enabled() bool {
	if v := os.Getenv("PRIMEFLOW_OTEL_ENABLED"); v != "" {
		return strings.EqualFold(v, "true") || v == "1"
	}
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}

// Setup installs the global tracer provider and W3C propagators. When tracing is
// disabled it still sets the propagators (cheap, and it means inbound trace
// context is preserved for a downstream that does export) and returns a no-op
// shutdown.
func Setup(ctx context.Context, service, version string) (ShutdownFunc, error) {
	// Always propagate: a request may carry trace context we want to forward.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	if !Enabled() {
		return noop, nil
	}

	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	exp, err := otlptracehttp.New(dialCtx) // reads OTEL_EXPORTER_OTLP_* itself
	if err != nil {
		return noop, err
	}

	if v := os.Getenv("OTEL_SERVICE_NAME"); v != "" {
		service = v
	}
	res, _ := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(service),
		semconv.ServiceVersion(version),
	))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	return func(c context.Context) error {
		sc, cancel := context.WithTimeout(c, 5*time.Second)
		defer cancel()
		return tp.Shutdown(sc)
	}, nil
}

// Tracer returns a named tracer from the global provider — a no-op tracer, and
// therefore free, when tracing is disabled.
func Tracer(name string) oteltrace.Tracer {
	return otel.Tracer(name)
}
