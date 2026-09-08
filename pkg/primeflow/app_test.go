// Tests for the wiring itself.
//
// Every other test in the tree builds a worker or an engine directly, which is
// convenient and misses the one code path every real deployment takes. Open()
// is where the pieces are assembled and where a field can be left nil without
// the compiler noticing — a worker's App.WorkerStore was unset on the database
// path for three commits, and nothing failed until a container was recreated.
// These tests run a worker through Open() in both modes so that class of
// mistake fails here instead of in production.
package primeflow_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/primex/primeflow/internal/store/postgres"
	"github.com/primex/primeflow/pkg/primeflow"
	"github.com/primex/primeflow/pkg/sdk"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// isolate clears the PRIMEFLOW_* variables Open() reads, so a developer's shell
// cannot decide which mode a test runs in.
func isolate(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"PRIMEFLOW_DATABASE_URL", "DATABASE_URL",
		"PRIMEFLOW_API_URL", "PRIMEFLOW_WORKER_TOKEN",
		"PRIMEFLOW_NATS_URL", "NATS_URL", "PRIMEFLOW_REDIS_URL", "REDIS_URL",
		"PRIMEFLOW_QUEUES", "PRIMEFLOW_WORKER_NAME", "PRIMEFLOW_METRICS_ADDR",
	} {
		t.Setenv(k, "")
	}
	// A worker's metrics listener would collide between parallel cases.
	t.Setenv("PRIMEFLOW_METRICS_ADDR", "127.0.0.1:0")
}

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("PRIMEFLOW_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set PRIMEFLOW_TEST_DATABASE_URL to run wiring tests")
	}
	return dsn
}

// runBriefly starts a worker through the App and stops it, returning whatever
// Run returned. A panic in the wiring surfaces here.
func runBriefly(t *testing.T, app *primeflow.App, d time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	var (
		wg  sync.WaitGroup
		err error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			// The wiring's failure mode is a nil field, which surfaces as a
			// panic deep in a worker goroutine. Catching it here names the test
			// that provoked it instead of killing the package's binary.
			if r := recover(); r != nil {
				err = fmt.Errorf("worker panicked, which means Open left something nil: %v", r)
			}
		}()
		err = app.ServeWorker(ctx)
	}()
	wg.Wait()
	return err
}

// The database path: Open() has to leave every field the worker dereferences
// set. Publishing the flow catalogue is the first thing a worker does and the
// exact call that panicked when WorkerStore was nil, so running one briefly and
// finding its flow registered is the assertion that matters.
func TestOpenWiresAWorkerOnTheDatabasePath(t *testing.T) {
	isolate(t)
	dsn := testDSN(t)
	ctx := context.Background()

	reg := sdk.NewRegistry()
	reg.Register("open-db-mode", func(c *sdk.Context) (any, error) { return nil, nil })

	app, err := primeflow.Open(ctx, primeflow.Options{
		DatabaseURL: dsn, Registry: reg, Logger: quiet(),
		MigrateOnStart: true,
		Queues:         []string{"open-db-lane"},
		WorkerName:     "open-db-worker",
		PollInterval:   50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer app.Close()

	if app.Store == nil {
		t.Error("Store is nil on the database path")
	}
	if app.WorkerStore == nil {
		t.Fatal("WorkerStore is nil: the worker would panic registering its first flow")
	}
	if app.Events == nil {
		t.Error("Events is nil: state changes would never reach the event log")
	}
	if app.Bus == nil || app.Metrics == nil || app.Log == nil {
		t.Error("Bus, Metrics or Log is nil")
	}

	if err := runBriefly(t, app, 2*time.Second); err != nil {
		t.Fatalf("ServeWorker: %v", err)
	}

	flows, err := app.Store.ListFlows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, f := range flows {
		if f.Name == "open-db-mode" {
			found = true
		}
	}
	if !found {
		t.Fatal("the worker did not publish its catalogue; the wiring is incomplete")
	}
	// The lane it polls is created lazily, and never as an upsert.
	if _, err := app.Store.GetWorkQueue(ctx, "open-db-lane"); err != nil {
		t.Fatalf("the worker did not ensure its lane: %v", err)
	}
}

// The API path: no database anywhere, and the same worker still boots, talks
// only to the URL it was given, and publishes its catalogue over HTTP.
func TestOpenWiresAWorkerOnTheAPIPath(t *testing.T) {
	isolate(t)

	var (
		mu   sync.Mutex
		seen = map[string]int{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.Method+" "+r.URL.Path]++
		mu.Unlock()
		if key := r.Header.Get("X-API-Key"); key != "pmx_wiring" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Header.Get("X-PrimeFlow-Worker-ID") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/lease"):
			_, _ = w.Write([]byte(`[]`))
		case strings.HasSuffix(r.URL.Path, "/stream"):
			<-r.Context().Done()
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer srv.Close()

	reg := sdk.NewRegistry()
	reg.Register("open-api-mode", func(c *sdk.Context) (any, error) { return nil, nil })

	app, err := primeflow.Open(context.Background(), primeflow.Options{
		APIURL: srv.URL, WorkerToken: "pmx_wiring", Registry: reg, Logger: quiet(),
		Queues:       []string{"open-api-lane"},
		WorkerName:   "open-api-worker",
		PollInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer app.Close()

	if app.Store != nil {
		t.Error("a worker on the API path must hold no database handle")
	}
	if app.WorkerStore == nil {
		t.Fatal("WorkerStore is nil: there would be nothing to execute against")
	}
	if app.Events != nil {
		t.Error("a worker on the API path must not emit events itself; the server does that for it")
	}

	if err := runBriefly(t, app, 2*time.Second); err != nil {
		t.Fatalf("ServeWorker: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, want := range []string{
		"POST /api/v1/worker/flows",
		"POST /api/v1/worker/queues/open-api-lane",
		"POST /api/v1/worker/lease",
	} {
		if seen[want] == 0 {
			t.Errorf("the worker never called %s; it reached %v", want, seen)
		}
	}
}

// Everything a worker on the API path cannot do has to say so, rather than
// dereference the database handle it does not have.
func TestAPIPathRefusesServerRolesClearly(t *testing.T) {
	isolate(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	app, err := primeflow.Open(context.Background(), primeflow.Options{
		APIURL: srv.URL, WorkerToken: "pmx_wiring", Logger: quiet(),
		Registry: sdk.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer app.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for name, err := range map[string]error{
		"ServeAPI": app.ServeAPI(ctx),
		"Migrate":  app.Migrate(ctx),
	} {
		if err == nil {
			t.Errorf("%s should refuse on the API path, it returned nil", name)
			continue
		}
		if !strings.Contains(err.Error(), "database") {
			t.Errorf("%s error should say a database is needed, got %q", name, err)
		}
	}
}

// With neither route configured, the error has to name both — a worker that
// cannot reach the orchestrator should not have to guess which knob it missed.
func TestOpenWithoutAnyRouteExplainsBothOptions(t *testing.T) {
	isolate(t)
	_, err := primeflow.Open(context.Background(), primeflow.Options{Logger: quiet()})
	if err == nil {
		t.Fatal("Open with no database and no API URL should fail")
	}
	for _, want := range []string{"PRIMEFLOW_DATABASE_URL", "PRIMEFLOW_API_URL", "PRIMEFLOW_WORKER_TOKEN"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error should mention %s: %q", want, err)
		}
	}
}

// A half-configured API path is a database worker, not a broken one: both
// variables are required together, and a lone API URL must not silently skip
// the database it was also given.
func TestAPIPathNeedsBothVariables(t *testing.T) {
	isolate(t)
	dsn := testDSN(t)
	// Make sure the schema exists before Open() is asked not to migrate.
	st, err := postgres.Open(context.Background(), dsn, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	app, err := primeflow.Open(context.Background(), primeflow.Options{
		DatabaseURL: dsn, APIURL: "http://example.invalid", // no token
		Registry: sdk.NewRegistry(), Logger: quiet(),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer app.Close()
	if app.Store == nil {
		t.Fatal("an API URL without a token should leave the worker on the database path")
	}
}
