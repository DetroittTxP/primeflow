// Command worker is a PrimeFlow worker carrying your flows.
//
// It is not a fork of PrimeFlow. It is a Go module of its own that imports two
// packages -- pkg/sdk to define flows, pkg/primeflow/worker to run them -- and
// the flows registered in main() are the only ones this binary can execute.
// The server holds no flow code, so adding a flow rebuilds this image alone.
//
//	go run .
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/DetroittTxP/primeflow/pkg/primeflow/worker"
	"github.com/DetroittTxP/primeflow/pkg/sdk"
)

// version is stamped at build time: -ldflags "-X main.version=$(git describe --tags --always)".
var version = "dev"

func main() {
	sdk.Flow("greet", greet,
		sdk.Description("The starter flow: echoes a name back through a checkpointed task"),
		sdk.Tags("example"),
		// A populated example, not a zero value: the non-zero fields become the
		// input placeholders the console shows on the Run dialog.
		sdk.ParamsSchema(GreetParams{Name: "world", Times: 1}),
		sdk.Retries(1),
		sdk.RetryDelay(30*time.Second),
		sdk.Timeout(5*time.Minute),
	)

	// Two ways to reach the orchestrator. A worker beside the database uses a
	// DSN; a worker at a site with nothing but outbound 443 uses the worker API
	// with a pool-scoped key. Setting neither is the common first mistake, so
	// it is worth failing on rather than waiting for a confusing dial error.
	hasDB := os.Getenv("PRIMEFLOW_DATABASE_URL") != "" || os.Getenv("DATABASE_URL") != ""
	hasAPI := os.Getenv("PRIMEFLOW_API_URL") != "" && os.Getenv("PRIMEFLOW_WORKER_TOKEN") != ""
	if !hasDB && !hasAPI {
		log.Fatal("set PRIMEFLOW_DATABASE_URL (a worker beside the database), " +
			"or PRIMEFLOW_API_URL and PRIMEFLOW_WORKER_TOKEN (a worker at a remote site)")
	}

	// pkg/primeflow/worker rather than pkg/primeflow: a worker binary has no
	// use for the API server, the console or the scheduler, and importing the
	// parent package would link all three.
	//
	// A zero Options reads PRIMEFLOW_QUEUES, PRIMEFLOW_CONCURRENCY,
	// PRIMEFLOW_WORKER_NAME and the rest from the environment, which is what
	// the compose file already sets.
	if err := worker.Run(context.Background(), worker.Options{}); err != nil {
		log.Fatal(err)
	}
}
