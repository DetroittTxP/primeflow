// Command myworker is a PrimeFlow worker carrying your flows.
//
// It is not a fork of PrimeFlow. It is a separate module that imports two
// packages -- pkg/sdk to define flows, pkg/primeflow/worker to run them -- and
// the flows registered in main() are the only ones this binary can execute.
//
//	go run .
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/DetroittTxP/primeflow/pkg/primeflow/worker"
	"github.com/DetroittTxP/primeflow/pkg/sdk"
)

// version is stamped at build time: -ldflags "-X main.version=$(git describe --tags --always)".
var version = "dev"

func main() {
	sdk.Flow("http-healthcheck", httpHealthcheck,
		sdk.Description("Probe a list of URLs and fail if any is unhealthy"),
		sdk.Tags("ops", "monitoring"),
		// A populated example, not a zero value: the non-zero fields become
		// the input placeholders the console shows.
		sdk.ParamsSchema(HealthcheckParams{
			Targets:        []string{"https://example.com/health"},
			ExpectStatus:   200,
			TimeoutSeconds: 10,
		}),
		sdk.Retries(2),
		sdk.RetryDelay(time.Minute),
		sdk.Timeout(10*time.Minute),
	)

	sdk.Flow("ping", ping,
		sdk.Description("Report which worker and pool executed the run -- the flow to deploy first on a new pool"),
		sdk.Tags("smoke"),
		sdk.ParamsSchema(struct{}{}),
		sdk.Retries(0),
		sdk.Timeout(time.Minute),
	)

	// Two ways to reach the orchestrator: a database connection, or -- for a
	// worker at a site with no route to the database -- the worker API.
	hasDB := os.Getenv("PRIMEFLOW_DATABASE_URL") != "" || os.Getenv("DATABASE_URL") != ""
	hasAPI := os.Getenv("PRIMEFLOW_API_URL") != "" && os.Getenv("PRIMEFLOW_WORKER_TOKEN") != ""
	if !hasDB && !hasAPI {
		log.Fatal("set PRIMEFLOW_API_URL and PRIMEFLOW_WORKER_TOKEN (the remote path a worker VM uses), " +
			"or PRIMEFLOW_DATABASE_URL for a worker running beside the database")
	}

	// -push (or PRIMEFLOW_PUSH=1) runs this binary as a push-pool receiver: it
	// listens for signed dispatches on PRIMEFLOW_PUSH_ADDR instead of polling.
	push := flag.Bool("push", os.Getenv("PRIMEFLOW_PUSH") == "1", "run as a push-pool receiver")
	flag.Parse()

	// pkg/primeflow/worker rather than pkg/primeflow: a worker binary has no
	// use for the API server, the console or the scheduler, and importing the
	// parent package would link all three.
	run := worker.Run
	if *push {
		run = worker.RunPush
	}
	// A zero Options reads PRIMEFLOW_QUEUES, PRIMEFLOW_CONCURRENCY,
	// PRIMEFLOW_WORKER_NAME and the rest from the environment, which is what
	// the compose and Kubernetes manifests set.
	if err := run(context.Background(), worker.Options{}); err != nil {
		log.Fatal(err)
	}
}
