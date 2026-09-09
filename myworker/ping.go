package main

import (
	"os"
	"time"

	"github.com/DetroittTxP/primeflow/pkg/sdk"
)

// ------------------------------------------------------------------- ping ---
//
// ping calls nothing external, so it can only fail for a reason of its own.
// That is what makes it the flow to run first against a new pool: if it does
// not start, the problem is the pool, the key or the queue name -- never the
// flow.

// PingResult records where the run actually executed, which is the question
// you have when a deployment is pinned to a pool.
type PingResult struct {
	Worker  string    `json:"worker"`
	Queue   string    `json:"queue"`
	Image   string    `json:"image"`
	Version string    `json:"version"`
	At      time.Time `json:"at"`
}

func ping(c *sdk.Context) (any, error) {
	r := PingResult{
		Worker:  workerName(),
		Queue:   c.Run().WorkQueue,
		Image:   os.Getenv("MYWORKER_IMAGE"),
		Version: version,
		At:      time.Now().UTC(),
	}
	c.Info("pong", "worker", r.Worker, "queue", r.Queue, "version", r.Version)
	return r, nil
}

func workerName() string {
	if n := os.Getenv("PRIMEFLOW_WORKER_NAME"); n != "" {
		return n
	}
	host, _ := os.Hostname()
	if host == "" {
		host = "worker"
	}
	return host
}
