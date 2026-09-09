package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/DetroittTxP/primeflow/pkg/sdk"
)

// GreetParams is what an operator supplies on the console's Run dialog, or a
// caller posts to the External API. Anything crossing that boundary is JSON,
// so the tags are the field names an API client actually sends.
type GreetParams struct {
	Name  string `json:"name"`
	Times int    `json:"times,omitempty"` // default 1
}

// GreetResult is the run result. Like every checkpointed value it has to
// round-trip through JSON.
type GreetResult struct {
	Greetings []string  `json:"greetings"`
	Worker    string    `json:"worker"`
	Queue     string    `json:"queue"`
	Version   string    `json:"version"`
	At        time.Time `json:"at"`
}

// greet is deliberately small, but it is a real flow rather than a print
// statement: each greeting is a checkpointed task, so a worker that crashes
// halfway through resumes at the one it had not reached instead of starting
// over. Everything else you write follows the same three moves.
func greet(c *sdk.Context) (any, error) {
	p, err := sdk.Params[GreetParams](c)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(p.Name) == "" {
		// Bad input is not a transient failure. Permanent skips the retry
		// budget rather than burning it re-running the same empty name.
		return nil, sdk.Permanent(errors.New("name must not be empty"))
	}
	times := p.Times
	if times <= 0 {
		times = 1
	}
	c.Info("greeting", "name", p.Name, "times", times)

	greetings := make([]string, 0, times)
	for i := range times {
		// Keyed by identity, not by position. The default key is positional,
		// which is fine until the loop's shape changes -- an explicit key means
		// a resumed run replays the greetings already recorded rather than
		// shifting every checkpoint after the change.
		line, err := sdk.Task(c, "greet", func(c *sdk.Context) (string, error) {
			return fmt.Sprintf("hello %s (%d/%d)", p.Name, i+1, times), nil
		}, sdk.TaskKey(fmt.Sprintf("greet:%d", i)))
		if err != nil {
			return nil, err
		}
		greetings = append(greetings, line)
	}

	// Artifacts before the return, always. A run that ends failed drops its
	// result, so anything left there is lost exactly when you want to read it.
	// An artifact survives, and the console renders this one as a table.
	if err := c.Markdown("greetings", "# Greetings\n\n"+strings.Join(greetings, "\n\n")); err != nil {
		c.Warn("artifact write failed", "err", err)
	}

	return GreetResult{
		Greetings: greetings,
		Worker:    workerName(),
		Queue:     c.Run().WorkQueue,
		Version:   version,
		At:        time.Now().UTC(),
	}, nil
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
