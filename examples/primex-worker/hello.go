package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/DetroittTxP/primeflow/pkg/sdk"
)

// ------------------------------------------------------------ hello-world ---
//
// hello-world is the smallest flow that still exercises everything a real one
// uses: typed parameters, checkpointed tasks, a log line and an artifact. It
// calls nothing external, so it runs wherever a worker runs and can only fail
// for a reason of its own -- which is what makes it the flow to deploy first
// when you want to prove a new pool actually picks work up.

// HelloParams is what an operator supplies.
type HelloParams struct {
	Name  string `json:"name"`
	Times int    `json:"times,omitempty"` // how many greetings, 1-10; default 1
}

// HelloResult is the run result. Like every checkpointed value it has to
// round-trip through JSON. Recording the worker and pool is what makes the
// result answer "where did this actually run", which is the question you have
// when a deployment is pinned to a pool.
type HelloResult struct {
	Greetings []string  `json:"greetings"`
	Worker    string    `json:"worker"`
	Queue     string    `json:"queue"`
	At        time.Time `json:"at"`
}

func helloWorld(c *sdk.Context) (any, error) {
	p, err := sdk.Params[HelloParams](c)
	if err != nil {
		return nil, err
	}
	name := strings.TrimSpace(p.Name)
	if name == "" {
		name = "world"
	}
	times := p.Times
	if times == 0 {
		times = 1
	}
	// A parameter the caller got wrong is not a transient failure: Permanent
	// skips the retry budget instead of burning it on the same bad input.
	if times < 0 || times > 10 {
		return nil, sdk.Permanent(errors.New("times must be between 1 and 10"))
	}
	c.Info("greeting", "name", name, "times", times)

	// One checkpoint per greeting, keyed by the name and index rather than left
	// to the default positional key -- so raising `times` later replays the
	// greetings already recorded instead of shifting every key after the first.
	greetings := make([]string, 0, times)
	for i := 1; i <= times; i++ {
		line, err := sdk.Task(c, "greet", func(c *sdk.Context) (string, error) {
			return fmt.Sprintf("Hello, %s! (%d/%d)", name, i, times), nil
		}, sdk.TaskKey(fmt.Sprintf("greet:%s:%d", name, i)))
		if err != nil {
			return nil, err
		}
		greetings = append(greetings, line)
	}

	who := workerName()
	_ = c.Markdown("greeting", fmt.Sprintf(
		"### Hello, %s\n\n- **Worker:** %s (pool `%s`)\n- **Greetings:** %d\n\n%s\n",
		name, who, c.Run().WorkQueue, len(greetings), "- "+strings.Join(greetings, "\n- ")))

	return HelloResult{
		Greetings: greetings,
		Worker:    who,
		Queue:     c.Run().WorkQueue,
		At:        time.Now().UTC(),
	}, nil
}
