package main

import (
	"context"
	"log"
	"time"

	"github.com/primex/primeflow/pkg/primeflow"
	"github.com/primex/primeflow/pkg/sdk"
)

type params struct {
	ID string `json:"id"`
}

func stepA(c *sdk.Context, id string) (string, error) { return "a:" + id, nil }
func stepB(c *sdk.Context, in string) (string, error) { return "b:" + in, nil }
func stepC(c *sdk.Context, in string) error           { c.Info("done", "in", in); return nil }

func myFlow(c *sdk.Context) (any, error) {
	p, err := sdk.Params[params](c)
	if err != nil {
		return nil, err
	}
	a, err := sdk.Task(c, "step-a", func(c *sdk.Context) (string, error) { return stepA(c, p.ID) },
		sdk.TaskRetries(3), sdk.TaskRetryDelay(2*time.Second))
	if err != nil {
		return nil, err
	}
	b, err := sdk.Task(c, "step-b", func(c *sdk.Context) (string, error) { return stepB(c, a) })
	if err != nil {
		return nil, err
	}
	return b, sdk.Do(c, "step-c", func(c *sdk.Context) error { return stepC(c, b) })
}

func main() {
	sdk.Flow("my-flow", myFlow, sdk.Retries(2))
	log.Fatal(primeflow.RunWorker(context.Background(), primeflow.Options{}))
}
