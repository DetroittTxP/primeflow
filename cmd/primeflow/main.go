// Command primeflow is the orchestration server, migrator and admin CLI.
//
// Flows live in your own binary, which imports pkg/sdk and pkg/primeflow and
// runs as a worker; this command is the part that has no application code in
// it. Run "primeflow help" for the subcommands.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/primex/primeflow/internal/authn"
	"github.com/primex/primeflow/internal/store"
	"github.com/primex/primeflow/pkg/primeflow"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	ctx := primeflow.WithSignals(context.Background())

	var err error
	switch cmd {
	case "server":
		err = cmdServer(ctx, args)
	case "migrate":
		err = cmdMigrate(ctx, args)
	case "user":
		err = cmdUser(ctx, args)
	case "seed":
		err = cmdSeed(ctx, args)
	case "run":
		err = cmdRun(ctx, args)
	case "runs":
		err = cmdRuns(ctx, args)
	case "queue":
		err = cmdQueue(ctx, args)
	case "deploy":
		err = cmdDeploy(ctx, args)
	case "help", "-h", "--help":
		usage()
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Print(`primeflow — durable workflow orchestration for PrimeX

Server:
  primeflow server [-addr :8080] [-no-ui]     run the API, UI, scheduler and automations
  primeflow migrate                           apply the database schema

Operator accounts (talk straight to the database, like migrate):
  primeflow user add -email a@x -password s [-role admin|operator|viewer]
  primeflow user list
  primeflow user passwd -email a@x -password new
  primeflow user role   -email a@x -role operator
  primeflow user deactivate -email a@x
  primeflow user reset-link -email a@x        print a one-time password-reset URL

Development fixtures (also straight to the database):
  primeflow seed [-runs 4] [-password s] [-no-users] [-no-migrate]
                                              queues, deployments and demo accounts

Admin (talks to a running server over the API):
  primeflow deploy -f deployments.json        create or update deployments
  primeflow run <deployment> [-param k=v]     trigger a deployment now
  primeflow runs [-state RUNNING] [-limit 20] list recent runs
  primeflow queue                             show queue depth and dispatch order
  primeflow queue pause|resume <name>         stop or start dispatch for a queue
  primeflow queue front <run-id>              promote a waiting run to the front
  primeflow queue priority <run-id> <0-100>   change a waiting run's priority

Environment:
  PRIMEFLOW_DATABASE_URL   postgres DSN                 (server, migrate)
  PRIMEFLOW_REDIS_URL      redis URL for live updates   (optional)
  PRIMEFLOW_API_URL        server URL for admin commands (default http://localhost:8080)
  PRIMEFLOW_API_TOKEN      bearer token, if the server requires one
`)
}

// -------------------------------------------------------------- server ---

func cmdServer(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	addr := fs.String("addr", "", "listen address (default :8080)")
	noUI := fs.Bool("no-ui", false, "disable the bundled operator console")
	withWorker := fs.Bool("with-worker", false, "also run a worker in this process (development only: it has no flows registered)")
	noMigrate := fs.Bool("no-migrate", false, "do not apply the schema on start")
	if err := fs.Parse(args); err != nil {
		return err
	}

	app, err := primeflow.Open(ctx, primeflow.Options{
		HTTPAddr:       *addr,
		DisableUI:      *noUI,
		MigrateOnStart: !*noMigrate,
	})
	if err != nil {
		return err
	}
	defer app.Close()

	if *withWorker {
		return app.ServeAll(ctx)
	}
	return app.ServeAPI(ctx)
}

func cmdMigrate(ctx context.Context, _ []string) error {
	app, err := primeflow.Open(ctx, primeflow.Options{})
	if err != nil {
		return err
	}
	defer app.Close()
	if err := app.Migrate(ctx); err != nil {
		return err
	}
	fmt.Println("schema applied")
	return nil
}

// ---------------------------------------------------------------- user ---

func cmdUser(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: primeflow user add|list|passwd|role|deactivate ...")
	}
	sub, rest := args[0], args[1:]

	fs := flag.NewFlagSet("user "+sub, flag.ExitOnError)
	email := fs.String("email", "", "account email")
	password := fs.String("password", "", "password (min 8 chars)")
	role := fs.String("role", "viewer", "admin | operator | viewer")
	if err := fs.Parse(rest); err != nil {
		return err
	}

	app, err := primeflow.Open(ctx, primeflow.Options{})
	if err != nil {
		return err
	}
	defer app.Close()
	st := app.Store

	switch sub {
	case "list":
		us, err := st.ListUsers(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("%-32s %-10s %-8s %s\n", "EMAIL", "ROLE", "STATUS", "LAST LOGIN")
		for _, u := range us {
			status := "active"
			if !u.Active {
				status = "disabled"
			}
			last := "never"
			if u.LastLoginAt != nil {
				last = u.LastLoginAt.Local().Format("2006-01-02 15:04")
			}
			fmt.Printf("%-32s %-10s %-8s %s\n", trunc(u.Email, 32), u.Role, status, last)
		}
		return nil

	case "add":
		if *email == "" || len(*password) < 8 {
			return fmt.Errorf("-email and a -password of at least 8 characters are required")
		}
		if !authn.ValidRole(*role) {
			return fmt.Errorf("-role must be admin, operator or viewer")
		}
		u, err := st.CreateUser(ctx, store.UserInput{
			ID: uuid.NewString(), Email: *email,
			PasswordHash: authn.HashPassword(*password), Role: *role, Active: true,
		})
		if err != nil {
			return err
		}
		fmt.Printf("created %s (%s)\n", u.Email, u.Role)
		return nil

	case "passwd":
		if *email == "" || len(*password) < 8 {
			return fmt.Errorf("-email and a -password of at least 8 characters are required")
		}
		u, err := st.GetUserByEmail(ctx, *email)
		if err != nil {
			return err
		}
		if _, err := st.UpdateUser(ctx, u.ID, nil, nil, authn.HashPassword(*password)); err != nil {
			return err
		}
		_ = st.DeleteUserSessions(ctx, u.ID)
		fmt.Printf("password updated for %s\n", u.Email)
		return nil

	case "role":
		if *email == "" || !authn.ValidRole(*role) {
			return fmt.Errorf("-email and a valid -role are required")
		}
		u, err := st.GetUserByEmail(ctx, *email)
		if err != nil {
			return err
		}
		if _, err := st.UpdateUser(ctx, u.ID, role, nil, ""); err != nil {
			return err
		}
		_ = st.DeleteUserSessions(ctx, u.ID)
		fmt.Printf("%s is now %s\n", u.Email, *role)
		return nil

	case "deactivate":
		if *email == "" {
			return fmt.Errorf("-email is required")
		}
		u, err := st.GetUserByEmail(ctx, *email)
		if err != nil {
			return err
		}
		no := false
		if _, err := st.UpdateUser(ctx, u.ID, nil, &no, ""); err != nil {
			return err
		}
		_ = st.DeleteUserSessions(ctx, u.ID)
		fmt.Printf("%s deactivated\n", u.Email)
		return nil

	case "reset-link":
		if *email == "" {
			return fmt.Errorf("-email is required")
		}
		u, err := st.GetUserByEmail(ctx, *email)
		if err != nil {
			return err
		}
		token, expires, err := st.CreatePasswordReset(ctx, u.ID, time.Hour)
		if err != nil {
			return err
		}
		fmt.Printf("reset link for %s (expires %s):\n  /reset.html?token=%s\n",
			u.Email, expires.Local().Format("2006-01-02 15:04"), token)
		return nil
	}
	return fmt.Errorf("unknown user subcommand %q", sub)
}

// --------------------------------------------------------- admin client ---

type client struct {
	base  string
	token string
	http  *http.Client
}

func newClient() *client {
	base := os.Getenv("PRIMEFLOW_API_URL")
	if base == "" {
		base = "http://localhost:8080"
	}
	return &client{
		base:  strings.TrimSuffix(base, "/"),
		token: os.Getenv("PRIMEFLOW_API_TOKEN"),
		http:  &http.Client{Timeout: 20 * time.Second},
	}
}

func (c *client) do(ctx context.Context, method, path string, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("calling %s: %w", c.base+path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(raw, &e)
		if e.Error == "" {
			e.Error = strings.TrimSpace(string(raw))
		}
		return fmt.Errorf("%s: %s", resp.Status, e.Error)
	}
	if out != nil && len(raw) > 0 {
		return json.Unmarshal(raw, out)
	}
	return nil
}

// -------------------------------------------------------------- deploy ---

func cmdDeploy(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("deploy", flag.ExitOnError)
	file := fs.String("f", "", "JSON file: one deployment object or an array of them")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return fmt.Errorf("-f is required")
	}
	raw, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	var many []json.RawMessage
	if err := json.Unmarshal(raw, &many); err != nil {
		many = []json.RawMessage{raw} // a single object is fine too
	}
	c := newClient()
	for _, d := range many {
		var out struct {
			Name string `json:"name"`
			ID   string `json:"id"`
		}
		if err := c.do(ctx, http.MethodPost, "/api/v1/deployments", json.RawMessage(d), &out); err != nil {
			return err
		}
		fmt.Printf("deployed %s (%s)\n", out.Name, out.ID)
	}
	return nil
}

// ----------------------------------------------------------------- run ---

func cmdRun(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var params multiFlag
	fs.Var(&params, "param", "parameter as key=value (repeatable); values parse as JSON when possible")
	priority := fs.Int("priority", 0, "override priority (0-100)")
	queue := fs.String("queue", "", "override work queue")
	jsonParams := fs.String("params-json", "", "parameters as a raw JSON object")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: primeflow run <deployment-name> [-param k=v]")
	}
	name := fs.Arg(0)

	body := map[string]any{}
	if *priority > 0 {
		body["priority"] = *priority
	}
	if *queue != "" {
		body["work_queue"] = *queue
	}
	switch {
	case *jsonParams != "":
		body["parameters"] = json.RawMessage(*jsonParams)
	case len(params) > 0:
		p := map[string]any{}
		for _, kv := range params {
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				return fmt.Errorf("bad -param %q, expected key=value", kv)
			}
			var parsed any
			if json.Unmarshal([]byte(v), &parsed) == nil {
				p[k] = parsed
			} else {
				p[k] = v
			}
		}
		body["parameters"] = p
	}

	c := newClient()
	var dep struct {
		ID string `json:"id"`
	}
	// Resolve the name to an id through the list endpoint so the CLI needs no
	// extra server route.
	var deps []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := c.do(ctx, http.MethodGet, "/api/v1/deployments", nil, &deps); err != nil {
		return err
	}
	for _, d := range deps {
		if d.Name == name {
			dep.ID = d.ID
			break
		}
	}
	if dep.ID == "" {
		return fmt.Errorf("no deployment named %q", name)
	}

	var run struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := c.do(ctx, http.MethodPost, "/api/v1/deployments/"+dep.ID+"/run", body, &run); err != nil {
		return err
	}
	fmt.Printf("queued %s (%s)\n", run.Name, run.ID)
	return nil
}

// ---------------------------------------------------------------- runs ---

func cmdRuns(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("runs", flag.ExitOnError)
	stateF := fs.String("state", "", "filter by state, comma separated")
	queue := fs.String("queue", "", "filter by work queue")
	limit := fs.Int("limit", 20, "how many to show")
	if err := fs.Parse(args); err != nil {
		return err
	}
	q := fmt.Sprintf("?limit=%d", *limit)
	if *stateF != "" {
		q += "&state=" + *stateF
	}
	if *queue != "" {
		q += "&queue=" + *queue
	}
	var out struct {
		Total int `json:"total"`
		Runs  []struct {
			ID          string    `json:"id"`
			Name        string    `json:"name"`
			FlowName    string    `json:"flow_name"`
			State       string    `json:"state"`
			WorkQueue   string    `json:"work_queue"`
			Priority    int       `json:"priority"`
			ScheduledAt time.Time `json:"scheduled_at"`
		} `json:"runs"`
	}
	if err := newClient().do(ctx, http.MethodGet, "/api/v1/runs"+q, nil, &out); err != nil {
		return err
	}
	fmt.Printf("%-38s %-22s %-11s %-12s %4s  %s\n", "RUN", "FLOW", "STATE", "QUEUE", "PRIO", "SCHEDULED")
	for _, r := range out.Runs {
		fmt.Printf("%-38s %-22s %-11s %-12s %4d  %s\n",
			trunc(r.Name, 38), trunc(r.FlowName, 22), r.State, trunc(r.WorkQueue, 12),
			r.Priority, r.ScheduledAt.Local().Format("2006-01-02 15:04:05"))
	}
	fmt.Printf("\n%d of %d\n", len(out.Runs), out.Total)
	return nil
}

// --------------------------------------------------------------- queue ---

func cmdQueue(ctx context.Context, args []string) error {
	c := newClient()
	if len(args) == 0 {
		return showQueues(ctx, c)
	}
	switch args[0] {
	case "pause", "resume":
		if len(args) < 2 {
			return fmt.Errorf("usage: primeflow queue %s <queue-name>", args[0])
		}
		if err := c.do(ctx, http.MethodPost, "/api/v1/queues/"+args[1]+"/"+args[0], nil, nil); err != nil {
			return err
		}
		fmt.Printf("queue %s %sd\n", args[1], args[0])
		return nil

	case "front":
		if len(args) < 2 {
			return fmt.Errorf("usage: primeflow queue front <run-id>")
		}
		if err := c.do(ctx, http.MethodPost, "/api/v1/runs/"+args[1]+"/front", nil, nil); err != nil {
			return err
		}
		fmt.Println("run promoted to the front of its queue")
		return nil

	case "priority":
		if len(args) < 3 {
			return fmt.Errorf("usage: primeflow queue priority <run-id> <0-100>")
		}
		var p int
		if _, err := fmt.Sscanf(args[2], "%d", &p); err != nil {
			return fmt.Errorf("priority must be a number")
		}
		if err := c.do(ctx, http.MethodPost, "/api/v1/runs/"+args[1]+"/priority",
			map[string]int{"priority": p}, nil); err != nil {
			return err
		}
		fmt.Printf("priority set to %d\n", p)
		return nil

	case "show":
		if len(args) < 2 {
			return fmt.Errorf("usage: primeflow queue show <queue-name>")
		}
		return showPending(ctx, c, args[1])
	}
	return fmt.Errorf("unknown queue subcommand %q", args[0])
}

func showQueues(ctx context.Context, c *client) error {
	var qs []struct {
		Name             string `json:"name"`
		Paused           bool   `json:"paused"`
		ConcurrencyLimit *int   `json:"concurrency_limit"`
		Ready            int    `json:"ready"`
		Scheduled        int    `json:"scheduled"`
		Running          int    `json:"running"`
		Failed24h        int    `json:"failed_24h"`
	}
	if err := c.do(ctx, http.MethodGet, "/api/v1/queues", nil, &qs); err != nil {
		return err
	}
	fmt.Printf("%-18s %-8s %6s %7s %9s %8s %10s\n",
		"QUEUE", "STATE", "LIMIT", "READY", "SCHEDULED", "RUNNING", "FAILED24H")
	for _, q := range qs {
		st := "active"
		if q.Paused {
			st = "paused"
		}
		limit := "-"
		if q.ConcurrencyLimit != nil {
			limit = fmt.Sprint(*q.ConcurrencyLimit)
		}
		fmt.Printf("%-18s %-8s %6s %7d %9d %8d %10d\n",
			trunc(q.Name, 18), st, limit, q.Ready, q.Scheduled, q.Running, q.Failed24h)
	}
	return nil
}

func showPending(ctx context.Context, c *client, queue string) error {
	var runs []struct {
		ID            string    `json:"id"`
		Name          string    `json:"name"`
		Priority      int       `json:"priority"`
		QueuePosition *int      `json:"queue_position"`
		ScheduledAt   time.Time `json:"scheduled_at"`
	}
	if err := c.do(ctx, http.MethodGet, "/api/v1/queues/"+queue+"/pending", nil, &runs); err != nil {
		return err
	}
	fmt.Printf("dispatch order for %q\n\n%-3s %-38s %5s %6s  %s\n", queue, "#", "RUN", "PRIO", "PINNED", "SCHEDULED")
	for i, r := range runs {
		pin := ""
		if r.QueuePosition != nil {
			pin = "yes"
		}
		fmt.Printf("%-3d %-38s %5d %6s  %s\n",
			i+1, trunc(r.Name, 38), r.Priority, pin, r.ScheduledAt.Local().Format("15:04:05"))
	}
	return nil
}

// -------------------------------------------------------------- helpers ---

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return s[:n]
	}
	return s[:n-1] + "…"
}
