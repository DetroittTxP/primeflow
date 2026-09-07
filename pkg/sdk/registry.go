package sdk

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// FlowFunc is the signature of a flow. Returning a non-nil value stores it as
// the run result, visible in the API and UI.
type FlowFunc func(c *Context) (any, error)

// FlowDef is a registered flow and its defaults. Deployments may override the
// retry and timeout settings per deployment.
type FlowDef struct {
	Name        string
	Version     string
	Description string
	Tags        []string
	Retries     int
	RetryDelay  time.Duration
	Timeout     time.Duration
	Fn          FlowFunc
}

// FlowOption configures a flow at registration time.
type FlowOption func(*FlowDef)

// Version labels the flow; deploying a new version keeps the old runs readable.
func Version(v string) FlowOption { return func(f *FlowDef) { f.Version = v } }

// Description documents the flow for the UI.
func Description(s string) FlowOption { return func(f *FlowDef) { f.Description = s } }

// Tags labels the flow and every run created from it.
func Tags(t ...string) FlowOption { return func(f *FlowDef) { f.Tags = t } }

// Retries sets how many times the whole flow is rescheduled after a failure.
// Task-level retries usually serve better; use this for whole-run recovery.
func Retries(n int) FlowOption { return func(f *FlowDef) { f.Retries = n } }

// RetryDelay sets the delay before a failed run is rescheduled.
func RetryDelay(d time.Duration) FlowOption { return func(f *FlowDef) { f.RetryDelay = d } }

// Timeout bounds one attempt of the whole flow.
func Timeout(d time.Duration) FlowOption { return func(f *FlowDef) { f.Timeout = d } }

// Registry holds the flows a worker can execute.
type Registry struct {
	mu    sync.RWMutex
	flows map[string]*FlowDef
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{flows: map[string]*FlowDef{}} }

// Default is the registry package-level Flow writes into. Most programs use it.
var Default = NewRegistry()

// Add registers a flow, replacing any earlier flow of the same name.
func (r *Registry) Add(f *FlowDef) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.flows[f.Name] = f
}

// Get looks a flow up by name.
func (r *Registry) Get(name string) (*FlowDef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.flows[name]
	return f, ok
}

// List returns every registered flow, alphabetically.
func (r *Registry) List() []*FlowDef {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*FlowDef, 0, len(r.flows))
	for _, f := range r.flows {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Names returns the registered flow names.
func (r *Registry) Names() []string {
	fs := r.List()
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.Name
	}
	return out
}

// Flow registers fn under name in the default registry and returns the
// definition so it can be inspected or deployed programmatically.
func Flow(name string, fn FlowFunc, opts ...FlowOption) *FlowDef {
	return Default.Register(name, fn, opts...)
}

// Register adds a flow to this registry.
func (r *Registry) Register(name string, fn FlowFunc, opts ...FlowOption) *FlowDef {
	if name == "" {
		panic("sdk: flow name must not be empty")
	}
	if fn == nil {
		panic(fmt.Sprintf("sdk: flow %q has no function", name))
	}
	f := &FlowDef{Name: name, Version: "1", Fn: fn, RetryDelay: 30 * time.Second}
	for _, o := range opts {
		o(f)
	}
	r.Add(f)
	return f
}

// Typed wraps a flow that takes decoded parameters, so authors do not repeat
// the Params call:
//
//	sdk.Flow("resize-vm", sdk.Typed(func(c *sdk.Context, p ResizeParams) (any, error) {
//	    ...
//	}))
func Typed[P any](fn func(c *Context, p P) (any, error)) FlowFunc {
	return func(c *Context) (any, error) {
		p, err := Params[P](c)
		if err != nil {
			return nil, err
		}
		return fn(c, p)
	}
}
