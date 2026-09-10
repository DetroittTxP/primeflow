package stdcapture

import (
	"fmt"
	stdlog "log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// collector is a Sink that remembers what it was given.
type collector struct {
	mu    sync.Mutex
	lines []Line
}

func (c *collector) sink(ln Line) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, ln)
}

func (c *collector) snapshot() []Line {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Line(nil), c.lines...)
}

func (c *collector) texts() []string {
	var out []string
	for _, l := range c.snapshot() {
		out = append(out, l.Text)
	}
	return out
}

// install points os.Stdout and os.Stderr at files this test can read — so the
// mirrored copy can be asserted on, and so a failure cannot scribble over the
// test runner's own output — then installs the capture on top.
//
// These tests share process-global state and must not run in parallel.
func install(t *testing.T) (release func(), mirrored func() string) {
	t.Helper()

	dir := t.TempDir()
	out, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	errf, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		t.Fatal(err)
	}

	savedOut, savedErr, savedLog := os.Stdout, os.Stderr, stdlog.Writer()
	os.Stdout, os.Stderr = out, errf

	stop, err := Install()
	if err != nil {
		os.Stdout, os.Stderr = savedOut, savedErr
		t.Fatal(err)
	}

	var once sync.Once
	release = func() {
		once.Do(func() {
			stop()
			os.Stdout, os.Stderr = savedOut, savedErr
			stdlog.SetOutput(savedLog)
			out.Close()
			errf.Close()
		})
	}
	t.Cleanup(release)

	mirrored = func() string {
		a, _ := os.ReadFile(filepath.Join(dir, "stdout"))
		b, _ := os.ReadFile(filepath.Join(dir, "stderr"))
		return string(a) + string(b)
	}
	return release, mirrored
}

// settle waits for the pump to reach everything written so far.
func settle() { Sync(2 * time.Second) }

func TestPrintFromOneRunIsAttributedToIt(t *testing.T) {
	_, mirrored := install(t)

	var c collector
	unbind := Bind("run-1", c.sink)
	defer unbind()

	fmt.Println("hello from the flow")
	settle()

	got := c.snapshot()
	if len(got) != 1 {
		t.Fatalf("want 1 line, got %+v", got)
	}
	if got[0].Text != "hello from the flow" {
		t.Errorf("text %q", got[0].Text)
	}
	if got[0].Stream != StreamStdout {
		t.Errorf("stream %q, want %q", got[0].Stream, StreamStdout)
	}
	if got[0].Ambiguous {
		t.Error("a line printed with one run bound is not ambiguous")
	}
	if got[0].At.IsZero() {
		t.Error("no timestamp")
	}

	// Capture adds to the worker's own stdout; it must not replace it.
	if !strings.Contains(mirrored(), "hello from the flow") {
		t.Errorf("the line did not reach the real stdout: %q", mirrored())
	}
}

func TestStderrAndTheStandardLoggerAreCaptured(t *testing.T) {
	install(t)

	var c collector
	defer Bind("run-1", c.sink)()

	fmt.Fprintln(os.Stderr, "straight to stderr")
	stdlog.SetFlags(0)
	stdlog.Println("through the log package")
	settle()

	for _, ln := range c.snapshot() {
		if ln.Stream != StreamStderr {
			t.Errorf("line %q on stream %q, want %q", ln.Text, ln.Stream, StreamStderr)
		}
	}
	if got := strings.Join(c.texts(), "|"); got != "straight to stderr|through the log package" {
		t.Fatalf("got %q", got)
	}
}

// Go hands no writer identity to the far end of a pipe, so a worker executing
// several runs at once cannot tell which of them printed a line. Every run in
// flight gets it, marked, rather than the line being dropped.
func TestConcurrentRunsShareTheLineMarkedAmbiguous(t *testing.T) {
	install(t)

	var a, b collector
	defer Bind("run-a", a.sink)()
	defer Bind("run-b", b.sink)()

	fmt.Println("who printed this?")
	settle()

	for name, c := range map[string]*collector{"run-a": &a, "run-b": &b} {
		got := c.snapshot()
		if len(got) != 1 {
			t.Fatalf("%s: want 1 line, got %+v", name, got)
		}
		if !got[0].Ambiguous {
			t.Errorf("%s: line should be marked ambiguous", name)
		}
		if got[0].Text != "who printed this?" {
			t.Errorf("%s: text %q", name, got[0].Text)
		}
	}
}

// A worker sitting idle still writes its own structured log to stdout. None of
// that belongs to a run.
func TestNothingIsRecordedWhileNoRunIsBound(t *testing.T) {
	_, mirrored := install(t)

	fmt.Println("worker chatter")
	settle()

	var c collector
	defer Bind("run-1", c.sink)()
	settle()

	if got := c.snapshot(); len(got) != 0 {
		t.Fatalf("a line printed before the bind must not be recorded: %+v", got)
	}
	if !strings.Contains(mirrored(), "worker chatter") {
		t.Errorf("the line did not reach the real stdout: %q", mirrored())
	}
}

// The whole point of the barrier: the pipe is read by a goroutine of its own,
// so the line a flow prints as its last statement is still in flight when the
// engine wants to unbind it.
func TestSyncDrainsTheLastLineBeforeUnbind(t *testing.T) {
	install(t)

	for i := range 50 {
		var c collector
		unbind := Bind(fmt.Sprintf("run-%d", i), c.sink)
		fmt.Println("the very last thing")
		Sync(2 * time.Second)
		unbind()

		if got := c.texts(); len(got) != 1 || got[0] != "the very last thing" {
			t.Fatalf("iteration %d: lost the tail: %q", i, got)
		}
	}
}

func TestBlankLinesAreMirroredButNotRecorded(t *testing.T) {
	_, mirrored := install(t)

	var c collector
	defer Bind("run-1", c.sink)()

	fmt.Print("\n\n")
	fmt.Println("content")
	settle()

	if got := c.texts(); len(got) != 1 || got[0] != "content" {
		t.Fatalf("want the content line alone, got %q", got)
	}
	if !strings.HasPrefix(mirrored(), "\n\ncontent\n") {
		t.Errorf("the blank lines should still reach stdout: %q", mirrored())
	}
}

// A flow that writes a large blob without a newline must not be buffered
// without limit.
func TestAnUnboundedLineIsBroken(t *testing.T) {
	install(t)

	var c collector
	defer Bind("run-1", c.sink)()

	fmt.Println(strings.Repeat("x", maxLine*2+7))
	settle()

	got := c.snapshot()
	if len(got) != 3 {
		t.Fatalf("want the line broken into 3, got %d", len(got))
	}
	total := 0
	for _, ln := range got {
		if len(ln.Text) > maxLine {
			t.Errorf("a piece is %d bytes, over the %d cap", len(ln.Text), maxLine)
		}
		total += len(ln.Text)
	}
	if total != maxLine*2+7 {
		t.Errorf("the pieces total %d bytes, want %d", total, maxLine*2+7)
	}
}

func TestReleaseRestoresTheRealStdout(t *testing.T) {
	release, _ := install(t)

	captured := os.Stdout
	if !Installed() {
		t.Fatal("Install did not take")
	}
	release()
	if Installed() {
		t.Fatal("release did not take")
	}
	if os.Stdout == captured {
		t.Error("os.Stdout was left pointing at the capture pipe")
	}
}

// Nesting is what a process that both serves and executes gets: the outer
// install must survive the inner release.
func TestNestedInstallsShareOneCapture(t *testing.T) {
	install(t)

	inner, err := Install()
	if err != nil {
		t.Fatal(err)
	}
	inner()
	if !Installed() {
		t.Fatal("the inner release tore down the outer install")
	}

	var c collector
	defer Bind("run-1", c.sink)()
	fmt.Println("still captured")
	settle()

	if got := c.texts(); len(got) != 1 || got[0] != "still captured" {
		t.Fatalf("got %q", got)
	}
}

// Sync is called on every run whether or not anything was ever printed, so it
// has to be cheap and it has to return.
func TestSyncWithoutAnInstallIsANoOp(t *testing.T) {
	if Installed() {
		t.Skip("a previous test left the capture installed")
	}
	done := make(chan struct{})
	go func() { Sync(time.Second); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Sync blocked with nothing installed")
	}
}
