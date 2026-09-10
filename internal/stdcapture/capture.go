// Package stdcapture routes what a flow prints on os.Stdout and os.Stderr into
// that run's own log, so `fmt.Println` in a flow lands on the run page beside
// the lines the flow wrote with c.Info.
//
// Go offers no hook on a write to os.Stdout: it is a *os.File, not an
// interface, so the only way to see what user code prints is to replace it with
// a pipe and read the other end. That erases the writing goroutine's identity
// along the way, which is why attribution here is by which runs are executing
// in this process rather than by who wrote the line:
//
//   - no run bound — the line is only mirrored to the process's real stdout,
//     exactly as it was before any of this existed;
//   - one run bound — the line is that run's, and is recorded as such;
//   - several bound — the line goes to all of them marked Ambiguous, because a
//     line an operator is hunting for is worse lost than shown twice.
//
// A run that owns its process never sees an ambiguous line: exec mode
// "process" and "kubernetes" give each run one, and so does an inline worker at
// concurrency 1.
//
// Every captured line is still written to the real stdout as well. A worker's
// container log is what an operator reaches for when the database is the thing
// that is broken, and capture must not be the reason it went quiet.
package stdcapture

import (
	"bufio"
	"errors"
	"io"
	stdlog "log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// Stream names carried on a captured Line.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
)

// maxLine bounds one captured line, matching what the store truncates to. A
// flow that writes a megabyte without a newline is split rather than buffered.
const maxLine = 8000

// Line is one line of output a flow printed.
type Line struct {
	Stream string // StreamStdout or StreamStderr
	Text   string // without its trailing newline
	At     time.Time
	// Ambiguous reports that more than one run was executing in this process
	// when the line arrived, so it could not be traced to one of them and was
	// delivered to every one of them.
	Ambiguous bool
}

// Sink receives the lines of one bound run. It is called on the reader
// goroutine, in order, and must not write to os.Stdout itself.
type Sink func(Line)

var (
	mu sync.RWMutex
	// sinks is keyed by run id so a release can find its own; fanout is the
	// snapshot delivery walks, rebuilt on every bind rather than per line.
	sinks  = map[string]Sink{}
	fanout []Sink
)

// Bind routes captured output to s for as long as the returned release has not
// been called. It is safe — and a no-op beyond the bookkeeping — when nothing
// has been installed.
func Bind(runID string, s Sink) (release func()) {
	if s == nil {
		return func() {}
	}
	mu.Lock()
	sinks[runID] = s
	rebuild()
	mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			mu.Lock()
			delete(sinks, runID)
			rebuild()
			mu.Unlock()
		})
	}
}

// rebuild refreshes the delivery snapshot. The caller holds mu for writing.
func rebuild() {
	fanout = make([]Sink, 0, len(sinks))
	for _, s := range sinks {
		fanout = append(fanout, s)
	}
}

func deliver(stream, text string, at time.Time) {
	mu.RLock()
	fs := fanout
	mu.RUnlock()
	if len(fs) == 0 {
		return
	}
	ln := Line{Stream: stream, Text: text, At: at, Ambiguous: len(fs) > 1}
	for _, s := range fs {
		s(ln)
	}
}

// ------------------------------------------------------------- install ------

type capture struct {
	outR, outW *os.File
	errR, errW *os.File
	origOut    *os.File
	origErr    *os.File
	origLog    io.Writer
	pumps      sync.WaitGroup
}

var (
	instMu sync.Mutex
	inst   *capture
	refs   int
)

// Install replaces os.Stdout and os.Stderr with pipes this package reads.
// Nested calls share one installation; the last release restores the originals.
//
// Call it after the process has built its own logger, so that the worker's
// structured log keeps the real stdout and is not fed back through the pipe.
func Install() (release func(), err error) {
	instMu.Lock()
	defer instMu.Unlock()

	if inst == nil {
		c, err := start()
		if err != nil {
			return func() {}, err
		}
		inst = c
	}
	refs++
	c := inst

	var once sync.Once
	return func() { once.Do(func() { stop(c) }) }, nil
}

// Installed reports whether os.Stdout is currently being captured.
func Installed() bool {
	instMu.Lock()
	defer instMu.Unlock()
	return inst != nil
}

func start() (*capture, error) {
	outR, outW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		outR.Close()
		outW.Close()
		return nil, err
	}

	c := &capture{
		outR: outR, outW: outW, errR: errR, errW: errW,
		origOut: os.Stdout, origErr: os.Stderr, origLog: stdlog.Writer(),
	}
	os.Stdout, os.Stderr = outW, errW
	// The standard log package resolved os.Stderr once, at its own
	// initialisation. Re-point it, or a flow reaching for log.Println — the
	// most likely thing after fmt.Println — would be the one print that is not
	// captured.
	stdlog.SetOutput(errW)

	c.pumps.Add(2)
	go func() { defer c.pumps.Done(); pump(outR, c.origOut, StreamStdout) }()
	go func() { defer c.pumps.Done(); pump(errR, c.origErr, StreamStderr) }()
	return c, nil
}

func stop(c *capture) {
	instMu.Lock()
	if inst != c {
		instMu.Unlock()
		return
	}
	refs--
	if refs > 0 {
		instMu.Unlock()
		return
	}
	os.Stdout, os.Stderr = c.origOut, c.origErr
	stdlog.SetOutput(c.origLog)
	inst = nil
	instMu.Unlock()

	// Closing the write ends ends both pumps once they have drained whatever
	// was still in the pipe. The wait is bounded: a sink still blocked on a
	// store that has stopped answering must not be the reason a process cannot
	// exit, and the read ends are then left to the process's own teardown
	// rather than closed under a goroutine still reading them.
	c.outW.Close()
	c.errW.Close()
	drained := make(chan struct{})
	go func() { c.pumps.Wait(); close(drained) }()
	select {
	case <-drained:
		c.outR.Close()
		c.errR.Close()
	case <-time.After(drainTimeout):
	}
}

// drainTimeout bounds how long a release waits for the pumps to finish.
const drainTimeout = 5 * time.Second

// ---------------------------------------------------------------- pump ------

// pump reads one pipe line by line until its write end is closed, mirroring
// what it reads to the file that fd used to be.
func pump(r *os.File, mirror *os.File, stream string) {
	br := bufio.NewReaderSize(r, 64*1024)
	var buf []byte
	for {
		frag, err := br.ReadSlice('\n')
		buf = append(buf, frag...)
		switch {
		case errors.Is(err, bufio.ErrBufferFull):
			// A line longer than the reader's buffer. Keep accumulating until
			// it is longer than any line worth keeping whole, then break it.
			if len(buf) < maxLine {
				continue
			}
		case err != nil:
			if len(buf) > 0 {
				handle(buf, mirror, stream)
			}
			return
		}
		handle(buf, mirror, stream)
		buf = buf[:0]
	}
}

func handle(raw []byte, mirror *os.File, stream string) {
	text := extractBarriers(string(raw))
	if mirror != nil && text != "" {
		_, _ = io.WriteString(mirror, text)
	}
	// A blank line is worth keeping on a terminal and not worth a database row;
	// a line that was nothing but a barrier is worth neither.
	line := strings.TrimRight(text, "\r\n")
	if line == "" {
		return
	}
	at := time.Now().UTC()
	// The mirror above got the line whole. What is recorded is broken up
	// instead of truncated, because the store would otherwise drop the tail of
	// a long line on the floor and say nothing about it.
	for len(line) > 0 {
		n := runeCut(line, maxLine)
		deliver(stream, line[:n], at)
		line = line[n:]
	}
}

// runeCut returns how much of s to take for a piece of at most n bytes, without
// splitting a rune. A string with no boundary to fall back on is cut at n
// anyway: a mangled character is better than a loop that never advances.
func runeCut(s string, n int) int {
	if len(s) <= n {
		return len(s)
	}
	for i := n; i > 0; i-- {
		if utf8.RuneStart(s[i]) {
			return i
		}
	}
	return n
}

// -------------------------------------------------------------- barrier -----

// The pipe is read by a goroutine of its own, so a line printed by the last
// statement of a flow is still in flight when that flow returns. Sync writes a
// marker and waits for the reader to reach it, which is what lets the engine
// unbind a finished run without dropping its final line — or, worse, handing it
// to whichever run is still executing.
//
// The marker is short enough to be written to a pipe atomically, and carries a
// NUL so it cannot be confused with anything a flow would print.
const (
	barrierPrefix = "\x00pfsync:"
	barrierSuffix = "\x00\n"
)

var (
	barrierMu sync.Mutex
	barriers  = map[string]chan struct{}{}
	barrierNo atomic.Uint64
)

// Sync blocks until everything written to os.Stdout and os.Stderr before the
// call has been delivered to the bound sinks, or until timeout elapses. It is a
// no-op when nothing is installed.
func Sync(timeout time.Duration) {
	instMu.Lock()
	c := inst
	instMu.Unlock()
	if c == nil {
		return
	}
	deadline := time.Now().Add(timeout)
	waitBarrier(c.outW, deadline)
	waitBarrier(c.errW, deadline)
}

func waitBarrier(w *os.File, deadline time.Time) {
	id := strconv.FormatUint(barrierNo.Add(1), 36)
	ch := make(chan struct{})

	barrierMu.Lock()
	barriers[id] = ch
	barrierMu.Unlock()
	defer func() {
		barrierMu.Lock()
		delete(barriers, id)
		barrierMu.Unlock()
	}()

	if _, err := io.WriteString(w, barrierPrefix+id+barrierSuffix); err != nil {
		return
	}
	t := time.NewTimer(time.Until(deadline))
	defer t.Stop()
	select {
	case <-ch:
	case <-t.C:
	}
}

// extractBarriers removes any markers from s, waking whoever is waiting on
// them. A marker can land in the middle of a line when a flow printed without a
// trailing newline, so this searches rather than compares.
func extractBarriers(s string) string {
	for {
		i := strings.Index(s, barrierPrefix)
		if i < 0 {
			return s
		}
		rest := s[i+len(barrierPrefix):]
		j := strings.Index(rest, barrierSuffix)
		if j < 0 {
			return s // truncated: leave it rather than eat a real line
		}
		signalBarrier(rest[:j])
		s = s[:i] + rest[j+len(barrierSuffix):]
	}
}

func signalBarrier(id string) {
	barrierMu.Lock()
	ch, ok := barriers[id]
	if ok {
		delete(barriers, id)
	}
	barrierMu.Unlock()
	if ok {
		close(ch)
	}
}
