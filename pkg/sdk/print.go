package sdk

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Log fields that describe where a line came from. A worker captures what a
// flow prints on os.Stdout and os.Stderr and records it under these, and the
// helpers below stamp the same fields, so the console renders a printed line
// the same way whichever route it took.
const (
	// LogFieldStream carries StreamStdout or StreamStderr.
	LogFieldStream = "stream"
	// LogFieldAmbiguous marks a captured line that could not be traced to a
	// single run — several were executing in the worker process at once — and
	// was therefore recorded against all of them. See internal/stdcapture.
	LogFieldAmbiguous = "ambiguous"
)

// Stream values for LogFieldStream.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
)

// maxPrintLine bounds a line written to a Context writer that never sees a
// newline, so a flow streaming a large body into one cannot grow the buffer
// without limit. It matches what the store truncates a message to.
const maxPrintLine = 8000

// ---------------------------------------------------------------- printing --
//
// A worker captures fmt.Println out of a flow on its own, so these exist for
// the times that is not enough: to be sure of the level a line lands at, and to
// have it attributed to this run even when the worker is executing several
// others in the same process. See internal/stdcapture for why that distinction
// exists at all.

// Print records its operands in the run log at INFO, formatting as fmt.Print.
func (c *Context) Print(v ...any) { c.printLines("INFO", StreamStdout, fmt.Sprint(v...)) }

// Printf records a formatted line in the run log at INFO.
func (c *Context) Printf(format string, v ...any) {
	c.printLines("INFO", StreamStdout, fmt.Sprintf(format, v...))
}

// Println records its operands in the run log at INFO, formatting as
// fmt.Println.
func (c *Context) Println(v ...any) { c.printLines("INFO", StreamStdout, fmt.Sprintln(v...)) }

// printLines records s as one log entry per line, so a multi-line print reads
// on the run page the way it would on a terminal. Blank lines are dropped:
// they are worth a row on neither.
func (c *Context) printLines(level, stream, s string) {
	at := time.Now().UTC()
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}
		c.rt.Log(LogEntry{
			Level: level, Message: line, At: at,
			Fields: map[string]any{LogFieldStream: stream},
		})
	}
}

// ----------------------------------------------------------------- writers --

// Stdout returns a writer whose every line becomes an INFO entry in the run
// log. It is what to hand a library that writes somewhere rather than returning
// something — a command's output, a third-party logger:
//
//	cmd := exec.CommandContext(c, "terraform", "apply", "-auto-approve")
//	cmd.Stdout, cmd.Stderr = c.Stdout(), c.Stderr()
//	err := cmd.Run()
//
// Close it to record a trailing line that never got its newline. exec.Cmd does
// not close the writers it is given, so a command whose last line is unfinished
// needs a defer.
func (c *Context) Stdout() *LogWriter { return &LogWriter{c: c, level: "INFO", stream: StreamStdout} }

// Stderr is Stdout at ERROR level, tagged as the error stream.
func (c *Context) Stderr() *LogWriter { return &LogWriter{c: c, level: "ERROR", stream: StreamStderr} }

// LogWriter turns each line written to it into a run log entry. It is safe for
// concurrent use.
type LogWriter struct {
	c      *Context
	level  string
	stream string

	mu  sync.Mutex
	buf []byte
}

// Write records every complete line in p and holds any remainder for the next
// call. It never reports an error: a log line the store could not take is not a
// reason to fail the command that produced it.
func (w *LogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		w.emit(string(w.buf[:i]))
		w.buf = w.buf[i+1:]
	}
	// A writer that is handed a long stream with no newline in it must not grow
	// until the run ends. What overflows is broken up rather than truncated, so
	// the tail of a long line still reaches the log.
	for len(w.buf) >= maxPrintLine {
		n := runeCut(w.buf, maxPrintLine)
		w.emit(string(w.buf[:n]))
		w.buf = w.buf[n:]
	}
	return len(p), nil
}

// runeCut returns how much of b to take for a piece of at most n bytes, without
// splitting a rune. A run of bytes with no boundary to fall back on is cut at n
// anyway: a mangled character is better than a loop that never advances.
func runeCut(b []byte, n int) int {
	if len(b) <= n {
		return len(b)
	}
	for i := n; i > 0; i-- {
		if utf8.RuneStart(b[i]) {
			return i
		}
	}
	return n
}

// Close records whatever was written without a closing newline.
func (w *LogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.buf) > 0 {
		w.emit(string(w.buf))
		w.buf = w.buf[:0]
	}
	return nil
}

func (w *LogWriter) emit(line string) {
	line = strings.TrimRight(line, "\r")
	if line == "" {
		return
	}
	w.c.rt.Log(LogEntry{
		Level: w.level, Message: line, At: time.Now().UTC(),
		Fields: map[string]any{LogFieldStream: w.stream},
	})
}
