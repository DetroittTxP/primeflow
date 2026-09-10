package sdk_test

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/DetroittTxP/primeflow/pkg/sdk"
)

func printCtx(f *fakeRuntime) *sdk.Context {
	return sdk.NewContext(context.Background(), f, sdk.RunInfo{RunID: "run-1"})
}

func TestPrintHelpersLogAtInfoTaggedAsStdout(t *testing.T) {
	f := newFake()
	c := printCtx(f)

	c.Print("one")
	c.Printf("two %d", 2)
	c.Println("three")

	if len(f.logs) != 3 {
		t.Fatalf("want 3 entries, got %d: %+v", len(f.logs), f.logs)
	}
	for i, want := range []string{"one", "two 2", "three"} {
		e := f.logs[i]
		if e.Message != want {
			t.Errorf("entry %d: message %q, want %q", i, e.Message, want)
		}
		if e.Level != "INFO" {
			t.Errorf("entry %d: level %q, want INFO", i, e.Level)
		}
		if e.Fields[sdk.LogFieldStream] != sdk.StreamStdout {
			t.Errorf("entry %d: stream %v, want %q", i, e.Fields[sdk.LogFieldStream], sdk.StreamStdout)
		}
		if e.At.IsZero() {
			t.Errorf("entry %d: no timestamp", i)
		}
	}
}

// A print spanning several lines reads on the run page the way it would on a
// terminal, rather than as one entry with newlines buried in it.
func TestPrintSplitsLinesAndDropsBlanks(t *testing.T) {
	f := newFake()
	c := printCtx(f)

	c.Printf("first\n\nsecond\r\n")
	c.Println()

	var got []string
	for _, e := range f.logs {
		got = append(got, e.Message)
	}
	if strings.Join(got, "|") != "first|second" {
		t.Fatalf("got %q, want [first second]", got)
	}
}

func TestLogWriterEmitsOneEntryPerLine(t *testing.T) {
	f := newFake()
	w := printCtx(f).Stdout()

	fmt.Fprintf(w, "alpha\nbra")
	if len(f.logs) != 1 || f.logs[0].Message != "alpha" {
		t.Fatalf("want the finished line only, got %+v", f.logs)
	}
	fmt.Fprintf(w, "vo\n")
	if len(f.logs) != 2 || f.logs[1].Message != "bravo" {
		t.Fatalf("want a line joined across writes, got %+v", f.logs)
	}
}

// exec.Cmd never closes the writers it is handed, so the only way a trailing
// line without a newline is recorded is a Close the flow author writes.
func TestLogWriterCloseFlushesTheUnfinishedLine(t *testing.T) {
	f := newFake()
	w := printCtx(f).Stderr()

	fmt.Fprint(w, "no newline here")
	if len(f.logs) != 0 {
		t.Fatalf("an unfinished line must wait, got %+v", f.logs)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(f.logs) != 1 || f.logs[0].Message != "no newline here" {
		t.Fatalf("want the remainder on Close, got %+v", f.logs)
	}
	if f.logs[0].Level != "ERROR" || f.logs[0].Fields[sdk.LogFieldStream] != sdk.StreamStderr {
		t.Fatalf("Stderr must record at ERROR on the stderr stream, got %+v", f.logs[0])
	}
}

// A writer handed a stream with no newline in it must not grow until the run
// ends.
func TestLogWriterBreaksAnUnboundedLine(t *testing.T) {
	f := newFake()
	w := printCtx(f).Stdout()

	fmt.Fprint(w, strings.Repeat("x", 20000))
	if len(f.logs) != 2 {
		t.Fatalf("want the overflow broken into 2 entries, got %d", len(f.logs))
	}
	for i, e := range f.logs {
		if len(e.Message) != 8000 {
			t.Errorf("entry %d is %d bytes, want 8000", i, len(e.Message))
		}
	}
	// The 4000-byte remainder is still waiting for its newline.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if len(f.logs) != 3 || len(f.logs[2].Message) != 4000 {
		t.Fatalf("want the remainder on Close, got %d entries", len(f.logs))
	}
}

// A line broken up by the overflow rule must not be cut through the middle of a
// character.
func TestLogWriterBreaksOnRuneBoundaries(t *testing.T) {
	f := newFake()
	w := printCtx(f).Stdout()

	// 3 bytes per rune: 8000 is not a multiple of 3, so a naive cut would split
	// the rune that straddles it.
	fmt.Fprintln(w, strings.Repeat("ก", 4000))
	for i, e := range f.logs {
		if !utf8.ValidString(e.Message) {
			t.Fatalf("entry %d was cut through a rune: %q", i, e.Message)
		}
	}
	if got := strings.Join(messages(f.logs), ""); got != strings.Repeat("ก", 4000) {
		t.Errorf("the pieces do not reassemble to the original")
	}
}

func messages(es []sdk.LogEntry) []string {
	out := make([]string, 0, len(es))
	for _, e := range es {
		out = append(out, e.Message)
	}
	return out
}

// The writers are what a third-party logger is pointed at, so they have to
// behave under the interface it holds them by.
func TestLogWriterDrivesTheStandardLogger(t *testing.T) {
	f := newFake()
	w := printCtx(f).Stdout()

	log.New(w, "", 0).Println("from a library")
	if len(f.logs) != 1 || f.logs[0].Message != "from a library" {
		t.Fatalf("got %+v", f.logs)
	}
}
