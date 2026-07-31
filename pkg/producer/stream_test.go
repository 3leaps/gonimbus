package producer

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// blockingReader yields prefix, then blocks until release is closed, then EOF.
// It models a slow or unbounded input: stdin still being written, or a large
// listing still arriving.
type blockingReader struct {
	prefix  string
	release <-chan struct{}
	done    bool
}

func (r *blockingReader) Read(p []byte) (int, error) {
	if r.prefix != "" {
		n := copy(p, r.prefix)
		r.prefix = r.prefix[n:]
		return n, nil
	}
	if !r.done {
		<-r.release
		r.done = true
	}
	return 0, io.EOF
}

// The overlap control. Under a full-materialization read — accumulating every
// line before any worker starts — the first line cannot be observed until the
// input is exhausted, so this test would block until the deadline. It must pass
// without the reader ever reaching EOF.
func TestStreamLinesDeliversBeforeInputIsExhausted(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	r := &blockingReader{prefix: "first\nsecond\n", release: release}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lines, _ := StreamLines(ctx, r, 0)

	for _, want := range []string{"first", "second"} {
		select {
		case got, ok := <-lines:
			if !ok {
				t.Fatalf("stream closed before delivering %q", want)
			}
			if got.Text != want {
				t.Fatalf("got %q, want %q", got.Text, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("no line delivered while the input was still open — "+
				"the producer is materializing its input before emitting (%q never arrived)", want)
		}
	}
}

func TestStreamLinesSkipsBlankAndNumbersInReadOrder(t *testing.T) {
	in := strings.NewReader("alpha\n\n   \nbeta\n\tgamma\t\n")

	lines, errc := StreamLines(context.Background(), in, 8)

	var got []Line
	for line := range lines {
		got = append(got, line)
	}
	if err := <-errc; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []Line{
		{Ordinal: 1, Text: "alpha"},
		{Ordinal: 2, Text: "beta"},
		{Ordinal: 3, Text: "gamma"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d lines (%+v), want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("line %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

// Cancellation is observed where the producer parks on send. The reader here
// never blocks, and nothing consumes, so the goroutine is deterministically
// parked on the first send when cancel lands — no race between the send path
// and the read path.
//
// Note the limitation this test deliberately does NOT assert: context cannot
// interrupt a blocking io.Reader.Read. If a provider read wedges, this stream
// stays parked inside Read until that read returns, whatever the context says.
// Cancelling a stuck source is the provider's responsibility (read deadlines),
// not something this seam can provide.
func TestStreamLinesCancellationClosesAndReportsContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	lines, errc := StreamLines(ctx, strings.NewReader("a\nb\nc\n"), 0)

	cancel()

	select {
	case <-drain(lines):
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not close after cancellation")
	}

	err := <-errc
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got error %v, want context.Canceled — cancellation must be "+
			"distinguishable from malformed input", err)
	}
}

func TestStreamLinesReportsScanError(t *testing.T) {
	// A single token longer than the scanner's max buffer is a scan error, not
	// EOF; it must surface rather than truncate the stream silently.
	huge := strings.Repeat("x", scanLineMax+1)
	lines, errc := StreamLines(context.Background(), strings.NewReader(huge), 1)

	for range lines {
	}
	if err := <-errc; err == nil {
		t.Fatal("oversized line did not report a scan error")
	}
}

// With a zero-capacity channel the producer cannot run ahead of the consumer,
// so retention is bounded by construction rather than by convention.
func TestStreamLinesRetentionIsBoundedByChannelCapacity(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	r := &blockingReader{prefix: "a\nb\nc\n", release: release}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lines, _ := StreamLines(ctx, r, 0)

	// Take one line, then confirm the producer parked instead of draining the
	// rest into memory.
	select {
	case got := <-lines:
		if got.Text != "a" {
			t.Fatalf("got %q, want %q", got.Text, "a")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first line never arrived")
	}

	// "b" is in flight on the unbuffered send; "c" must not have been read yet.
	// Draining exactly one more proves progress without unbounded read-ahead.
	select {
	case got := <-lines:
		if got.Text != "b" {
			t.Fatalf("got %q, want %q", got.Text, "b")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second line never arrived")
	}
}

func drain(lines <-chan Line) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range lines {
		}
	}()
	return done
}
