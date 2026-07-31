// Package producer runs partition-plan lanes that enumerate source objects and
// stream them to downstream processing.
//
// Experimental: this package will churn while lane execution is built out. The
// CLI is an adapter over it, per ADR-0006 — the package owns execution and the
// command owns configuration.
//
// The defining constraint is that enumeration must overlap processing. A
// producer that materializes its input before work starts reintroduces exactly
// the serial ceiling partitioning exists to remove, and does so invisibly: the
// run still completes, just never faster than one enumerator.
//
// Lane count is topology, not a resource budget. The provider load a run may
// place on a source is governed by a separate shared admission contract, so
// adding lanes redistributes work rather than multiplying concurrency against
// the source.
package producer

import (
	"bufio"
	"context"
	"io"
	"strings"
)

// Line is one enumerated input line together with its position in the stream.
// Ordinal is 1-based and assigned in read order so a diagnostic can name the
// offending input without the reader having to buffer what it already emitted.
type Line struct {
	Ordinal int
	Text    string
}

// StreamLines reads r and emits non-empty trimmed lines incrementally.
//
// This replaces the full-materialization read: the previous shape accumulated
// every line into a slice before any worker started, so a large or slow input
// delayed all processing until the last line arrived, and an unbounded input
// could not be processed at all. Here the first line is deliverable as soon as
// it is read.
//
// Cancellation is observed at send points. It cannot interrupt a blocking
// io.Reader.Read: if the source wedges, this goroutine stays parked inside
// Read until that read returns, whatever the context says. Bounding a stuck
// source is the provider responsibility (read deadlines), not this seam.
//
// The returned channel is closed when the input is exhausted or the context is
// cancelled. Read the error channel after the line channel closes; it yields at
// most one error.
func StreamLines(ctx context.Context, r io.Reader, buffer int) (<-chan Line, <-chan error) {
	if buffer < 0 {
		buffer = 0
	}
	lines := make(chan Line, buffer)
	errc := make(chan error, 1)

	go func() {
		defer close(lines)
		defer close(errc)

		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, scanLineInitial), scanLineMax)

		ordinal := 0
		for scanner.Scan() {
			text := strings.TrimSpace(scanner.Text())
			if text == "" {
				continue
			}
			ordinal++
			select {
			case lines <- Line{Ordinal: ordinal, Text: text}:
			case <-ctx.Done():
				// Cancellation is not a scan error: report the context cause so a
				// caller can distinguish an operator stop from malformed input.
				errc <- ctx.Err()
				return
			}
		}
		if err := scanner.Err(); err != nil {
			errc <- err
		}
	}()

	return lines, errc
}

const (
	scanLineInitial = 64 * 1024
	scanLineMax     = 1024 * 1024
)
