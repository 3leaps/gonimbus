package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// withInterruptCancel bridges SIGINT/SIGTERM to cancellation of the returned
// context, so an interrupted command unwinds through its deferred cleanup — for a
// build, that is what releases and removes the index-set authority it holds.
//
// A second signal is not absorbed: the handler is uninstalled on the first one,
// restoring the platform default, so a repeated interrupt terminates at once.
// Cleanup is best effort — untrappable termination runs none of it, and lease
// detection and reclaim remain the contract for that residue.
//
// The returned stop function uninstalls the handler and cancels the context; it
// is safe to call more than once and must be called (defer) by the caller.
func withInterruptCancel(parent context.Context) (context.Context, func()) {
	ctx, stop, _ := newInterruptBridge(parent)
	return ctx, stop
}

// newInterruptBridge is withInterruptCancel plus a channel closed once the
// bridge's goroutine has finished — meaning the signal handler is uninstalled
// and the bridge can no longer intercept anything. Tests use it to wait for that
// state deterministically instead of sleeping; production callers do not need it.
func newInterruptBridge(parent context.Context) (context.Context, func(), <-chan struct{}) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		// Uninstall on whichever branch wins. Leaving the handler registered after
		// the goroutine exits would keep signals routed to a channel nobody reads:
		// during the unwind that follows a parent-context cancellation, an operator's
		// interrupt would be buffered or dropped instead of terminating the process.
		defer close(done)
		select {
		case <-ch:
			// Restore default handling before cancelling, so any caller that observes
			// the cancellation can rely on a repeated interrupt terminating at once.
			signal.Stop(ch)
			cancel()
		case <-ctx.Done():
			signal.Stop(ch)
		}
	}()
	return ctx, func() {
		signal.Stop(ch)
		cancel()
		<-done
	}, done
}
