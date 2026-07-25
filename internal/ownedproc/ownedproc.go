// Package ownedproc runs a child process with exactly one owner of its wait.
//
// A started process has two things a test needs: a way to signal it, and a way
// to learn that it exited. Splitting those between a body and a teardown path
// invites two waiters on one child, and concurrent waits can reap the wrong
// process once an id is reused (go.dev/issue/67642). One goroutine therefore
// owns the wait for a child's whole life; everything else observes completion
// through Done and signals through the retained process object.
//
// Test-support only. It lives under internal/ and is imported solely by _test.go
// files; nothing here is part of the product surface.
package ownedproc

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"time"
)

// ErrStopTimeout reports that a stopped child did not reach its wait owner's
// completion within the caller's bound.
var ErrStopTimeout = errors.New("owned child did not complete after being signalled")

// Child is a started process whose wait has exactly one owner.
type Child struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error

	// waitFn is the sole wait, counted so a control can assert it ran exactly
	// once for the child's lifetime.
	waitFn    func() error
	waitCalls atomic.Int64
	killCalls atomic.Int64
	stopped   atomic.Bool
}

// Start starts cmd and takes ownership of waiting on it. The caller owns
// teardown and must arrange for Stop to run.
func Start(cmd *exec.Cmd) (*Child, error) {
	if cmd == nil {
		return nil, fmt.Errorf("command is required")
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	child := &Child{cmd: cmd, done: make(chan struct{})}
	child.waitFn = func() error {
		child.waitCalls.Add(1)
		return cmd.Wait()
	}
	go func() {
		err := child.waitFn()
		// Store the result, then publish completion. Nothing may observe an exit
		// before Done is closed, or teardown could decide the child is still
		// running after it has already been reaped.
		child.err = err
		close(child.done)
	}()
	return child, nil
}

// Done is closed once the sole wait has completed and the result is stored.
func (c *Child) Done() <-chan struct{} { return c.done }

// Err returns the wait result. It is meaningful only after Done is closed.
func (c *Child) Err() error { return c.err }

// PID reports the child's process id, for diagnostics and for asking the
// operating system about it.
//
// The retained process object is deliberately not exported. Handing it out would
// hand out a second way to wait on this child, which is the one thing the
// ownership rule exists to prevent; callers signal through Signal instead.
func (c *Child) PID() int { return c.cmd.Process.Pid }

// Signal sends sig to the retained process.
func (c *Child) Signal(sig os.Signal) error { return c.cmd.Process.Signal(sig) }

// AwaitExit blocks until the wait owner completes, and reports whether it did so
// within the bound.
func (c *Child) AwaitExit(timeout time.Duration) (error, bool) {
	select {
	case <-c.done:
		return c.err, true
	case <-time.After(timeout):
		return nil, false
	}
}

// Stop is teardown. A child that has already exited is left alone; otherwise the
// retained process is killed and Stop joins the existing wait owner. It never
// waits on the process itself, so there is never a second waiter.
func (c *Child) Stop(timeout time.Duration) error {
	select {
	case <-c.done:
		return nil
	default:
	}
	c.stopped.Store(true)
	c.killCalls.Add(1)
	_ = c.cmd.Process.Kill()
	select {
	case <-c.done:
		return nil
	case <-time.After(timeout):
		return ErrStopTimeout
	}
}

// WaitCalls reports how many times the sole wait ran. Any value but one means
// the ownership rule was broken.
func (c *Child) WaitCalls() int64 { return c.waitCalls.Load() }

// KillCalls reports how many times teardown signalled the child.
func (c *Child) KillCalls() int64 { return c.killCalls.Load() }
