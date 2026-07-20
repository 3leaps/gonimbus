package reflow

import (
	"context"
	"sync"
	"time"
)

// memoryLedger is the run-level byte-admission ledger shared by both
// execution paths: every copy reserves its bounded buffer bytes before any
// limiter token or provider action, and the total outstanding reservation
// never exceeds the effective memory budget. Waiters queue FIFO — the head of
// the line is granted strictly first, so a large waiter cannot be starved by
// a stream of smaller requests arriving behind it. Waits are context-
// cancellable and counted, with peak reservation and total wait time exposed
// as pressure telemetry so a memory-bound run is distinguishable from a
// starved producer.
type memoryLedger struct {
	mu        sync.Mutex
	capacity  int64
	reserved  int64
	peak      int64
	waits     int64
	waitNanos int64
	queue     []*ledgerWaiter
	clock     func() time.Time
}

type ledgerWaiter struct {
	need    int64
	ready   chan struct{}
	granted bool
}

func newMemoryLedger(capacityBytes int64) *memoryLedger {
	if capacityBytes < 1 {
		capacityBytes = 1
	}
	return &memoryLedger{capacity: capacityBytes, clock: time.Now}
}

// copyReservationBytes is the allocator-identical reservation arithmetic: a
// known positive size reserves min(size, retryBufferCap) — exactly what the
// retry body may hold in memory (larger bodies spool) — and any non-positive
// size reserves the conservative maximum. The transfer contract treats a
// non-positive expected size as optional/absent (omitted JSON sizes decode to
// zero), so zero must never be inferred as a known empty object: a later Get
// can still produce a real body that the allocator buffers.
func copyReservationBytes(sourceSize, retryBufferCap int64) int64 {
	if sourceSize <= 0 || sourceSize > retryBufferCap {
		return retryBufferCap
	}
	return sourceSize
}

// Reserve blocks until bytes can be admitted under the capacity, the context
// is cancelled, or the request is granted. The returned release function is
// exactly-once safe. A request above capacity is clamped to capacity so it
// can always eventually be granted.
func (l *memoryLedger) Reserve(ctx context.Context, bytes int64) (func(), error) {
	if bytes < 0 {
		bytes = 0
	}
	if bytes > l.capacity {
		bytes = l.capacity
	}

	l.mu.Lock()
	if len(l.queue) == 0 && l.reserved+bytes <= l.capacity {
		l.grantLocked(bytes)
		l.mu.Unlock()
		return l.releaseOnce(bytes), nil
	}
	w := &ledgerWaiter{need: bytes, ready: make(chan struct{})}
	l.queue = append(l.queue, w)
	l.waits++
	start := l.clock()
	l.mu.Unlock()

	select {
	case <-w.ready:
		l.mu.Lock()
		l.waitNanos += l.clock().Sub(start).Nanoseconds()
		l.mu.Unlock()
		return l.releaseOnce(bytes), nil
	case <-ctx.Done():
		l.mu.Lock()
		l.waitNanos += l.clock().Sub(start).Nanoseconds()
		granted := w.granted
		if !granted {
			for i, queued := range l.queue {
				if queued == w {
					l.queue = append(l.queue[:i], l.queue[i+1:]...)
					break
				}
			}
			// Removing a queued waiter can make followers admissible — a
			// cancelled 2MiB head must not leave a now-fitting 1MiB follower
			// asleep until an unrelated release.
			l.drainLocked()
		}
		l.mu.Unlock()
		if granted {
			// Lost the race: the grant landed as the context fired — undo it.
			l.releaseOnce(bytes)()
		}
		return nil, ctx.Err()
	}
}

// grantLocked admits bytes and tracks the peak. Callers must hold l.mu.
func (l *memoryLedger) grantLocked(bytes int64) {
	l.reserved += bytes
	if l.reserved > l.peak {
		l.peak = l.reserved
	}
}

// drainLocked grants queued waiters strictly head-first while they fit.
// Callers must hold l.mu.
func (l *memoryLedger) drainLocked() {
	for len(l.queue) > 0 {
		head := l.queue[0]
		if l.reserved+head.need > l.capacity {
			return
		}
		l.grantLocked(head.need)
		head.granted = true
		close(head.ready)
		l.queue = l.queue[1:]
	}
}

func (l *memoryLedger) releaseOnce(bytes int64) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			l.reserved -= bytes
			l.drainLocked()
			l.mu.Unlock()
		})
	}
}

type memoryLedgerStats struct {
	PeakReservedBytes int64
	Waits             int64
	WaitTotal         time.Duration
}

func (l *memoryLedger) Stats() memoryLedgerStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return memoryLedgerStats{
		PeakReservedBytes: l.peak,
		Waits:             l.waits,
		WaitTotal:         time.Duration(l.waitNanos),
	}
}
