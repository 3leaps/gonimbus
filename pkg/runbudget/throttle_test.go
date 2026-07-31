package runbudget

import (
	"context"
	"sync"
	"testing"
	"time"
)

// manualClock drives time by hand so the rate and throttle controls assert what
// the budget does rather than how fast the machine running them is.
type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*manualTimer
}

type manualTimer struct {
	at      time.Time
	fn      func()
	stopped bool
}

func (t *manualTimer) Stop() bool {
	t.stopped = true
	return true
}

func newManualClock() *manualClock {
	return &manualClock{now: time.Unix(1700000000, 0)}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) AfterFunc(d time.Duration, fn func()) stopper {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &manualTimer{at: c.now.Add(d), fn: fn}
	c.timers = append(c.timers, t)
	return t
}

// Advance moves the clock and fires every timer that came due, outside the
// clock lock so a callback may take the budget lock and schedule again.
func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var due []*manualTimer
	kept := c.timers[:0]
	for _, t := range c.timers {
		switch {
		case t.stopped:
		case !t.at.After(now):
			due = append(due, t)
		default:
			kept = append(kept, t)
		}
	}
	c.timers = kept
	c.mu.Unlock()

	for _, t := range due {
		t.fn()
	}
}

func mustThrottle(t *testing.T, b *Budget, d Domain, retryAfter time.Duration) {
	t.Helper()
	if err := b.Throttle(d, retryAfter); err != nil {
		t.Fatalf("Throttle(%s, %s): %v", d, retryAfter, err)
	}
}

func manualBudget(t *testing.T, limits Limits) (*Budget, *manualClock) {
	t.Helper()
	b := mustNew(t, limits)
	clock := newManualClock()
	b.now = clock.Now
	b.afterFunc = clock.AfterFunc
	// The budget captured the wall clock when it was built; reset per-domain
	// state so the first refill measures from the manual clock's epoch.
	b.state = make(map[Domain]*domainState)
	return b, clock
}

// awaitAdmission reports whether a pending acquire completed shortly after the
// clock moved. It is a bounded poll rather than a sleep so a failure says the
// budget did not admit, not that the test guessed a duration.
func awaitAdmission(t *testing.T, got <-chan *Lease) *Lease {
	t.Helper()
	select {
	case lease := <-got:
		return lease
	case <-time.After(2 * time.Second):
		t.Fatal("request was never admitted")
		return nil
	}
}

func TestRequestStartRateIsBounded(t *testing.T) {
	b, clock := manualBudget(t, Limits{RequestsPerSecond: 10, Burst: 2})

	// The burst is available immediately.
	first := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet})
	second := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet})
	defer first.Release()
	defer second.Release()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan *Lease, 1)
	go func() {
		lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainA}, Class: OpGet})
		if err == nil {
			got <- lease
		}
	}()
	waitForWaiters(t, b, 1)

	// Releasing does not help: the constraint is the start rate, not concurrency.
	select {
	case <-got:
		t.Fatal("a third request started immediately at a burst of two")
	case <-time.After(50 * time.Millisecond):
	}

	// One token accrues in a tenth of a second at ten per second.
	clock.Advance(100 * time.Millisecond)
	awaitAdmission(t, got).Release()
}

// A throttle observed by any consumer slows every consumer on that domain. N
// lanes must produce one coordinated backoff, not N independent discoveries of
// the same throttle.
func TestThrottleIsBroadcastAcrossConsumersOfADomain(t *testing.T) {
	b, clock := manualBudget(t, Limits{RequestsPerSecond: 100, Burst: 100})

	// Prime the domain so its state exists, then throttle it.
	primer := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet})
	primer.Release()

	if factor := b.Snapshot().Domains[0].RateFactor; factor != 1 {
		t.Fatalf("rate factor %v before any throttle, want 1", factor)
	}

	mustThrottle(t, b, domainA, 250*time.Millisecond)

	if factor := b.Snapshot().Domains[0].RateFactor; factor != 0.5 {
		t.Fatalf("rate factor %v after one throttle, want 0.5", factor)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const consumers = 4
	admitted := make(chan struct{}, consumers)
	for i := 0; i < consumers; i++ {
		go func() {
			lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainA}, Class: OpGet, Consumer: "lane"})
			if err == nil {
				admitted <- struct{}{}
				lease.Release()
			}
		}()
	}
	waitForWaiters(t, b, consumers)

	// Every consumer is paused, not just the one that saw the throttle.
	select {
	case <-admitted:
		t.Fatal("a consumer started a request while the domain was paused")
	case <-time.After(50 * time.Millisecond):
	}

	clock.Advance(300 * time.Millisecond)
	for i := 0; i < consumers; i++ {
		select {
		case <-admitted:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d consumers resumed after the pause elapsed", i, consumers)
		}
	}
}

func TestThrottleDoesNotCrossDomains(t *testing.T) {
	b, _ := manualBudget(t, Limits{RequestsPerSecond: 100, Burst: 100})

	// Both domains must be live before the throttle. Throttling a domain while
	// the other has no state yet would pass whatever the implementation does,
	// because there would be nothing for it to wrongly affect.
	for _, d := range []Domain{domainA, domainB} {
		lease := acquire(t, b, Request{Domains: []Domain{d}, Class: OpGet})
		lease.Release()
	}
	if live := len(b.Snapshot().Domains); live != 2 {
		t.Fatalf("expected both domains live before the throttle, got %d", live)
	}

	mustThrottle(t, b, domainA, time.Hour)

	for _, s := range b.Snapshot().Domains {
		switch {
		case s.Domain.Equal(domainA):
			if s.RateFactor != 0.5 {
				t.Fatalf("throttled domain A rate factor %v, want 0.5", s.RateFactor)
			}
		case s.Domain.Equal(domainB):
			if s.RateFactor != 1 {
				t.Fatalf("domain B rate factor %v after throttling domain A, want 1", s.RateFactor)
			}
		}
	}

	// B shares no quota with A, so it must still admit while A is paused.
	lease := acquire(t, b, Request{Domains: []Domain{domainB}, Class: OpGet})
	lease.Release()
}

// Backoff is multiplicative and recovery additive, so a domain that is
// genuinely saturated does not oscillate straight back into throttling.
func TestThrottleBacksOffFastAndRecoversSlowly(t *testing.T) {
	b, clock := manualBudget(t, Limits{RequestsPerSecond: 100, Burst: 100})

	lease := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet})
	lease.Release()

	for i := 0; i < 3; i++ {
		mustThrottle(t, b, domainA, 0)
	}
	if got := b.Snapshot().Domains[0].RateFactor; got != 0.125 {
		t.Fatalf("rate factor %v after three throttles, want 0.125", got)
	}

	// Recovery accrues with time, not with a single successful request.
	clock.Advance(time.Second)
	next := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet})
	next.Release()

	recovered := b.Snapshot().Domains[0].RateFactor
	if recovered <= 0.125 {
		t.Fatalf("rate factor %v did not recover after a second", recovered)
	}
	if recovered >= 1 {
		t.Fatalf("rate factor %v recovered to full in one second; recovery is not gradual", recovered)
	}
}

// A throttled domain must keep making progress rather than collapsing to a
// stall only time can lift.
func TestThrottleFloorsRatherThanStalls(t *testing.T) {
	b, _ := manualBudget(t, Limits{RequestsPerSecond: 100, Burst: 100})

	lease := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet})
	lease.Release()

	for i := 0; i < 50; i++ {
		mustThrottle(t, b, domainA, 0)
	}
	if got := b.Snapshot().Domains[0].RateFactor; got < minRateFactor {
		t.Fatalf("rate factor %v fell below the floor %v", got, minRateFactor)
	}
}
