package crawler

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
)

// RequestBudget bounds provider request work across every Crawler it is injected
// into, so a run that fans out over several Crawlers stays inside one ceiling
// instead of multiplying an operator-set one per Crawler.
//
// It owns both ceilings deliberately. Config.Concurrency bounds how many prefix
// listings run at once and Config.RateLimit bounds how often requests start;
// carrying only the first would let an injected budget hold concurrency global
// while the request rate silently scaled with the number of Crawlers. A caller
// reading "shared request budget" would reasonably expect both, so both live
// here, behind an opaque surface that exposes neither the permit channel nor the
// rate limiter.
//
// When a budget is injected, Config.Concurrency and Config.RateLimit on the
// individual Crawler no longer apply: both ceilings are run-global and come from
// the budget. A Crawler with no injected budget keeps a private one built from
// its own Config, which is exactly today's per-Crawler behavior.
//
// A RequestBudget is safe for concurrent use and may be injected into any number
// of Crawlers.
type RequestBudget struct {
	// permits is a token bucket: a token in the channel is one available listing
	// slot. Acquire receives, release sends. Capacity is the run-global
	// concurrency ceiling and never changes, so the sum of in-flight listings
	// across every Crawler sharing this budget cannot exceed it.
	permits chan struct{}
	limiter *rate.Limiter
}

// NewRequestBudget returns a budget carrying a run-global listing-concurrency
// ceiling and request-rate ceiling.
//
// concurrency below 1 resolves to the package default. rateLimit of zero or less
// means unlimited, matching Config.RateLimit.
func NewRequestBudget(concurrency int, rateLimit float64) *RequestBudget {
	if concurrency < 1 {
		concurrency = DefaultConfig().Concurrency
	}
	b := &RequestBudget{permits: make(chan struct{}, concurrency)}
	for i := 0; i < concurrency; i++ {
		b.permits <- struct{}{}
	}
	if rateLimit > 0 {
		b.limiter = rate.NewLimiter(rate.Limit(rateLimit), 1)
	}
	return b
}

// Concurrency reports the run-global listing-concurrency ceiling.
func (b *RequestBudget) Concurrency() int { return cap(b.permits) }

// lease reserves one permit for a single Crawler and returns its handle.
//
// The reservation exists because a permit is held for an entire paginated prefix
// listing, not for one request. Without it, a Crawler that starts early can hold
// every permit for the length of its listings while another Crawler sharing the
// budget sits idle with work to do. One guaranteed permit each means no Crawler
// can be starved outright, while everything above the reservations stays
// work-conserving and globally capped.
//
// Leasing never blocks. When no permit is free to reserve the Crawler simply
// runs without one, drawing entirely from the shared pool. Callers that need the
// guarantee keep the number of leases at or below Concurrency, which is what
// makes the reservation always available in practice.
func (b *RequestBudget) lease() *budgetLease {
	l := &budgetLease{budget: b, reservation: make(chan struct{}, 1)}
	select {
	case <-b.permits:
		l.reservation <- struct{}{}
	default:
	}
	return l
}

// budgetLease is one Crawler's handle on a shared RequestBudget.
type budgetLease struct {
	budget *RequestBudget

	// reservation holds this lease's dedicated permit while it is available. It
	// is a channel rather than a flag so that a caller already waiting on the
	// shared pool can be woken by its own reservation coming free — waiting on
	// one source while the other frees up is a deadlock, since a lease that owns
	// every permit it is using has nothing left to release into the pool it is
	// parked on. It is empty when the permit is out on a listing, and stays empty
	// after retire.
	reservation chan struct{}

	mu sync.Mutex
	// retired routes a permit coming back from a listing to the shared pool
	// instead of to this lease. It is guarded together with the drain in retire
	// so a permit released concurrently cannot land in a reservation nobody will
	// ever claim again, which would strand run-global capacity.
	retired bool
}

// acquireListSlot blocks until this Crawler may begin one prefix listing and
// returns the release for that slot. The returned release must be called exactly
// once. An error means the context ended before a slot was available and no
// release is returned.
//
// Both permit sources are waited on together: the lease's own reservation and
// the shared pool.
func (l *budgetLease) acquireListSlot(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Prefer an immediately available permit over parking, so a lease with a free
	// reservation never waits behind the scheduler.
	select {
	case <-l.reservation:
		return l.releaseReservation, nil
	case <-l.budget.permits:
		return l.releaseShared, nil
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.reservation:
		return l.releaseReservation, nil
	case <-l.budget.permits:
		return l.releaseShared, nil
	}
}

// releaseReservation returns a permit that came from this lease's reservation.
func (l *budgetLease) releaseReservation() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.retired {
		// The lease finished while this listing was still running, so its permit
		// belongs to the run now rather than to this lease.
		l.budget.permits <- struct{}{}
		return
	}
	l.reservation <- struct{}{}
}

func (l *budgetLease) releaseShared() {
	l.budget.permits <- struct{}{}
}

// retire returns this lease's reserved permit to the shared pool. It is called
// when a Crawler has finished its work, so crawlers still working can draw on
// the full budget rather than only the permits their peers never reserved.
// Retiring twice is a no-op.
func (l *budgetLease) retire() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.retired {
		return
	}
	l.retired = true
	select {
	case <-l.reservation:
		// The permit is idle here; hand it straight back.
		l.budget.permits <- struct{}{}
	default:
		// The permit is out on a listing. releaseReservation forwards it to the
		// shared pool when that listing ends; returning one here as well would
		// invent capacity the budget never had.
	}
}

// waitForRequest blocks until the run-global request-rate ceiling allows one
// request to start. It returns immediately when the budget is unlimited.
func (l *budgetLease) waitForRequest(ctx context.Context) error {
	if l.budget.limiter == nil {
		return nil
	}
	return l.budget.limiter.Wait(ctx)
}
