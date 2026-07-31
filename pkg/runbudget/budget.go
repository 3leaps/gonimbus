package runbudget

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// Limits is the ceiling a run places on its own work.
//
// The dimensions are deliberately not all the same scope, because the resources
// they describe are not the same kind of resource:
//
//   - RequestsPerSecond, Burst, InFlight and OpenBodies are provider-scoped, and
//     apply per quota domain. They describe load leaving the process, which is
//     metered by whatever the provider meters.
//   - MemoryBytes is run-global. It describes buffers held inside this process,
//     which one heap holds regardless of how many providers a run talks to. A
//     per-domain memory ceiling would not be a ceiling at all: two disjoint
//     domains would each be entitled to the whole limit, and a request spanning
//     several domains would be charged for one buffer in each of their ledgers.
//
// A zero value in any dimension means that dimension is unbounded. This is
// deliberate — a caller bounding only memory should not have to enumerate every
// operation class — but an all-zero Limits bounds nothing, which New refuses so
// that "no budget" is never reached by omission.
type Limits struct {
	// RequestsPerSecond bounds the rate at which requests may start against one
	// quota domain.
	RequestsPerSecond float64
	// Burst is how many request starts may bunch together on one domain. It
	// defaults to one second's worth of rate when a rate is set and this is
	// zero, because a bucket with no burst admits nothing.
	Burst int
	// InFlight bounds concurrent requests per operation class, per domain.
	// Classes absent from the map are unbounded; an unknown class is refused by
	// New.
	InFlight map[OpClass]int
	// OpenBodies bounds response bodies held open at once against one domain. A
	// body is charged until it is closed, not until the request completes,
	// because that is how long the provider connection is actually held.
	OpenBodies int
	// MemoryBytes bounds buffer reservations held concurrently across the whole
	// run. Unlike the others this is not per domain — see the type comment.
	MemoryBytes int64
}

func (l Limits) bounded() bool {
	if l.RequestsPerSecond > 0 || l.OpenBodies > 0 || l.MemoryBytes > 0 {
		return true
	}
	for _, n := range l.InFlight {
		if n > 0 {
			return true
		}
	}
	return false
}

func (l Limits) validate() error {
	// NaN and infinity survive a comparison with zero, and every downstream use
	// — token arithmetic, the interval until the next token — is undefined for
	// them. A budget configured with either would admit or stall arbitrarily.
	if math.IsNaN(l.RequestsPerSecond) {
		return fmt.Errorf("runbudget: requests per second must be a real number, got NaN")
	}
	if math.IsInf(l.RequestsPerSecond, 0) {
		return fmt.Errorf("runbudget: requests per second must be finite, got %v", l.RequestsPerSecond)
	}
	if l.RequestsPerSecond < 0 {
		return fmt.Errorf("runbudget: requests per second must not be negative, got %v", l.RequestsPerSecond)
	}
	if l.Burst < 0 {
		return fmt.Errorf("runbudget: burst must not be negative, got %d", l.Burst)
	}
	if l.OpenBodies < 0 {
		return fmt.Errorf("runbudget: open body ceiling must not be negative, got %d", l.OpenBodies)
	}
	if l.MemoryBytes < 0 {
		return fmt.Errorf("runbudget: memory ceiling must not be negative, got %d", l.MemoryBytes)
	}
	for class, n := range l.InFlight {
		if err := class.Validate(); err != nil {
			return fmt.Errorf("runbudget: in-flight ceiling: %w", err)
		}
		if n < 0 {
			return fmt.Errorf("runbudget: in-flight ceiling for %q must not be negative, got %d", class, n)
		}
	}
	if !l.bounded() {
		return fmt.Errorf("runbudget: limits bound nothing; supply at least one ceiling or state explicitly that the run is unbudgeted")
	}
	return nil
}

// Request describes one provider request a consumer wants to start.
type Request struct {
	// Domains are the quota domains this request consumes. A provider that
	// meters more than one scope at once — say a per-account rate and a
	// per-bucket rate — returns all of them, and the request is admitted only
	// when every one of them can admit it. It does not pick one.
	Domains []Domain
	// Class is the kind of request.
	Class OpClass
	// MemoryBytes is the buffer reservation held until it is released. It is
	// charged once against the run, not once per domain: one buffer is one
	// buffer however many quotas the request touches.
	MemoryBytes int64
	// OpensBody declares that the request will hold a response body open.
	OpensBody bool
	// Consumer names the caller for diagnostics — a lane, a worker pool. It has
	// no effect on admission: fairness here is by arrival, not by consumer,
	// because weighting consumers would make lane count matter again.
	Consumer string
}

func (r Request) validate(limits Limits) error {
	if len(r.Domains) == 0 {
		return fmt.Errorf("runbudget: request names no quota domain")
	}
	seen := make(map[Domain]struct{}, len(r.Domains))
	for _, d := range r.Domains {
		if err := d.Validate(); err != nil {
			return err
		}
		if _, dup := seen[d]; dup {
			// Charging the same domain twice for one request would consume two
			// permits for one unit of provider load and could deadlock a
			// request against itself at a ceiling of one.
			return fmt.Errorf("runbudget: request names domain %s more than once", d)
		}
		seen[d] = struct{}{}
	}
	if err := r.Class.Validate(); err != nil {
		return err
	}
	if r.MemoryBytes < 0 {
		return fmt.Errorf("runbudget: request memory must not be negative, got %d", r.MemoryBytes)
	}
	// A request larger than the ceiling can never be admitted. Refusing is the
	// only honest answer: blocking would present a permanent stall as
	// backpressure.
	if limits.MemoryBytes > 0 && r.MemoryBytes > limits.MemoryBytes {
		return fmt.Errorf("runbudget: request reserves %d bytes, above the ceiling of %d; it can never be admitted",
			r.MemoryBytes, limits.MemoryBytes)
	}
	if n, ok := limits.InFlight[r.Class]; ok && n == 0 {
		return fmt.Errorf("runbudget: operation class %q is ceilinged at zero; it can never be admitted", r.Class)
	}
	return nil
}

// Budget admits provider requests against per-domain and run-global ceilings.
//
// One Budget serves every consumer of a run. Admission is fair by arrival
// within each domain: a blocked waiter holds back younger waiters that need any
// of the same domains, and only those. Serving whoever happens to fit would let
// a stream of small requests starve a large one indefinitely, and a starved
// consumer is a lane that never finishes; ordering globally instead would let
// one congested quota stall traffic to every unrelated quota in the run.
type Budget struct {
	mu     sync.Mutex
	limits Limits
	state  map[Domain]*domainState
	queue  []*waiter
	timer  stopper

	// memoryHeld is run-global: buffers live in one heap regardless of how many
	// providers the run talks to.
	memoryHeld int64

	// Injected for deterministic tests. Production uses the wall clock.
	now       func() time.Time
	afterFunc func(time.Duration, func()) stopper
}

// stopper is the part of a timer this package uses. It exists so a test can
// supply a clock it drives by hand instead of waiting on wall time, which is
// what makes the rate and throttle controls deterministic rather than slow.
type stopper interface {
	Stop() bool
}

type domainState struct {
	inFlight   map[OpClass]int
	openBodies int

	tokens     float64
	lastRefill time.Time

	// rateFactor scales the configured rate in response to provider throttling.
	// It is shared: a throttle observed by any consumer slows every consumer on
	// that domain, so N lanes produce one coordinated backoff rather than N
	// independent ones.
	rateFactor  float64
	pausedUntil time.Time
}

type waiter struct {
	req      Request
	ready    chan struct{}
	admitted bool
}

const (
	// throttleDecrease is the multiplicative factor applied on a throttle
	// signal. Halving is the standard congestion response: back off fast.
	throttleDecrease = 0.5
	// throttleRecoveryPerSecond is the additive recovery toward full rate.
	// Recovery is slow relative to backoff so a domain that is genuinely
	// saturated does not oscillate back into throttling.
	throttleRecoveryPerSecond = 0.05
	// minRateFactor keeps a throttled domain making progress rather than
	// collapsing to a stall from which only time can recover it.
	minRateFactor = 0.05
)

// New builds a budget over the given limits.
func New(limits Limits) (*Budget, error) {
	if err := limits.validate(); err != nil {
		return nil, err
	}
	if limits.RequestsPerSecond > 0 && limits.Burst == 0 {
		limits.Burst = int(limits.RequestsPerSecond)
		if limits.Burst < 1 {
			limits.Burst = 1
		}
	}
	inFlight := make(map[OpClass]int, len(limits.InFlight))
	for class, n := range limits.InFlight {
		inFlight[class] = n
	}
	limits.InFlight = inFlight

	return &Budget{
		limits:    limits,
		state:     make(map[Domain]*domainState),
		now:       time.Now,
		afterFunc: func(d time.Duration, f func()) stopper { return time.AfterFunc(d, f) },
	}, nil
}

// Acquire blocks until the run may start the request, and returns a lease that
// holds its share of the budget until retired.
//
// It honors context cancellation while waiting. A cancelled waiter leaves the
// queue without consuming anything; if it is cancelled in the same moment it is
// admitted, the admission is released rather than leaked.
func (b *Budget) Acquire(ctx context.Context, req Request) (*Lease, error) {
	if err := req.validate(b.limits); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	domains := make([]Domain, len(req.Domains))
	copy(domains, req.Domains)
	sortDomains(domains)
	req.Domains = domains

	w := &waiter{req: req, ready: make(chan struct{})}

	b.mu.Lock()
	b.queue = append(b.queue, w)
	b.pumpLocked()
	b.mu.Unlock()

	select {
	case <-w.ready:
		return &Lease{
			budget:      b,
			req:         req,
			requestHeld: true,
			bodyHeld:    req.OpensBody,
			memoryHeld:  req.MemoryBytes,
		}, nil
	case <-ctx.Done():
		b.mu.Lock()
		if w.admitted {
			// Admitted in the same moment the context was cancelled. The
			// resources are held by this waiter and nobody else will release
			// them, so release them here rather than returning an error and
			// leaking a permit for the life of the run.
			b.retireRequestLocked(req)
			if req.OpensBody {
				b.retireBodyLocked(req)
			}
			b.retireMemoryLocked(req.MemoryBytes)
		} else {
			b.removeLocked(w)
		}
		// Pump either way. A departing waiter may have been the only fairness
		// block on a younger one: with an 80-of-100 holder that is not going
		// anywhere, cancelling a queued 30-byte waiter is the event that lets a
		// queued 20-byte waiter run, and no release or timer will follow to
		// discover that later.
		b.pumpLocked()
		b.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Throttle records that a provider throttled a request against this domain.
//
// The response is shared state, not per-caller: every consumer drawing on the
// domain slows down together, and retryAfter pauses new starts for all of them.
// That is the difference between one coordinated backoff and N consumers each
// discovering the same throttle independently.
//
// An unusable domain is an error rather than a silent no-op. Discarding it would
// make provider feedback disappear at exactly the moment it matters, and it is
// inconsistent with refusing the same domain on the request path.
func (b *Budget) Throttle(d Domain, retryAfter time.Duration) error {
	if err := d.Validate(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	st := b.stateLocked(d)
	now := b.now()
	b.refillLocked(st, now)

	st.rateFactor *= throttleDecrease
	if st.rateFactor < minRateFactor {
		st.rateFactor = minRateFactor
	}
	// Reducing the refill rate alone leaves whatever balance had already
	// accrued, so a full pre-signal burst could still be spent immediately after
	// a throttle and the backoff would only take effect once it drained. Cap the
	// balance to the reduced factor so the signal takes effect now.
	if burst := b.effectiveBurstLocked(st); st.tokens > burst {
		st.tokens = burst
	}
	if retryAfter > 0 {
		if until := now.Add(retryAfter); until.After(st.pausedUntil) {
			st.pausedUntil = until
		}
	}
	b.pumpLocked()
	return nil
}

// DomainUsage is a point-in-time view of one domain's provider-scoped usage.
type DomainUsage struct {
	Domain     Domain
	InFlight   map[OpClass]int
	OpenBodies int
	RateFactor float64
}

// Usage is a point-in-time view of the whole budget, for progress reporting and
// for evidence that consumer count did not multiply provider load.
//
// MemoryHeld sits here rather than on DomainUsage because it is run-global.
type Usage struct {
	Domains    []DomainUsage
	MemoryHeld int64
	Waiters    int
}

// Snapshot reports current usage.
func (b *Budget) Snapshot() Usage {
	b.mu.Lock()
	defer b.mu.Unlock()

	domains := make([]Domain, 0, len(b.state))
	for d := range b.state {
		domains = append(domains, d)
	}
	sortDomains(domains)

	out := make([]DomainUsage, 0, len(domains))
	for _, d := range domains {
		st := b.state[d]
		inFlight := make(map[OpClass]int, len(st.inFlight))
		for class, n := range st.inFlight {
			if n > 0 {
				inFlight[class] = n
			}
		}
		out = append(out, DomainUsage{
			Domain:     d,
			InFlight:   inFlight,
			OpenBodies: st.openBodies,
			RateFactor: st.rateFactor,
		})
	}
	return Usage{Domains: out, MemoryHeld: b.memoryHeld, Waiters: len(b.queue)}
}

func (b *Budget) stateLocked(d Domain) *domainState {
	st, ok := b.state[d]
	if !ok {
		st = &domainState{
			inFlight:   make(map[OpClass]int, len(knownClasses)),
			tokens:     float64(b.limits.Burst),
			lastRefill: b.now(),
			rateFactor: 1,
		}
		b.state[d] = st
	}
	return st
}

// effectiveBurstLocked is the balance a domain may hold given its current
// throttle state. It floors at one token so a heavily throttled domain can still
// eventually start a request rather than becoming permanently unable to.
func (b *Budget) effectiveBurstLocked(st *domainState) float64 {
	if b.limits.Burst <= 0 {
		return 0
	}
	burst := float64(b.limits.Burst) * st.rateFactor
	if burst < 1 {
		burst = 1
	}
	return burst
}

// refillLocked advances a domain's token bucket and its recovery from throttle.
func (b *Budget) refillLocked(st *domainState, now time.Time) {
	elapsed := now.Sub(st.lastRefill)
	if elapsed <= 0 {
		return
	}
	st.lastRefill = now

	if st.rateFactor < 1 {
		st.rateFactor += throttleRecoveryPerSecond * elapsed.Seconds()
		if st.rateFactor > 1 {
			st.rateFactor = 1
		}
	}
	if b.limits.RequestsPerSecond <= 0 {
		return
	}
	st.tokens += b.limits.RequestsPerSecond * st.rateFactor * elapsed.Seconds()
	if burst := b.effectiveBurstLocked(st); st.tokens > burst {
		st.tokens = burst
	}
}

// memoryCeilingLocked is the largest total reservation the ledger may hold.
//
// An unconfigured ceiling means no operator limit, not unlimited arithmetic: the
// ledger is an int64, so MaxInt64 is the bound whether or not anyone chose one.
// Without this, an unbounded budget adds without checking and wraps to MinInt64,
// after which every subsequent retirement reads as corrupt.
func (b *Budget) memoryCeilingLocked() int64 {
	if b.limits.MemoryBytes > 0 {
		return b.limits.MemoryBytes
	}
	return math.MaxInt64
}

// readyAtLocked reports whether a request can be admitted now; if not, the
// earliest time at which its time-based constraints would clear, and whether the
// run-global memory pool was one of the reasons.
//
// A zero wake time means the block is a resource the request must wait to be
// released, which a release will pump rather than a timer. The memory flag is
// reported separately because memory is the one dimension shared by every
// domain, so it needs its own place in the fairness order — see pumpLocked.
func (b *Budget) readyAtLocked(req Request, now time.Time) (ready bool, wake time.Time, memoryBlocked bool) {
	ready = true

	// Run-global first: memory is not per domain.
	ceiling := b.memoryCeilingLocked()
	if b.memoryHeld < 0 || b.memoryHeld > ceiling {
		panic(fmt.Sprintf("runbudget: memory accounting is corrupt: %d held against a bound of %d",
			b.memoryHeld, ceiling))
	}
	// Subtract rather than add. Held plus requested can wrap past MaxInt64 and
	// compare as negative, admitting a reservation far above the ceiling; the
	// invariant above makes this difference non-negative.
	if req.MemoryBytes > ceiling-b.memoryHeld {
		ready = false
		memoryBlocked = true
	}

	for _, d := range req.Domains {
		st := b.stateLocked(d)
		b.refillLocked(st, now)

		if n, ok := b.limits.InFlight[req.Class]; ok && n > 0 && st.inFlight[req.Class] >= n {
			ready = false
		}
		if req.OpensBody && b.limits.OpenBodies > 0 && st.openBodies >= b.limits.OpenBodies {
			ready = false
		}
		if st.pausedUntil.After(now) {
			ready = false
			if st.pausedUntil.After(wake) {
				wake = st.pausedUntil
			}
		}
		if b.limits.RequestsPerSecond > 0 && st.tokens < 1 {
			ready = false
			rate := b.limits.RequestsPerSecond * st.rateFactor
			if rate > 0 {
				need := time.Duration((1 - st.tokens) / rate * float64(time.Second))
				if at := now.Add(need); at.After(wake) {
					wake = at
				}
			}
		}
	}
	return ready, wake, memoryBlocked
}

func (b *Budget) takeLocked(req Request) {
	b.memoryHeld += req.MemoryBytes
	for _, d := range req.Domains {
		st := b.stateLocked(d)
		st.inFlight[req.Class]++
		if req.OpensBody {
			st.openBodies++
		}
		if b.limits.RequestsPerSecond > 0 {
			st.tokens--
		}
	}
}

// retireRequestLocked returns the in-flight slot for a request.
func (b *Budget) retireRequestLocked(req Request) {
	for _, d := range req.Domains {
		st, ok := b.state[d]
		if !ok {
			panic(fmt.Sprintf("runbudget: retiring a request against unknown domain %s", d))
		}
		if st.inFlight[req.Class] <= 0 {
			panic(fmt.Sprintf("runbudget: in-flight accounting underflow retiring %q on %s", req.Class, d))
		}
		st.inFlight[req.Class]--
	}
}

// retireBodyLocked returns the open-body slot for a request.
func (b *Budget) retireBodyLocked(req Request) {
	for _, d := range req.Domains {
		st, ok := b.state[d]
		if !ok {
			panic(fmt.Sprintf("runbudget: retiring a body against unknown domain %s", d))
		}
		if st.openBodies <= 0 {
			panic(fmt.Sprintf("runbudget: open-body accounting underflow on %s", d))
		}
		st.openBodies--
	}
}

// retireMemoryLocked returns a memory reservation.
//
// Underflow panics rather than clamping. Clamping to zero would let a double
// retirement quietly hand the run capacity it never released, which is the
// failure a ceiling exists to prevent; the same reasoning the standard library
// applies to a negative WaitGroup counter.
func (b *Budget) retireMemoryLocked(bytes int64) {
	if bytes == 0 {
		return
	}
	if bytes < 0 || bytes > b.memoryHeld {
		panic(fmt.Sprintf("runbudget: memory accounting underflow: retiring %d bytes with %d held",
			bytes, b.memoryHeld))
	}
	b.memoryHeld -= bytes
}

func (b *Budget) removeLocked(target *waiter) {
	for i, w := range b.queue {
		if w == target {
			b.queue = append(b.queue[:i], b.queue[i+1:]...)
			return
		}
	}
}

// pumpLocked admits every waiter that may proceed, in arrival order.
//
// Fairness is per domain, not global. A waiter that cannot proceed blocks
// younger waiters that need any domain it needs, and only those. This is the
// rule that makes both halves true:
//
//   - No starvation. A large reservation is never bypassed by a stream of small
//     ones on the same domain, because once it is blocked, everything younger
//     that touches that domain queues behind it.
//   - No cross-domain interference. A composite request waiting on a busy
//     domain does not stall traffic to the unrelated domains it does not touch.
//     Global arrival order would make one congested quota throttle every other
//     quota in the run, which is the opposite of what a shared budget is for.
//
// Memory is the exception, and it has to be, because it is the one resource
// every domain draws on. Two requests against unrelated providers are not
// unrelated when they want the same buffer pool, so a waiter blocked on memory
// excludes younger requests that also want memory, whatever domain they are on.
// Work reserving no memory still passes: it does not contend for the thing the
// older waiter is queued for, so holding it back would be the global ordering
// this rule exists to avoid.
func (b *Budget) pumpLocked() {
	now := b.now()
	var wake time.Time

	for {
		blocked := make(map[Domain]struct{})
		memoryReserved := false
		wake = time.Time{}
		admitted := false

		for i := 0; i < len(b.queue); i++ {
			w := b.queue[i]
			if blocksOn(w.req.Domains, blocked) || (memoryReserved && w.req.MemoryBytes > 0) {
				markBlocked(w.req.Domains, blocked)
				continue
			}
			ready, at, memoryBlocked := b.readyAtLocked(w.req, now)
			if memoryBlocked {
				memoryReserved = true
			}
			if ready {
				b.takeLocked(w.req)
				w.admitted = true
				close(w.ready)
				b.queue = append(b.queue[:i], b.queue[i+1:]...)
				admitted = true
				// Resources changed, so re-scan from the front: a waiter passed
				// over a moment ago may now be admissible.
				break
			}
			markBlocked(w.req.Domains, blocked)
			if !at.IsZero() && (wake.IsZero() || at.Before(wake)) {
				wake = at
			}
		}
		if !admitted {
			break
		}
	}

	if len(b.queue) > 0 {
		b.scheduleLocked(wake, now)
		return
	}
	b.stopTimerLocked()
}

func blocksOn(domains []Domain, blocked map[Domain]struct{}) bool {
	for _, d := range domains {
		if _, ok := blocked[d]; ok {
			return true
		}
	}
	return false
}

func markBlocked(domains []Domain, blocked map[Domain]struct{}) {
	for _, d := range domains {
		blocked[d] = struct{}{}
	}
}

// scheduleLocked arranges a re-pump when the head is blocked only by time.
// Resource blocks need no timer: whoever holds the resource pumps on release.
func (b *Budget) scheduleLocked(wake, now time.Time) {
	b.stopTimerLocked()
	if wake.IsZero() || !wake.After(now) {
		return
	}
	b.timer = b.afterFunc(wake.Sub(now), func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.pumpLocked()
	})
}

func (b *Budget) stopTimerLocked() {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
}

// Lease is one admitted request's hold on the budget.
//
// Its three reservations retire independently, because they do not end together:
// a provider call can complete while its body is still being streamed, and a
// body can close while a buffer decoded from it is still live. A lease that
// returned them as one would make a consumer choose between releasing a ceiling
// it still needs and holding two it does not.
//
// Every retirement is idempotent, and Release is the fail-safe that returns
// whatever is left — so a deferred Release after explicit retirement is correct
// rather than a double-count.
type Lease struct {
	budget      *Budget
	req         Request
	mu          sync.Mutex
	requestHeld bool
	bodyHeld    bool
	memoryHeld  int64
}

// CompleteRequest retires the in-flight request slot, leaving any open body and
// memory reservation held.
func (l *Lease) CompleteRequest() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.requestHeld {
		return
	}
	l.requestHeld = false

	l.budget.mu.Lock()
	l.budget.retireRequestLocked(l.req)
	l.budget.pumpLocked()
	l.budget.mu.Unlock()
}

// CloseBody retires the open-body slot, leaving the request slot and memory
// reservation held.
//
// A body slot models a held provider connection, so it is charged until the body
// is actually closed. A consumer that streams a response should close it as soon
// as it is drained; holding it until Release would ceiling the run on bodies it
// is no longer reading.
func (l *Lease) CloseBody() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.bodyHeld {
		return
	}
	l.bodyHeld = false

	l.budget.mu.Lock()
	l.budget.retireBodyLocked(l.req)
	l.budget.pumpLocked()
	l.budget.mu.Unlock()
}

// ReleaseMemory retires the buffer reservation, leaving the request slot and any
// open body held.
func (l *Lease) ReleaseMemory() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.memoryHeld == 0 {
		return
	}
	bytes := l.memoryHeld
	l.memoryHeld = 0

	l.budget.mu.Lock()
	l.budget.retireMemoryLocked(bytes)
	l.budget.pumpLocked()
	l.budget.mu.Unlock()
}

// Release retires everything the lease still holds.
//
// It is the fail-safe, not the ordinary path: a consumer that knows when each
// resource is done should retire them as they finish. It is idempotent, so
// deferring it is always safe.
func (l *Lease) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()

	request, body, bytes := l.requestHeld, l.bodyHeld, l.memoryHeld
	l.requestHeld, l.bodyHeld, l.memoryHeld = false, false, 0
	if !request && !body && bytes == 0 {
		return
	}

	l.budget.mu.Lock()
	if request {
		l.budget.retireRequestLocked(l.req)
	}
	if body {
		l.budget.retireBodyLocked(l.req)
	}
	l.budget.retireMemoryLocked(bytes)
	l.budget.pumpLocked()
	l.budget.mu.Unlock()
}
