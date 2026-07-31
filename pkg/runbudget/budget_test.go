package runbudget

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	domainA = Domain{Version: 1, ID: "account-a"}
	domainB = Domain{Version: 1, ID: "bucket-b"}
)

func mustNew(t *testing.T, limits Limits) *Budget {
	t.Helper()
	b, err := New(limits)
	if err != nil {
		t.Fatalf("New(%+v): %v", limits, err)
	}
	return b
}

func acquire(t *testing.T, b *Budget, req Request) *Lease {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	lease, err := b.Acquire(ctx, req)
	if err != nil {
		t.Fatalf("Acquire(%+v): %v", req, err)
	}
	return lease
}

// waitForWaiters blocks until the queue holds at least n waiters, so a test can
// establish arrival order without sleeping on a guess.
func waitForWaiters(t *testing.T, b *Budget, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if b.Snapshot().Waiters >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %d queued waiters, got %d", n, b.Snapshot().Waiters)
		}
		time.Sleep(time.Millisecond)
	}
}

func inFlight(t *testing.T, b *Budget, d Domain, class OpClass) int {
	t.Helper()
	for _, u := range b.Snapshot().Domains {
		if u.Domain.Equal(d) {
			return u.InFlight[class]
		}
	}
	return 0
}

func TestNewRefusesUnusableLimits(t *testing.T) {
	cases := []struct {
		name    string
		limits  Limits
		wantSub string
	}{
		{"bounds nothing", Limits{}, "bound nothing"},
		{"negative rate", Limits{RequestsPerSecond: -1}, "must not be negative"},
		{"negative memory", Limits{MemoryBytes: -1}, "must not be negative"},
		{"negative open bodies", Limits{OpenBodies: -1}, "must not be negative"},
		{
			// A misspelled class would otherwise be silently unbounded, and the
			// budget would report itself as bounding load it was not bounding.
			name:    "unknown operation class",
			limits:  Limits{InFlight: map[OpClass]int{"gett": 4}},
			wantSub: "unknown operation class",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.limits); err == nil {
				t.Fatal("expected refusal")
			} else if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.wantSub)
			}
		})
	}
}

func TestAcquireRefusesUnsatisfiableRequests(t *testing.T) {
	b := mustNew(t, Limits{
		InFlight:    map[OpClass]int{OpGet: 2, OpHead: 0},
		MemoryBytes: 1024,
	})

	cases := []struct {
		name    string
		req     Request
		wantSub string
	}{
		{"no domain", Request{Class: OpGet}, "names no quota domain"},
		{
			// One request consuming a domain twice would take two permits for
			// one unit of provider load, and would deadlock against itself at a
			// ceiling of one.
			name:    "duplicate domain",
			req:     Request{Domains: []Domain{domainA, domainA}, Class: OpGet},
			wantSub: "more than once",
		},
		{"unknown class", Request{Domains: []Domain{domainA}, Class: "fetch"}, "unknown operation class"},
		{"zero-version domain", Request{Domains: []Domain{{ID: "x"}}, Class: OpGet}, "version must be positive"},
		{"empty domain id", Request{Domains: []Domain{{Version: 1, ID: ""}}, Class: OpGet}, "id must not be empty"},
		{
			// A JSON round trip would replace the offending bytes, so two distinct
			// domains could come back equal and merge quotas that do not share one.
			name:    "domain id is not valid UTF-8",
			req:     Request{Domains: []Domain{{Version: 1, ID: "bucket-\xff"}}, Class: OpGet},
			wantSub: "not valid UTF-8",
		},
		{
			// Blocking forever would present a permanent stall as backpressure.
			name:    "reservation above the ceiling",
			req:     Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 2048},
			wantSub: "can never be admitted",
		},
		{
			name:    "class ceilinged at zero",
			req:     Request{Domains: []Domain{domainA}, Class: OpHead},
			wantSub: "can never be admitted",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if lease, err := b.Acquire(ctx, tc.req); err == nil {
				lease.Release()
				t.Fatal("expected refusal")
			} else if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.wantSub)
			}
		})
	}
}

// The property the package exists for. Many consumers — lanes, workers — draw on
// one budget, so the load reaching the provider is set by the budget and not by
// how many consumers the run happens to have.
func TestLaneCountDoesNotMultiplyProviderLoad(t *testing.T) {
	const ceiling = 3
	for _, consumers := range []int{1, 4, 16, 64} {
		b := mustNew(t, Limits{InFlight: map[OpClass]int{OpGet: ceiling}})

		var (
			mu      sync.Mutex
			current int
			peak    int
		)
		release := make(chan struct{})
		var started, done sync.WaitGroup
		started.Add(consumers)
		done.Add(consumers)

		for i := 0; i < consumers; i++ {
			go func() {
				defer done.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainA}, Class: OpGet})
				if err != nil {
					started.Done()
					return
				}
				mu.Lock()
				current++
				if current > peak {
					peak = current
				}
				mu.Unlock()
				started.Done()

				<-release
				mu.Lock()
				current--
				mu.Unlock()
				lease.Release()
			}()
		}

		// Let the admitted set settle, then check nothing beyond the ceiling ever
		// held a permit at once.
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		observed := peak
		mu.Unlock()
		close(release)
		done.Wait()

		if observed > ceiling {
			t.Fatalf("%d consumers drove %d concurrent requests against a ceiling of %d; "+
				"lane count is multiplying provider load", consumers, observed, ceiling)
		}
		if observed == 0 {
			t.Fatalf("%d consumers admitted nothing", consumers)
		}
	}
}

// A request against several domains is admitted only when every one of them can
// admit it, and takes nothing at all until then. A partial hold would let a
// request blocked on one domain occupy permits on another.
func TestCompositeDomainAcquisitionIsAllOrNothing(t *testing.T) {
	b := mustNew(t, Limits{InFlight: map[OpClass]int{OpGet: 1}})

	// Fill domain B only.
	blocker := acquire(t, b, Request{Domains: []Domain{domainB}, Class: OpGet})

	composite := Request{Domains: []Domain{domainA, domainB}, Class: OpGet}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan *Lease, 1)
	go func() {
		lease, err := b.Acquire(ctx, composite)
		if err == nil {
			got <- lease
		}
	}()
	waitForWaiters(t, b, 1)

	if n := inFlight(t, b, domainA, OpGet); n != 0 {
		t.Fatalf("a request blocked on domain B is holding %d permits on domain A", n)
	}

	blocker.Release()
	select {
	case lease := <-got:
		if n := inFlight(t, b, domainA, OpGet); n != 1 {
			t.Fatalf("composite request holds %d permits on domain A, want 1", n)
		}
		if n := inFlight(t, b, domainB, OpGet); n != 1 {
			t.Fatalf("composite request holds %d permits on domain B, want 1", n)
		}
		lease.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("composite request never admitted after its blocking domain freed")
	}
}

// A waiter blocked on one of its domains holds back younger waiters on *any* of
// its domains, including ones that are momentarily free. Letting them bypass
// would leave the blocked waiter to starve under steady traffic to the domain
// it shares — the case per-domain arrival order exists to prevent.
func TestBlockedCompositeRequestIsNotBypassedOnItsFreeDomain(t *testing.T) {
	b := mustNew(t, Limits{InFlight: map[OpClass]int{OpGet: 1}})

	blocker := acquire(t, b, Request{Domains: []Domain{domainB}, Class: OpGet})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	order := make(chan string, 2)
	go func() {
		lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainA, domainB}, Class: OpGet, Consumer: "composite"})
		if err != nil {
			return
		}
		order <- "composite"
		lease.Release()
	}()
	waitForWaiters(t, b, 1)

	go func() {
		lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainA}, Class: OpGet, Consumer: "a-only"})
		if err != nil {
			return
		}
		order <- "a-only"
		lease.Release()
	}()
	waitForWaiters(t, b, 2)

	// Domain A is free, but the younger A-only request must not take it.
	if n := inFlight(t, b, domainA, OpGet); n != 0 {
		t.Fatalf("a younger request took %d permits on domain A ahead of an older waiter that needs it", n)
	}

	blocker.Release()

	select {
	case first := <-order:
		if first != "composite" {
			t.Fatalf("first admission was %q; a younger request bypassed an older waiter on a shared domain", first)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("nothing admitted after the blocking domain freed")
	}
}

// The other half of the same rule: a blocked waiter must not stall domains it
// does not touch. Global arrival order would let one congested quota throttle
// every unrelated quota in the run.
func TestBlockedWaiterDoesNotStallUnrelatedDomains(t *testing.T) {
	b := mustNew(t, Limits{InFlight: map[OpClass]int{OpGet: 1}})
	domainC := Domain{Version: 1, ID: "bucket-c"}

	blocker := acquire(t, b, Request{Domains: []Domain{domainB}, Class: OpGet})
	defer blocker.Release()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainA, domainB}, Class: OpGet})
		if err == nil {
			lease.Release()
		}
	}()
	waitForWaiters(t, b, 1)

	// C shares nothing with the blocked waiter, so it must flow.
	unrelated := acquire(t, b, Request{Domains: []Domain{domainC}, Class: OpGet})
	unrelated.Release()
}

func TestDistinctDomainsDoNotContend(t *testing.T) {
	b := mustNew(t, Limits{InFlight: map[OpClass]int{OpGet: 1}})

	a := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet})
	defer a.Release()

	// Same ID under a different version is a different domain: a provider that
	// renumbers its scheme must not be assumed to mean the same quota.
	renumbered := acquire(t, b, Request{Domains: []Domain{{Version: 2, ID: domainA.ID}}, Class: OpGet})
	defer renumbered.Release()

	other := acquire(t, b, Request{Domains: []Domain{domainB}, Class: OpGet})
	defer other.Release()
}

func TestOperationClassesAreAccountedSeparately(t *testing.T) {
	b := mustNew(t, Limits{InFlight: map[OpClass]int{OpList: 1, OpGet: 1}})

	list := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpList})
	defer list.Release()

	// A listing at its ceiling must not block a read: they are not
	// interchangeable load.
	get := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet})
	defer get.Release()
}

// A body slot models a held provider connection, so it is charged until the body
// is closed rather than until the call returns.
func TestOpenBodySlotIsHeldUntilCloseBody(t *testing.T) {
	b := mustNew(t, Limits{OpenBodies: 1, MemoryBytes: 1 << 20})

	first := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet, OpensBody: true, MemoryBytes: 100})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan *Lease, 1)
	go func() {
		lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainA}, Class: OpGet, OpensBody: true})
		if err == nil {
			got <- lease
		}
	}()
	waitForWaiters(t, b, 1)

	// Closing the body frees the slot while the lease keeps its reservation.
	first.CloseBody()

	select {
	case lease := <-got:
		if held := b.Snapshot().MemoryHeld; held != 100 {
			t.Fatalf("closing the body changed the memory reservation: %d held, want 100", held)
		}
		lease.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("closing a body did not free the open-body slot")
	}

	first.Release()
	if held := b.Snapshot().MemoryHeld; held != 0 {
		t.Fatalf("memory %d still held after release, want 0", held)
	}
	// CloseBody after Release must not double-credit the slot.
	first.CloseBody()
	if bodies := b.Snapshot().Domains[0].OpenBodies; bodies != 0 {
		t.Fatalf("open bodies %d after release, want 0", bodies)
	}
}

// A double release that decremented twice would hand out permits the run does
// not have, which is worse than leaking one.
func TestReleaseIsIdempotent(t *testing.T) {
	b := mustNew(t, Limits{InFlight: map[OpClass]int{OpGet: 2}, MemoryBytes: 1000, OpenBodies: 2})

	lease := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 100, OpensBody: true})
	other := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 100, OpensBody: true})

	lease.Release()
	lease.Release()
	lease.Release()

	usage := b.Snapshot()
	if got := usage.Domains[0].InFlight[OpGet]; got != 1 {
		t.Fatalf("in-flight %d after repeated release of one of two leases, want 1", got)
	}
	if usage.MemoryHeld != 100 {
		t.Fatalf("memory %d after repeated release, want 100", usage.MemoryHeld)
	}
	if usage.Domains[0].OpenBodies != 1 {
		t.Fatalf("open bodies %d after repeated release, want 1", usage.Domains[0].OpenBodies)
	}
	other.Release()
}

// Admission is by arrival. Serving whoever happens to fit lets a stream of small
// requests starve a large one indefinitely, and a starved consumer is a lane
// that never finishes.
func TestAdmissionIsFairByArrival(t *testing.T) {
	b := mustNew(t, Limits{MemoryBytes: 100})

	holder := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 100})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	order := make(chan string, 4)
	big := make(chan struct{})
	go func() {
		lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 100, Consumer: "big"})
		if err != nil {
			return
		}
		order <- "big"
		close(big)
		<-ctx.Done()
		lease.Release()
	}()
	waitForWaiters(t, b, 1)

	for i := 0; i < 3; i++ {
		go func() {
			lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 1, Consumer: "small"})
			if err != nil {
				return
			}
			order <- "small"
			lease.Release()
		}()
	}
	waitForWaiters(t, b, 4)

	holder.Release()

	select {
	case first := <-order:
		if first != "big" {
			t.Fatalf("first admission was %q; a later small request jumped a queued large one", first)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("nothing was admitted after the holder released")
	}
	<-big
}

// A cancelled waiter must leave the queue holding nothing. A waiter that took
// resources on its way out would leak a permit for the life of the run.
func TestCancelledWaiterConsumesNothing(t *testing.T) {
	b := mustNew(t, Limits{InFlight: map[OpClass]int{OpGet: 1}})

	holder := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet})

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := b.Acquire(ctx, Request{Domains: []Domain{domainA}, Class: OpGet})
		errc <- err
	}()
	waitForWaiters(t, b, 1)

	cancel()
	if err := <-errc; err == nil {
		t.Fatal("cancelled Acquire returned a lease")
	}

	if queued := b.Snapshot().Waiters; queued != 0 {
		t.Fatalf("%d waiters left queued after cancellation", queued)
	}
	holder.Release()

	// The permit the holder returned must be available, not consumed by the
	// waiter that walked away.
	next := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet})
	next.Release()

	if n := inFlight(t, b, domainA, OpGet); n != 0 {
		t.Fatalf("in-flight %d after everything released, want 0", n)
	}
}

// A waiter can be admitted in the same moment its context is cancelled, and
// then nobody is left holding the lease that owns the permit. Whichever branch
// wins, the permit must not stay taken.
//
// The race is not directly schedulable, so this drives it repeatedly and checks
// the invariant after every attempt: a leak does not merely fail once, it
// accumulates until nothing can be admitted at all.
func TestCancellationRacingAdmissionNeverLeaksPermits(t *testing.T) {
	b := mustNew(t, Limits{InFlight: map[OpClass]int{OpGet: 1}})
	req := Request{Domains: []Domain{domainA}, Class: OpGet}

	for i := 0; i < 300; i++ {
		holder := acquire(t, b, req)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan *Lease, 1)
		go func() {
			lease, err := b.Acquire(ctx, req)
			if err != nil {
				done <- nil
				return
			}
			done <- lease
		}()
		waitForWaiters(t, b, 1)

		// Cancel and free the permit at the same time: the waiter may be
		// admitted by the release or torn down by the cancellation.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); cancel() }()
		go func() { defer wg.Done(); holder.Release() }()
		wg.Wait()

		if lease := <-done; lease != nil {
			lease.Release()
		}

		if n := inFlight(t, b, domainA, OpGet); n != 0 {
			t.Fatalf("iteration %d: %d permits still held after every lease was released "+
				"and the waiter returned; an admission raced by cancellation was leaked", i, n)
		}
		if queued := b.Snapshot().Waiters; queued != 0 {
			t.Fatalf("iteration %d: %d waiters left queued", i, queued)
		}
	}
}

func TestAcquireRefusesAnAlreadyCancelledContext(t *testing.T) {
	b := mustNew(t, Limits{InFlight: map[OpClass]int{OpGet: 4}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainA}, Class: OpGet}); err == nil {
		lease.Release()
		t.Fatal("expected refusal on a cancelled context")
	}
	if n := inFlight(t, b, domainA, OpGet); n != 0 {
		t.Fatalf("in-flight %d after a refused acquire, want 0", n)
	}
}
