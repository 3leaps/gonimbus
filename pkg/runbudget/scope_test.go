package runbudget

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"
)

// A departing waiter can be the only fairness block on a younger one. Nothing
// else will discover that: no resource was released and no timer is pending, so
// without a re-pump the younger waiter stays queued for the life of the run.
func TestCancellingABlockedWaiterAdmitsTheOneBehindIt(t *testing.T) {
	b := mustNew(t, Limits{MemoryBytes: 100})

	// A long-lived holder that is not going anywhere.
	holder := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 80})
	defer holder.Release()

	olderCtx, cancelOlder := context.WithCancel(context.Background())
	defer cancelOlder()
	go func() {
		lease, err := b.Acquire(olderCtx, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 30, Consumer: "older"})
		if err == nil {
			lease.Release()
		}
	}()
	waitForWaiters(t, b, 1)

	youngerCtx, cancelYounger := context.WithCancel(context.Background())
	defer cancelYounger()
	admitted := make(chan *Lease, 1)
	go func() {
		lease, err := b.Acquire(youngerCtx, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 20, Consumer: "younger"})
		if err == nil {
			admitted <- lease
		}
	}()
	waitForWaiters(t, b, 2)

	// 20 bytes are free but reserved for the older waiter's turn.
	select {
	case <-admitted:
		t.Fatal("the younger waiter was admitted while an older one was queued on the same domain")
	case <-time.After(50 * time.Millisecond):
	}

	// The older waiter leaves. Nothing is released and no timer is due, so only
	// a re-pump on removal can admit the younger one.
	cancelOlder()

	select {
	case lease := <-admitted:
		lease.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("the younger waiter was stranded after the older one cancelled; " +
			"removing a waiter did not re-drive the queue")
	}
}

// Buffers live in one heap however many providers a run talks to. A per-domain
// memory ledger would entitle each disjoint domain to the whole limit, so the
// configured ceiling would not be a ceiling.
func TestMemoryCeilingIsRunGlobalNotPerDomain(t *testing.T) {
	b := mustNew(t, Limits{MemoryBytes: 100})

	first := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 60})
	defer first.Release()

	if held := b.Snapshot().MemoryHeld; held != 60 {
		t.Fatalf("memory held %d, want 60", held)
	}

	// A different domain draws on the same run-global pool.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainB}, Class: OpGet, MemoryBytes: 60}); err == nil {
		held := b.Snapshot().MemoryHeld
		lease.Release()
		t.Fatalf("a second domain reserved 60 more bytes against a 100-byte ceiling (%d held); "+
			"the memory ceiling is per domain rather than run-global", held)
	}

	// What does fit is admitted.
	fits := acquire(t, b, Request{Domains: []Domain{domainB}, Class: OpGet, MemoryBytes: 40})
	if held := b.Snapshot().MemoryHeld; held != 100 {
		t.Fatalf("memory held %d after a fitting second reservation, want 100", held)
	}
	fits.Release()
}

// Making memory run-global made two requests on unrelated providers contend for
// it, and per-domain fairness does not cover that: a younger request on another
// domain could take the remainder ahead of an older large reservation and keep
// it blocked even after its own holder released.
func TestGlobalMemoryFairnessCrossesDomains(t *testing.T) {
	b := mustNew(t, Limits{MemoryBytes: 100})

	holder := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 20})

	olderCtx, cancelOlder := context.WithCancel(context.Background())
	defer cancelOlder()
	olderIn := make(chan *Lease, 1)
	go func() {
		lease, err := b.Acquire(olderCtx, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 90, Consumer: "older"})
		if err == nil {
			olderIn <- lease
		}
	}()
	waitForWaiters(t, b, 1)

	// 80 bytes are free, but they are spoken for by the older waiter.
	youngerCtx, cancelYounger := context.WithCancel(context.Background())
	defer cancelYounger()
	youngerIn := make(chan *Lease, 1)
	go func() {
		lease, err := b.Acquire(youngerCtx, Request{Domains: []Domain{domainB}, Class: OpGet, MemoryBytes: 80, Consumer: "younger"})
		if err == nil {
			youngerIn <- lease
		}
	}()
	waitForWaiters(t, b, 2)

	select {
	case lease := <-youngerIn:
		held := b.Snapshot().MemoryHeld
		lease.Release()
		t.Fatalf("a younger request on another domain took the memory an older waiter was queued for "+
			"(%d held); global memory has only domain-local fairness", held)
	case <-time.After(100 * time.Millisecond):
	}

	// Releasing the older waiter's own holder must be enough to admit it.
	holder.Release()
	select {
	case lease := <-olderIn:
		lease.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("the older waiter was still blocked after its own holder released")
	}
}

// Work that reserves no memory does not contend for the pool, so holding it
// behind a memory-blocked waiter would be the global ordering the per-domain
// rule exists to avoid.
func TestMemoryBlockedWaiterDoesNotStallZeroMemoryWork(t *testing.T) {
	b := mustNew(t, Limits{MemoryBytes: 100, InFlight: map[OpClass]int{OpGet: 8}})

	holder := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 100})
	defer holder.Release()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 50})
		if err == nil {
			lease.Release()
		}
	}()
	waitForWaiters(t, b, 1)

	// Reserves nothing from the pool, so it must not queue behind the blocked one.
	free := acquire(t, b, Request{Domains: []Domain{domainB}, Class: OpGet})
	free.Release()
}

// A waiter blocked on a provider dimension rather than on memory must not
// exclude younger memory requests: the pool is not what it is waiting for.
func TestSlotBlockedWaiterDoesNotReserveTheMemoryPool(t *testing.T) {
	b := mustNew(t, Limits{MemoryBytes: 1000, InFlight: map[OpClass]int{OpGet: 1}})

	holder := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 10})
	defer holder.Release()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		// Blocked on domain A's in-flight slot, not on memory.
		lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 10})
		if err == nil {
			lease.Release()
		}
	}()
	waitForWaiters(t, b, 1)

	// Plenty of memory, unrelated domain: must be admitted.
	other := acquire(t, b, Request{Domains: []Domain{domainB}, Class: OpGet, MemoryBytes: 500})
	other.Release()
}

// Unbounded means no operator ceiling; it cannot mean unrepresentable
// accounting. The ledger is an int64, so MaxInt64 bounds it whether or not
// anyone configured a limit.
func TestUnboundedMemoryLedgerDoesNotWrap(t *testing.T) {
	b := mustNew(t, Limits{InFlight: map[OpClass]int{OpGet: 4}})

	huge := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: math.MaxInt64})

	if held := b.Snapshot().MemoryHeld; held != math.MaxInt64 {
		t.Fatalf("memory held %d after reserving MaxInt64, want MaxInt64", held)
	}

	// One more byte cannot be represented, so it waits rather than wrapping.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainB}, Class: OpGet, MemoryBytes: 1}); err == nil {
		held := b.Snapshot().MemoryHeld
		lease.Release()
		t.Fatalf("a second reservation was admitted past MaxInt64 (%d held); the ledger wrapped", held)
	}
	if held := b.Snapshot().MemoryHeld; held < 0 {
		t.Fatalf("memory ledger is negative (%d); it wrapped", held)
	}

	// It proceeds once room is returned, and the ledger stays sane throughout.
	huge.Release()
	if held := b.Snapshot().MemoryHeld; held != 0 {
		t.Fatalf("memory held %d after release, want 0", held)
	}
	after := acquire(t, b, Request{Domains: []Domain{domainB}, Class: OpGet, MemoryBytes: 1})
	after.Release()
}

// A composite request holds one buffer, not one per quota it touches.
func TestCompositeRequestChargesMemoryOnce(t *testing.T) {
	b := mustNew(t, Limits{MemoryBytes: 100})

	lease := acquire(t, b, Request{Domains: []Domain{domainA, domainB}, Class: OpGet, MemoryBytes: 60})
	defer lease.Release()

	if held := b.Snapshot().MemoryHeld; held != 60 {
		t.Fatalf("a two-domain request holding one 60-byte buffer reports %d held; "+
			"the reservation is being charged once per domain", held)
	}
}

// Near the top of the int64 range, held plus requested wraps negative and
// compares as under the ceiling, admitting a reservation far above it.
func TestMemoryCeilingArithmeticDoesNotWrap(t *testing.T) {
	b := mustNew(t, Limits{MemoryBytes: math.MaxInt64})

	held := int64(math.MaxInt64 - 1024)
	first := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: held})
	defer first.Release()

	// This fits in the remaining 1024 bytes.
	fits := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 1024})

	// This does not. Added to what is held it overflows.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 4096}); err == nil {
		total := b.Snapshot().MemoryHeld
		lease.Release()
		t.Fatalf("a reservation past the ceiling was admitted (%d held); the ceiling comparison wrapped", total)
	}
	fits.Release()
}

// Reducing the refill rate alone leaves whatever balance already accrued, so the
// full pre-signal burst could still be spent immediately and the backoff would
// only bite once it drained.
func TestThrottleWithoutRetryAfterDrainsThePreSignalBurst(t *testing.T) {
	const burst = 100
	b, _ := manualBudget(t, Limits{RequestsPerSecond: 100, Burst: burst})

	// Spend one token so the domain exists with a nearly full balance.
	primer := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet})
	primer.Release()

	mustThrottle(t, b, domainA, 0)

	// Count starts available without the clock moving at all.
	started := 0
	for i := 0; i < burst*2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainA}, Class: OpGet})
		cancel()
		if err != nil {
			break
		}
		started++
		lease.Release()
	}

	// The factor halved, so at most half the burst may remain spendable.
	if maxAllowed := burst / 2; started > maxAllowed {
		t.Fatalf("%d starts were available immediately after a throttle with no retry-after, "+
			"above the %d the halved factor allows; the pre-signal burst was not drained",
			started, maxAllowed)
	}
	if started == 0 {
		t.Fatal("a throttled domain could not start anything at all; backoff became a stall")
	}
}

// Capping the balance at the moment of the signal is not enough on its own: the
// next refill re-accrues, and if it tops up to the unthrottled burst the backoff
// is undone one tick later. The previous control froze the clock, so it never
// reached a refill and could not see this.
func TestThrottledDomainDoesNotReaccrueTheFullBurst(t *testing.T) {
	const burst = 100
	b, clock := manualBudget(t, Limits{RequestsPerSecond: 100, Burst: burst})

	primer := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet})
	primer.Release()
	mustThrottle(t, b, domainA, 0)

	// The window has to be long enough that accrual would exceed the reduced
	// ceiling — otherwise the cap never binds and the control proves nothing —
	// and short enough that recovery has not returned the factor to full. At
	// rate 100 and burst 100 that means more than a second.
	clock.Advance(3 * time.Second)

	// One start forces the refill, so the factor read next reflects the recovery
	// already applied rather than the pre-advance value Snapshot would report.
	primed := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet})
	primed.Release()

	factor := b.Snapshot().Domains[0].RateFactor
	if factor >= 1 {
		t.Fatalf("rate factor recovered to %v; the window is too long to test the cap", factor)
	}
	if accrued := 100 * factor * 3; accrued <= float64(burst)*factor {
		t.Fatalf("accrual %v does not exceed the reduced ceiling %v; the cap is not under test",
			accrued, float64(burst)*factor)
	}

	started := 1
	for i := 0; i < burst*2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		lease, err := b.Acquire(ctx, Request{Domains: []Domain{domainA}, Class: OpGet})
		cancel()
		if err != nil {
			break
		}
		started++
		lease.Release()
	}

	// The balance must be bounded by the throttled ceiling, not the configured one.
	if allowed := int(float64(burst)*factor) + 1; started > allowed {
		t.Fatalf("%d starts were available after a refill on a throttled domain (factor %v, "+
			"ceiling %d); the refill re-accrued the unthrottled burst", started, factor, allowed)
	}
	if started == 0 {
		t.Fatal("a throttled domain accrued nothing across a full second")
	}
}

// Each reservation ends at a different moment: a provider call can complete while
// its body is still streaming, and a body can close while a buffer decoded from
// it is still live. Retiring one must not return the others.
func TestLeaseRetiresEachDimensionIndependently(t *testing.T) {
	newLease := func(t *testing.T) (*Budget, *Lease) {
		t.Helper()
		b := mustNew(t, Limits{InFlight: map[OpClass]int{OpGet: 4}, OpenBodies: 4, MemoryBytes: 1000})
		lease := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet, OpensBody: true, MemoryBytes: 100})
		return b, lease
	}

	assertHeld := func(t *testing.T, b *Budget, requests, bodies int, memory int64) {
		t.Helper()
		usage := b.Snapshot()
		if got := usage.Domains[0].InFlight[OpGet]; got != requests {
			t.Fatalf("in-flight %d, want %d", got, requests)
		}
		if got := usage.Domains[0].OpenBodies; got != bodies {
			t.Fatalf("open bodies %d, want %d", got, bodies)
		}
		if usage.MemoryHeld != memory {
			t.Fatalf("memory held %d, want %d", usage.MemoryHeld, memory)
		}
	}

	t.Run("request completes, body and memory stay", func(t *testing.T) {
		b, lease := newLease(t)
		lease.CompleteRequest()
		assertHeld(t, b, 0, 1, 100)
		lease.CompleteRequest() // idempotent
		assertHeld(t, b, 0, 1, 100)
		lease.Release()
		assertHeld(t, b, 0, 0, 0)
	})

	t.Run("body closes, request and memory stay", func(t *testing.T) {
		b, lease := newLease(t)
		lease.CloseBody()
		assertHeld(t, b, 1, 0, 100)
		lease.CloseBody() // idempotent
		assertHeld(t, b, 1, 0, 100)
		lease.Release()
		assertHeld(t, b, 0, 0, 0)
	})

	t.Run("memory releases, request and body stay", func(t *testing.T) {
		b, lease := newLease(t)
		lease.ReleaseMemory()
		assertHeld(t, b, 1, 1, 0)
		lease.ReleaseMemory() // idempotent
		assertHeld(t, b, 1, 1, 0)
		lease.Release()
		assertHeld(t, b, 0, 0, 0)
	})

	t.Run("release after full explicit retirement is a no-op", func(t *testing.T) {
		b, lease := newLease(t)
		other := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet, OpensBody: true, MemoryBytes: 100})

		lease.CompleteRequest()
		lease.CloseBody()
		lease.ReleaseMemory()
		assertHeld(t, b, 1, 1, 100)

		// The deferred fail-safe must not credit back the other lease's hold.
		lease.Release()
		assertHeld(t, b, 1, 1, 100)
		other.Release()
		assertHeld(t, b, 0, 0, 0)
	})
}

// The lease guards idempotency, so no public sequence can double-retire. That
// makes the accounting guards unreachable from outside — and an unreachable
// guard that silently clamps is indistinguishable from one that fails, which is
// exactly how a bookkeeping bug in this package would escape. These drive the
// internal retirement paths directly, because the invariant they protect belongs
// to this package rather than to its callers.
func TestAccountingUnderflowFailsLoudly(t *testing.T) {
	req := Request{Domains: []Domain{domainA}, Class: OpGet, OpensBody: true, MemoryBytes: 100}

	cases := []struct {
		name    string
		retire  func(*Budget)
		wantSub string
	}{
		{
			name:    "memory retired beyond what is held",
			retire:  func(b *Budget) { b.retireMemoryLocked(150) },
			wantSub: "memory accounting underflow",
		},
		{
			name: "memory retired twice",
			retire: func(b *Budget) {
				b.retireMemoryLocked(100)
				b.retireMemoryLocked(100)
			},
			wantSub: "memory accounting underflow",
		},
		{
			name: "request slot retired twice",
			retire: func(b *Budget) {
				b.retireRequestLocked(req)
				b.retireRequestLocked(req)
			},
			wantSub: "in-flight accounting underflow",
		},
		{
			name: "body slot retired twice",
			retire: func(b *Budget) {
				b.retireBodyLocked(req)
				b.retireBodyLocked(req)
			},
			wantSub: "open-body accounting underflow",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := mustNew(t, Limits{InFlight: map[OpClass]int{OpGet: 4}, OpenBodies: 4, MemoryBytes: 1000})
			lease := acquire(t, b, req)
			_ = lease

			defer func() {
				recovered := recover()
				if recovered == nil {
					t.Fatalf("retiring more than was held did not fail; the accounting was clamped, "+
						"which hands the run capacity it never released (memory now %d)",
						b.Snapshot().MemoryHeld)
				}
				if msg, ok := recovered.(string); !ok || !strings.Contains(msg, tc.wantSub) {
					t.Fatalf("panic %v does not mention %q", recovered, tc.wantSub)
				}
			}()

			b.mu.Lock()
			defer b.mu.Unlock()
			tc.retire(b)
		})
	}
}

// A reservation retired while the ceiling is unbounded still has to balance: the
// guards must not be skipped just because no limit is configured.
func TestAccountingBalancesWithAnUnboundedMemoryCeiling(t *testing.T) {
	b := mustNew(t, Limits{InFlight: map[OpClass]int{OpGet: 2}})
	lease := acquire(t, b, Request{Domains: []Domain{domainA}, Class: OpGet, MemoryBytes: 4096})

	if held := b.Snapshot().MemoryHeld; held != 4096 {
		t.Fatalf("memory held %d with an unbounded ceiling, want 4096", held)
	}
	lease.Release()
	if held := b.Snapshot().MemoryHeld; held != 0 {
		t.Fatalf("memory held %d after release, want 0", held)
	}
}

func TestNewRefusesNonFiniteRate(t *testing.T) {
	cases := []struct {
		name    string
		rate    float64
		wantSub string
	}{
		{"NaN", math.NaN(), "must be a real number"},
		{"positive infinity", math.Inf(1), "must be finite"},
		{"negative infinity", math.Inf(-1), "must be finite"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A comparison with zero admits all three, and every downstream use of
			// the value is undefined for them.
			if _, err := New(Limits{RequestsPerSecond: tc.rate}); err == nil {
				t.Fatal("expected refusal")
			} else if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.wantSub)
			}
		})
	}
}

// A whitespace-only ID is a legal opaque value. Rejecting it, or trimming it,
// would be this package normalizing an identity whose contract is exact equality.
func TestDomainIDBytesAreNotNormalized(t *testing.T) {
	spaced := Domain{Version: 1, ID: "  "}
	if err := spaced.Validate(); err != nil {
		t.Fatalf("a whitespace-only domain id was refused: %v", err)
	}
	if spaced.Equal(Domain{Version: 1, ID: " "}) {
		t.Fatal("two ids differing only in length compared equal; the id is being normalized")
	}

	b := mustNew(t, Limits{InFlight: map[OpClass]int{OpGet: 1}})
	held := acquire(t, b, Request{Domains: []Domain{spaced}, Class: OpGet})
	defer held.Release()

	// A different-length whitespace id is a different quota, so it must not
	// contend with the one above.
	other := acquire(t, b, Request{Domains: []Domain{{Version: 1, ID: " "}}, Class: OpGet})
	other.Release()
}

// A domain id is provider-supplied opaque text that reaches logs.
func TestDomainStringQuotesTheID(t *testing.T) {
	got := Domain{Version: 1, ID: "bucket\nrate=unlimited"}.String()
	if strings.Contains(got, "\n") {
		t.Fatalf("diagnostic %q carries a raw newline; a domain id could forge a log line", got)
	}
	if !strings.Contains(got, `\n`) {
		t.Fatalf("diagnostic %q does not show the escaped newline", got)
	}
}

// Silently discarding provider feedback is inconsistent with refusing the same
// domain on the request path, and loses the signal when it matters most.
func TestThrottleReportsAnUnusableDomain(t *testing.T) {
	b := mustNew(t, Limits{RequestsPerSecond: 10})

	for _, d := range []Domain{{}, {Version: 1, ID: ""}, {Version: 0, ID: "x"}, {Version: 1, ID: "bad-\xff"}} {
		if err := b.Throttle(d, time.Second); err == nil {
			t.Fatalf("Throttle accepted unusable domain %#v", d)
		}
	}
	if live := len(b.Snapshot().Domains); live != 0 {
		t.Fatalf("a refused throttle created %d domain ledgers", live)
	}
}
