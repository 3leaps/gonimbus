package crawler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// concurrencyProbe records the peak number of simultaneous listings so the
// budget's ceiling can be asserted on observed aggregate work rather than on
// configuration.
type concurrencyProbe struct {
	mu     sync.Mutex
	active int
	peak   int
	starts int
}

func (p *concurrencyProbe) enter() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active++
	p.starts++
	if p.active > p.peak {
		p.peak = p.active
	}
}

func (p *concurrencyProbe) exit() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active--
}

func (p *concurrencyProbe) peakConcurrency() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}

// probeProvider counts overlapping List calls across every crawler using it.
type probeProvider struct {
	probe *concurrencyProbe
	delay time.Duration
}

func (p *probeProvider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	p.probe.enter()
	defer p.probe.exit()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(p.delay):
	}
	return &provider.ListResult{
		Objects:     []provider.ObjectSummary{{Key: opts.Prefix + "obj.txt", Size: 1}},
		IsTruncated: false,
	}, nil
}

func (p *probeProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}
func (p *probeProvider) Close() error { return nil }

// sharedTestRateLimit is the run-global request ceiling, in requests/sec, used
// by both the budget and each crawler's own Config in these tests.
const sharedTestRateLimit = 50.0

func laneCrawler(t *testing.T, p provider.Provider, budget *RequestBudget, prefixes []string, jobID string) *Crawler {
	t.Helper()
	m, err := match.New(match.Config{Includes: []string{"**"}})
	require.NoError(t, err)
	cfg := DefaultConfig()
	// The per-crawler ceilings are set deliberately: concurrency is generous so
	// that if the shared budget is not the binding constraint the crawlers exceed
	// it and the assertion fails, and the rate matches the budget's so that a
	// per-crawler limiter would produce exactly the N-times-the-operator's-rate
	// defect rather than an unlimited one.
	cfg.Concurrency = 16
	cfg.RateLimit = sharedTestRateLimit
	return New(p, m, newMockWriter(), jobID, cfg).
		WithRequestBudget(budget).
		WithPrefixes(prefixes)
}

func manyPrefixes(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("p%02d/", i))
	}
	return out
}

// TestSharedBudgetBoundsAggregateListConcurrency proves the injected budget
// bounds listing work across ALL crawlers sharing it, not per crawler. Without a
// shared budget each crawler enforces its own ceiling, so N crawlers would issue
// N times the operator's concurrency against the provider.
func TestSharedBudgetBoundsAggregateListConcurrency(t *testing.T) {
	const ceiling = 3
	probe := &concurrencyProbe{}
	p := &probeProvider{probe: probe, delay: 5 * time.Millisecond}
	budget := NewRequestBudget(ceiling, 0)

	lanes := make([]*Crawler, 0, 4)
	for i := 0; i < 4; i++ {
		lanes = append(lanes, laneCrawler(t, p, budget, manyPrefixes(6), fmt.Sprintf("lane-%d", i)))
	}

	var wg sync.WaitGroup
	for _, c := range lanes {
		wg.Add(1)
		go func(c *Crawler) {
			defer wg.Done()
			_, err := c.Run(context.Background())
			assert.NoError(t, err)
		}(c)
	}
	wg.Wait()

	assert.LessOrEqual(t, probe.peakConcurrency(), ceiling,
		"aggregate in-flight listings across all crawlers must stay within the run-global ceiling")
	assert.Greater(t, probe.peakConcurrency(), 1,
		"the budget must still permit parallel listing, or the ceiling assertion above is vacuous")
}

// TestPrivateBudgetKeepsPerCrawlerCeiling proves a crawler with no injected
// budget is unchanged: its own Config.Concurrency is the ceiling.
func TestPrivateBudgetKeepsPerCrawlerCeiling(t *testing.T) {
	const ceiling = 2
	probe := &concurrencyProbe{}
	p := &probeProvider{probe: probe, delay: 5 * time.Millisecond}

	m, err := match.New(match.Config{Includes: []string{"**"}})
	require.NoError(t, err)
	cfg := DefaultConfig()
	cfg.Concurrency = ceiling
	c := New(p, m, newMockWriter(), "solo", cfg).WithPrefixes(manyPrefixes(8))

	_, err = c.Run(context.Background())
	require.NoError(t, err)
	assert.LessOrEqual(t, probe.peakConcurrency(), ceiling)
	assert.Greater(t, probe.peakConcurrency(), 1)
}

// TestSharedBudgetBoundsAggregateRequestRate proves the request-rate ceiling is
// also run-global. A per-crawler limiter would let N crawlers issue N times the
// operator's configured rate — the operator asks for a ceiling and silently gets
// a multiple of it.
func TestSharedBudgetBoundsAggregateRequestRate(t *testing.T) {
	const rateLimit = sharedTestRateLimit // requests/sec, shared across every crawler
	probe := &concurrencyProbe{}
	p := &probeProvider{probe: probe, delay: 0}
	budget := NewRequestBudget(8, rateLimit)

	lanes := make([]*Crawler, 0, 4)
	for i := 0; i < 4; i++ {
		lanes = append(lanes, laneCrawler(t, p, budget, manyPrefixes(5), fmt.Sprintf("lane-%d", i)))
	}

	start := time.Now()
	var wg sync.WaitGroup
	for _, c := range lanes {
		wg.Add(1)
		go func(c *Crawler) {
			defer wg.Done()
			_, err := c.Run(context.Background())
			assert.NoError(t, err)
		}(c)
	}
	wg.Wait()
	elapsed := time.Since(start)

	probe.mu.Lock()
	starts := probe.starts
	probe.mu.Unlock()

	// The ceiling accounts for the limiter's burst as well as its sustained rate:
	// a token-bucket limiter admits up to `burst` requests immediately and
	// `rate * elapsed` thereafter.
	const burst = 1
	allowed := rateLimit*elapsed.Seconds() + burst
	assert.LessOrEqual(t, float64(starts), allowed,
		"aggregate request starts (%d in %s) must stay within the run-global rate ceiling", starts, elapsed)
	assert.Equal(t, 20, starts, "every prefix must still be listed exactly once")
}

// TestLeaseReservesOnePermitPerCrawler proves each crawler admitted within the
// ceiling gets a dedicated permit, which is what keeps a crawler from being
// starved by peers holding permits for whole paginated listings.
func TestLeaseReservesOnePermitPerCrawler(t *testing.T) {
	budget := NewRequestBudget(3, 0)
	a, b, c := budget.lease(), budget.lease(), budget.lease()
	assert.Len(t, a.reservation, 1)
	assert.Len(t, b.reservation, 1)
	assert.Len(t, c.reservation, 1)
	assert.Empty(t, budget.permits, "every permit is reserved once the ceiling is fully leased")

	// Leasing beyond the ceiling degrades rather than blocking: the extra crawler
	// simply draws from the shared pool.
	extra := budget.lease()
	assert.Empty(t, extra.reservation)
}

// TestReservationPreventsStarvation proves the guarantee the reservation exists
// for: a crawler holding every unreserved permit cannot stop a peer from
// starting work.
func TestReservationPreventsStarvation(t *testing.T) {
	budget := NewRequestBudget(2, 0)
	busy, waiting := budget.lease(), budget.lease()

	// Both permits are spoken for as reservations, so there is no shared capacity
	// left once busy starts listing.
	releaseReserved, err := busy.acquireListSlot(context.Background())
	require.NoError(t, err)
	require.Empty(t, budget.permits, "the peer's reservation must not be available to busy")

	// waiting can still start immediately, on its own reservation.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	releaseWaiting, err := waiting.acquireListSlot(ctx)
	require.NoError(t, err, "a reserved crawler must never be starved by a peer")

	releaseReserved()
	releaseWaiting()
}

// TestRetiredCrawlerReturnsReservedPermit proves the reservation is dynamic, not
// a static partition: a crawler that finished its prefixes hands its permit back
// so crawlers still working can use the full budget rather than the residue.
func TestRetiredCrawlerReturnsReservedPermit(t *testing.T) {
	budget := NewRequestBudget(2, 0)
	done, working := budget.lease(), budget.lease()
	require.Empty(t, budget.permits, "both permits are reserved")

	done.retire()
	require.Len(t, budget.permits, 1, "a retired crawler must return its reserved permit to the shared pool")

	// The still-working crawler can now hold two slots at once: its own
	// reservation plus the returned permit.
	first, err := working.acquireListSlot(context.Background())
	require.NoError(t, err)
	second, err := working.acquireListSlot(context.Background())
	require.NoError(t, err, "returned capacity must be usable by a crawler still working")
	first()
	second()
}

// TestRetireDuringActiveListingReturnsPermitOnRelease proves retiring while the
// reserved permit is mid-listing neither strands the permit nor invents a second
// one: it is handed back exactly once, when that listing ends.
func TestRetireDuringActiveListingReturnsPermitOnRelease(t *testing.T) {
	budget := NewRequestBudget(1, 0)
	lease := budget.lease()
	require.Empty(t, budget.permits)

	release, err := lease.acquireListSlot(context.Background())
	require.NoError(t, err)

	lease.retire()
	require.Empty(t, budget.permits, "a permit in use must not be handed back before its listing ends")

	release()
	require.Len(t, budget.permits, 1, "the permit returns exactly once, on release")

	// Retiring again must not fabricate capacity.
	lease.retire()
	require.Len(t, budget.permits, 1)
}

// TestRetiredLeaseDrawsFromSharedPool proves a retired lease that is somehow used
// again takes from the shared pool rather than re-using a permit it gave back.
func TestRetiredLeaseDrawsFromSharedPool(t *testing.T) {
	budget := NewRequestBudget(2, 0)
	lease := budget.lease()
	lease.retire()
	require.Len(t, budget.permits, 2)

	release, err := lease.acquireListSlot(context.Background())
	require.NoError(t, err)
	require.Len(t, budget.permits, 1)
	release()
	require.Len(t, budget.permits, 2)
}

// TestAcquireListSlotHonoursCancellation proves a crawler waiting on a saturated
// budget gives up on cancellation instead of holding the run open.
func TestAcquireListSlotHonoursCancellation(t *testing.T) {
	budget := NewRequestBudget(1, 0)
	holder, waiter := budget.lease(), budget.lease()
	release, err := holder.acquireListSlot(context.Background())
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	var acquired atomic.Bool
	done := make(chan error, 1)
	go func() {
		r, err := waiter.acquireListSlot(ctx)
		if err == nil {
			acquired.Store(true)
			r()
		}
		done <- err
	}()

	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
		assert.False(t, acquired.Load())
	case <-time.After(2 * time.Second):
		t.Fatal("acquireListSlot did not honour cancellation")
	}
}

// progressFailingWriter rejects the initial progress record, so a crawler can be
// made to return before its listing stage ever starts.
type progressFailingWriter struct{ mockWriter }

func (w *progressFailingWriter) WriteProgress(context.Context, *output.ProgressRecord) error {
	return errors.New("writer rejected progress")
}

// TestRunRetiresLeaseWhenItFailsBeforeListing pins the lease lifecycle on the
// failure path. Retirement used to happen only inside the listing stage, so a
// crawler that refused before listing — an initial progress write that fails,
// here — returned still holding its reservation. On a shared budget that permit
// was unreachable for the rest of the run, so one transient output failure
// permanently reduced aggregate capacity.
func TestRunRetiresLeaseWhenItFailsBeforeListing(t *testing.T) {
	budget := NewRequestBudget(1, 0)
	probe := &concurrencyProbe{}
	p := &probeProvider{probe: probe, delay: time.Millisecond}

	m, err := match.New(match.Config{Includes: []string{"**"}})
	require.NoError(t, err)
	failing := New(p, m, &progressFailingWriter{}, "fails-early", DefaultConfig()).
		WithRequestBudget(budget).
		WithPrefixes([]string{"a/"})
	_, err = failing.Run(context.Background())
	require.Error(t, err, "the crawler must surface its writer failure")

	// A healthy crawler sharing the same budget must still be able to list.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	healthy := laneCrawler(t, p, budget, []string{"b/"}, "healthy")
	_, err = healthy.Run(ctx)
	require.NoError(t, err, "a failed peer must not strand the shared budget's capacity")
	require.Greater(t, probe.peakConcurrency(), 0, "the healthy crawler must actually have listed")
}

// TestRebindingRequestBudgetDoesNotStrandCapacity pins the other boundary of the
// same defect class: replacing a crawler's budget must return the share it
// already held. Overwriting the lease left the first reservation owned by
// nothing, so on a one-permit budget the crawler could block forever waiting for
// a permit it was itself holding.
func TestRebindingRequestBudgetDoesNotStrandCapacity(t *testing.T) {
	budget := NewRequestBudget(1, 0)
	probe := &concurrencyProbe{}
	p := &probeProvider{probe: probe, delay: time.Millisecond}

	m, err := match.New(match.Config{Includes: []string{"**"}})
	require.NoError(t, err)
	c := New(p, m, newMockWriter(), "rebound", DefaultConfig()).
		WithRequestBudget(budget).
		WithRequestBudget(budget). // same budget, rebound
		WithRequestBudget(NewRequestBudget(1, 0)).
		WithRequestBudget(budget). // and back again
		WithPrefixes(manyPrefixes(3))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = c.Run(ctx)
	require.NoError(t, err, "rebinding must not deadlock the crawl")

	// Every permit must be back in the pool once the run is done.
	require.Len(t, budget.permits, budget.Concurrency(),
		"the budget must end the run with its full capacity")
}

// TestRetiredRunReturnsFullCapacity proves a completed run leaves the shared
// budget whole, so a reusable budget does not decay across runs.
func TestRetiredRunReturnsFullCapacity(t *testing.T) {
	const ceiling = 3
	budget := NewRequestBudget(ceiling, 0)
	probe := &concurrencyProbe{}
	p := &probeProvider{probe: probe, delay: time.Millisecond}

	for i := 0; i < 3; i++ {
		c := laneCrawler(t, p, budget, manyPrefixes(4), fmt.Sprintf("run-%d", i))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := c.Run(ctx)
		cancel()
		require.NoError(t, err)
		require.Len(t, budget.permits, ceiling, "run %d must return every permit", i)
	}
}
