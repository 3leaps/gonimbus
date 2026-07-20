package reflow

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/transfer"
)

func TestCopyReservationBytesArithmetic(t *testing.T) {
	const cap16 = int64(16) << 20
	cases := []struct {
		name string
		size int64
		cap  int64
		want int64
	}{
		{name: "small known size reserves the size", size: 1 << 10, cap: cap16, want: 1 << 10},
		{name: "size at cap reserves the cap", size: cap16, cap: cap16, want: cap16},
		{name: "size above cap reserves the cap (spooled body)", size: cap16 * 4, cap: cap16, want: cap16},
		{name: "unknown size reserves the conservative cap", size: -1, cap: cap16, want: cap16},
		{name: "absent size (zero) reserves the conservative cap — never inferred known-empty", size: 0, cap: cap16, want: cap16},
		{name: "shrunken cap bounds the reservation", size: 3 << 20, cap: 2 << 20, want: 2 << 20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, copyReservationBytes(tc.size, tc.cap))
		})
	}
}

func TestMemoryLedgerSmallObjectsFullWidth(t *testing.T) {
	ledger := newMemoryLedger(16 << 20)
	var releases []func()
	for i := 0; i < 32; i++ {
		release, err := ledger.Reserve(context.Background(), 1<<10)
		require.NoError(t, err)
		releases = append(releases, release)
	}
	stats := ledger.Stats()
	require.Zero(t, stats.Waits, "32 KiB of small reservations must admit without waiting under a 16MiB budget")
	require.Equal(t, int64(32)<<10, stats.PeakReservedBytes)
	for _, release := range releases {
		release()
	}
}

func TestMemoryLedgerLargeObjectsSelfLimit(t *testing.T) {
	const mib = int64(1) << 20
	ledger := newMemoryLedger(4 * mib)

	var held []func()
	for i := 0; i < 4; i++ {
		release, err := ledger.Reserve(context.Background(), mib)
		require.NoError(t, err)
		held = append(held, release)
	}

	granted := make(chan struct{})
	go func() {
		release, err := ledger.Reserve(context.Background(), mib)
		require.NoError(t, err)
		defer release()
		close(granted)
	}()

	select {
	case <-granted:
		t.Fatal("fifth 1MiB reservation must block under a full 4MiB budget")
	case <-time.After(50 * time.Millisecond):
	}

	held[0]()
	select {
	case <-granted:
	case <-time.After(2 * time.Second):
		t.Fatal("released capacity must grant the queued waiter")
	}

	stats := ledger.Stats()
	require.Equal(t, int64(1), stats.Waits)
	require.Equal(t, 4*mib, stats.PeakReservedBytes, "peak admission never exceeds the budget")
	for _, release := range held[1:] {
		release()
	}
}

func TestMemoryLedgerFIFOHeadOfLine(t *testing.T) {
	const mib = int64(1) << 20
	ledger := newMemoryLedger(4 * mib)

	holdRelease, err := ledger.Reserve(context.Background(), 3*mib)
	require.NoError(t, err)

	largeGranted := make(chan struct{})
	go func() {
		release, err := ledger.Reserve(context.Background(), 2*mib)
		require.NoError(t, err)
		defer release()
		close(largeGranted)
	}()
	// Ensure the large waiter is enqueued before the small one.
	require.Eventually(t, func() bool {
		ledger.mu.Lock()
		defer ledger.mu.Unlock()
		return len(ledger.queue) == 1
	}, time.Second, time.Millisecond)

	smallGranted := make(chan struct{})
	go func() {
		release, err := ledger.Reserve(context.Background(), mib)
		require.NoError(t, err)
		defer release()
		close(smallGranted)
	}()

	// The small request would fit right now (3+1 <= 4), but it must not jump
	// the queued large head — that head-of-line discipline is the starvation
	// guarantee.
	select {
	case <-smallGranted:
		t.Fatal("small waiter must not be granted ahead of the queued large waiter")
	case <-largeGranted:
		t.Fatal("large waiter cannot be granted while 3MiB is held")
	case <-time.After(50 * time.Millisecond):
	}

	holdRelease()
	select {
	case <-largeGranted:
	case <-time.After(2 * time.Second):
		t.Fatal("large head must be granted once capacity frees")
	}
	select {
	case <-smallGranted:
	case <-time.After(2 * time.Second):
		t.Fatal("small waiter must be granted after the head")
	}
}

func TestMemoryLedgerCancelledHeadPromotesAdmissibleFollower(t *testing.T) {
	const mib = int64(1) << 20
	ledger := newMemoryLedger(4 * mib)

	holdRelease, err := ledger.Reserve(context.Background(), 3*mib)
	require.NoError(t, err)

	headCtx, cancelHead := context.WithCancel(context.Background())
	headErr := make(chan error, 1)
	go func() {
		_, reserveErr := ledger.Reserve(headCtx, 2*mib)
		headErr <- reserveErr
	}()
	require.Eventually(t, func() bool {
		ledger.mu.Lock()
		defer ledger.mu.Unlock()
		return len(ledger.queue) == 1
	}, time.Second, time.Millisecond)

	followerGranted := make(chan struct{})
	go func() {
		release, reserveErr := ledger.Reserve(context.Background(), mib)
		require.NoError(t, reserveErr)
		defer release()
		close(followerGranted)
	}()
	require.Eventually(t, func() bool {
		ledger.mu.Lock()
		defer ledger.mu.Unlock()
		return len(ledger.queue) == 2
	}, time.Second, time.Millisecond)

	// The follower fits (3+1 <= 4) but must stay behind the queued 2MiB head.
	select {
	case <-followerGranted:
		t.Fatal("follower must not bypass the queued head")
	case <-time.After(50 * time.Millisecond):
	}

	// Cancelling the head must promote the now-admissible follower promptly —
	// WITHOUT the unrelated 3MiB holder releasing first.
	cancelHead()
	select {
	case reserveErr := <-headErr:
		require.ErrorIs(t, reserveErr, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled head must return promptly")
	}
	select {
	case <-followerGranted:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelling the head must drain the now-admissible follower")
	}

	holdRelease()
	ledger.mu.Lock()
	require.Zero(t, ledger.reserved)
	require.Empty(t, ledger.queue)
	ledger.mu.Unlock()
}

func TestMemoryLedgerCancellationReleasesWaiter(t *testing.T) {
	const mib = int64(1) << 20
	ledger := newMemoryLedger(2 * mib)

	holdRelease, err := ledger.Reserve(context.Background(), 2*mib)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, reserveErr := ledger.Reserve(ctx, mib)
		errCh <- reserveErr
	}()
	require.Eventually(t, func() bool {
		ledger.mu.Lock()
		defer ledger.mu.Unlock()
		return len(ledger.queue) == 1
	}, time.Second, time.Millisecond)

	cancel()
	select {
	case reserveErr := <-errCh:
		require.ErrorIs(t, reserveErr, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled waiter must return promptly")
	}

	// No reservation or queue entry leaks: the full budget is reservable again.
	holdRelease()
	release, err := ledger.Reserve(context.Background(), 2*mib)
	require.NoError(t, err)
	release()
	ledger.mu.Lock()
	require.Zero(t, ledger.reserved)
	require.Empty(t, ledger.queue)
	ledger.mu.Unlock()
}

func TestMemoryLedgerReleaseExactlyOnce(t *testing.T) {
	const mib = int64(1) << 20
	ledger := newMemoryLedger(2 * mib)
	release, err := ledger.Reserve(context.Background(), mib)
	require.NoError(t, err)
	release()
	release() // second call must be a no-op, never double-crediting capacity
	ledger.mu.Lock()
	require.Zero(t, ledger.reserved, "double release must not drive reservation negative")
	ledger.mu.Unlock()
	got, err := ledger.Reserve(context.Background(), 2*mib)
	require.NoError(t, err)
	got()
}

func TestMemoryLedgerMixedSizesMakeProgress(t *testing.T) {
	const mib = int64(1) << 20
	ledger := newMemoryLedger(4 * mib)
	sizes := []int64{mib, 4 * mib, 512 << 10, 2 * mib, 4 * mib, 64 << 10, 3 * mib, mib}

	var wg sync.WaitGroup
	var completed atomic.Int64
	for _, size := range sizes {
		wg.Add(1)
		go func(need int64) {
			defer wg.Done()
			release, err := ledger.Reserve(context.Background(), need)
			require.NoError(t, err)
			time.Sleep(time.Millisecond)
			release()
			completed.Add(1)
		}(size)
	}
	wg.Wait()
	require.Equal(t, int64(len(sizes)), completed.Load(), "mixed-size workload must fully drain without starvation")
	stats := ledger.Stats()
	require.LessOrEqual(t, stats.PeakReservedBytes, 4*mib)
}

func TestLimiterNormalizesLedgerCapAgainstBudget(t *testing.T) {
	const mib = int64(1) << 20

	t.Run("absent cap derives min(default, budget)", func(t *testing.T) {
		limiter := NewConcurrencyLimiter(ConcurrencyConfig{
			RequestedCeiling:           4,
			EffectiveCeiling:           4,
			MemoryBudgetEffectiveBytes: mib,
		})
		require.Equal(t, mib, limiter.RetryBufferCap(),
			"a budgeted config may never fall back to an allocator cap above its budget")
		release, err := limiter.ReserveCopyMemory(context.Background(), 2*mib)
		require.NoError(t, err)
		release()
		stats := limiter.Snapshot()
		require.Equal(t, mib, stats.MemoryReservedPeakBytes,
			"reservation and allocator cap must agree")
		require.Equal(t, mib, stats.RetryBufferCapBytes,
			"the normalized cap must be recorded, not zero")
	})

	t.Run("over-budget explicit cap clamps to the budget", func(t *testing.T) {
		limiter := NewConcurrencyLimiter(ConcurrencyConfig{
			RequestedCeiling:           4,
			EffectiveCeiling:           4,
			MemoryBudgetEffectiveBytes: mib,
			RetryBufferCapBytes:        16 * mib,
		})
		require.Equal(t, mib, limiter.RetryBufferCap())
		require.Equal(t, mib, limiter.Snapshot().RetryBufferCapBytes)
	})

	t.Run("genuinely unbudgeted config keeps the transfer default fallback", func(t *testing.T) {
		limiter := NewConcurrencyLimiter(ConcurrencyConfig{RequestedCeiling: 4, EffectiveCeiling: 4})
		require.Equal(t, transfer.DefaultRetryBufferMaxMemoryBytes, limiter.RetryBufferCap())
		require.Zero(t, limiter.Snapshot().RetryBufferCapBytes)
	})
}

func TestLimiterReserveCopyMemoryIntegration(t *testing.T) {
	const mib = int64(1) << 20
	cfg := ConcurrencyConfig{
		RequestedCeiling:           8,
		EffectiveCeiling:           8,
		AdaptiveEnabled:            false,
		MemoryBudgetEffectiveBytes: 4 * mib,
		RetryBufferCapBytes:        2 * mib,
	}
	limiter := NewConcurrencyLimiter(cfg)

	release, err := limiter.ReserveCopyMemory(context.Background(), -1)
	require.NoError(t, err)
	release()
	stats := limiter.Snapshot()
	require.Equal(t, 2*mib, stats.MemoryReservedPeakBytes,
		"unknown size must reserve the conservative retry cap")
	require.Zero(t, stats.MemoryReservationWaits)

	// Configs without a resolved budget govern nothing and never block.
	unbudgeted := NewConcurrencyLimiter(ConcurrencyConfig{RequestedCeiling: 4, EffectiveCeiling: 4})
	releaseNoop, err := unbudgeted.ReserveCopyMemory(context.Background(), 1<<30)
	require.NoError(t, err)
	releaseNoop()
	require.Zero(t, unbudgeted.Snapshot().MemoryReservedPeakBytes)
}
