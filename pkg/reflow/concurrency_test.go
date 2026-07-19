package reflow

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/transfer"
)

func TestResolveConcurrencyResourceCapFailLow(t *testing.T) {
	cfg := ResolveConcurrency(1000, true, ResourceProbe{
		MemoryLimitBytes: func() (int64, string, error) {
			return 0, "", errors.New("probe unavailable")
		},
		FDSoftLimit: func() (int64, error) {
			return 4096, nil
		},
	})

	require.Equal(t, 1000, cfg.RequestedCeiling)
	require.Equal(t, 16, cfg.EffectiveCeiling)
	require.Equal(t, "resource_capped:memory:detection_unavailable", cfg.CeilingReason)
	require.Equal(t, 16, cfg.Initial)
	require.True(t, cfg.AdaptiveEnabled)
}

func TestConcurrencyLimiterTimeAvgActiveDeterministic(t *testing.T) {
	limiter := NewConcurrencyLimiter(ConcurrencyConfig{
		RequestedCeiling: 4,
		EffectiveCeiling: 4,
		AdaptiveEnabled:  false,
	})
	base := time.Unix(1000, 0)
	step := 0
	steps := []time.Duration{0, time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second}
	limiter.clock = func() time.Time {
		d := steps[step]
		step++
		return base.Add(d)
	}
	limiter.startedAt = base
	limiter.lastTransition = base

	releaseA, err := limiter.Acquire(context.Background()) // t=0s: active 0->1
	require.NoError(t, err)
	releaseB, err := limiter.Acquire(context.Background()) // t=1s: active 1->2
	require.NoError(t, err)
	releaseA() // t=2s: active 2->1
	releaseB() // t=3s: active 1->0

	// Integral: 1s@1 + 1s@2 + 1s@1 + 1s@0 = 4 worker-seconds over 4 seconds.
	stats := limiter.Snapshot() // t=4s
	require.Equal(t, 1.0, stats.ConcurrencyTimeAvgActive)
	require.Equal(t, 2, stats.ConcurrencyMaxActive)
}

func TestConcurrencyLimiterOccupancyWindowExcludesSetup(t *testing.T) {
	limiter := NewConcurrencyLimiter(ConcurrencyConfig{
		RequestedCeiling: 4,
		EffectiveCeiling: 4,
		AdaptiveEnabled:  false,
	})
	base := time.Unix(2000, 0)
	step := 0
	steps := []time.Duration{
		10 * time.Second, // ResetOccupancyWindow: 10s of setup elapsed before the window opens
		10 * time.Second, // Acquire
		12 * time.Second, // release
		12 * time.Second, // Snapshot
	}
	limiter.clock = func() time.Time {
		d := steps[step]
		step++
		return base.Add(d)
	}
	limiter.startedAt = base
	limiter.lastTransition = base

	limiter.ResetOccupancyWindow()
	release, err := limiter.Acquire(context.Background())
	require.NoError(t, err)
	release()

	// 2s at one active worker over a 2s window: setup before the reset must
	// not dilute the average (unreset it would read 2/12 ≈ 0.167).
	stats := limiter.Snapshot()
	require.Equal(t, 1.0, stats.ConcurrencyTimeAvgActive)
}

func TestResolveConcurrencyWithBudgetArithmetic(t *testing.T) {
	const gib = int64(1 << 30)
	const mib = int64(1 << 20)
	fdOK := func() (int64, error) { return 4096, nil }
	probeWith := func(limit int64, source string, err error) ResourceProbe {
		return ResourceProbe{
			MemoryLimitBytes: func() (int64, string, error) { return limit, source, err },
			FDSoftLimit:      fdOK,
		}
	}

	t.Run("operator budget sizes the ceiling and is recorded", func(t *testing.T) {
		cfg := ResolveConcurrencyWithBudget(32, true, probeWith(gib, "cgroup_v2", nil), 128*mib)
		require.Equal(t, 8, cfg.EffectiveCeiling, "128MiB / 16MiB per-worker = 8")
		require.Equal(t, "resource_capped:memory:operator_budget", cfg.CeilingReason)
		require.Equal(t, gib, cfg.MemoryLimitBytes)
		require.Equal(t, "cgroup_v2", cfg.MemoryLimitSource)
		require.Equal(t, 128*mib, cfg.MemoryBudgetRequestedBytes)
		require.Equal(t, 128*mib, cfg.MemoryBudgetEffectiveBytes)
		require.Equal(t, "operator", cfg.MemoryBudgetSource)
	})

	t.Run("operator budget above detected limit clamps to the limit", func(t *testing.T) {
		cfg := ResolveConcurrencyWithBudget(128, true, probeWith(gib, "cgroup_v2", nil), 2*gib)
		require.Equal(t, 64, cfg.EffectiveCeiling, "clamped budget 1GiB / 16MiB = 64")
		require.Equal(t, "resource_capped:memory:operator_budget", cfg.CeilingReason)
		require.Equal(t, 2*gib, cfg.MemoryBudgetRequestedBytes)
		require.Equal(t, gib, cfg.MemoryBudgetEffectiveBytes)
		require.Equal(t, "operator_clamped_to_limit", cfg.MemoryBudgetSource)
	})

	t.Run("operator budget authoritative when detection unavailable", func(t *testing.T) {
		cfg := ResolveConcurrencyWithBudget(64, true, probeWith(0, "", errors.New("probe failed")), 2*gib)
		require.Equal(t, 64, cfg.EffectiveCeiling, "2GiB / 16MiB = 128 caps nothing at requested 64")
		require.Equal(t, "requested", cfg.CeilingReason)
		require.Equal(t, "detection_unavailable", cfg.MemoryLimitSource)
		require.Equal(t, 2*gib, cfg.MemoryBudgetRequestedBytes)
		require.Equal(t, 2*gib, cfg.MemoryBudgetEffectiveBytes)
		require.Equal(t, "operator", cfg.MemoryBudgetSource,
			"no detected limit means the operator value applies unclamped")
	})

	t.Run("derived budget records fraction arithmetic", func(t *testing.T) {
		cfg := ResolveConcurrency(1000, true, probeWith(gib, "physical_ram", nil))
		require.Equal(t, 16, cfg.EffectiveCeiling, "25% of 1GiB / 16MiB = 16")
		require.Equal(t, "resource_capped:memory:physical_ram", cfg.CeilingReason)
		require.Equal(t, gib, cfg.MemoryLimitBytes)
		require.Equal(t, "physical_ram", cfg.MemoryLimitSource)
		require.Zero(t, cfg.MemoryBudgetRequestedBytes)
		require.Equal(t, gib/4, cfg.MemoryBudgetEffectiveBytes)
		require.Equal(t, "derived", cfg.MemoryBudgetSource)
	})

	t.Run("resolved retry cap shares the budget bound", func(t *testing.T) {
		cfg := ResolveConcurrencyWithBudget(32, true, probeWith(gib, "cgroup_v2", nil), 128*mib)
		require.Equal(t, int64(16)*mib, cfg.RetryBufferCapBytes,
			"budget above the transfer default keeps the 16MiB per-copy cap")
	})

	t.Run("sub-16MiB detected limit is never exceeded (derived)", func(t *testing.T) {
		cfg := ResolveConcurrency(64, true, probeWith(8*mib, "cgroup_v2", nil))
		require.Equal(t, 2*mib, cfg.MemoryBudgetEffectiveBytes, "25% of 8MiB")
		require.LessOrEqual(t, cfg.MemoryBudgetEffectiveBytes, cfg.MemoryLimitBytes)
		require.Equal(t, 2*mib, cfg.RetryBufferCapBytes, "per-copy cap shrinks to the budget")
		require.Equal(t, 1, cfg.EffectiveCeiling)
	})

	t.Run("sub-16MiB detected limit is never exceeded (operator clamp)", func(t *testing.T) {
		cfg := ResolveConcurrencyWithBudget(4, true, probeWith(mib, "cgroup_v2", nil), 64*mib)
		require.Equal(t, "operator_clamped_to_limit", cfg.MemoryBudgetSource)
		require.Equal(t, mib, cfg.MemoryBudgetEffectiveBytes,
			"a record claiming clamped-to-limit must not admit above the limit")
		require.LessOrEqual(t, cfg.MemoryBudgetEffectiveBytes, cfg.MemoryLimitBytes)
		require.Equal(t, mib, cfg.RetryBufferCapBytes)
		require.Equal(t, 1, cfg.EffectiveCeiling)
	})

	t.Run("budget never exceeds any detected limit", func(t *testing.T) {
		for _, limit := range []int64{mib, 8 * mib, 64 * mib, gib, 64 * gib} {
			for _, operator := range []int64{0, 64 * mib, 2 * gib} {
				cfg := ResolveConcurrencyWithBudget(256, true, probeWith(limit, "cgroup_v2", nil), operator)
				require.LessOrEqual(t, cfg.MemoryBudgetEffectiveBytes, cfg.MemoryLimitBytes,
					"limit=%d operator=%d", limit, operator)
				require.LessOrEqual(t, cfg.RetryBufferCapBytes, cfg.MemoryBudgetEffectiveBytes,
					"limit=%d operator=%d", limit, operator)
			}
		}
	})

	t.Run("direct construction falls back to the transfer default cap", func(t *testing.T) {
		limiter := NewConcurrencyLimiter(ConcurrencyConfig{RequestedCeiling: 4, EffectiveCeiling: 4})
		require.Equal(t, transfer.DefaultRetryBufferMaxMemoryBytes, limiter.RetryBufferCap())
	})

	t.Run("limiter snapshot carries the memory fields", func(t *testing.T) {
		cfg := ResolveConcurrencyWithBudget(32, true, probeWith(gib, "cgroup_v2", nil), 128*mib)
		stats := NewConcurrencyLimiter(cfg).Snapshot()
		require.Equal(t, gib, stats.MemoryLimitBytes)
		require.Equal(t, "cgroup_v2", stats.MemoryLimitSource)
		require.Equal(t, 128*mib, stats.MemoryBudgetRequestedBytes)
		require.Equal(t, 128*mib, stats.MemoryBudgetEffectiveBytes)
		require.Equal(t, "operator", stats.MemoryBudgetSource)
	})
}

func TestMemoryLimitFromChainPrecedence(t *testing.T) {
	const gib = int64(1 << 30)
	platform := func(limit int64, source string, err error) func() (int64, string, error) {
		return func() (int64, string, error) { return limit, source, err }
	}
	physical := func(limit int64, err error) func() (int64, error) {
		return func() (int64, error) { return limit, err }
	}
	cases := []struct {
		name       string
		platform   func() (int64, string, error)
		runtime    func() int64
		physical   func() (int64, error)
		wantLimit  int64
		wantSource string
	}{
		{
			name:       "cgroup binds when it is the lowest candidate",
			platform:   platform(2*gib, "cgroup_v2", nil),
			runtime:    func() int64 { return 8 * gib },
			physical:   physical(64*gib, nil),
			wantLimit:  2 * gib,
			wantSource: "cgroup_v2",
		},
		{
			name:       "physical RAM binds below a higher runtime limit",
			platform:   platform(0, "", nil),
			runtime:    func() int64 { return 64 * gib },
			physical:   physical(8*gib, nil),
			wantLimit:  8 * gib,
			wantSource: "physical_ram",
		},
		{
			name:       "physical RAM binds below a higher cgroup limit",
			platform:   platform(32*gib, "cgroup_v2", nil),
			runtime:    func() int64 { return 0 },
			physical:   physical(8*gib, nil),
			wantLimit:  8 * gib,
			wantSource: "physical_ram",
		},
		{
			name:       "runtime binds when it is the lowest of all three",
			platform:   platform(2*gib, "cgroup_v2", nil),
			runtime:    func() int64 { return 1 * gib },
			physical:   physical(8*gib, nil),
			wantLimit:  1 * gib,
			wantSource: "runtime",
		},
		{
			name:       "platform error still considers remaining candidates",
			platform:   platform(0, "", errors.New("unreadable")),
			runtime:    func() int64 { return 8 * gib },
			physical:   physical(64*gib, nil),
			wantLimit:  8 * gib,
			wantSource: "runtime",
		},
		{
			name:       "physical probe error still considers remaining candidates",
			platform:   platform(0, "", nil),
			runtime:    func() int64 { return 8 * gib },
			physical:   physical(0, errors.New("probe failed")),
			wantLimit:  8 * gib,
			wantSource: "runtime",
		},
		{
			name:       "no candidate falls back to conservative default",
			platform:   platform(0, "", errors.New("unreadable")),
			runtime:    func() int64 { return 0 },
			physical:   physical(0, errors.New("probe failed")),
			wantLimit:  resourceDefaultMemoryLimit,
			wantSource: "detection_unavailable",
		},
		{
			name:       "absent candidates fall back to conservative default",
			platform:   platform(0, "", nil),
			runtime:    func() int64 { return 0 },
			physical:   physical(0, nil),
			wantLimit:  resourceDefaultMemoryLimit,
			wantSource: "detection_unavailable",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			limit, source, err := memoryLimitFromChain(tc.platform, tc.runtime, tc.physical)
			require.NoError(t, err)
			require.Equal(t, tc.wantLimit, limit)
			require.Equal(t, tc.wantSource, source)
		})
	}
}

func TestDefaultMemoryLimitDetectionSmoke(t *testing.T) {
	limit, source, err := defaultMemoryLimitBytes()
	require.NoError(t, err)
	require.Positive(t, limit)
	require.Contains(t, []string{
		"cgroup_v2", "cgroup_v1", "runtime", "physical_ram", "detection_unavailable",
	}, source)
	if runtime.GOOS == "darwin" || runtime.GOOS == "linux" {
		require.NotEqual(t, "detection_unavailable", source,
			"memory detection must succeed on %s: physical RAM is always probeable", runtime.GOOS)
	}
}

func TestPhysicalMemoryProbeSmoke(t *testing.T) {
	limit, err := defaultPhysicalMemoryBytes()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		require.Zero(t, limit)
		return
	}
	require.NoError(t, err)
	require.Positive(t, limit)
}

func TestConcurrencyLimiterThrottleAndConnectionFreeze(t *testing.T) {
	limiter := NewConcurrencyLimiter(ConcurrencyConfig{
		RequestedCeiling: 8,
		EffectiveCeiling: 8,
		CeilingReason:    "requested",
		AdaptiveEnabled:  true,
		Floor:            1,
		Initial:          4,
	})

	limiter.ObserveThrottle()
	snapshot := limiter.Snapshot()
	require.Equal(t, 2, snapshot.ConcurrencyFinal)
	require.Equal(t, int64(1), snapshot.ConcurrencyThrottleBackoffs)

	for i := 0; i < concurrencyThrottleCooldown+concurrencyCleanIncreaseEvery-1; i++ {
		limiter.ObserveSuccess()
	}
	require.Equal(t, 2, limiter.Snapshot().ConcurrencyFinal)

	limiter.ObserveConnectionError()
	for i := 0; i < concurrencyCleanIncreaseEvery-1; i++ {
		limiter.ObserveSuccess()
	}
	snapshot = limiter.Snapshot()
	require.Equal(t, 2, snapshot.ConcurrencyFinal)
	require.Equal(t, int64(1), snapshot.ConcurrencyConnectionErrorFreezes)

	limiter.ObserveSuccess()
	snapshot = limiter.Snapshot()
	require.Equal(t, 3, snapshot.ConcurrencyFinal)
	require.Equal(t, int64(1), snapshot.ConcurrencyAdditiveIncreases)
}

func TestConcurrencyLimiterObserveProviderResult(t *testing.T) {
	limiter := NewConcurrencyLimiter(ConcurrencyConfig{
		RequestedCeiling: 8,
		EffectiveCeiling: 8,
		CeilingReason:    "requested",
		AdaptiveEnabled:  true,
		Floor:            1,
		Initial:          4,
	})

	limiter.ObserveProviderResult(&provider.ProviderError{
		Op:       "PutObject",
		Provider: provider.ProviderS3,
		Err:      provider.ErrThrottled,
	})
	require.Equal(t, 2, limiter.Snapshot().ConcurrencyFinal)

	limiter.ObserveProviderResult(&provider.ProviderError{
		Op:       "PutObject",
		Provider: provider.ProviderS3,
		Err:      provider.ErrProviderUnavailable,
	})
	require.Equal(t, int64(1), limiter.Snapshot().ConcurrencyConnectionErrorFreezes)
	require.Equal(t, 2, limiter.Snapshot().ConcurrencyFinal)
}

func TestConcurrencyConnectionError(t *testing.T) {
	require.True(t, ConcurrencyConnectionError(io.ErrUnexpectedEOF))
	require.True(t, ConcurrencyConnectionError(&net.DNSError{Name: "object-store.example", Err: "no such host"}))
	require.True(t, ConcurrencyConnectionError(&provider.ProviderError{Op: "GetObject", Provider: provider.ProviderS3, Err: syscall.ECONNRESET}))
	require.True(t, ConcurrencyConnectionError(errors.New("net/http: TLS handshake timeout")))
	require.False(t, ConcurrencyConnectionError(provider.ErrThrottled))
}

func TestResolveConcurrencyNoAdaptiveStartsAtEffectiveCeiling(t *testing.T) {
	cfg := ResolveConcurrency(100, false, ResourceProbe{
		MemoryLimitBytes: func() (int64, string, error) {
			return transfer.DefaultRetryBufferMaxMemoryBytes * 12, "test", nil
		},
		FDSoftLimit: func() (int64, error) {
			return 4096, nil
		},
	})

	require.False(t, cfg.AdaptiveEnabled)
	require.Equal(t, 3, cfg.EffectiveCeiling)
	require.Equal(t, 3, cfg.Initial)
}

func TestConcurrencyLimiterAcquireHonorsCurrentConcurrency(t *testing.T) {
	limiter := NewConcurrencyLimiter(ConcurrencyConfig{
		RequestedCeiling: 2,
		EffectiveCeiling: 2,
		CeilingReason:    "requested",
		AdaptiveEnabled:  false,
		Floor:            1,
		Initial:          2,
	})

	release1, err := limiter.Acquire(testContext(t))
	require.NoError(t, err)
	defer release1()
	release2, err := limiter.Acquire(testContext(t))
	require.NoError(t, err)
	defer release2()

	ctx, cancel := contextWithTimeout(t, 20*time.Millisecond)
	defer cancel()
	_, err = limiter.Acquire(ctx)
	require.Error(t, err)
	require.Equal(t, 2, limiter.Snapshot().ConcurrencyMaxActive)
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := contextWithTimeout(t, time.Second)
	t.Cleanup(cancel)
	return ctx
}

func contextWithTimeout(t *testing.T, d time.Duration) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), d)
}
