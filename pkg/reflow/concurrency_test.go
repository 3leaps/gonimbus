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
			name:       "cgroup limit wins over runtime and physical",
			platform:   platform(2*gib, "cgroup_v2", nil),
			runtime:    func() int64 { return 8 * gib },
			physical:   physical(64*gib, nil),
			wantLimit:  2 * gib,
			wantSource: "cgroup_v2",
		},
		{
			name:       "platform error falls through to runtime",
			platform:   platform(0, "", errors.New("unreadable")),
			runtime:    func() int64 { return 8 * gib },
			physical:   physical(64*gib, nil),
			wantLimit:  8 * gib,
			wantSource: "runtime",
		},
		{
			name:       "platform absent falls through to runtime",
			platform:   platform(0, "", nil),
			runtime:    func() int64 { return 8 * gib },
			physical:   physical(64*gib, nil),
			wantLimit:  8 * gib,
			wantSource: "runtime",
		},
		{
			name:       "no runtime limit falls through to physical RAM",
			platform:   platform(0, "", nil),
			runtime:    func() int64 { return 0 },
			physical:   physical(64*gib, nil),
			wantLimit:  64 * gib,
			wantSource: "physical_ram",
		},
		{
			name:       "physical probe error falls back to conservative default",
			platform:   platform(0, "", nil),
			runtime:    func() int64 { return 0 },
			physical:   physical(0, errors.New("probe failed")),
			wantLimit:  resourceDefaultMemoryLimit,
			wantSource: "detection_unavailable",
		},
		{
			name:       "physical probe absent falls back to conservative default",
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
