package reflow

import (
	"context"
	"errors"
	"io"
	"net"
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
	require.Equal(t, "resource_capped:memory:conservative_default", cfg.CeilingReason)
	require.Equal(t, 16, cfg.Initial)
	require.True(t, cfg.AdaptiveEnabled)
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
