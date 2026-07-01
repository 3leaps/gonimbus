package reflowprobe

import (
	"context"
	"errors"
	"testing"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/require"
)

type retryLimiter struct {
	active           int
	acquires         int
	observed         []error
	observedReleased bool
}

func (l *retryLimiter) Acquire(context.Context) (func(), error) {
	l.active++
	l.acquires++
	return func() { l.active-- }, nil
}

func (l *retryLimiter) ObserveProviderResult(err error) {
	if l.active == 0 {
		l.observedReleased = true
	}
	l.observed = append(l.observed, err)
}

func TestRunRetriesOnlyThrottledResults(t *testing.T) {
	limiter := &retryLimiter{}
	attempts := 0

	got, err := Run(context.Background(), limiter, func(context.Context) (string, error) {
		attempts++
		if attempts == 1 {
			return "", provider.ErrThrottled
		}
		return "ok", nil
	})

	require.NoError(t, err)
	require.Equal(t, "ok", got)
	require.Equal(t, 2, attempts)
	require.Equal(t, 2, limiter.acquires)
	require.Len(t, limiter.observed, 2)
	require.True(t, limiter.observedReleased, "provider results must be observed after releasing the limiter slot")
}

func TestRunStopsOnNonThrottle(t *testing.T) {
	limiter := &retryLimiter{}
	want := errors.New("not retryable")

	_, err := Run(context.Background(), limiter, func(context.Context) (string, error) {
		return "", want
	})

	require.ErrorIs(t, err, want)
	require.Equal(t, 1, limiter.acquires)
	require.Len(t, limiter.observed, 1)
}

func TestRunReturnsContextCancellationDuringRetryDelay(t *testing.T) {
	limiter := &retryLimiter{}
	ctx, cancel := context.WithCancel(context.Background())

	_, err := Run(ctx, limiter, func(context.Context) (string, error) {
		cancel()
		return "", provider.ErrThrottled
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, limiter.acquires)
	require.Len(t, limiter.observed, 1)
}
