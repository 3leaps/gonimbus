package reflowprobe

import (
	"context"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider"
)

const (
	// MaxAttempts is the bounded retry budget for throttled probe operations.
	MaxAttempts = 3
	// RetryDelay is the context-aware delay between throttled probe attempts.
	RetryDelay = 10 * time.Millisecond
)

// Limiter is the concurrency limiter surface required by probe retries.
type Limiter interface {
	Acquire(context.Context) (func(), error)
	ObserveProviderResult(error)
}

// Run executes op under limiter control and retries throttled provider results.
func Run[T any](ctx context.Context, limiter Limiter, op func(context.Context) (T, error)) (T, error) {
	var zero T
	for attempt := 1; ; attempt++ {
		release, err := limiter.Acquire(ctx)
		if err != nil {
			return zero, err
		}
		result, err := op(ctx)
		release()
		limiter.ObserveProviderResult(err)
		if err == nil || !provider.IsThrottled(err) || attempt >= MaxAttempts {
			return result, err
		}
		if err := sleep(ctx, RetryDelay); err != nil {
			return zero, err
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
