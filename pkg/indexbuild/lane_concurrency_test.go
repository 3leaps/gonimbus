package indexbuild

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// rendezvousClock releases callers only once `width` of them have arrived at
// once, so a test can prove the engine really does call Clock concurrently
// rather than merely tolerating a clock that is safe to call concurrently.
//
// If the engine serialized clock calls — the global per-record lock this slice
// deliberately does not take — the first caller would wait for a second that
// cannot arrive, and the rendezvous times out.
type rendezvousClock struct {
	width int
	value time.Time

	mu       sync.Mutex
	waiting  int
	released chan struct{}
	timedOut bool
	metOnce  bool
}

func newRendezvousClock(width int, value time.Time) *rendezvousClock {
	return &rendezvousClock{width: width, value: value, released: make(chan struct{})}
}

func (c *rendezvousClock) Now() time.Time {
	c.mu.Lock()
	if c.metOnce || c.timedOut {
		c.mu.Unlock()
		return c.value
	}
	c.waiting++
	if c.waiting >= c.width {
		c.metOnce = true
		close(c.released)
		c.mu.Unlock()
		return c.value
	}
	released := c.released
	c.mu.Unlock()

	select {
	case <-released:
	case <-time.After(5 * time.Second):
		c.mu.Lock()
		c.timedOut = true
		if !c.metOnce {
			close(c.released)
			c.metOnce = true
		}
		c.mu.Unlock()
	}
	return c.value
}

func (c *rendezvousClock) concurrentCallsObserved() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.metOnce && !c.timedOut
}

// TestLaneWritersCallClockConcurrently pins the documented Clock contract as a
// real requirement rather than a note: a multi-lane build stamps observation
// times from several lanes at once.
//
// It also pins that the engine does not take a global per-record lock around the
// clock. Such a lock would serialize these calls and the rendezvous could never
// be met.
func TestLaneWritersCallClockConcurrently(t *testing.T) {
	clock := newRendezvousClock(2, time.Date(2026, 7, 7, 12, 2, 0, 0, time.UTC))
	cfg := laneTestConfig(t, "lanes-clock", laneSitePrefixes(4))
	cfg.MaxJournalLanes = 4
	cfg.Crawl.Concurrency = 4
	cfg.Clock = clock.Now

	_, err := NewRunner(cfg).Build(context.Background())
	require.NoError(t, err)
	require.True(t, clock.concurrentCallsObserved(),
		"lane writers must stamp observations concurrently; a serialized clock call would never meet the rendezvous")
}

// unguardedEventSink deliberately holds mutable state with no synchronization.
// It stands in for a caller-supplied sink written before lanes existed, when
// engine event delivery was single-goroutine by construction.
type unguardedEventSink struct {
	events []Event
}

func (s *unguardedEventSink) OnEvent(_ context.Context, event Event) error {
	s.events = append(s.events, event)
	return nil
}

// deniedProvider fails every listing with a non-fatal access-denied error, so
// each lane emits error records — and with them engine events — concurrently.
type deniedProvider struct{}

func (deniedProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return nil, provider.ErrAccessDenied
}
func (deniedProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}
func (deniedProvider) Close() error { return nil }

// TestConcurrentLaneEventsAreSerialized pins the EventSink disposition. Crawl
// errors are reported from every lane at once through one caller-supplied sink
// that promises nothing about concurrency, so the engine serializes delivery at
// the runner boundary instead of widening the sink's contract.
//
// Run under -race, an unserialized delivery path is a data race on the sink's
// own state.
func TestConcurrentLaneEventsAreSerialized(t *testing.T) {
	sink := &unguardedEventSink{}
	cfg := laneTestConfig(t, "lanes-events", laneSitePrefixes(4))
	cfg.MaxJournalLanes = 4
	cfg.Crawl.Concurrency = 4
	cfg.Source = Source{Provider: deniedProvider{}, ProviderName: "s3"}
	cfg.Events = sink

	// Every lane's listing is denied, so the run reports errors and publishes
	// nothing. The point here is the delivery path, not the outcome.
	_, err := NewRunner(cfg).Build(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "errors; snapshot not published")
	require.NotEmpty(t, sink.events, "each lane must have reported its crawl error")
}

// TestSerializedEventSinkDeliversOneAtATime pins the wrapper directly, so the
// property holds even where the engine is not the caller.
func TestSerializedEventSinkDeliversOneAtATime(t *testing.T) {
	inner := &unguardedEventSink{}
	sink := newSerializedEventSink(inner)
	require.NotNil(t, sink)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			require.NoError(t, sink.OnEvent(context.Background(), Event{Type: EventTypeCrawlError, RunID: "run"}))
		}(i)
	}
	wg.Wait()
	require.Len(t, inner.events, 16, "every delivery lands exactly once")
}

// TestSerializedEventSinkKeepsNilDisabled proves wrapping does not turn a
// disabled sink into an active one.
func TestSerializedEventSinkKeepsNilDisabled(t *testing.T) {
	require.Nil(t, newSerializedEventSink(nil))
}
