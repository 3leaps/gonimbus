package reflow

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/require"
)

// poolConcurrency returns a fixed (non-adaptive) concurrency config so pool
// tests are independent of host resource probing.
func poolConcurrency(ceiling int) ConcurrencyConfig {
	return ConcurrencyConfig{
		RequestedCeiling: ceiling,
		EffectiveCeiling: ceiling,
		CeilingReason:    "requested",
		AdaptiveEnabled:  false,
		Floor:            1,
		Initial:          ceiling,
	}
}

func poolInputLines(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/%02d.xml","source_key":"a/%02d.xml","source_etag":"etag-%02d","source_size_bytes":7,"dest_rel_key":"a/%02d.xml"}}`+"\n", i, i, i, i)
	}
	return b.String()
}

func seedPoolFixtures(src *copyMemoryProvider, n int) {
	for i := 0; i < n; i++ {
		src.putFixture(fmt.Sprintf("a/%02d.xml", i), "payload", fmt.Sprintf("etag-%02d", i))
	}
}

// barrierGetProvider wraps a copyMemoryProvider and records the maximum number
// of concurrent GetObject calls. The first floor entrants wait on a barrier
// until floor calls are in flight, with a bounded fail-open deadline so a
// serial execution path fails the max-in-flight assertion instead of hanging.
type barrierGetProvider struct {
	*copyMemoryProvider
	floor    int64
	deadline time.Duration

	entered     atomic.Int64
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
	releaseOnce sync.Once
	release     chan struct{}
}

func newBarrierGetProvider(base *copyMemoryProvider, floor int, deadline time.Duration) *barrierGetProvider {
	return &barrierGetProvider{
		copyMemoryProvider: base,
		floor:              int64(floor),
		deadline:           deadline,
		release:            make(chan struct{}),
	}
}

func (p *barrierGetProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	cur := p.inFlight.Add(1)
	defer p.inFlight.Add(-1)
	for {
		max := p.maxInFlight.Load()
		if cur <= max || p.maxInFlight.CompareAndSwap(max, cur) {
			break
		}
	}
	if p.entered.Add(1) == p.floor {
		p.releaseOnce.Do(func() { close(p.release) })
	}
	select {
	case <-p.release:
	case <-time.After(p.deadline):
		// Fail open: a serial path proceeds and the assertion fails later.
		p.releaseOnce.Do(func() { close(p.release) })
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
	return p.copyMemoryProvider.GetObject(ctx, key)
}

// serializedGuardSink fails the run if the engine ever delivers two sink
// callbacks concurrently — the EventSink serialization contract.
type serializedGuardSink struct {
	collectSink
	inFlight atomic.Int32
	violated atomic.Bool
}

func (s *serializedGuardSink) enter() func() {
	if s.inFlight.Add(1) > 1 {
		s.violated.Store(true)
	}
	return func() { s.inFlight.Add(-1) }
}

func (s *serializedGuardSink) OnRun(ctx context.Context, rec RunRecord) error {
	defer s.enter()()
	return s.collectSink.OnRun(ctx, rec)
}

func (s *serializedGuardSink) OnRecord(ctx context.Context, rec Record) error {
	defer s.enter()()
	return s.collectSink.OnRecord(ctx, rec)
}

func (s *serializedGuardSink) OnWarning(ctx context.Context, w Warning) error {
	defer s.enter()()
	return s.collectSink.OnWarning(ctx, w)
}

func (s *serializedGuardSink) OnError(ctx context.Context, e ErrorEvent) error {
	defer s.enter()()
	return s.collectSink.OnError(ctx, e)
}

func (s *serializedGuardSink) OnSummary(ctx context.Context, rec SummaryRecord) error {
	defer s.enter()()
	return s.collectSink.OnSummary(ctx, rec)
}

// memCheckpoint is a minimal in-memory CheckpointStore for engine pool tests.
type memCheckpoint struct {
	mu       sync.Mutex
	done     map[string]string // sourceURI|destURI -> status
	items    []CheckpointItem
	observed map[string]bool
}

func newMemCheckpoint() *memCheckpoint {
	return &memCheckpoint{done: map[string]string{}, observed: map[string]bool{}}
}

func (m *memCheckpoint) key(sourceURI, destURI string) string { return sourceURI + "|" + destURI }

func (m *memCheckpoint) markDone(sourceURI, destURI, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.done[m.key(sourceURI, destURI)] = status
}

func (m *memCheckpoint) ItemDone(_ context.Context, sourceURI, destURI string) (bool, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	status, ok := m.done[m.key(sourceURI, destURI)]
	return ok, status, nil
}

func (m *memCheckpoint) UpsertItem(_ context.Context, item CheckpointItem) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = append(m.items, item)
	return nil
}

func (m *memCheckpoint) DestKeyObserved(_ context.Context, destKey string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.observed[destKey], nil
}

func (m *memCheckpoint) MarkDestKeyObserved(_ context.Context, destKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observed[destKey] = true
	return nil
}

func (m *memCheckpoint) NoteDestKeySource(context.Context, string, string, string, int64) error {
	return nil
}
func (m *memCheckpoint) NoteCollision(context.Context, CheckpointCollision) error { return nil }
func (m *memCheckpoint) Close() error                                             { return nil }

func (m *memCheckpoint) itemStatuses() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]int{}
	for _, item := range m.items {
		out[item.Status]++
	}
	return out
}

// TestRunnerRecordStreamPoolMaxInFlight is the engine half of the dual-path
// behavioral gate: the record-stream runner must fan copies out to the resolved
// concurrency ceiling, proven by observed max in-flight — the assertion the
// pre-GON-058 serial loop fails.
func TestRunnerRecordStreamPoolMaxInFlight(t *testing.T) {
	const (
		objects      = 32
		ceiling      = 8
		barrierFloor = 4
	)
	srcBase := newCopyMemoryProvider()
	seedPoolFixtures(srcBase, objects)
	src := newBarrierGetProvider(srcBase, barrierFloor, 2*time.Second)
	dst := newCopyMemoryProvider()

	sink := &serializedGuardSink{}
	cfg := copyConfig(dst, sink)
	cfg.Concurrency = poolConcurrency(ceiling)
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	summary, err := runner.Run(context.Background(), RecordStreamSource{
		Records: strings.NewReader(poolInputLines(objects)),
		Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
	})
	require.NoError(t, err)

	require.GreaterOrEqual(t, src.maxInFlight.Load(), int64(barrierFloor),
		"max concurrent GetObject calls=%d; engine record-stream path must execute concurrently (>= %d)", src.maxInFlight.Load(), barrierFloor)
	require.Equal(t, int64(objects), summary.Statuses["complete"])
	require.Equal(t, int64(objects), summary.Statuses["in_progress"])
	require.Zero(t, summary.Errors)
	require.Zero(t, summary.InvalidInputs)
	for i := 0; i < objects; i++ {
		require.Equal(t, []byte("payload"), dst.body(fmt.Sprintf("data/a/%02d.xml", i)))
	}

	require.False(t, sink.violated.Load(), "EventSink delivery must be serialized under the worker pool")
	require.Len(t, sink.runs, 1)
	require.Equal(t, ExecutionPathEngine, sink.runs[0].ExecutionPath)
	require.Len(t, sink.summaries, 1)
	require.Equal(t, ExecutionPathEngine, sink.summaries[0].ExecutionPath)
	require.GreaterOrEqual(t, summary.ConcurrencyMaxActive, barrierFloor,
		"limiter max-active must reflect pooled execution")

	// Exactly one terminal record per accepted input, and per-object transitions
	// stay ordered (in_progress observed before the terminal record).
	terminalByKey := map[string]int{}
	inProgressSeen := map[string]bool{}
	for _, rec := range sink.records {
		switch rec.Status {
		case "in_progress":
			inProgressSeen[rec.SourceKey] = true
		default:
			require.True(t, inProgressSeen[rec.SourceKey], "terminal record for %s before its in_progress", rec.SourceKey)
			terminalByKey[rec.SourceKey]++
		}
	}
	require.Len(t, terminalByKey, objects)
	for key, count := range terminalByKey {
		require.Equal(t, 1, count, "exactly one terminal record for %s", key)
	}
}

// TestRunnerRecordStreamSameDestKeyArbitration proves the engine-owned per-dest-
// key arbiter: concurrent workers targeting one destination key serialize, so
// exactly one conditional PUT is attempted and every other record resolves
// through the collision path.
func TestRunnerRecordStreamSameDestKeyArbitration(t *testing.T) {
	const objects = 16
	src := newCopyMemoryProvider()
	dst := newCopyMemoryProvider()

	var lines strings.Builder
	for i := 0; i < objects; i++ {
		key := fmt.Sprintf("a/%02d.xml", i)
		src.putFixture(key, "payload", fmt.Sprintf("etag-%02d", i))
		fmt.Fprintf(&lines, `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/%s","source_key":"%s","source_etag":"etag-%02d","source_size_bytes":7,"dest_rel_key":"same/key.xml"}}`+"\n", key, key, i)
	}

	sink := &serializedGuardSink{}
	cfg := copyConfig(dst, sink)
	cfg.Concurrency = poolConcurrency(8)
	// The durable observed-mark (written inside the arbiter gate) is what makes
	// exactly-one-conditional-PUT deterministic: the gate entry itself is
	// deleted once idle, so late-arriving same-key workers rely on the store.
	cfg.Checkpoint = newMemCheckpoint()
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	summary, err := runner.Run(context.Background(), copySource(src, lines.String()))
	require.NoError(t, err)

	require.Equal(t, []byte("payload"), dst.body("data/same/key.xml"))
	require.Equal(t, int64(1), summary.Statuses["complete"])
	require.Equal(t, int64(objects-1), summary.Statuses["skipped"])
	require.Equal(t, int64(objects-1), summary.Collisions["duplicate"])
	require.Zero(t, summary.Errors)

	// Two capability preflight puts plus exactly ONE object conditional PUT:
	// the arbiter gate's observed memo must keep every later same-key worker off
	// the conditional-PUT path entirely.
	require.Len(t, dst.preconditions(), 3,
		"same-dest-key records must arbitrate to a single conditional PUT (got %d preconditions)", len(dst.preconditions()))
	require.False(t, sink.violated.Load())
}

// TestRunnerRecordStreamPoolResume proves checkpoint-driven resume semantics
// survive pooled execution: done items skip with resume reasons, remaining items
// copy, and every accepted input lands exactly one terminal checkpoint upsert.
func TestRunnerRecordStreamPoolResume(t *testing.T) {
	const objects = 12
	src := newCopyMemoryProvider()
	seedPoolFixtures(src, objects)
	dst := newCopyMemoryProvider()

	checkpoint := newMemCheckpoint()
	for i := 0; i < objects/2; i++ {
		checkpoint.markDone(
			fmt.Sprintf("s3://source-bucket/a/%02d.xml", i),
			fmt.Sprintf("s3://dest-bucket/data/a/%02d.xml", i),
			"complete",
		)
	}

	sink := &serializedGuardSink{}
	cfg := copyConfig(dst, sink)
	cfg.Concurrency = poolConcurrency(6)
	cfg.Checkpoint = checkpoint
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	summary, err := runner.Run(context.Background(), copySource(src, poolInputLines(objects)))
	require.NoError(t, err)

	require.Equal(t, int64(objects/2), summary.Statuses["complete"])
	require.Equal(t, int64(objects/2), summary.Statuses["skipped"])
	require.Zero(t, summary.Errors)
	for _, rec := range sink.records {
		if rec.Status == "skipped" {
			require.Equal(t, "resume.complete", rec.Reason)
		}
	}
	statuses := checkpoint.itemStatuses()
	require.Equal(t, objects/2, statuses["complete"])
	require.Equal(t, objects/2, statuses["skipped"])
	require.False(t, sink.violated.Load())
}

// TestRunnerRecordStreamPoolCancellation proves a canceled context aborts the
// pooled run with the context error and without emitting a terminal summary.
func TestRunnerRecordStreamPoolCancellation(t *testing.T) {
	const objects = 32
	srcBase := newCopyMemoryProvider()
	seedPoolFixtures(srcBase, objects)
	// Barrier never releases by count (floor > objects); cancellation unblocks it.
	src := newBarrierGetProvider(srcBase, objects+1, 30*time.Second)
	dst := newCopyMemoryProvider()

	sink := &serializedGuardSink{}
	cfg := copyConfig(dst, sink)
	cfg.Concurrency = poolConcurrency(8)
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for src.inFlight.Load() < 4 {
			time.Sleep(time.Millisecond)
		}
		cancel()
	}()

	_, err = runner.Run(ctx, RecordStreamSource{
		Records: strings.NewReader(poolInputLines(objects)),
		Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
	})
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, sink.summaries, "no terminal summary after cancellation")
}
