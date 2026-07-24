package reflow

import (
	"context"
	"errors"
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
// It counts write multiplicity per destination key (E1: exactly one durable
// observed-mark and one terminal upsert per established object) and supports
// per-operation failure injection for the checkpoint-failure disposition tests.
type memCheckpoint struct {
	mu        sync.Mutex
	done      map[string]string // sourceURI|destURI -> status
	items     []CheckpointItem
	observed  map[string]bool
	markCalls map[string]int

	failMark   error
	failUpsert error
	failNote   error
}

func newMemCheckpoint() *memCheckpoint {
	return &memCheckpoint{done: map[string]string{}, observed: map[string]bool{}, markCalls: map[string]int{}}
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
	if m.failUpsert != nil {
		return m.failUpsert
	}
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
	m.markCalls[destKey]++
	if m.failMark != nil {
		return m.failMark
	}
	m.observed[destKey] = true
	return nil
}

func (m *memCheckpoint) NoteDestKeySource(context.Context, string, string, string, int64) error {
	if m.failNote != nil {
		return m.failNote
	}
	return nil
}

func (m *memCheckpoint) markCount(destKey string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.markCalls[destKey]
}

func (m *memCheckpoint) upsertCountByDestKey(destKey string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, item := range m.items {
		if item.DestKey == destKey {
			n++
		}
	}
	return n
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

// firstGetSignalProvider closes signal the first time any worker begins a
// GetObject — i.e., the first dispatched record has reached a worker.
type firstGetSignalProvider struct {
	*copyMemoryProvider
	once   sync.Once
	signal chan struct{}
}

func (p *firstGetSignalProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	p.once.Do(func() { close(p.signal) })
	return p.copyMemoryProvider.GetObject(ctx, key)
}

// gatedStreamReader serves the first record-stream line immediately, then blocks
// every later Read until proceed is closed (or a deadline fails the read). A
// producer that streams dispatches line 0, a worker starts its GetObject, proceed
// closes, and the remaining lines flow. A producer that enumerates the whole
// stream before dispatching blocks reading line 1 before any record reaches a
// worker to close proceed, so the deadline errors the read.
type gatedStreamReader struct {
	lines    []string
	idx      int
	proceed  <-chan struct{}
	deadline time.Duration
}

func (r *gatedStreamReader) Read(p []byte) (int, error) {
	if r.idx >= len(r.lines) {
		return 0, io.EOF
	}
	if r.idx >= 1 {
		select {
		case <-r.proceed:
		case <-time.After(r.deadline):
			return 0, fmt.Errorf("producer read line %d before any record was dispatched: stream-overlap broken", r.idx)
		}
	}
	n := copy(p, r.lines[r.idx])
	r.idx++
	return n, nil
}

// TestRunnerRecordStreamProducerStreamsToPool is the producer-seam negative
// control: it proves the reader stage dispatches records as it enumerates rather
// than enumerating the whole stream first. A worker must begin processing record 0
// before the producer can read record 1; an enumerate-then-dispatch regression
// deadlocks the gated reader past its deadline and fails the run. (The existing
// max-in-flight test does not catch this — buffering then dispatching all records
// still reaches the fan-out floor.)
func TestRunnerRecordStreamProducerStreamsToPool(t *testing.T) {
	const objects = 8
	srcBase := newCopyMemoryProvider()
	seedPoolFixtures(srcBase, objects)
	signal := make(chan struct{})
	src := &firstGetSignalProvider{copyMemoryProvider: srcBase, signal: signal}
	dst := newCopyMemoryProvider()

	lines := make([]string, objects)
	for i := 0; i < objects; i++ {
		lines[i] = fmt.Sprintf(`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/%02d.xml","source_key":"a/%02d.xml","source_etag":"etag-%02d","source_size_bytes":7,"dest_rel_key":"a/%02d.xml"}}`+"\n", i, i, i, i)
	}

	sink := &serializedGuardSink{}
	cfg := copyConfig(dst, sink)
	cfg.Concurrency = poolConcurrency(4)
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	reader := &gatedStreamReader{lines: lines, proceed: signal, deadline: 2 * time.Second}
	summary, err := runner.Run(context.Background(), RecordStreamSource{
		Records: reader,
		Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
	})
	require.NoError(t, err, "producer must stream records to the pool as it enumerates (no enumerate-then-dispatch)")
	require.Equal(t, int64(objects), summary.Statuses["complete"])
	require.Zero(t, summary.Errors)
	require.Zero(t, summary.InvalidInputs)
	for i := 0; i < objects; i++ {
		require.Equal(t, []byte("payload"), dst.body(fmt.Sprintf("data/a/%02d.xml", i)))
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
	checkpoint := newMemCheckpoint()
	cfg.Checkpoint = checkpoint
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
	// E1: one establishment = exactly one durable observed-mark write.
	require.Equal(t, 1, checkpoint.markCount("data/same/key.xml"),
		"same-key fan-in must produce exactly one durable observed-mark")
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

// TestRunnerRecordStreamSingleDurableMarkPerObject is the E1 write-multiplicity
// gate: a fresh successful copy performs exactly one durable observed-mark and
// one terminal item upsert per destination key — never two.
func TestRunnerRecordStreamSingleDurableMarkPerObject(t *testing.T) {
	const objects = 8
	src := newCopyMemoryProvider()
	seedPoolFixtures(src, objects)
	dst := newCopyMemoryProvider()

	sink := &serializedGuardSink{}
	cfg := copyConfig(dst, sink)
	cfg.Concurrency = poolConcurrency(4)
	checkpoint := newMemCheckpoint()
	cfg.Checkpoint = checkpoint
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	summary, err := runner.Run(context.Background(), copySource(src, poolInputLines(objects)))
	require.NoError(t, err)
	require.Equal(t, int64(objects), summary.Statuses["complete"])

	for i := 0; i < objects; i++ {
		destKey := fmt.Sprintf("data/a/%02d.xml", i)
		require.Equal(t, 1, checkpoint.markCount(destKey), "exactly one durable observed-mark for %s", destKey)
		require.Equal(t, 1, checkpoint.upsertCountByDestKey(destKey), "exactly one terminal upsert for %s", destKey)
	}
}

// TestRunnerRecordStreamAuxiliaryMarkFailureWarnsAndCompletes pins the
// checkpoint-failure disposition for auxiliary arbitration state: the durable
// observed-mark and NoteDestKeySource may fail with a typed warning while the
// object still completes (correctness held by the gate memo plus the
// conditional/fallback collision path).
func TestRunnerRecordStreamAuxiliaryMarkFailureWarnsAndCompletes(t *testing.T) {
	src := newCopyMemoryProvider()
	seedPoolFixtures(src, 1)
	dst := newCopyMemoryProvider()

	sink := &serializedGuardSink{}
	cfg := copyConfig(dst, sink)
	cfg.Concurrency = poolConcurrency(2)
	checkpoint := newMemCheckpoint()
	checkpoint.failMark = fmt.Errorf("injected mark failure")
	checkpoint.failNote = fmt.Errorf("injected note failure")
	cfg.Checkpoint = checkpoint
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	summary, err := runner.Run(context.Background(), copySource(src, poolInputLines(1)))
	require.NoError(t, err, "auxiliary state failures must not fail the run")
	require.Equal(t, int64(1), summary.Statuses["complete"])
	require.Zero(t, summary.Errors)
	require.Equal(t, []byte("payload"), dst.body("data/a/00.xml"))

	codes := map[string]int{}
	for _, w := range sink.warnings {
		codes[w.Code]++
	}
	require.GreaterOrEqual(t, codes[warningCodeArbitrationStateWrite], 2,
		"typed arbitration-state warnings for mark + note failures; got %v", codes)
}

// TestRunnerRecordStreamTerminalUpsertFailureNeverAcksComplete pins the strict
// half of the disposition: the terminal UpsertItem is the resume authority — on
// failure the object is reported failed (typed), the run exits non-zero, and a
// healthy-store resume converges without a second land.
func TestRunnerRecordStreamTerminalUpsertFailureNeverAcksComplete(t *testing.T) {
	src := newCopyMemoryProvider()
	seedPoolFixtures(src, 1)
	dst := newCopyMemoryProvider()

	sink := &serializedGuardSink{}
	cfg := copyConfig(dst, sink)
	cfg.Concurrency = poolConcurrency(2)
	checkpoint := newMemCheckpoint()
	checkpoint.failUpsert = fmt.Errorf("injected terminal upsert failure")
	cfg.Checkpoint = checkpoint
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	summary, err := runner.Run(context.Background(), copySource(src, poolInputLines(1)))
	var objErr *ObjectErrorsError
	require.ErrorAs(t, err, &objErr, "terminal upsert failure must surface as a non-zero object-error run")
	require.Zero(t, summary.Statuses["complete"], "no complete terminal on a store that could not record it")
	require.Equal(t, int64(1), summary.Statuses["failed"])
	require.Equal(t, int64(1), summary.Errors)

	// The destination mutation itself succeeded; a resume against a healthy
	// store must converge without a second land.
	checkpoint.mu.Lock()
	checkpoint.failUpsert = nil
	checkpoint.mu.Unlock()
	sink2 := &serializedGuardSink{}
	cfg2 := copyConfig(dst, sink2)
	cfg2.Concurrency = poolConcurrency(2)
	cfg2.Checkpoint = checkpoint
	runner2, err := NewRunner(cfg2)
	require.NoError(t, err)
	putsBefore := len(dst.preconditions())
	summary2, err := runner2.Run(context.Background(), copySource(src, poolInputLines(1)))
	require.NoError(t, err)
	require.Equal(t, int64(1), summary2.Statuses["skipped"], "resume converges via collision-duplicate skip")
	// The second run adds exactly its two capability-preflight conditional puts
	// and NO object-level conditional PUT — the object does not land twice.
	require.Equal(t, putsBefore+2, len(dst.preconditions()), "no second object conditional PUT on convergence")
}

// TestRunnerRecordStreamResolverSerialAndIdentityGated is the engine half of
// the E2 contract: SourceResolver is invoked only from the serial reader stage
// (never concurrently) even while workers overlap, and a record with a
// divergent source root is refused as INVALID_INPUT before resolution.
func TestRunnerRecordStreamResolverSerialAndIdentityGated(t *testing.T) {
	const objects = 12
	srcBase := newCopyMemoryProvider()
	seedPoolFixtures(srcBase, objects)
	src := newBarrierGetProvider(srcBase, 4, 2*time.Second)
	dst := newCopyMemoryProvider()

	var (
		resolveInFlight  atomic.Int32
		resolveConcurred atomic.Bool
		resolveCalls     atomic.Int32
	)
	resolver := func(context.Context, string) (provider.Provider, error) {
		if resolveInFlight.Add(1) > 1 {
			resolveConcurred.Store(true)
		}
		defer resolveInFlight.Add(-1)
		resolveCalls.Add(1)
		return src, nil
	}

	lines := poolInputLines(objects) +
		`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://other-bucket/x/y.xml","source_key":"x/y.xml","source_etag":"etag-x","source_size_bytes":7,"dest_rel_key":"x/y.xml"}}` + "\n"

	sink := &serializedGuardSink{}
	cfg := copyConfig(dst, sink)
	cfg.Concurrency = poolConcurrency(8)
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	summary, err := runner.Run(context.Background(), RecordStreamSource{
		Records: strings.NewReader(lines),
		Resolve: resolver,
	})
	var invalidErr *InvalidInputsError
	require.ErrorAs(t, err, &invalidErr)

	require.False(t, resolveConcurred.Load(), "SourceResolver must never be invoked concurrently")
	require.Equal(t, int32(objects), resolveCalls.Load(),
		"resolver is called once per valid record and never for the divergent-root record")
	require.Equal(t, int64(1), summary.InvalidInputs)
	require.Equal(t, int64(objects), summary.Statuses["complete"])
	require.GreaterOrEqual(t, src.maxInFlight.Load(), int64(4), "workers still overlap while resolution stays serial")
}

// TestRunnerZeroConcurrencyConfigResolves is the E3 gate: the documented
// zero-value Config.Concurrency resolves to internally consistent defaults —
// pool size, limiter, and run-record fields all derive from one normalized
// config, and no run reports a requested/effective pair it does not apply.
func TestRunnerZeroConcurrencyConfigResolves(t *testing.T) {
	src := newCopyMemoryProvider()
	seedPoolFixtures(src, 2)
	dst := newCopyMemoryProvider()

	sink := &serializedGuardSink{}
	cfg := copyConfig(dst, sink)
	cfg.Concurrency = ConcurrencyConfig{} // documented zero value
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	summary, err := runner.Run(context.Background(), copySource(src, poolInputLines(2)))
	require.NoError(t, err)

	require.Len(t, sink.runs, 1)
	run := sink.runs[0]
	require.Equal(t, concurrencyDefaultRequested, run.Parallel, "zero config resolves to the default requested ceiling")
	require.Equal(t, run.Parallel, run.ConcurrencyCeilingRequested)
	require.GreaterOrEqual(t, run.ConcurrencyCeilingEffective, 1)
	require.LessOrEqual(t, run.ConcurrencyCeilingEffective, run.ConcurrencyCeilingRequested)
	require.NotEmpty(t, run.ConcurrencyCeilingReason)
	require.LessOrEqual(t, summary.ConcurrencyMaxActive, summary.ConcurrencyCeilingEffective,
		"observed max-active must not exceed the reported effective ceiling")

	// A partial config (requested only) resolves through the same arithmetic.
	cfg2 := copyConfig(dst, &serializedGuardSink{})
	cfg2.Concurrency = ConcurrencyConfig{RequestedCeiling: 4, AdaptiveEnabled: true}
	runner2, err := NewRunner(cfg2)
	require.NoError(t, err)
	got := runner2.Config().Concurrency
	require.Equal(t, 4, got.RequestedCeiling)
	require.GreaterOrEqual(t, got.EffectiveCeiling, 1)
	require.LessOrEqual(t, got.EffectiveCeiling, 4)
	require.GreaterOrEqual(t, got.Initial, got.Floor)
	require.LessOrEqual(t, got.Initial, got.EffectiveCeiling)
}

// TestNormalizeConcurrencyInvariants pins the normalized-config post-condition
// 1 <= Floor <= Initial <= EffectiveCeiling <= RequestedCeiling on partial and
// inconsistent public configs.
func TestNormalizeConcurrencyInvariants(t *testing.T) {
	cases := []struct {
		name string
		in   ConcurrencyConfig
	}{
		{name: "overlarge floor", in: ConcurrencyConfig{RequestedCeiling: 8, EffectiveCeiling: 4, Initial: 4, Floor: 8, AdaptiveEnabled: true}},
		{name: "fixed partial no initial", in: ConcurrencyConfig{RequestedCeiling: 8, EffectiveCeiling: 8, AdaptiveEnabled: false}},
		{name: "adaptive partial no initial", in: ConcurrencyConfig{RequestedCeiling: 64, EffectiveCeiling: 64, AdaptiveEnabled: true}},
		{name: "effective above requested", in: ConcurrencyConfig{RequestedCeiling: 2, EffectiveCeiling: 9, AdaptiveEnabled: true}},
		{name: "requested only", in: ConcurrencyConfig{RequestedCeiling: 4, AdaptiveEnabled: true}},
		{name: "zero value", in: ConcurrencyConfig{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeConcurrency(tc.in)
			require.GreaterOrEqual(t, got.Floor, 1)
			require.LessOrEqual(t, got.Floor, got.Initial)
			require.LessOrEqual(t, got.Initial, got.EffectiveCeiling)
			require.LessOrEqual(t, got.EffectiveCeiling, got.RequestedCeiling)
			require.NotEmpty(t, got.CeilingReason)
			if !got.AdaptiveEnabled {
				require.Equal(t, got.EffectiveCeiling, got.Initial,
					"fixed mode has no ramp: Initial must equal the effective ceiling")
			}
		})
	}

	// Fixed partial config specifically: reported effective must be executed.
	fixed := normalizeConcurrency(ConcurrencyConfig{RequestedCeiling: 8, EffectiveCeiling: 8, AdaptiveEnabled: false})
	require.Equal(t, 8, fixed.Initial)
}

// TestLimiterThrottleNeverExceedsEffectiveAfterNormalize is the AIMD half of
// the invariant: with an over-large floor normalized down, multiplicative
// decrease can never recover observed concurrency above the effective ceiling.
func TestLimiterThrottleNeverExceedsEffectiveAfterNormalize(t *testing.T) {
	cfg := normalizeConcurrency(ConcurrencyConfig{RequestedCeiling: 8, EffectiveCeiling: 4, Initial: 4, Floor: 8, AdaptiveEnabled: true})
	require.LessOrEqual(t, cfg.Floor, cfg.EffectiveCeiling)

	limiter := NewConcurrencyLimiter(cfg)
	for i := 0; i < 32; i++ {
		limiter.ObserveThrottle()
		limiter.ObserveSuccess()
		snap := limiter.Snapshot()
		require.LessOrEqual(t, snap.ConcurrencyFinal, snap.ConcurrencyCeilingEffective,
			"post-throttle concurrency must never exceed the resolved effective ceiling")
	}
}

// TestNewConcurrencyLimiterEnforcesInvariantDirectly proves the PUBLIC
// constructor holds the invariant without any prior normalization: an
// inconsistent config passed straight to NewConcurrencyLimiter can never
// snapshot — including after throttle recovery — a concurrency above the
// effective ceiling it reports.
func TestNewConcurrencyLimiterEnforcesInvariantDirectly(t *testing.T) {
	cases := []ConcurrencyConfig{
		{RequestedCeiling: 8, EffectiveCeiling: 4, Initial: 4, Floor: 8, AdaptiveEnabled: true},
		{RequestedCeiling: 8, EffectiveCeiling: 4, Initial: 9, Floor: 8, AdaptiveEnabled: true},
		{RequestedCeiling: 2, EffectiveCeiling: 9, Floor: 5, AdaptiveEnabled: true},
		{RequestedCeiling: 8, EffectiveCeiling: 8, Initial: 1, AdaptiveEnabled: false},
		{},
	}
	for i, in := range cases {
		limiter := NewConcurrencyLimiter(in)
		snap := limiter.Snapshot()
		require.GreaterOrEqual(t, snap.ConcurrencyFloor, 1, "case %d", i)
		require.LessOrEqual(t, snap.ConcurrencyFloor, snap.ConcurrencyInitial, "case %d", i)
		require.LessOrEqual(t, snap.ConcurrencyInitial, snap.ConcurrencyCeilingEffective, "case %d", i)
		require.LessOrEqual(t, snap.ConcurrencyCeilingEffective, snap.ConcurrencyCeilingRequested, "case %d", i)
		if !in.AdaptiveEnabled && in.EffectiveCeiling >= 1 {
			require.Equal(t, snap.ConcurrencyCeilingEffective, snap.ConcurrencyInitial,
				"case %d: fixed mode must run at the ceiling it reports", i)
		}
		for j := 0; j < 16; j++ {
			limiter.ObserveThrottle()
			limiter.ObserveSuccess()
			after := limiter.Snapshot()
			require.LessOrEqual(t, after.ConcurrencyFinal, after.ConcurrencyCeilingEffective,
				"case %d: throttle recovery must never exceed the reported effective ceiling", i)
		}
	}
}

// TestRunnerFixedPartialConfigExecutesReportedCeiling is the behavioral gate
// for the fixed-mode normalization: a partial non-adaptive config that reports
// effective=8 must actually overlap to that ceiling, proven by the barrier.
func TestRunnerFixedPartialConfigExecutesReportedCeiling(t *testing.T) {
	const (
		objects      = 16
		barrierFloor = 4
	)
	srcBase := newCopyMemoryProvider()
	seedPoolFixtures(srcBase, objects)
	src := newBarrierGetProvider(srcBase, barrierFloor, 2*time.Second)
	dst := newCopyMemoryProvider()

	sink := &serializedGuardSink{}
	cfg := copyConfig(dst, sink)
	// Partial fixed config: no Initial, no Floor — pre-normalization this
	// executed serially (Initial floored to 1) while reporting 8.
	cfg.Concurrency = ConcurrencyConfig{RequestedCeiling: 8, EffectiveCeiling: 8, CeilingReason: "requested", AdaptiveEnabled: false}
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	summary, err := runner.Run(context.Background(), RecordStreamSource{
		Records: strings.NewReader(poolInputLines(objects)),
		Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, src.maxInFlight.Load(), int64(barrierFloor),
		"fixed partial config must execute at its reported ceiling, not serially")
	require.Equal(t, int64(objects), summary.Statuses["complete"])
	require.LessOrEqual(t, summary.ConcurrencyMaxActive, summary.ConcurrencyCeilingEffective)
	require.Equal(t, 8, summary.ConcurrencyCeilingEffective)
	require.Equal(t, 8, summary.ConcurrencyInitial, "normalized fixed config reports the initial it executes")
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

var (
	errSinkSentinel     = errors.New("sink sentinel fatal")
	errProducerSentinel = errors.New("producer sentinel stop-discovery")
)

// failFirstOnRecordSink returns sentinel on the first OnRecord (the first
// in_progress a worker emits), modelling a worker-fatal, and counts calls.
type failFirstOnRecordSink struct {
	collectSink
	sentinel error
	calls    atomic.Int32
}

func (s *failFirstOnRecordSink) OnRecord(ctx context.Context, rec Record) error {
	if s.calls.Add(1) == 1 {
		return s.sentinel
	}
	return s.collectSink.OnRecord(ctx, rec)
}

// blockThenErrorReader serves the first line, then blocks the next Read until
// unblock is closed and returns err — used to coincide a scanner read error with
// a cancelled context.
type blockThenErrorReader struct {
	line    string
	served  bool
	unblock <-chan struct{}
	err     error
}

func (r *blockThenErrorReader) Read(p []byte) (int, error) {
	if !r.served {
		r.served = true
		return copy(p, r.line), nil
	}
	<-r.unblock
	return 0, r.err
}

// lineThenSentinelReader serves the first line, then returns sentinel on the next
// Read — the producer stop-discovery-with-drain trigger. Before returning the
// sentinel it waits for workerEntered (when set) so the admitted task is already
// inside GetObject — a later cancellation cannot skip an unstarted task — then
// closes errorReturned (when set).
type lineThenSentinelReader struct {
	line          string
	served        bool
	sentinel      error
	workerEntered <-chan struct{}
	errorReturned chan struct{}
}

func (r *lineThenSentinelReader) Read(p []byte) (int, error) {
	if !r.served {
		r.served = true
		return copy(p, r.line), nil
	}
	// Wait until the admitted task is inside GetObject before the producer errors:
	// a later cancel-on-producer-error mutation then cancels a task already started
	// rather than skipping an unstarted one, so the guard fails promptly instead of
	// hanging on an unstarted worker.
	if r.workerEntered != nil {
		<-r.workerEntered
	}
	// Close errorReturned immediately before handing the producer its sentinel, so
	// a test can release the admitted GetObject only after the producer has begun
	// returning its error — never a blind delay that could release the copy first.
	if r.errorReturned != nil {
		close(r.errorReturned)
		r.errorReturned = nil
	}
	return 0, r.sentinel
}

// gatedThenCtxGetProvider signals getStarted on the first GetObject, blocks until
// proceed is closed, then waits up to observeWindow for context cancellation
// before reading. A drain-preserving runner never cancels the pool on a producer
// error, so the wait elapses and the admitted GetObject completes; a runner that
// (wrongly) cancels on producer error cancels the pool, the wait observes it, and
// the object never lands — caught every run regardless of goroutine scheduling.
type gatedThenCtxGetProvider struct {
	*copyMemoryProvider
	once          sync.Once
	getStarted    chan struct{}
	proceed       <-chan struct{}
	observeWindow time.Duration
}

func (p *gatedThenCtxGetProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	p.once.Do(func() { close(p.getStarted) })
	<-p.proceed
	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	case <-time.After(p.observeWindow):
	}
	return p.copyMemoryProvider.GetObject(ctx, key)
}

// TestRunnerRecordStreamWorkerFatalInterruptsBlockedProducer proves the producer
// is driven with the pool context: a worker-fatal must interrupt a producer
// blocked in context-aware enumeration/resolution promptly, not leave the run
// hung until that call returns. Regression control for DR-4.1-1 — with the parent
// context passed instead, the blocked resolver never observes the worker-fatal
// cancellation and this test times out.
func TestRunnerRecordStreamWorkerFatalInterruptsBlockedProducer(t *testing.T) {
	srcBase := newCopyMemoryProvider()
	seedPoolFixtures(srcBase, 2)
	dst := newCopyMemoryProvider()
	sink := &failFirstOnRecordSink{sentinel: errSinkSentinel}
	cfg := copyConfig(dst, sink)
	cfg.Concurrency = poolConcurrency(2)
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	var resolveCalls atomic.Int32
	resolve := func(ctx context.Context, _ string) (provider.Provider, error) {
		if resolveCalls.Add(1) == 1 {
			return srcBase, nil // line 0 resolves and dispatches
		}
		<-ctx.Done() // line 1 blocks on the callback context until the pool cancels
		return nil, ctx.Err()
	}

	done := make(chan error, 1)
	go func() {
		_, e := runner.Run(context.Background(), RecordStreamSource{
			Records: strings.NewReader(poolInputLines(2)),
			Resolve: resolve,
		})
		done <- e
	}()

	select {
	case e := <-done:
		require.ErrorIs(t, e, errSinkSentinel, "worker-fatal error must win precedence")
		require.Empty(t, sink.summaries, "no terminal summary after a fatal")
		// The line-1 resolver wakes with the cancellation the worker-fatal caused;
		// that must not manufacture a "failed to connect" error/record after the
		// fatal. Only the one rejected in_progress reaches the sink.
		require.Equal(t, int32(1), sink.calls.Load(), "cancelled resolution must not emit a post-fatal record")
		require.Empty(t, sink.records, "no record beyond the rejected in_progress")
		require.Empty(t, sink.errs, "no spurious error event from the cancelled resolver")
	case <-time.After(3 * time.Second):
		t.Fatal("worker-fatal did not interrupt the ctx-blocked producer (DR-4.1-1 regression)")
	}
}

// TestRunnerRecordStreamCancelThenReadErrorPrecedence pins F1: when a parent
// cancellation and a scanner read error coincide, the run returns
// context.Canceled — cancellation outranks the scanner error, matching the inline
// teardown. With the producer returning scanner.Err() before the context check,
// the read error would win instead.
func TestRunnerRecordStreamCancelThenReadErrorPrecedence(t *testing.T) {
	srcBase := newCopyMemoryProvider()
	seedPoolFixtures(srcBase, 1)
	signal := make(chan struct{})
	src := &firstGetSignalProvider{copyMemoryProvider: srcBase, signal: signal}
	dst := newCopyMemoryProvider()

	sink := &serializedGuardSink{}
	cfg := copyConfig(dst, sink)
	cfg.Concurrency = poolConcurrency(2)
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	unblock := make(chan struct{})
	line0 := fmt.Sprintf(`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/00.xml","source_key":"a/00.xml","source_etag":"etag-00","source_size_bytes":7,"dest_rel_key":"a/00.xml"}}` + "\n")
	reader := &blockThenErrorReader{line: line0, unblock: unblock, err: errors.New("simulated scanner read failure")}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-signal       // worker began line 0's GetObject; producer is blocked reading line 1
		cancel()       // cancel the parent -> pool context cancels
		close(unblock) // reader now returns its read error on the same pass
	}()

	_, err = runner.Run(ctx, RecordStreamSource{
		Records: reader,
		Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
	})
	require.ErrorIs(t, err, context.Canceled, "cancellation must outrank a coincident scanner read error")
	require.NotContains(t, err.Error(), "simulated scanner read failure")
	require.Empty(t, sink.summaries, "no terminal summary after cancellation")
}

// TestRunnerRecordStreamProducerErrorDrainsAdmitted pins the stop-discovery-with-
// drain mode (F2a): a producer-returned error stops enumeration and fails the run,
// but an already-admitted copy still completes. A runner that cancels the pool on
// a producer error would fail the admitted GetObject and drop the object.
func TestRunnerRecordStreamProducerErrorDrainsAdmitted(t *testing.T) {
	srcBase := newCopyMemoryProvider()
	seedPoolFixtures(srcBase, 1)
	getStarted := make(chan struct{})
	proceed := make(chan struct{})
	errorReturned := make(chan struct{})
	src := &gatedThenCtxGetProvider{copyMemoryProvider: srcBase, getStarted: getStarted, proceed: proceed, observeWindow: 200 * time.Millisecond}
	dst := newCopyMemoryProvider()

	sink := &serializedGuardSink{}
	cfg := copyConfig(dst, sink)
	cfg.Concurrency = poolConcurrency(2)
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	line0 := fmt.Sprintf(`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/00.xml","source_key":"a/00.xml","source_etag":"etag-00","source_size_bytes":7,"dest_rel_key":"a/00.xml"}}` + "\n")
	reader := &lineThenSentinelReader{line: line0, sentinel: errProducerSentinel, workerEntered: getStarted, errorReturned: errorReturned}

	done := make(chan error, 1)
	go func() {
		_, e := runner.Run(context.Background(), RecordStreamSource{
			Records: reader,
			Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
		})
		done <- e
	}()

	// The reader gates the sentinel on getStarted, so errorReturned fires only after
	// the admitted task is already inside GetObject: a wrongful cancel cannot skip it.
	<-errorReturned // the producer is handing back its sentinel with the copy admitted
	close(proceed)  // the admitted GetObject now waits its observe window for any (wrongful) cancellation

	err = <-done
	require.ErrorIs(t, err, errProducerSentinel, "producer stop-discovery error is returned")
	require.Equal(t, []byte("payload"), dst.body("data/a/00.xml"), "admitted work must drain to completion")
	require.Empty(t, sink.summaries, "no terminal summary when the run fails")
}

// TestRunnerRecordStreamWorkerFatalSkipsQueued pins the fatal-cancel mode (F2b):
// with a single worker, the first in_progress fails with a sentinel while later
// records are queued; the pool cancels, queued work is skipped, exactly one sink
// record call occurs, nothing lands, and no summary is written.
func TestRunnerRecordStreamWorkerFatalSkipsQueued(t *testing.T) {
	const objects = 8
	srcBase := newCopyMemoryProvider()
	seedPoolFixtures(srcBase, objects)
	dst := newCopyMemoryProvider()
	sink := &failFirstOnRecordSink{sentinel: errSinkSentinel}
	cfg := copyConfig(dst, sink)
	cfg.Concurrency = poolConcurrency(1) // single worker so later records queue behind the failing one
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	_, err = runner.Run(context.Background(), RecordStreamSource{
		Records: strings.NewReader(poolInputLines(objects)),
		Resolve: func(context.Context, string) (provider.Provider, error) { return srcBase, nil },
	})
	require.ErrorIs(t, err, errSinkSentinel)
	require.Equal(t, int32(1), sink.calls.Load(), "worker-fatal must skip queued work: only the failing in_progress reaches the sink")
	for i := 0; i < objects; i++ {
		require.Empty(t, dst.body(fmt.Sprintf("data/a/%02d.xml", i)), "no object lands after the first-record fatal")
	}
	require.Empty(t, sink.summaries, "no terminal summary after a fatal")
}

// TestRunnerRecordStreamResolveErrorEmitsFailureWhenLive proves the DR-4.1-R2-1
// guard is scoped to cancellation only: an ordinary resolve error with a live
// context still surfaces the "failed to connect to provider" error event and a
// failed terminal, exactly as before, and the run exits non-zero.
func TestRunnerRecordStreamResolveErrorEmitsFailureWhenLive(t *testing.T) {
	srcBase := newCopyMemoryProvider()
	seedPoolFixtures(srcBase, 1)
	dst := newCopyMemoryProvider()

	sink := &serializedGuardSink{}
	cfg := copyConfig(dst, sink)
	cfg.Concurrency = poolConcurrency(2)
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	resolveErr := errors.New("provider unreachable")
	_, err = runner.Run(context.Background(), RecordStreamSource{
		Records: strings.NewReader(poolInputLines(1)),
		Resolve: func(context.Context, string) (provider.Provider, error) { return nil, resolveErr },
	})
	var objErr *ObjectErrorsError
	require.ErrorAs(t, err, &objErr, "a live-context resolve failure exits non-zero")
	require.Len(t, sink.errs, 1, "the failure still emits an error event")
	require.Equal(t, int64(1), sink.summaries[0].Errors, "and a counted object error")
	require.False(t, sink.violated.Load())
}
