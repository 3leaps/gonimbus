package reflowstate

import (
	"errors"
	"sync/atomic"
	"time"
)

// BarrierOutcome classifies how an admitted exec request's post-enqueue barrier
// completed from the *caller's* observation of awaitExecResult. It is
// control-flow-neutral telemetry only: it never drives admission, batching, or
// fail-closed decisions.
//
// Waiter-side, not durable truth: BarrierCanceled (and a cancel-vs-done select
// race) can count "canceled" even when the write still commits under baseCtx.
// Writer-side RequestRefusals (from commit results) can therefore diverge from
// BarrierRefusal under cancel storms. Bind rules and capacity automation must
// not treat BarrierOK / BarrierCanceled as a commit ledger; prefer WriterStats
// batch/commit fields and published durability semantics.
type BarrierOutcome uint8

const (
	// BarrierOK is a published nil result (durably committed success observed
	// by the waiter).
	BarrierOK BarrierOutcome = iota
	// BarrierRefusal is a published non-nil result that is not a terminal
	// writer lifecycle error (per-request logical refusal under the publish
	// invariant: success path done values are nil or unwrapped refusal only).
	BarrierRefusal
	// BarrierWriterFailed is a terminal writer failure (batch fatal).
	BarrierWriterFailed
	// BarrierWriterClosed is a graceful-close lifecycle outcome for a request
	// that was not published a result.
	BarrierWriterClosed
	// BarrierCanceled is a caller-context cancellation while waiting on the
	// barrier (the write may still commit under baseCtx).
	BarrierCanceled
)

// String returns a stable label for logs and tests.
func (o BarrierOutcome) String() string {
	switch o {
	case BarrierOK:
		return "ok"
	case BarrierRefusal:
		return "refusal"
	case BarrierWriterFailed:
		return "writer_failed"
	case BarrierWriterClosed:
		return "writer_closed"
	case BarrierCanceled:
		return "canceled"
	default:
		return "unknown"
	}
}

// WriterStats is an immutable aggregate snapshot of checkpoint writer
// diagnostics. It deliberately excludes paths, SQL text, object keys, URIs,
// and auth material.
//
// C1 surface is snapshot-only: there is no public callback observer. Always-on
// atomics update on the existing control path and are read via WriterStats().
// That keeps measure-first telemetry off the durability acknowledgement path
// (post-COMMIT result publish and sole-writer progress are not gated on sinks).
//
// Queue occupancy fields are approximate: depth is sampled as len(execCh)
// immediately before a successful send. Concurrent drain/admit races mean
// samples are not a perfect queue integral; peak is a CAS-tracked max of
// those samples.
//
// Experimental: the package is Experimental; this surface may change with only
// an in-release note.
type WriterStats struct {
	// MaxBatch is the coordinator's admission/coalesce bound (also the exec
	// channel capacity).
	MaxBatch int

	// QueueDepthSamples is how many successful exec admissions contributed a
	// depth sample. QueueDepthSum / QueueDepthSamples is the mean sample.
	// QueueDepthPeak is the maximum sample observed (approximate occupancy).
	QueueDepthSamples int64
	QueueDepthSum     int64
	QueueDepthPeak    int64

	// Admissions counts successful exec-channel enqueues. AdmissionWaitNanos
	// is the sum of admission-select wall times for those enqueues;
	// AdmissionWaitMaxNanos is the max single wait. AdmissionBlocked counts
	// admits whose pre-send depth sample equaled MaxBatch (channel full /
	// backpressured at sample time).
	Admissions            int64
	AdmissionWaitNanos    int64
	AdmissionWaitMaxNanos int64
	AdmissionBlocked      int64

	// Barrier* cover post-enqueue-to-result waits for exec requests only
	// (reads are not counted). Outcome counters partition Barriers.
	Barriers            int64
	BarrierWaitNanos    int64
	BarrierWaitMaxNanos int64
	BarrierOK           int64
	BarrierRefusal      int64
	BarrierWriterFailed int64
	BarrierWriterClosed int64
	BarrierCanceled     int64

	// Batches is the number of write transactions assembled. BatchSizeSum /
	// Batches is mean size in request units (not SQL statements).
	// BatchSizeMax is the largest coalesced batch. Coarse histogram buckets:
	// 1 | 2–8 | 9–32 | 33–128 | 129+.
	Batches          int64
	BatchSizeSum     int64
	BatchSizeMax     int64
	BatchSize1       int64
	BatchSize2To8    int64
	BatchSize9To32   int64
	BatchSize33To128 int64
	BatchSize129Plus int64

	// Commits counts batch write attempts (success and fatal).
	// BatchDuration* is wall time of the full batch write path inside
	// c.commit(batch): BeginTx, per-request SQL/savepoints, and tx.Commit().
	// It is NOT storage COMMIT alone; bind rules must not treat it as
	// SQLite COMMIT sensitivity. CommitFatals counts terminal batch failures.
	// RequestRefusals counts per-request logical refusals on successfully
	// committed batches (distinct from CommitFatals).
	Commits               int64
	BatchDurationNanos    int64
	BatchDurationMaxNanos int64
	CommitFatals          int64
	RequestRefusals       int64

	// SavepointsCreated counts SAVEPOINT statements issued in commit.
	// SavepointsElided counts raw-exec requests that skipped SAVEPOINT/RELEASE
	// when ElideRawExecSavepoints is enabled (experimental).
	SavepointsCreated int64
	SavepointsElided  int64
}

// writerMetrics holds always-on cheap counters. Submit-side fields are updated
// from many goroutines via atomics; writer-side fields are updated only from
// the single writer goroutine but still stored as atomics so WriterStats() is
// race-safe from any goroutine.
type writerMetrics struct {
	queueDepthSamples atomic.Int64
	queueDepthSum     atomic.Int64
	queueDepthPeak    atomic.Int64

	admissions            atomic.Int64
	admissionWaitNanos    atomic.Int64
	admissionWaitMaxNanos atomic.Int64
	admissionBlocked      atomic.Int64

	barriers            atomic.Int64
	barrierWaitNanos    atomic.Int64
	barrierWaitMaxNanos atomic.Int64
	barrierOK           atomic.Int64
	barrierRefusal      atomic.Int64
	barrierWriterFailed atomic.Int64
	barrierWriterClosed atomic.Int64
	barrierCanceled     atomic.Int64

	batches          atomic.Int64
	batchSizeSum     atomic.Int64
	batchSizeMax     atomic.Int64
	batchSize1       atomic.Int64
	batchSize2To8    atomic.Int64
	batchSize9To32   atomic.Int64
	batchSize33To128 atomic.Int64
	batchSize129Plus atomic.Int64

	commits               atomic.Int64
	batchDurationNanos    atomic.Int64
	batchDurationMaxNanos atomic.Int64
	commitFatals          atomic.Int64
	requestRefusals       atomic.Int64

	savepointsCreated atomic.Int64
	savepointsElided  atomic.Int64
}

func (m *writerMetrics) noteAdmission(depth, maxBatch int, wait time.Duration) {
	m.admissions.Add(1)
	m.queueDepthSamples.Add(1)
	m.queueDepthSum.Add(int64(depth))
	atomicMaxInt64(&m.queueDepthPeak, int64(depth))

	nanos := wait.Nanoseconds()
	m.admissionWaitNanos.Add(nanos)
	atomicMaxInt64(&m.admissionWaitMaxNanos, nanos)
	if depth >= maxBatch {
		m.admissionBlocked.Add(1)
	}
}

func (m *writerMetrics) noteBarrier(wait time.Duration, outcome BarrierOutcome) {
	m.barriers.Add(1)
	nanos := wait.Nanoseconds()
	m.barrierWaitNanos.Add(nanos)
	atomicMaxInt64(&m.barrierWaitMaxNanos, nanos)
	switch outcome {
	case BarrierOK:
		m.barrierOK.Add(1)
	case BarrierRefusal:
		m.barrierRefusal.Add(1)
	case BarrierWriterFailed:
		m.barrierWriterFailed.Add(1)
	case BarrierWriterClosed:
		m.barrierWriterClosed.Add(1)
	case BarrierCanceled:
		m.barrierCanceled.Add(1)
	}
}

func (m *writerMetrics) noteBatch(size int, duration time.Duration, refusals int, fatal bool) {
	m.batches.Add(1)
	m.batchSizeSum.Add(int64(size))
	atomicMaxInt64(&m.batchSizeMax, int64(size))
	switch {
	case size <= 1:
		m.batchSize1.Add(1)
	case size <= 8:
		m.batchSize2To8.Add(1)
	case size <= 32:
		m.batchSize9To32.Add(1)
	case size <= 128:
		m.batchSize33To128.Add(1)
	default:
		m.batchSize129Plus.Add(1)
	}

	m.commits.Add(1)
	nanos := duration.Nanoseconds()
	m.batchDurationNanos.Add(nanos)
	atomicMaxInt64(&m.batchDurationMaxNanos, nanos)
	if fatal {
		m.commitFatals.Add(1)
		return
	}
	if refusals > 0 {
		m.requestRefusals.Add(int64(refusals))
	}
}

func (m *writerMetrics) snapshot(maxBatch int) WriterStats {
	return WriterStats{
		MaxBatch: maxBatch,

		QueueDepthSamples: m.queueDepthSamples.Load(),
		QueueDepthSum:     m.queueDepthSum.Load(),
		QueueDepthPeak:    m.queueDepthPeak.Load(),

		Admissions:            m.admissions.Load(),
		AdmissionWaitNanos:    m.admissionWaitNanos.Load(),
		AdmissionWaitMaxNanos: m.admissionWaitMaxNanos.Load(),
		AdmissionBlocked:      m.admissionBlocked.Load(),

		Barriers:            m.barriers.Load(),
		BarrierWaitNanos:    m.barrierWaitNanos.Load(),
		BarrierWaitMaxNanos: m.barrierWaitMaxNanos.Load(),
		BarrierOK:           m.barrierOK.Load(),
		BarrierRefusal:      m.barrierRefusal.Load(),
		BarrierWriterFailed: m.barrierWriterFailed.Load(),
		BarrierWriterClosed: m.barrierWriterClosed.Load(),
		BarrierCanceled:     m.barrierCanceled.Load(),

		Batches:          m.batches.Load(),
		BatchSizeSum:     m.batchSizeSum.Load(),
		BatchSizeMax:     m.batchSizeMax.Load(),
		BatchSize1:       m.batchSize1.Load(),
		BatchSize2To8:    m.batchSize2To8.Load(),
		BatchSize9To32:   m.batchSize9To32.Load(),
		BatchSize33To128: m.batchSize33To128.Load(),
		BatchSize129Plus: m.batchSize129Plus.Load(),

		Commits:               m.commits.Load(),
		BatchDurationNanos:    m.batchDurationNanos.Load(),
		BatchDurationMaxNanos: m.batchDurationMaxNanos.Load(),
		CommitFatals:          m.commitFatals.Load(),
		RequestRefusals:       m.requestRefusals.Load(),

		SavepointsCreated: m.savepointsCreated.Load(),
		SavepointsElided:  m.savepointsElided.Load(),
	}
}

func atomicMaxInt64(v *atomic.Int64, n int64) {
	for {
		old := v.Load()
		if n <= old {
			return
		}
		if v.CompareAndSwap(old, n) {
			return
		}
	}
}

// barrierOutcomeFromResult maps a value received on req.done (or resolve's
// lifecycle preference) to a BarrierOutcome. Publish invariant on the success
// path: done carries nil or an unwrapped logical refusal only; batch fatals
// carry ErrWriterFailed. Any other non-nil non-lifecycle error is classified as
// BarrierRefusal under that invariant — do not put arbitrary errors on done.
func barrierOutcomeFromResult(err error) BarrierOutcome {
	if err == nil {
		return BarrierOK
	}
	if errors.Is(err, ErrWriterFailed) {
		return BarrierWriterFailed
	}
	if errors.Is(err, ErrWriterClosed) {
		return BarrierWriterClosed
	}
	return BarrierRefusal
}
