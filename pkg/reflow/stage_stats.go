package reflow

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/3leaps/gonimbus/pkg/transfer"
)

// StageStats accumulates sterile, process-local object-path stage timings for
// one reflow run. Safe for concurrent workers. Counts and nanoseconds only —
// no keys, URIs, paths, or provider identifiers.
//
// Measure-first attribution for GON-067: not product throughput marketing.
type StageStats struct {
	permitWaits              atomic.Int64
	permitWaitNanos          atomic.Int64
	permitWaitMaxNanos       atomic.Int64
	sourcePermitWaits        atomic.Int64
	sourcePermitWaitNanos    atomic.Int64
	sourcePermitWaitMaxNanos atomic.Int64
	destPermitWaits          atomic.Int64
	destPermitWaitNanos      atomic.Int64
	destPermitWaitMaxNanos   atomic.Int64
	probePermitWaits         atomic.Int64
	probePermitWaitNanos     atomic.Int64
	probePermitWaitMaxNanos  atomic.Int64
	sourceReads              atomic.Int64
	sourceReadNanos          atomic.Int64
	sourceReadMaxNanos       atomic.Int64
	destWrites               atomic.Int64
	destWriteNanos           atomic.Int64
	destWriteMaxNanos        atomic.Int64
	coupledCopies            atomic.Int64
	coupledCopyNanos         atomic.Int64
	coupledCopyMaxNanos      atomic.Int64
	collisionProbes          atomic.Int64
	collisionProbeNanos      atomic.Int64
	collisionProbeMaxNanos   atomic.Int64
	checkpointWrites         atomic.Int64
	checkpointWriteNanos     atomic.Int64
	checkpointWriteMaxNanos  atomic.Int64
	phaseSplitCopies         atomic.Int64
	coupledCopyOps           atomic.Int64

	// Peaks / policy filled at Snapshot time from the limiter when provided.
	sourceMaxActive            int
	destMaxActive              int
	probeMaxActive             int
	destDomainMultiplier       int
	destDomainCeilingEffective int
}

func newStageStats() *StageStats {
	return &StageStats{}
}

// NewStageStats constructs an empty StageStats accumulator for a reflow run.
// Used by the CLI pool path; the engine constructs one inside prepareRun.
func NewStageStats() *StageStats {
	return newStageStats()
}

func (s *StageStats) observeMax(max *atomic.Int64, v int64) {
	for {
		cur := max.Load()
		if v <= cur {
			return
		}
		if max.CompareAndSwap(cur, v) {
			return
		}
	}
}

func (s *StageStats) recordPermitWait(d time.Duration) {
	if s == nil {
		return
	}
	n := d.Nanoseconds()
	s.permitWaits.Add(1)
	s.permitWaitNanos.Add(n)
	s.observeMax(&s.permitWaitMaxNanos, n)
}

func (s *StageStats) recordDomainPermitWait(domain string, d time.Duration) {
	if s == nil {
		return
	}
	s.recordPermitWait(d)
	n := d.Nanoseconds()
	switch domain {
	case "source":
		s.sourcePermitWaits.Add(1)
		s.sourcePermitWaitNanos.Add(n)
		s.observeMax(&s.sourcePermitWaitMaxNanos, n)
	case "dest":
		s.destPermitWaits.Add(1)
		s.destPermitWaitNanos.Add(n)
		s.observeMax(&s.destPermitWaitMaxNanos, n)
	case "probe":
		s.probePermitWaits.Add(1)
		s.probePermitWaitNanos.Add(n)
		s.observeMax(&s.probePermitWaitMaxNanos, n)
	}
}

// AttachLimiterPeaks copies per-domain peaks and dest-domain policy from the
// limiter into the stage-stats snapshot (called once at run end).
func (s *StageStats) AttachLimiterPeaks(lim *ConcurrencyLimiter) {
	if s == nil || lim == nil {
		return
	}
	snap := lim.Snapshot()
	s.sourceMaxActive = snap.ConcurrencyMaxActiveSource
	s.destMaxActive = snap.ConcurrencyMaxActiveDest
	s.probeMaxActive = snap.ConcurrencyMaxActiveProbe
	s.destDomainMultiplier = snap.DestDomainMultiplier
	s.destDomainCeilingEffective = snap.DestDomainCeilingEffective
}

func (s *StageStats) recordSourceRead(d time.Duration) {
	if s == nil {
		return
	}
	n := d.Nanoseconds()
	s.sourceReads.Add(1)
	s.sourceReadNanos.Add(n)
	s.observeMax(&s.sourceReadMaxNanos, n)
}

func (s *StageStats) recordDestWrite(d time.Duration) {
	if s == nil {
		return
	}
	n := d.Nanoseconds()
	s.destWrites.Add(1)
	s.destWriteNanos.Add(n)
	s.observeMax(&s.destWriteMaxNanos, n)
}

func (s *StageStats) recordCoupledCopy(d time.Duration) {
	if s == nil {
		return
	}
	n := d.Nanoseconds()
	s.coupledCopies.Add(1)
	s.coupledCopyNanos.Add(n)
	s.observeMax(&s.coupledCopyMaxNanos, n)
	s.coupledCopyOps.Add(1)
}

func (s *StageStats) recordPhaseSplitCopy() {
	if s == nil {
		return
	}
	s.phaseSplitCopies.Add(1)
}

func (s *StageStats) recordCollisionProbe(d time.Duration) {
	if s == nil {
		return
	}
	n := d.Nanoseconds()
	s.collisionProbes.Add(1)
	s.collisionProbeNanos.Add(n)
	s.observeMax(&s.collisionProbeMaxNanos, n)
}

// Snapshot returns an immutable sterile record for JSONL emission.
func (s *StageStats) Snapshot() ObjectPathStageStatsRecord {
	if s == nil {
		return ObjectPathStageStatsRecord{}
	}
	return ObjectPathStageStatsRecord{
		PermitWaits:                s.permitWaits.Load(),
		PermitWaitNanos:            s.permitWaitNanos.Load(),
		PermitWaitMaxNanos:         s.permitWaitMaxNanos.Load(),
		SourceReads:                s.sourceReads.Load(),
		SourceReadNanos:            s.sourceReadNanos.Load(),
		SourceReadMaxNanos:         s.sourceReadMaxNanos.Load(),
		DestWrites:                 s.destWrites.Load(),
		DestWriteNanos:             s.destWriteNanos.Load(),
		DestWriteMaxNanos:          s.destWriteMaxNanos.Load(),
		CoupledCopies:              s.coupledCopies.Load(),
		CoupledCopyNanos:           s.coupledCopyNanos.Load(),
		CoupledCopyMaxNanos:        s.coupledCopyMaxNanos.Load(),
		CollisionProbes:            s.collisionProbes.Load(),
		CollisionProbeNanos:        s.collisionProbeNanos.Load(),
		CollisionProbeMaxNanos:     s.collisionProbeMaxNanos.Load(),
		CheckpointWrites:           s.checkpointWrites.Load(),
		CheckpointWriteNanos:       s.checkpointWriteNanos.Load(),
		CheckpointWriteMaxNanos:    s.checkpointWriteMaxNanos.Load(),
		PhaseSplitCopies:           s.phaseSplitCopies.Load(),
		CoupledCopyOps:             s.coupledCopyOps.Load(),
		SourcePermitWaits:          s.sourcePermitWaits.Load(),
		SourcePermitWaitNanos:      s.sourcePermitWaitNanos.Load(),
		SourcePermitWaitMaxNanos:   s.sourcePermitWaitMaxNanos.Load(),
		DestPermitWaits:            s.destPermitWaits.Load(),
		DestPermitWaitNanos:        s.destPermitWaitNanos.Load(),
		DestPermitWaitMaxNanos:     s.destPermitWaitMaxNanos.Load(),
		ProbePermitWaits:           s.probePermitWaits.Load(),
		ProbePermitWaitNanos:       s.probePermitWaitNanos.Load(),
		ProbePermitWaitMaxNanos:    s.probePermitWaitMaxNanos.Load(),
		SourceMaxActive:            s.sourceMaxActive,
		DestMaxActive:              s.destMaxActive,
		ProbeMaxActive:             s.probeMaxActive,
		DestDomainMultiplier:       s.destDomainMultiplier,
		DestDomainCeilingEffective: s.destDomainCeilingEffective,
	}
}

// limiterCopyGate acquires one concurrency token per sequential copy stage so
// source-read of object B can overlap dest-write of object A under the same
// effective ceiling. Memory reservation remains outside the gate (whole copy).
type limiterCopyGate struct {
	limiter *ConcurrencyLimiter
	stats   *StageStats
}

func newLimiterCopyGate(limiter *ConcurrencyLimiter, stats *StageStats) *limiterCopyGate {
	return &limiterCopyGate{limiter: limiter, stats: stats}
}

// NewLimiterCopyGate returns a transfer.CopyGate that acquires one concurrency
// token per sequential copy stage and records sterile stage timings.
func NewLimiterCopyGate(limiter *ConcurrencyLimiter, stats *StageStats) transfer.CopyGate {
	return newLimiterCopyGate(limiter, stats)
}

func (g *limiterCopyGate) Do(ctx context.Context, stage string, fn func(context.Context) error) error {
	waitStart := time.Now()
	release, domain, err := g.acquireForStage(ctx, stage)
	if err != nil {
		return err
	}
	// Empty domain means acquireForStage already recorded per-domain waits
	// (coupled path takes both domains).
	if domain != "" {
		g.stats.recordDomainPermitWait(domain, time.Since(waitStart))
	}
	defer release()

	start := time.Now()
	err = fn(ctx)
	elapsed := time.Since(start)
	switch stage {
	case transfer.CopyStageSourceRead:
		g.stats.recordSourceRead(elapsed)
		if err == nil {
			g.stats.recordPhaseSplitCopy()
		}
	case transfer.CopyStageDestWrite:
		g.stats.recordDestWrite(elapsed)
	case transfer.CopyStageCoupled:
		g.stats.recordCoupledCopy(elapsed)
	}
	return err
}

// acquireForStage returns release, domain label for wait accounting, err.
// Coupled acquires both domains and records each wait itself (domain "").
func (g *limiterCopyGate) acquireForStage(ctx context.Context, stage string) (func(), string, error) {
	switch stage {
	case transfer.CopyStageSourceRead:
		rel, err := g.limiter.AcquireSource(ctx)
		return rel, "source", err
	case transfer.CopyStageDestWrite:
		rel, err := g.limiter.AcquireDest(ctx)
		return rel, "dest", err
	case transfer.CopyStageCoupled:
		// Streaming multipart keeps source and dest open together.
		// Dest may admit more tokens than source under PR2 bias; coupled
		// objects still need one of each, so concurrent multiparts track min.
		waitSrc := time.Now()
		releaseSrc, err := g.limiter.AcquireSource(ctx)
		if err != nil {
			return nil, "", err
		}
		g.stats.recordDomainPermitWait("source", time.Since(waitSrc))
		waitDst := time.Now()
		releaseDst, err := g.limiter.AcquireDest(ctx)
		if err != nil {
			releaseSrc()
			return nil, "", err
		}
		g.stats.recordDomainPermitWait("dest", time.Since(waitDst))
		return func() {
			releaseDst()
			releaseSrc()
		}, "", nil
	default:
		rel, err := g.limiter.Acquire(ctx)
		return rel, "probe", err
	}
}

// ObjectPathStageStatsEmitter is an optional EventSink extension. When the
// engine's Events value implements it, finishRun emits sterile stage stats
// after the summary. CLI JSONL adapters implement this; test sinks need not.
type ObjectPathStageStatsEmitter interface {
	OnObjectPathStageStats(ctx context.Context, rec ObjectPathStageStatsRecord) error
}
