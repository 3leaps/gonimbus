package reflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/transfer"
)

func TestStageStatsSnapshotAggregates(t *testing.T) {
	s := NewStageStats()
	s.recordPermitWait(10 * time.Millisecond)
	s.recordPermitWait(5 * time.Millisecond)
	s.recordSourceRead(20 * time.Millisecond)
	s.recordDestWrite(30 * time.Millisecond)
	s.recordPhaseSplitCopy()
	s.recordCollisionProbe(2 * time.Millisecond)

	snap := s.Snapshot()
	if snap.PermitWaits != 2 {
		t.Fatalf("permit_waits=%d", snap.PermitWaits)
	}
	if snap.PermitWaitNanos < int64(15*time.Millisecond) {
		t.Fatalf("permit_wait_nanos=%d", snap.PermitWaitNanos)
	}
	if snap.PermitWaitMaxNanos < int64(10*time.Millisecond) {
		t.Fatalf("permit_wait_max=%d", snap.PermitWaitMaxNanos)
	}
	if snap.SourceReads != 1 || snap.DestWrites != 1 {
		t.Fatalf("source=%d dest=%d", snap.SourceReads, snap.DestWrites)
	}
	if snap.PhaseSplitCopies != 1 {
		t.Fatalf("phase_split=%d", snap.PhaseSplitCopies)
	}
	if snap.CollisionProbes != 1 {
		t.Fatalf("collision_probes=%d", snap.CollisionProbes)
	}
}

func TestLimiterCopyGateRecordsStages(t *testing.T) {
	limiter := NewConcurrencyLimiter(ConcurrencyConfig{
		RequestedCeiling: 2,
		EffectiveCeiling: 2,
		Initial:          2,
		Floor:            1,
		AdaptiveEnabled:  false,
	})
	stats := NewStageStats()
	gate := NewLimiterCopyGate(limiter, stats)

	if err := gate.Do(context.Background(), transfer.CopyStageSourceRead, func(context.Context) error {
		time.Sleep(time.Millisecond)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := gate.Do(context.Background(), transfer.CopyStageDestWrite, func(context.Context) error {
		time.Sleep(time.Millisecond)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	snap := stats.Snapshot()
	if snap.PermitWaits != 2 {
		t.Fatalf("permit_waits=%d", snap.PermitWaits)
	}
	if snap.SourceReads != 1 || snap.DestWrites != 1 {
		t.Fatalf("source=%d dest=%d", snap.SourceReads, snap.DestWrites)
	}
	if snap.PhaseSplitCopies != 1 {
		t.Fatalf("phase_split=%d", snap.PhaseSplitCopies)
	}
}

func TestLimiterCopyGateDualDomainAllowsSourceAndDestUnderCeilingOne(t *testing.T) {
	// Dual-domain permits: under ceiling 1, a dest-write must not block a
	// concurrent source-read of another object (global single-token coupling
	// would serialize them).
	limiter := NewConcurrencyLimiter(ConcurrencyConfig{
		RequestedCeiling: 1,
		EffectiveCeiling: 1,
		Initial:          1,
		Floor:            1,
		AdaptiveEnabled:  false,
	})
	stats := NewStageStats()
	gate := NewLimiterCopyGate(limiter, stats)

	var wg sync.WaitGroup
	started := make(chan struct{})
	releaseA := make(chan struct{})
	// Worker A holds dest stage until we say so.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = gate.Do(context.Background(), transfer.CopyStageDestWrite, func(context.Context) error {
			close(started)
			<-releaseA
			return nil
		})
	}()
	<-started
	// Worker B should still acquire source-read under ceiling 1.
	doneB := make(chan error, 1)
	go func() {
		doneB <- gate.Do(context.Background(), transfer.CopyStageSourceRead, func(context.Context) error {
			return nil
		})
	}()
	select {
	case err := <-doneB:
		if err != nil {
			t.Fatalf("worker B: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker B blocked — dual-domain permits did not decouple source from dest under ceiling 1")
	}
	close(releaseA)
	wg.Wait()

	// Same-domain still serializes under ceiling 1.
	started2 := make(chan struct{})
	releaseC := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = gate.Do(context.Background(), transfer.CopyStageSourceRead, func(context.Context) error {
			close(started2)
			<-releaseC
			return nil
		})
	}()
	<-started2
	doneD := make(chan error, 1)
	go func() {
		doneD <- gate.Do(context.Background(), transfer.CopyStageSourceRead, func(context.Context) error {
			return nil
		})
	}()
	select {
	case <-doneD:
		t.Fatal("second source-read must wait while first holds the source domain under ceiling 1")
	case <-time.After(50 * time.Millisecond):
		// expected: still waiting
	}
	close(releaseC)
	select {
	case err := <-doneD:
		if err != nil {
			t.Fatalf("worker D: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker D did not complete after source domain released")
	}
	wg.Wait()
}
