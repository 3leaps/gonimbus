package reflowthroughput

import (
	"fmt"

	"github.com/3leaps/gonimbus/pkg/reflow"
)

// CheckCheckpointWriterStatsAdmission enforces the measurement-harness
// fail-closed contract for retained C1 evidence (GON-066 / entarch P1):
//
//   - exactly one gonimbus.reflow.checkpoint_writer_stats.v1 record
//   - that record appears only after the single summary
//   - structural integrity of the aggregate snapshot
//
// Product emission may remain best-effort; this gate is the strict enforcement
// boundary for checkpoint-scale (and any other profile that opts in).
func CheckCheckpointWriterStatsAdmission(p ParsedReflowOutput) error {
	if p.CheckpointWriterStatsCount != 1 {
		return fmt.Errorf("expected exactly one %s, got %d (absent/duplicate makes the arm non-comparable)",
			reflow.CheckpointWriterStatsRecordType, p.CheckpointWriterStatsCount)
	}
	if p.CheckpointWriterStatsBeforeSummary != 0 {
		return fmt.Errorf("%s must appear after the single summary (saw %d before summary)",
			reflow.CheckpointWriterStatsRecordType, p.CheckpointWriterStatsBeforeSummary)
	}
	if p.CheckpointWriterStats == nil {
		return fmt.Errorf("%s parsed nil despite count=1", reflow.CheckpointWriterStatsRecordType)
	}
	if err := ValidateCheckpointWriterStatsStructure(*p.CheckpointWriterStats); err != nil {
		return err
	}
	return nil
}

// ValidateCheckpointWriterStatsStructure pins the sterile aggregate invariants
// required for an honest retained arm. A successful comparable arm must not
// carry writer-fatal/closed/canceled barrier outcomes.
func ValidateCheckpointWriterStatsStructure(st reflow.CheckpointWriterStatsRecord) error {
	if st.MaxBatch < 1 {
		return fmt.Errorf("checkpoint writer stats: MaxBatch=%d, want > 0", st.MaxBatch)
	}
	if st.QueueDepthSamples != st.Admissions {
		return fmt.Errorf("checkpoint writer stats: QueueDepthSamples=%d != Admissions=%d",
			st.QueueDepthSamples, st.Admissions)
	}
	if st.Barriers != st.Admissions {
		return fmt.Errorf("checkpoint writer stats: Barriers=%d != Admissions=%d",
			st.Barriers, st.Admissions)
	}
	outcomeSum := st.BarrierOK + st.BarrierRefusal + st.BarrierWriterFailed +
		st.BarrierWriterClosed + st.BarrierCanceled
	if outcomeSum != st.Barriers {
		return fmt.Errorf("checkpoint writer stats: barrier outcome sum=%d != Barriers=%d",
			outcomeSum, st.Barriers)
	}
	if st.Batches != st.Commits {
		return fmt.Errorf("checkpoint writer stats: Batches=%d != Commits=%d",
			st.Batches, st.Commits)
	}
	histSum := st.BatchSize1 + st.BatchSize2To8 + st.BatchSize9To32 +
		st.BatchSize33To128 + st.BatchSize129Plus
	if histSum != st.Batches {
		return fmt.Errorf("checkpoint writer stats: batch histogram sum=%d != Batches=%d",
			histSum, st.Batches)
	}
	if st.BatchSizeSum != st.Admissions {
		return fmt.Errorf("checkpoint writer stats: BatchSizeSum=%d != Admissions=%d",
			st.BatchSizeSum, st.Admissions)
	}
	if st.BatchSizeMax > int64(st.MaxBatch) {
		return fmt.Errorf("checkpoint writer stats: BatchSizeMax=%d > MaxBatch=%d",
			st.BatchSizeMax, st.MaxBatch)
	}
	if st.QueueDepthPeak > int64(st.MaxBatch) {
		return fmt.Errorf("checkpoint writer stats: QueueDepthPeak=%d > MaxBatch=%d",
			st.QueueDepthPeak, st.MaxBatch)
	}
	// Honest retained arm: no terminal writer lifecycle or cancel-masked barriers.
	if st.BarrierWriterFailed != 0 || st.BarrierWriterClosed != 0 || st.BarrierCanceled != 0 {
		return fmt.Errorf("checkpoint writer stats: retained arm rejects barrier outcomes failed=%d closed=%d canceled=%d (must be zero)",
			st.BarrierWriterFailed, st.BarrierWriterClosed, st.BarrierCanceled)
	}
	if st.CommitFatals != 0 {
		return fmt.Errorf("checkpoint writer stats: CommitFatals=%d, want 0 for honest retained arm", st.CommitFatals)
	}
	return nil
}

// profileRequiresCheckpointWriterStats reports whether a harness profile must
// fail closed on missing/duplicate/invalid writer stats before publishing a report.
func profileRequiresCheckpointWriterStats(profile string) bool {
	return profile == ProfileCheckpointScale
}
