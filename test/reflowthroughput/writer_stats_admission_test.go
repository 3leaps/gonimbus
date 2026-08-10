package reflowthroughput

import (
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/pkg/reflow"
)

func validStats(admissions int64) reflow.CheckpointWriterStatsRecord {
	// Single-request batches: Batches == Admissions, hist all in BatchSize1.
	return reflow.CheckpointWriterStatsRecord{
		MaxBatch:          256,
		QueueDepthSamples: admissions,
		Admissions:        admissions,
		Barriers:          admissions,
		BarrierOK:         admissions,
		Batches:           admissions,
		BatchSizeSum:      admissions,
		BatchSizeMax:      1,
		BatchSize1:        admissions,
		Commits:           admissions,
	}
}

func TestValidateCheckpointWriterStatsStructureOK(t *testing.T) {
	t.Parallel()
	if err := ValidateCheckpointWriterStatsStructure(validStats(10)); err != nil {
		t.Fatal(err)
	}
}

func TestValidateCheckpointWriterStatsStructureRejects(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		mut  func(*reflow.CheckpointWriterStatsRecord)
	}{
		{"max_batch_zero", func(s *reflow.CheckpointWriterStatsRecord) { s.MaxBatch = 0 }},
		{"barrier_partition", func(s *reflow.CheckpointWriterStatsRecord) { s.BarrierOK = s.Admissions - 1 }},
		{"canceled", func(s *reflow.CheckpointWriterStatsRecord) {
			s.BarrierCanceled = 1
			s.BarrierOK = s.Admissions - 1
		}},
		{"hist_sum", func(s *reflow.CheckpointWriterStatsRecord) { s.BatchSize1 = 0 }},
		{"batch_size_sum", func(s *reflow.CheckpointWriterStatsRecord) { s.BatchSizeSum = 0 }},
		{"peak_over_max", func(s *reflow.CheckpointWriterStatsRecord) { s.QueueDepthPeak = 999 }},
		{"admission_blocked_over", func(s *reflow.CheckpointWriterStatsRecord) { s.AdmissionBlocked = s.Admissions + 1 }},
		{"neg_admissions", func(s *reflow.CheckpointWriterStatsRecord) { s.Admissions = -1 }},
		{"neg_blocked", func(s *reflow.CheckpointWriterStatsRecord) { s.AdmissionBlocked = -1 }},
		{"neg_peak", func(s *reflow.CheckpointWriterStatsRecord) { s.QueueDepthPeak = -1 }},
		{"neg_barrier_wait", func(s *reflow.CheckpointWriterStatsRecord) { s.BarrierWaitNanos = -1 }},
		{"neg_batch_dur", func(s *reflow.CheckpointWriterStatsRecord) { s.BatchDurationNanos = -1 }},
		{"neg_queue_sum", func(s *reflow.CheckpointWriterStatsRecord) { s.QueueDepthSum = -1 }},
		{"neg_batch_size_max", func(s *reflow.CheckpointWriterStatsRecord) { s.BatchSizeMax = -1 }},
		{"neg_commits", func(s *reflow.CheckpointWriterStatsRecord) { s.Commits = -1 }},
		{"neg_refusals", func(s *reflow.CheckpointWriterStatsRecord) { s.RequestRefusals = -1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := validStats(4)
			tc.mut(&st)
			if err := ValidateCheckpointWriterStatsStructure(st); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestCheckCheckpointWriterStatsAdmissionAbsent(t *testing.T) {
	t.Parallel()
	var p ParsedReflowOutput
	if err := CheckCheckpointWriterStatsAdmission(p); err == nil {
		t.Fatal("expected error for missing stats")
	}
}

func TestCheckCheckpointWriterStatsAdmissionDuplicate(t *testing.T) {
	t.Parallel()
	st := validStats(2)
	p := ParsedReflowOutput{
		CheckpointWriterStats:      &st,
		CheckpointWriterStatsCount: 2,
	}
	if err := CheckCheckpointWriterStatsAdmission(p); err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("expected exactly-one error, got %v", err)
	}
}

func TestCheckCheckpointWriterStatsAdmissionPreSummary(t *testing.T) {
	t.Parallel()
	st := validStats(2)
	p := ParsedReflowOutput{
		CheckpointWriterStats:              &st,
		CheckpointWriterStatsCount:         1,
		CheckpointWriterStatsBeforeSummary: 1,
	}
	if err := CheckCheckpointWriterStatsAdmission(p); err == nil || !strings.Contains(err.Error(), "after the single summary") {
		t.Fatalf("expected pre-summary error, got %v", err)
	}
}

func TestCheckCheckpointWriterStatsAdmissionOK(t *testing.T) {
	t.Parallel()
	st := validStats(3)
	p := ParsedReflowOutput{
		CheckpointWriterStats:      &st,
		CheckpointWriterStatsCount: 1,
	}
	if err := CheckCheckpointWriterStatsAdmission(p); err != nil {
		t.Fatal(err)
	}
}

func TestParseRejectsPreSummaryStatsOnlyWhenStrict(t *testing.T) {
	// Soft parse still succeeds (stats optional for smoke); counts surface for gate.
	t.Parallel()
	stdout := strings.Join([]string{
		`{"type":"gonimbus.reflow.run.v1","data":{"dest_uri":"x","checkpoint_path":"y","dry_run":false,"resume":false,"parallel":1,"adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":1,"concurrency_ceiling_requested":1,"concurrency_ceiling_effective":1,"concurrency_ceiling_reason":"requested","concurrency_final":1,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":1}}`,
		`{"type":"gonimbus.reflow.checkpoint_writer_stats.v1","data":{"max_batch":256,"admissions":1,"queue_depth_samples":1,"barriers":1,"barrier_ok":1,"batches":1,"batch_size_sum":1,"batch_size_max":1,"batch_size_1":1,"commits":1}}`,
		`{"type":"gonimbus.reflow.summary.v1","data":{"dest_uri":"x","dry_run":false,"on_collision":"skip-if-duplicate","adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":1,"concurrency_ceiling_requested":1,"concurrency_ceiling_effective":1,"concurrency_ceiling_reason":"requested","concurrency_final":1,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":1,"dest_ifabsent_honored":null,"fallback_active":false,"ifabsent_fallback_objects":0,"statuses":{"complete":1},"errors":0,"invalid_inputs":0}}`,
	}, "\n") + "\n"
	p, err := ParseReflowStdout([]byte(stdout))
	if err != nil {
		t.Fatal(err)
	}
	if p.CheckpointWriterStatsBeforeSummary != 1 || p.CheckpointWriterStatsCount != 1 {
		t.Fatalf("before=%d count=%d", p.CheckpointWriterStatsBeforeSummary, p.CheckpointWriterStatsCount)
	}
	if err := CheckCheckpointWriterStatsAdmission(p); err == nil {
		t.Fatal("admission must reject pre-summary stats")
	}
}

func TestParseDuplicateStatsCount(t *testing.T) {
	t.Parallel()
	stdout := strings.Join([]string{
		`{"type":"gonimbus.reflow.run.v1","data":{"dest_uri":"x","checkpoint_path":"y","dry_run":false,"resume":false,"parallel":1,"adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":1,"concurrency_ceiling_requested":1,"concurrency_ceiling_effective":1,"concurrency_ceiling_reason":"requested","concurrency_final":1,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":1}}`,
		`{"type":"gonimbus.reflow.summary.v1","data":{"dest_uri":"x","dry_run":false,"on_collision":"skip-if-duplicate","adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":1,"concurrency_ceiling_requested":1,"concurrency_ceiling_effective":1,"concurrency_ceiling_reason":"requested","concurrency_final":1,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":1,"dest_ifabsent_honored":null,"fallback_active":false,"ifabsent_fallback_objects":0,"statuses":{"complete":1},"errors":0,"invalid_inputs":0}}`,
		`{"type":"gonimbus.reflow.checkpoint_writer_stats.v1","data":{"max_batch":256,"admissions":1,"queue_depth_samples":1,"barriers":1,"barrier_ok":1,"batches":1,"batch_size_sum":1,"batch_size_max":1,"batch_size_1":1,"commits":1}}`,
		`{"type":"gonimbus.reflow.checkpoint_writer_stats.v1","data":{"max_batch":256,"admissions":2,"queue_depth_samples":2,"barriers":2,"barrier_ok":2,"batches":2,"batch_size_sum":2,"batch_size_max":1,"batch_size_1":2,"commits":2}}`,
	}, "\n") + "\n"
	p, err := ParseReflowStdout([]byte(stdout))
	if err != nil {
		t.Fatal(err)
	}
	if p.CheckpointWriterStatsCount != 2 {
		t.Fatalf("count=%d", p.CheckpointWriterStatsCount)
	}
	if err := CheckCheckpointWriterStatsAdmission(p); err == nil {
		t.Fatal("admission must reject duplicates")
	}
}

func TestProfileRequiresCheckpointWriterStats(t *testing.T) {
	t.Parallel()
	if !profileRequiresCheckpointWriterStats(ProfileCheckpointScale) {
		t.Fatal("checkpoint-scale must require stats")
	}
	if profileRequiresCheckpointWriterStats(ProfileSmoke) {
		t.Fatal("smoke must not require stats (optional plumbing)")
	}
}
