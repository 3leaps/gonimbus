package reflowthroughput

import "fmt"

// Frozen schedule IDs for checkpoint-scale counterbalancing.
const (
	ScheduleDiskFirstOdd  = "disk_first_odd"  // pair order D/T, T/D, D/T
	ScheduleTmpfsFirstOdd = "tmpfs_first_odd" // reverse: T/D, D/T, T/D
)

// ScheduleStep is one planned arm in a checkpoint-scale run.
type ScheduleStep struct {
	// PairIndex is 0 for warmups; 1..N for retained matched pairs.
	PairIndex int
	// Warmup marks discarded arms (not retained in report / Δe2e).
	Warmup          bool
	Parallel        int
	CheckpointClass string // disk | tmpfs
	MemoryEnvelope  string
	// Ordinal is 0-based position in the full execution order (including warmups).
	Ordinal int
}

// CheckpointScaleSchedule builds the frozen counterbalanced plan for a profile.
// scheduleID must be ScheduleDiskFirstOdd or ScheduleTmpfsFirstOdd.
//
// Layout:
//  1. one discarded warmup per class (first parallel point, first memory arm)
//  2. three matched pairs over ParallelPoints in order, alternating first-class
//     as D/T, T/D, D/T (or reverse for tmpfs_first_odd)
func CheckpointScaleSchedule(spec ProfileSpec, scheduleID string) ([]ScheduleStep, error) {
	if spec.Name != ProfileCheckpointScale {
		return nil, fmt.Errorf("CheckpointScaleSchedule only for %s, got %s", ProfileCheckpointScale, spec.Name)
	}
	if len(spec.ParallelPoints) != BindPairCount {
		return nil, fmt.Errorf("checkpoint-scale requires exactly %d parallel points (one per pair), got %d",
			BindPairCount, len(spec.ParallelPoints))
	}
	classes := spec.CheckpointClasses
	if len(classes) != 2 {
		return nil, fmt.Errorf("checkpoint-scale requires exactly 2 checkpoint classes, got %v", classes)
	}
	// Normalize expected class set.
	hasDisk, hasTmpfs := false, false
	for _, c := range classes {
		switch c {
		case "disk":
			hasDisk = true
		case "tmpfs":
			hasTmpfs = true
		}
	}
	if !hasDisk || !hasTmpfs {
		return nil, fmt.Errorf("checkpoint-scale classes must include disk and tmpfs, got %v", classes)
	}
	arms := spec.MemoryArms
	if len(arms) == 0 {
		return nil, fmt.Errorf("checkpoint-scale requires at least one memory arm")
	}
	env := arms[0].Label

	var pairFirst []string
	switch scheduleID {
	case ScheduleDiskFirstOdd:
		pairFirst = []string{"disk", "tmpfs", "disk"}
	case ScheduleTmpfsFirstOdd:
		pairFirst = []string{"tmpfs", "disk", "tmpfs"}
	default:
		return nil, fmt.Errorf("unknown schedule id %q", scheduleID)
	}

	steps := make([]ScheduleStep, 0, 2+2*BindPairCount)
	ord := 0
	// Warmups: one per class, discarded.
	for _, ck := range []string{"disk", "tmpfs"} {
		steps = append(steps, ScheduleStep{
			PairIndex: 0, Warmup: true, Parallel: spec.ParallelPoints[0],
			CheckpointClass: ck, MemoryEnvelope: env, Ordinal: ord,
		})
		ord++
	}
	for i, par := range spec.ParallelPoints {
		first := pairFirst[i]
		second := "tmpfs"
		if first == "tmpfs" {
			second = "disk"
		}
		for _, ck := range []string{first, second} {
			steps = append(steps, ScheduleStep{
				PairIndex: i + 1, Warmup: false, Parallel: par,
				CheckpointClass: ck, MemoryEnvelope: env, Ordinal: ord,
			})
			ord++
		}
	}
	return steps, nil
}

// ValidateCheckpointScaleSchedule checks counterbalancing invariants on a plan.
func ValidateCheckpointScaleSchedule(steps []ScheduleStep) error {
	var warmDisk, warmTmpfs int
	type pairOrd struct {
		first string
		pars  []int
		cks   []string
	}
	pairs := map[int]*pairOrd{}
	for _, s := range steps {
		if s.Warmup {
			switch s.CheckpointClass {
			case "disk":
				warmDisk++
			case "tmpfs":
				warmTmpfs++
			}
			continue
		}
		p := pairs[s.PairIndex]
		if p == nil {
			p = &pairOrd{}
			pairs[s.PairIndex] = p
		}
		if p.first == "" {
			p.first = s.CheckpointClass
		}
		p.pars = append(p.pars, s.Parallel)
		p.cks = append(p.cks, s.CheckpointClass)
	}
	if warmDisk != 1 || warmTmpfs != 1 {
		return fmt.Errorf("warmup count disk=%d tmpfs=%d, want 1 each", warmDisk, warmTmpfs)
	}
	if len(pairs) != BindPairCount {
		return fmt.Errorf("retained pairs=%d, want %d", len(pairs), BindPairCount)
	}
	diskFirst, tmpfsFirst := 0, 0
	for i := 1; i <= BindPairCount; i++ {
		p := pairs[i]
		if p == nil || len(p.cks) != 2 {
			return fmt.Errorf("pair %d incomplete: %v", i, p)
		}
		if p.pars[0] != p.pars[1] {
			return fmt.Errorf("pair %d parallel mismatch %v", i, p.pars)
		}
		set := map[string]bool{p.cks[0]: true, p.cks[1]: true}
		if !set["disk"] || !set["tmpfs"] {
			return fmt.Errorf("pair %d classes %v", i, p.cks)
		}
		if p.first == "disk" {
			diskFirst++
		} else {
			tmpfsFirst++
		}
	}
	// Neither class first in all pairs.
	if diskFirst == BindPairCount || tmpfsFirst == BindPairCount {
		return fmt.Errorf("class order not counterbalanced: disk-first=%d tmpfs-first=%d", diskFirst, tmpfsFirst)
	}
	return nil
}
