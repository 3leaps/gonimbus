package reflowthroughput

import (
	"fmt"
	"strings"
)

// Known profile names. Unknown names hard-reject; empty defaults to smoke.
const (
	ProfileSmoke            = "smoke"
	ProfileReflowSaturation = "reflow-saturation"
	ProfileCeilingLift      = "ceiling-lift"
	ProfileCheckpoint       = "checkpoint"
	ProfileCheckpointScale  = "checkpoint-scale"
	ProfileFullPipe         = "fullpipe-ab"
	ProfileProbeSaturation  = "probe-saturation"
)

// Memory-arm labels. These name which lever bounds the run's memory, not a
// direction of change: the product binds the LOWEST detected limit, so an
// operator GOMEMLIMIT constrains the envelope and the absence of any override
// lets the physical-memory probe bind it.
const (
	// MemoryArmProbeBound applies no memory override — the product's detection
	// chain binds the limit (cgroup / physical RAM), and the budget is derived.
	MemoryArmProbeBound = "probe_bound"
	// MemoryArmGOMEMLIMIT constrains the limit with an operator GOMEMLIMIT,
	// which the chain selects when it is the lowest candidate.
	MemoryArmGOMEMLIMIT = "gomemlimit_constrained"
	// MemoryArmOperatorBudget sets the budget directly with --memory-budget,
	// leaving limit detection alone.
	MemoryArmOperatorBudget = "operator_budget"
)

// MemoryArm declares one memory envelope for a profile's points. The concrete
// GOMEMLIMIT / budget values are operator-supplied at invocation; the harness
// never invents them.
type MemoryArm struct {
	// Label is the reported memory_envelope value.
	Label string
	// UseGOMEMLIMIT sources the arm's GOMEMLIMIT from operator options.
	UseGOMEMLIMIT bool
	// UseMemoryBudget sources the arm's --memory-budget from operator options.
	UseMemoryBudget bool
}

// ProfileSpec declares a named measurement profile.
type ProfileSpec struct {
	Name   string
	Recipe Recipe
	// ExecutionShape: reflow_only | full_pipe
	ExecutionShape string
	// ParallelPoints for reflow --parallel sweeps.
	ParallelPoints []int
	// ProbeConcurrencyPoints for probe sweeps / full-pipe pairs.
	ProbeConcurrencyPoints []int
	// FullPipePairs is (probe, reflow) pairs for full-pipe profiles.
	FullPipePairs [][2]int
	// RequireOccupancy enables fixed-mode saturation occupancy sampling.
	RequireOccupancy bool
	// NoAdaptive forces --no-adaptive.
	NoAdaptive bool
	// MemoryArms declares the memory envelopes each parallel point is run
	// under. Empty means a single arm with no memory override. See MemoryArm.
	MemoryArms []MemoryArm
	// CheckpointClasses lists disk and/or tmpfs discriminators.
	CheckpointClasses []string
	// Description is operator-facing sterile help.
	Description string
}

// ResolveProfile maps a name to a spec. Empty name → smoke.
func ResolveProfile(name string) (ProfileSpec, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = ProfileSmoke
	}
	switch name {
	case ProfileSmoke:
		return ProfileSpec{
			Name:              ProfileSmoke,
			Recipe:            DefaultSmokeRecipe(),
			ExecutionShape:    "reflow_only",
			ParallelPoints:    []int{2},
			NoAdaptive:        true,
			RequireOccupancy:  false,
			CheckpointClasses: []string{"disk"},
			Description:       "Credential-free local smoke: generator, reflow-only transfer, structural gates.",
		}, nil
	case ProfileReflowSaturation:
		// Methodology: --parallel {8,32,64,128,256,512}; occupancy on fixed mode.
		return ProfileSpec{
			Name:              ProfileReflowSaturation,
			Recipe:            SaturationRecipe(),
			ExecutionShape:    "reflow_only",
			ParallelPoints:    []int{8, 32, 64, 128, 256, 512},
			NoAdaptive:        true,
			RequireOccupancy:  true,
			CheckpointClasses: []string{"disk"},
			Description:       "Reflow-saturation sweep on pre-frozen input (serial-dispatch / clamp detector).",
		}, nil
	case ProfileCeilingLift:
		// Three memory envelopes over the same sweep, so the effect of each
		// lever is read as a ratio against the others rather than in absolute
		// terms: constrained by GOMEMLIMIT, bound by the product's own
		// detection chain, and set directly by --memory-budget. Both operator
		// values are supplied at invocation; the harness never invents them.
		return ProfileSpec{
			Name:           ProfileCeilingLift,
			Recipe:         SaturationRecipe(),
			ExecutionShape: "reflow_only",
			ParallelPoints: []int{8, 32, 64, 128, 256, 512},
			NoAdaptive:     true,
			MemoryArms: []MemoryArm{
				{Label: MemoryArmGOMEMLIMIT, UseGOMEMLIMIT: true},
				{Label: MemoryArmProbeBound},
				{Label: MemoryArmOperatorBudget, UseMemoryBudget: true},
			},
			CheckpointClasses: []string{"disk"},
			Description:       "Ceiling-lift sweep across three memory envelopes: GOMEMLIMIT-constrained, detection-bound, and --memory-budget.",
		}, nil
	case ProfileCheckpoint:
		return ProfileSpec{
			Name:           ProfileCheckpoint,
			Recipe:         SaturationRecipe(),
			ExecutionShape: "reflow_only",
			ParallelPoints: []int{64, 256},
			NoAdaptive:     true,
			MemoryArms: []MemoryArm{
				{Label: MemoryArmGOMEMLIMIT, UseGOMEMLIMIT: true},
				{Label: MemoryArmProbeBound},
			},
			CheckpointClasses: []string{"disk", "tmpfs"},
			Description:       "Checkpoint discriminator: disk vs tmpfs class (paths never reported), under constrained and detection-bound memory.",
		}, nil
	case ProfileCheckpointScale:
		// Writer-contention discriminator at a lifted ceiling: the same
		// disk-vs-tmpfs checkpoint-class methodology as ProfileCheckpoint, but
		// on a large tiny-object corpus and at higher --parallel points, so
		// concurrent terminal checkpoint upserts contend on the single-connection
		// state writer. The probe_bound arm applies no memory override, so the
		// product's detection chain lifts the ceiling and the writer — not the
		// memory clamp — becomes the binding wall. Scale the corpus via
		// OBJECT_COUNT / SIZE_BYTES / PARTITIONS.
		// RequireOccupancy stays off (as in ProfileCheckpoint): the measurement is
		// the disk-vs-tmpfs timing ratio, not a saturation floor. A tiny-object
		// corpus drains before high --parallel points fill, so a hard occupancy
		// floor would fight the design; the product's own concurrency_max_active /
		// concurrency_ceiling_effective telemetry still records that the ceiling
		// lifted.
		return ProfileSpec{
			Name:           ProfileCheckpointScale,
			Recipe:         ScaleRecipe(),
			ExecutionShape: "reflow_only",
			ParallelPoints: []int{64, 256, 512},
			NoAdaptive:     true,
			MemoryArms: []MemoryArm{
				{Label: MemoryArmProbeBound},
			},
			CheckpointClasses: []string{"disk", "tmpfs"},
			Description:       "Checkpoint-scale: single-writer contention discriminator (disk vs tmpfs) at a detection-lifted ceiling on a large, overridable corpus.",
		}, nil
	case ProfileFullPipe:
		return ProfileSpec{
			Name:           ProfileFullPipe,
			Recipe:         DefaultSmokeRecipe(),
			ExecutionShape: "full_pipe",
			// Small declared pair set — not a Cartesian product.
			FullPipePairs: [][2]int{
				{4, 4},
				{8, 8},
				{16, 8},
			},
			CheckpointClasses: []string{"disk"},
			Description:       "Full-pipe canary: content probe | tap | reflow with per-stage rates.",
		}, nil
	case ProfileProbeSaturation:
		return ProfileSpec{
			Name:                   ProfileProbeSaturation,
			Recipe:                 DefaultSmokeRecipe(),
			ExecutionShape:         "probe_drain",
			ProbeConcurrencyPoints: []int{1, 4, 8, 16},
			CheckpointClasses:      []string{"disk"},
			Description:            "Probe-saturation: producer-only probe with draining sink (no reflow).",
		}, nil
	default:
		return ProfileSpec{}, fmt.Errorf("unknown profile %q (known: smoke, reflow-saturation, ceiling-lift, checkpoint, checkpoint-scale, fullpipe-ab, probe-saturation)", name)
	}
}

// ListProfiles returns known profile names for help text.
func ListProfiles() []string {
	return []string{
		ProfileSmoke,
		ProfileReflowSaturation,
		ProfileCeilingLift,
		ProfileCheckpoint,
		ProfileCheckpointScale,
		ProfileFullPipe,
		ProfileProbeSaturation,
	}
}
