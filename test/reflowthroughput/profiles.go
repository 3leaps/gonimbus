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
	ProfileFullPipe         = "fullpipe-ab"
	ProfileProbeSaturation  = "probe-saturation"
)

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
	// RequireGOMEMLIMIT when true requires operator GOMEMLIMIT (never auto-set).
	RequireGOMEMLIMIT bool
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
		return ProfileSpec{
			Name:              ProfileCeilingLift,
			Recipe:            SaturationRecipe(),
			ExecutionShape:    "reflow_only",
			ParallelPoints:    []int{8, 32, 64, 128, 256, 512},
			NoAdaptive:        true,
			RequireGOMEMLIMIT: true,
			CheckpointClasses: []string{"disk"},
			Description:       "Ceiling-lift sweep; requires operator-supplied GOMEMLIMIT (never auto-raised).",
		}, nil
	case ProfileCheckpoint:
		return ProfileSpec{
			Name:              ProfileCheckpoint,
			Recipe:            SaturationRecipe(),
			ExecutionShape:    "reflow_only",
			ParallelPoints:    []int{64, 256},
			NoAdaptive:        true,
			CheckpointClasses: []string{"disk", "tmpfs"},
			Description:       "Checkpoint discriminator: disk vs tmpfs class (paths never reported).",
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
		return ProfileSpec{}, fmt.Errorf("unknown profile %q (known: smoke, reflow-saturation, ceiling-lift, checkpoint, fullpipe-ab, probe-saturation)", name)
	}
}

// ListProfiles returns known profile names for help text.
func ListProfiles() []string {
	return []string{
		ProfileSmoke,
		ProfileReflowSaturation,
		ProfileCeilingLift,
		ProfileCheckpoint,
		ProfileFullPipe,
		ProfileProbeSaturation,
	}
}
