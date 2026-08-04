package reflowthroughput

import "math"

// Frozen numeric constants (GON-066 C1 bind rule — pre-registered).
const (
	BindFBlkMin   = 0.05 // blocked fraction floor for arm writer pressure
	BindROccMin   = 0.50 // occupancy ratio floor
	BindFBarMin   = 0.10 // barrier-wait capacity fraction floor
	BindTauE2E    = 0.15 // min |Δe2e| for material pair
	BindEpsTie    = 0.02 // |Δe2e| < ε ⇒ tie (not material)
	BindPairCount = 3
)

// BarrierWaitCapacityFraction is f_bar = W_b / (T_ns * C_eff).
//
// BarrierWaitNanos is aggregate concurrent waiter-time; dividing by wall alone
// yields average concurrent waiters (can exceed 1). Capacity normalization by
// effective concurrency yields a unitless fraction of arm capacity with no
// silent cap. C_eff must be >= 1.
func BarrierWaitCapacityFraction(barrierWaitNanos, e2eWallNanos int64, effectiveConcurrency int) float64 {
	if e2eWallNanos <= 0 || effectiveConcurrency < 1 {
		return math.NaN()
	}
	den := float64(e2eWallNanos) * float64(effectiveConcurrency)
	return float64(barrierWaitNanos) / den
}

// ArmPressureInputs is the synthetic/measure surface for one arm's writer pressure.
type ArmPressureInputs struct {
	Admissions           int64
	AdmissionBlocked     int64
	QueueDepthPeak       int64
	MaxBatch             int
	BarrierWaitNanos     int64
	E2EWallNanos         int64
	EffectiveConcurrency int
	// StatsOK is true when admission (exactly-one post-summary + structure) passed.
	StatsOK bool
}

// ArmHasWriterPressure implements the frozen per-arm pressure predicate.
// Pressure requires: StatsOK, A>=1, r_occ >= r_occ^min, and (f_blk >= f_blk^min OR f_bar >= f_bar^min).
func ArmHasWriterPressure(a ArmPressureInputs) bool {
	if !a.StatsOK || a.Admissions < 1 || a.MaxBatch < 1 {
		return false
	}
	fBlk := float64(a.AdmissionBlocked) / float64(a.Admissions)
	rOcc := float64(a.QueueDepthPeak) / float64(a.MaxBatch)
	fBar := BarrierWaitCapacityFraction(a.BarrierWaitNanos, a.E2EWallNanos, a.EffectiveConcurrency)
	if math.IsNaN(fBar) {
		return false
	}
	if rOcc < BindROccMin {
		return false
	}
	return fBlk >= BindFBlkMin || fBar >= BindFBarMin
}

// PairInputs is one matched disk/tmpfs pair at the disposition layer.
type PairInputs struct {
	// Invalid marks failed/honesty/missing-stats/non-comparable pairs.
	Invalid bool
	// PairPressure is true when both arms show writer pressure.
	PairPressure bool
	// DeltaE2E is (T_disk - T_tmpfs) / max(T_disk, T_tmpfs).
	DeltaE2E float64
}

// E2EDelta computes the frozen pair e2e relative shift.
// Either arm with T <= 0 is non-comparable (NaN), matching the freeze.
func E2EDelta(tDisk, tTmpfs float64) float64 {
	if tDisk <= 0 || tTmpfs <= 0 {
		return math.NaN()
	}
	m := math.Max(tDisk, tTmpfs)
	return (tDisk - tTmpfs) / m
}

// NormalizeMeasuredBinaryCommit maps probe output to the measured-binary commit
// field. Empty becomes "unknown". Worktree/instrument identity must never be
// passed here (GON-066 R3) — this helper intentionally has no fallback args.
func NormalizeMeasuredBinaryCommit(probeCommit string) string {
	if probeCommit == "" {
		return "unknown"
	}
	return probeCommit
}

// PairIsMaterial reports |Δ| >= τ_e2e.
func PairIsMaterial(delta float64) bool {
	if math.IsNaN(delta) {
		return false
	}
	return math.Abs(delta) >= BindTauE2E
}

// PairIsTie reports |Δ| < ε_tie (and not NaN).
func PairIsTie(delta float64) bool {
	if math.IsNaN(delta) {
		return false
	}
	return math.Abs(delta) < BindEpsTie
}

// BindDisposition is the frozen set-level outcome.
type BindDisposition string

const (
	BindBinds        BindDisposition = "binds"
	BindDoesNotBind  BindDisposition = "does_not_bind"
	BindInconclusive BindDisposition = "inconclusive"
)

// ClassifyBindSet implements the frozen three-pair disposition.
//
// does_not_bind requires: all three pairs valid, none have pair pressure,
// all three are material e2e, and all three share the same Δ sign.
// (A single non-material/tie pair → inconclusive — matches T5.)
func ClassifyBindSet(pairs []PairInputs) BindDisposition {
	if len(pairs) != BindPairCount {
		return BindInconclusive
	}
	for _, p := range pairs {
		if p.Invalid || math.IsNaN(p.DeltaE2E) {
			return BindInconclusive
		}
	}

	pressureCount := 0
	materialCount := 0
	var sign float64
	signSet := false
	sameSign := true
	for _, p := range pairs {
		if p.PairPressure {
			pressureCount++
		}
		if PairIsMaterial(p.DeltaE2E) {
			materialCount++
			s := 1.0
			if p.DeltaE2E < 0 {
				s = -1.0
			}
			if !signSet {
				sign = s
				signSet = true
			} else if s != sign {
				sameSign = false
			}
		}
	}

	if pressureCount == BindPairCount && materialCount == BindPairCount && sameSign {
		return BindBinds
	}
	// does_not_bind: zero pair pressure, all three material, same direction.
	if pressureCount == 0 && materialCount == BindPairCount && sameSign {
		return BindDoesNotBind
	}
	return BindInconclusive
}
