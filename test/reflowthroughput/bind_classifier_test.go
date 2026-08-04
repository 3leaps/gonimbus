package reflowthroughput

import (
	"math"
	"testing"
)

func TestBarrierWaitCapacityFraction(t *testing.T) {
	t.Parallel()
	// 1e9 ns wait over 1s wall at C=10 → 0.1
	got := BarrierWaitCapacityFraction(1e9, 1e9, 10)
	if math.Abs(got-0.1) > 1e-12 {
		t.Fatalf("got %v want 0.1", got)
	}
	// Without capacity, same W_b/T_ns would be 1.0 (average waiters); with C=1 equals that.
	if BarrierWaitCapacityFraction(1e9, 1e9, 1) != 1.0 {
		t.Fatal("C=1 should equal wall ratio")
	}
	// No silent cap: can exceed 1 when waiters pile up relative to capacity.
	if BarrierWaitCapacityFraction(5e9, 1e9, 2) != 2.5 {
		t.Fatalf("uncapped capacity fraction")
	}
	if !math.IsNaN(BarrierWaitCapacityFraction(1, 0, 1)) {
		t.Fatal("want NaN for zero wall")
	}
}

func pressureYes() ArmPressureInputs {
	// f_blk=0.10, r_occ=0.6, f_bar=0 (via blk path)
	return ArmPressureInputs{
		Admissions: 100, AdmissionBlocked: 10, QueueDepthPeak: 60, MaxBatch: 100,
		BarrierWaitNanos: 0, E2EWallNanos: 1e9, EffectiveConcurrency: 8, StatsOK: true,
	}
}

func TestArmHasWriterPressureThresholds(t *testing.T) {
	t.Parallel()
	// f_blk exactly at min
	a := pressureYes()
	a.AdmissionBlocked = 5 // 0.05
	if !ArmHasWriterPressure(a) {
		t.Fatal("f_blk == min should pressure (with r_occ ok)")
	}
	a.AdmissionBlocked = 4 // 0.04 < min; f_bar=0
	if ArmHasWriterPressure(a) {
		t.Fatal("below f_blk min without f_bar must not pressure")
	}
	// f_bar path: W_b/(T*C)=0.10
	a.AdmissionBlocked = 0
	a.BarrierWaitNanos = int64(0.10 * 1e9 * 8) // 0.1 * T * C
	if !ArmHasWriterPressure(a) {
		t.Fatal("f_bar == min should pressure")
	}
	a.BarrierWaitNanos = int64(0.10*1e9*8) - 1
	if ArmHasWriterPressure(a) {
		t.Fatal("f_bar just below min")
	}
	// r_occ below min blocks even with f_blk high
	a = pressureYes()
	a.QueueDepthPeak = 40 // 0.4
	if ArmHasWriterPressure(a) {
		t.Fatal("r_occ below min")
	}
	// threshold+epsilon f_blk
	a = pressureYes()
	a.AdmissionBlocked = 6 // 0.06
	if !ArmHasWriterPressure(a) {
		t.Fatal("+eps f_blk")
	}
}

func pair(p bool, d float64) PairInputs {
	return PairInputs{PairPressure: p, DeltaE2E: d}
}

func TestClassifyBindSetTruthTable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		ps   []PairInputs
		want BindDisposition
	}{
		{"T1", []PairInputs{pair(true, 0.20), pair(true, 0.18), pair(true, 0.25)}, BindBinds},
		{"T2", []PairInputs{pair(true, 0.20), pair(true, 0.18), pair(true, -0.20)}, BindInconclusive},
		{"T3", []PairInputs{pair(true, 0.20), pair(true, 0.18), pair(false, 0.22)}, BindInconclusive},
		{"T4", []PairInputs{pair(false, 0.20), pair(false, 0.18), pair(false, 0.25)}, BindDoesNotBind},
		{"T5", []PairInputs{pair(false, 0.20), pair(false, 0.01), pair(false, 0.18)}, BindInconclusive},
		{"T6", []PairInputs{pair(true, 0.20), pair(true, 0.10), pair(true, 0.18)}, BindInconclusive},
		{"T7", []PairInputs{{Invalid: true, DeltaE2E: 0.2}, pair(true, 0.2), pair(true, 0.2)}, BindInconclusive},
		// does_not_bind boundary: all 3 material at tau exactly, no pressure
		{"T4-tau", []PairInputs{pair(false, 0.15), pair(false, 0.15), pair(false, 0.15)}, BindDoesNotBind},
		// just below tau on one → inconclusive
		{"material-eps", []PairInputs{pair(false, 0.15), pair(false, 0.149), pair(false, 0.15)}, BindInconclusive},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyBindSet(tc.ps)
			if got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}

func TestE2EDelta(t *testing.T) {
	t.Parallel()
	if d := E2EDelta(1.2, 1.0); math.Abs(d-0.2/1.2) > 1e-12 {
		t.Fatalf("got %v", d)
	}
	if !math.IsNaN(E2EDelta(0, 0)) {
		t.Fatal("want NaN for zero both")
	}
	if !math.IsNaN(E2EDelta(-1, 1)) {
		t.Fatal("want NaN when either arm <= 0")
	}
	if !math.IsNaN(E2EDelta(1, 0)) {
		t.Fatal("want NaN when tmpfs <= 0")
	}
}

func TestArmHasWriterPressureROccThresholds(t *testing.T) {
	t.Parallel()
	// r_occ exact min with f_blk path
	a := ArmPressureInputs{
		Admissions: 100, AdmissionBlocked: 10, QueueDepthPeak: 50, MaxBatch: 100,
		BarrierWaitNanos: 0, E2EWallNanos: 1e9, EffectiveConcurrency: 8, StatsOK: true,
	}
	if !ArmHasWriterPressure(a) {
		t.Fatal("r_occ == 0.50 should pass with f_blk ok")
	}
	a.QueueDepthPeak = 49
	if ArmHasWriterPressure(a) {
		t.Fatal("r_occ just below min")
	}
	a.QueueDepthPeak = 51
	if !ArmHasWriterPressure(a) {
		t.Fatal("r_occ +eps")
	}
}

func TestPairIsTieAndMaterialEps(t *testing.T) {
	t.Parallel()
	// epsilon_tie = 0.02
	if !PairIsTie(0.019) {
		t.Fatal("below eps is tie")
	}
	if PairIsTie(0.02) {
		t.Fatal("|d|==eps is not < eps")
	}
	if PairIsMaterial(0.149) {
		t.Fatal("below tau")
	}
	if !PairIsMaterial(0.15) {
		t.Fatal("at tau material")
	}
	if !PairIsMaterial(0.151) {
		t.Fatal("+eps material")
	}
}

func TestNormalizeMeasuredBinaryCommit(t *testing.T) {
	t.Parallel()
	if NormalizeMeasuredBinaryCommit("") != "unknown" {
		t.Fatal("empty -> unknown")
	}
	if NormalizeMeasuredBinaryCommit("abc") != "abc" {
		t.Fatal("preserve")
	}
	// Helper has no worktree arg — cannot fall back.
}
