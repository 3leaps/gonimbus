package reflowthroughput

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestServiceDemandArmSmoke(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "svc.db")
	// Small work quantum for unit speed (not freeze quantum).
	rep, err := RunServiceDemandArm(ctx, ServiceDemandArmConfig{
		StorePath:     path,
		Submitters:    8,
		Units:         200, // 600 admissions
		MinAdmissions: 600,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Honest {
		t.Fatalf("honest: %s", rep.HonestyMessage)
	}
	if rep.WriterStats.Admissions < 600 {
		t.Fatalf("admissions=%d", rep.WriterStats.Admissions)
	}
	if rep.WriterStats.BarrierOK != rep.WriterStats.Admissions {
		t.Fatalf("barrier_ok=%d admissions=%d", rep.WriterStats.BarrierOK, rep.WriterStats.Admissions)
	}
	if rep.MutationsPerSec <= 0 || rep.WallSeconds <= 0 {
		t.Fatalf("metrics: %+v", rep)
	}
	if rep.AdmissionsPerBatch < 1 {
		t.Fatalf("admissions/batch=%v", rep.AdmissionsPerBatch)
	}
	t.Logf("submitters=8 units=200 wall=%.3fs mut/s=%.0f util=%.3f batch=%.2f occ=%.3f",
		rep.WallSeconds, rep.MutationsPerSec, rep.WriterUtilization, rep.AdmissionsPerBatch, rep.ReachableOccupancy)
}

func TestRotateSubmitterGrid(t *testing.T) {
	t.Parallel()
	g0 := RotateSubmitterGrid(0)
	if len(g0) != len(ServiceDemandSubmitterGrid) || g0[0] != 8 {
		t.Fatalf("rep0=%v", g0)
	}
	g1 := RotateSubmitterGrid(1)
	if g1[0] != 32 || g1[len(g1)-1] != 8 {
		t.Fatalf("rep1=%v", g1)
	}
}

func TestServiceDemandArmHighSubmittersShort(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	path := filepath.Join(t.TempDir(), "svc-hi.db")
	rep, err := RunServiceDemandArm(ctx, ServiceDemandArmConfig{
		StorePath:  path,
		Submitters: 64,
		Units:      2000, // 6k admissions — structural, not freeze quantum
	})
	if err != nil {
		t.Fatal(err)
	}
	if rep.ReachableOccupancy > 1.01 {
		t.Fatalf("reachable occupancy %v > 1", rep.ReachableOccupancy)
	}
	t.Logf("‖64 wall=%.3fs mut/s=%.0f util=%.3f batch=%.2f load=%.2f",
		rep.WallSeconds, rep.MutationsPerSec, rep.WriterUtilization, rep.AdmissionsPerBatch, rep.BarrierWaitLoad)
}
