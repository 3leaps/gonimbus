package reflowthroughput

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/reflow"
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
	if rep.WriterStats.Admissions != 600 {
		t.Fatalf("admissions=%d want exact 600", rep.WriterStats.Admissions)
	}
	if rep.WriterStats.BarrierOK != rep.WriterStats.Admissions ||
		rep.WriterStats.Barriers != rep.WriterStats.Admissions {
		t.Fatalf("barrier_ok=%d barriers=%d admissions=%d",
			rep.WriterStats.BarrierOK, rep.WriterStats.Barriers, rep.WriterStats.Admissions)
	}
	if rep.WriterStats.BatchSizeSum != rep.WriterStats.Admissions {
		t.Fatalf("batch_size_sum=%d admissions=%d", rep.WriterStats.BatchSizeSum, rep.WriterStats.Admissions)
	}
	if rep.WriterStats.MaxBatch != ServiceDemandProductMaxBatch {
		t.Fatalf("MaxBatch=%d", rep.WriterStats.MaxBatch)
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

func TestServiceDemandFormalSchedules(t *testing.T) {
	t.Parallel()
	wantR1 := []int{8, 32, 64, 128, 256, 512}
	wantR2 := []int{512, 256, 128, 64, 32, 8}
	wantR3 := []int{64, 256, 8, 512, 32, 128}
	for id, want := range map[ServiceDemandScheduleID][]int{
		ServiceDemandScheduleR1: wantR1,
		ServiceDemandScheduleR2: wantR2,
		ServiceDemandScheduleR3: wantR3,
	} {
		got := ServiceDemandFormalSchedules[id]
		if len(got) != len(want) {
			t.Fatalf("%s=%v want %v", id, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s=%v want %v", id, got, want)
			}
		}
	}
	for _, rep := range []int{1, 2, 3} {
		id, err := FormalScheduleForRep(rep)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := ServiceDemandFormalSchedules[id]; !ok {
			t.Fatalf("missing schedule %s", id)
		}
	}
	if _, err := FormalScheduleForRep(0); err == nil {
		t.Fatal("rep 0 must not map to formal schedule")
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

func TestRunServiceDemandSetTiny(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	// 1 formal rep × 6 arms × 30 units = small structural set (not freeze quantum).
	set, err := RunServiceDemandSet(ctx, ServiceDemandSetConfig{
		RootDir:        t.TempDir(),
		FormalReps:     []int{1},
		Units:          30,
		MinAdmissions:  90,
		SkipAPFSCheck:  true,
		MediumOverride: "test_tmp",
	})
	if err != nil {
		t.Fatal(err)
	}
	if set.SchemaVersion != ServiceDemandReportSchema {
		t.Fatalf("schema=%s", set.SchemaVersion)
	}
	if set.Medium != "test_tmp" || set.MixID != ServiceDemandMixID {
		t.Fatalf("medium/mix: %s %s", set.Medium, set.MixID)
	}
	if set.Formal {
		t.Fatal("tiny set must be non-formal")
	}
	if len(set.Cells) != 6 {
		t.Fatalf("cells=%d want 6", len(set.Cells))
	}
	if set.InstrumentSHABefore == "" || set.InstrumentSHAAfter != set.InstrumentSHABefore {
		t.Fatalf("instrument sha before/after: %q %q", set.InstrumentSHABefore, set.InstrumentSHAAfter)
	}
	if set.InstrumentCommitBefore == "" {
		t.Fatal("instrument commit empty")
	}
	for _, c := range set.Cells {
		if c.ScheduleID != ServiceDemandScheduleR1 {
			t.Fatalf("schedule=%s", c.ScheduleID)
		}
		if !c.Honest {
			t.Fatalf("cell n=%d dishonest: %s", c.Submitters, c.HonestyMessage)
		}
		if c.WriterStats.Admissions != 90 {
			t.Fatalf("n=%d admissions=%d", c.Submitters, c.WriterStats.Admissions)
		}
	}
	// Tiny quantum will almost always be inconclusive under CV/util gates — OK.
	t.Logf("disposition=%s msg=%s", set.Disposition, set.DispositionMessage)
}

func TestFormalRequiresWantFormalShape(t *testing.T) {
	t.Parallel()
	// Even with a pure-valid cell matrix, MediumOverride must not yield formal.
	// We only exercise the Formal assignment path via RunServiceDemandSet with
	// a tiny non-300k set + override; Formal must stay false.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	set, err := RunServiceDemandSet(ctx, ServiceDemandSetConfig{
		RootDir:        t.TempDir(),
		FormalReps:     []int{1, 2, 3},
		Units:          5,
		MediumOverride: ServiceDemandMedium, // claim apfs_disk without APFS check
		SkipAPFSCheck:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if set.Formal {
		t.Fatal("MediumOverride/SkipAPFSCheck must not produce Formal=true")
	}
	if set.Disposition == ServiceDemandMechanismBinds || set.Disposition == ServiceDemandMechanismDoesNot {
		t.Fatalf("authoritative disposition on non-formal set: %s", set.Disposition)
	}
}

func TestWriteServiceDemandSetReportNoOverwrite(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "rep.json")
	rep := ServiceDemandSetReport{SchemaVersion: ServiceDemandReportSchema}
	if err := WriteServiceDemandSetReport(path, rep); err != nil {
		t.Fatal(err)
	}
	if err := WriteServiceDemandSetReport(path, rep); err == nil {
		t.Fatal("expected refuse overwrite")
	}
	if _, err := RunFormalServiceDemandBaseline(context.Background(), t.TempDir(), ""); err == nil {
		t.Fatal("empty report path must fail")
	}
}

func TestRefuseExistingStoreFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "exists.db")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := refuseExistingStoreFiles(path); err == nil {
		t.Fatal("expected refuse existing main db")
	}
	// clean path ok
	if err := refuseExistingStoreFiles(filepath.Join(dir, "fresh.db")); err != nil {
		t.Fatal(err)
	}
}

func TestValidateServiceDemandFormalSetNegative(t *testing.T) {
	t.Parallel()
	// Good formal-shaped synthetic (honest stats + schedule).
	good := syntheticFormalCells(map[int]cellSynth{
		8:   {mut: 80e3, util: 0.40, bar: 1e6},
		32:  {mut: 70e3, util: 0.60, bar: 2e6},
		64:  {mut: 55e3, util: 0.90, bar: 5e6},
		128: {mut: 50e3, util: 0.92, bar: 8e6},
		256: {mut: 48e3, util: 0.94, bar: 12e6},
		512: {mut: 47e3, util: 0.95, bar: 15e6},
	}, 3)
	if err := ValidateServiceDemandFormalSet(good); err != nil {
		t.Fatalf("good formal: %v", err)
	}
	// Duplicate N=8, omit N=32 on rep1 — entarch negative control.
	bad := append([]ServiceDemandCell(nil), good...)
	// Find rep1 n=32 and change to n=8 (duplicate)
	for i := range bad {
		if bad[i].Rep == 1 && bad[i].Submitters == 32 {
			bad[i].Submitters = 8
			bad[i].ServiceDemandArmReport.Submitters = 8
			// also break ordinal/schedule consistency intentionally
			bad[i].Ordinal = 0
			break
		}
	}
	if err := ValidateServiceDemandFormalSet(bad); err == nil {
		t.Fatal("malformed set must fail ValidateServiceDemandFormalSet")
	}
	d, msg := ClassifyServiceDemandV2(bad)
	if d != ServiceDemandInconclusive {
		t.Fatalf("malformed must be inconclusive got %s (%s)", d, msg)
	}
	// Wrong schedule ID
	wrongSched := append([]ServiceDemandCell(nil), good...)
	wrongSched[0].ScheduleID = ServiceDemandScheduleR2
	if err := ValidateServiceDemandFormalSet(wrongSched); err == nil {
		t.Fatal("wrong schedule must fail")
	}
	// Target drift
	drift := append([]ServiceDemandCell(nil), good...)
	drift[0].AdmissionsTarget = 299_999
	if err := ValidateServiceDemandFormalSet(drift); err == nil {
		t.Fatal("target drift must fail")
	}
	// Dishonest stats (CommitFatals)
	dish := append([]ServiceDemandCell(nil), good...)
	dish[0].WriterStats.CommitFatals = 1
	if err := ValidateServiceDemandFormalSet(dish); err == nil {
		t.Fatal("dishonest stats must fail re-derived honesty")
	}
}

func TestRunServiceDemandArmRefusesExisting(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "pre.db")
	if err := os.WriteFile(path, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := RunServiceDemandArm(ctx, ServiceDemandArmConfig{
		StorePath: path, Submitters: 2, Units: 5,
	})
	if err == nil {
		t.Fatal("expected refuse existing store")
	}
}

func TestClassifyServiceDemandV2Synthetic(t *testing.T) {
	t.Parallel()
	// mechanism_binds: high util, plateau, barrier rise, CV=0 (3 identical reps).
	binds := syntheticFormalCells(map[int]cellSynth{
		8:   {mut: 80e3, util: 0.40, bar: 1e6},
		32:  {mut: 70e3, util: 0.60, bar: 2e6},
		64:  {mut: 55e3, util: 0.90, bar: 5e6},
		128: {mut: 50e3, util: 0.92, bar: 8e6},
		256: {mut: 48e3, util: 0.94, bar: 12e6},
		512: {mut: 47e3, util: 0.95, bar: 15e6}, // ≤1.05×128; bar ≥2×64
	}, 3)
	d, msg := ClassifyServiceDemandV2(binds)
	if d != ServiceDemandMechanismBinds {
		t.Fatalf("want mechanism_binds got %s (%s)", d, msg)
	}

	// does_not_bind: low util high-N, strong scale 64→256.
	dn := syntheticFormalCells(map[int]cellSynth{
		8:   {mut: 10e3, util: 0.10, bar: 1e5},
		32:  {mut: 20e3, util: 0.15, bar: 1e5},
		64:  {mut: 30e3, util: 0.20, bar: 1e5},
		128: {mut: 50e3, util: 0.30, bar: 1e5},
		256: {mut: 60e3, util: 0.40, bar: 1e5}, // util < 0.50; ≥1.5×64
		512: {mut: 70e3, util: 0.45, bar: 1e5},
	}, 3)
	d, msg = ClassifyServiceDemandV2(dn)
	if d != ServiceDemandMechanismDoesNot {
		t.Fatalf("want mechanism_does_not_bind got %s (%s)", d, msg)
	}

	// inconclusive: high CV at 512.
	noisy := syntheticFormalCells(map[int]cellSynth{
		8:   {mut: 80e3, util: 0.40, bar: 1e6},
		32:  {mut: 70e3, util: 0.60, bar: 2e6},
		64:  {mut: 55e3, util: 0.90, bar: 5e6},
		128: {mut: 50e3, util: 0.92, bar: 8e6},
		256: {mut: 48e3, util: 0.94, bar: 12e6},
		512: {mut: 47e3, util: 0.95, bar: 15e6},
	}, 3)
	for i := range noisy {
		if noisy[i].Submitters == 512 {
			switch noisy[i].Rep {
			case 1:
				noisy[i].MutationsPerSec = 40e3
			case 2:
				noisy[i].MutationsPerSec = 80e3
			case 3:
				noisy[i].MutationsPerSec = 50e3
			}
		}
	}
	d, msg = ClassifyServiceDemandV2(noisy)
	if d != ServiceDemandInconclusive {
		t.Fatalf("want inconclusive for high CV got %s (%s)", d, msg)
	}
}

type cellSynth struct {
	mut, util, bar float64
}

// syntheticFormalCells builds schedule-correct cells with re-derivable honest WriterStats.
func syntheticFormalCells(byN map[int]cellSynth, reps int) []ServiceDemandCell {
	var out []ServiceDemandCell
	for rep := 1; rep <= reps; rep++ {
		schedID, err := FormalScheduleForRep(rep)
		if err != nil {
			// only 1..3 are formal; skip invalid
			continue
		}
		order := ServiceDemandFormalSchedules[schedID]
		for ord, n := range order {
			s, ok := byN[n]
			if !ok {
				s = cellSynth{}
			}
			target := ServiceDemandAdmissionsPerArm
			out = append(out, ServiceDemandCell{
				Rep:        rep,
				Ordinal:    ord,
				ScheduleID: schedID,
				Submitters: n,
				ServiceDemandArmReport: ServiceDemandArmReport{
					Submitters:           n,
					AdmissionsTarget:     target,
					MutationsPerSec:      s.mut,
					WriterUtilization:    s.util,
					BarrierWaitMeanNanos: s.bar,
					Honest:               true,
					WriterStats:          honestSyntheticStats(target),
				},
			})
		}
	}
	return out
}

func honestSyntheticStats(target int64) reflow.CheckpointWriterStatsRecord {
	return reflow.CheckpointWriterStatsRecord{
		MaxBatch:     ServiceDemandProductMaxBatch,
		Admissions:   target,
		Barriers:     target,
		BarrierOK:    target,
		BatchSizeSum: target,
		Batches:      1,
		Commits:      1,
	}
}

func TestServiceDemandFormalShapeGate(t *testing.T) {
	t.Parallel()
	// Metrics that would bind if formal — but only 1 rep → inconclusive.
	oneRep := syntheticFormalCells(map[int]cellSynth{
		8:   {mut: 80e3, util: 0.40, bar: 1e6},
		32:  {mut: 70e3, util: 0.60, bar: 2e6},
		64:  {mut: 55e3, util: 0.90, bar: 5e6},
		128: {mut: 50e3, util: 0.92, bar: 8e6},
		256: {mut: 48e3, util: 0.94, bar: 12e6},
		512: {mut: 47e3, util: 0.95, bar: 15e6},
	}, 1)
	d, msg := ClassifyServiceDemandV2(oneRep)
	if d != ServiceDemandInconclusive {
		t.Fatalf("single-rep must be inconclusive got %s (%s)", d, msg)
	}
}
