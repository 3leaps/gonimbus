package reflowthroughput

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/reflow"
)

// TestFormalServiceDemandBaselineLive runs the gated formal 3×6×300k set.
// Opt-in only — wall-clock is long (hours possible on busy hosts).
//
//	GONIMBUS_FORMAL_SERVICE_DEMAND=1 \
//	GONIMBUS_FORMAL_ROOT=~/dev/temp/.../arms \
//	GONIMBUS_FORMAL_REPORT=~/dev/temp/.../report.json \
//	go test ./test/reflowthroughput -run TestFormalServiceDemandBaselineLive -count=1 -timeout 6h -v
func TestFormalServiceDemandBaselineLive(t *testing.T) {
	if os.Getenv("GONIMBUS_FORMAL_SERVICE_DEMAND") != "1" {
		t.Skip("set GONIMBUS_FORMAL_SERVICE_DEMAND=1 for live formal baseline")
	}
	root := os.Getenv("GONIMBUS_FORMAL_ROOT")
	report := os.Getenv("GONIMBUS_FORMAL_REPORT")
	if root == "" || report == "" {
		t.Fatal("GONIMBUS_FORMAL_ROOT and GONIMBUS_FORMAL_REPORT required")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Logf("formal baseline root=%s report=%s tip_instrument_probe_via_set", root, report)
	start := time.Now()
	set, err := RunFormalServiceDemandBaseline(context.Background(), root, report)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("formal baseline failed after %s: %v", elapsed, err)
	}
	t.Logf("formal=%v disposition=%s msg=%s cells=%d wall=%s instrument=%s dirty=%v",
		set.Formal, set.Disposition, set.DispositionMessage, len(set.Cells), elapsed,
		set.InstrumentCommitBefore, set.InstrumentDirtyBefore)
	if !set.Formal {
		t.Fatalf("expected formal=true: %s", set.DispositionMessage)
	}
	if len(set.Cells) != 18 {
		t.Fatalf("cells=%d want 18", len(set.Cells))
	}
}

// TestPhaseAServiceDemandABLive runs control (flag off) vs treatment (elision)
// at N=32,256,512 × 3 reps × formal 300k. Opt-in; long wall-clock.
//
//	GONIMBUS_PHASE_A_AB=1 GONIMBUS_PHASE_A_ROOT=~/dev/temp/... \
//	go test ./test/reflowthroughput -run TestPhaseAServiceDemandABLive -count=1 -timeout 6h -v
func TestPhaseAServiceDemandABLive(t *testing.T) {
	if os.Getenv("GONIMBUS_PHASE_A_AB") != "1" {
		t.Skip("set GONIMBUS_PHASE_A_AB=1 for Phase A local A/B")
	}
	root := os.Getenv("GONIMBUS_PHASE_A_ROOT")
	if root == "" {
		t.Fatal("GONIMBUS_PHASE_A_ROOT required")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	points := []int{32, 256, 512}
	type cell struct {
		n, rep              int
		elide               bool
		muts, util, barMean float64
		wall                float64
		spCreated, spElided int64
		honest              bool
	}
	var cells []cell
	ctx := context.Background()
	startAll := time.Now()
	for _, elide := range []bool{false, true} {
		label := "control"
		if elide {
			label = "treatment"
		}
		for rep := 1; rep <= 3; rep++ {
			for _, n := range points {
				path := filepath.Join(root, fmt.Sprintf("%s-rep%d-n%d.db", label, rep, n))
				arm, err := RunServiceDemandArm(ctx, ServiceDemandArmConfig{
					StorePath:              path,
					Submitters:             n,
					ElideRawExecSavepoints: elide,
				})
				if err != nil {
					t.Fatalf("%s rep=%d n=%d: %v", label, rep, n, err)
				}
				cells = append(cells, cell{
					n: n, rep: rep, elide: elide,
					muts: arm.MutationsPerSec, util: arm.WriterUtilization,
					barMean: arm.BarrierWaitMeanNanos, wall: arm.WallSeconds,
					spCreated: arm.WriterStats.SavepointsCreated,
					spElided:  arm.WriterStats.SavepointsElided,
					honest:    arm.Honest,
				})
				t.Logf("%s rep=%d n=%d mut/s=%.0f util=%.3f barMean=%.0f spC=%d spE=%d wall=%.1fs",
					label, rep, n, arm.MutationsPerSec, arm.WriterUtilization, arm.BarrierWaitMeanNanos,
					arm.WriterStats.SavepointsCreated, arm.WriterStats.SavepointsElided, arm.WallSeconds)
			}
		}
	}
	// Median by (elide,n)
	med := func(elide bool, n int, pick func(cell) float64) float64 {
		var xs []float64
		for _, c := range cells {
			if c.elide == elide && c.n == n {
				xs = append(xs, pick(c))
			}
		}
		return medianFloat(xs)
	}
	t.Logf("total_wall=%s", time.Since(startAll))
	// Local gates from EXECUTION-PLAN
	for _, n := range []int{256, 512} {
		cMut := med(false, n, func(c cell) float64 { return c.muts })
		tMut := med(true, n, func(c cell) float64 { return c.muts })
		cBar := med(false, n, func(c cell) float64 { return c.barMean })
		tBar := med(true, n, func(c cell) float64 { return c.barMean })
		mutGain := (tMut - cMut) / cMut
		barDrop := (cBar - tBar) / cBar
		t.Logf("gate n=%d mut gain=%.1f%% (want>=15) bar drop=%.1f%% (want>=15) cMut=%.0f tMut=%.0f cBar=%.0f tBar=%.0f",
			n, mutGain*100, barDrop*100, cMut, tMut, cBar, tBar)
		if mutGain < 0.15 {
			t.Errorf("n=%d mut/s gain %.1f%% < 15%%", n, mutGain*100)
		}
		if barDrop < 0.15 {
			t.Errorf("n=%d barrier-mean drop %.1f%% < 15%%", n, barDrop*100)
		}
	}
	c32 := med(false, 32, func(c cell) float64 { return c.muts })
	t32 := med(true, 32, func(c cell) float64 { return c.muts })
	c32b := med(false, 32, func(c cell) float64 { return c.barMean })
	t32b := med(true, 32, func(c cell) float64 { return c.barMean })
	if (c32-t32)/c32 > 0.10 {
		t.Errorf("n=32 mut/s regression %.1f%% > 10%%", (c32-t32)/c32*100)
	}
	if (t32b-c32b)/c32b > 0.10 {
		t.Errorf("n=32 barrier regression %.1f%% > 10%%", (t32b-c32b)/c32b*100)
	}
	// Treatment must elide some savepoints; control must elide none.
	for _, c := range cells {
		if !c.elide && c.spElided != 0 {
			t.Errorf("control n=%d rep=%d elided=%d", c.n, c.rep, c.spElided)
		}
		if c.elide && c.spElided < 1 {
			t.Errorf("treatment n=%d rep=%d elided=%d want >0", c.n, c.rep, c.spElided)
		}
		if !c.honest {
			t.Errorf("dishonest n=%d elide=%v", c.n, c.elide)
		}
	}
}

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
