package reflowthroughput

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/reflowstate"
)

// Provider-free checkpoint service-demand grid (service-demand formal contract).
// No object copy or provider in the timed region.

const (
	// ServiceDemandAdmissionsPerUnit is Mark + Note + Upsert (three admissions).
	ServiceDemandAdmissionsPerUnit = 3
	// ServiceDemandAdmissionsPerArm is the formal work quantum (100k units).
	ServiceDemandAdmissionsPerArm int64 = 300_000
	// ServiceDemandMixID identifies the terminal mutation mix contract.
	ServiceDemandMixID = "mark_note_upsert_v1"
	// ServiceDemandMedium is the quorum medium for formal sets on this host class.
	ServiceDemandMedium = "apfs_disk"
	// ServiceDemandReportSchema is the sterile set-report schema version.
	ServiceDemandReportSchema = "gonimbus.service_demand_set.v1"
	// ServiceDemandProductMaxBatch is the product MaxBatch (unchanged).
	ServiceDemandProductMaxBatch = 256
)

// ServiceDemandSubmitterGrid is the freeze-v2 concurrency steps (canonical order).
var ServiceDemandSubmitterGrid = []int{8, 32, 64, 128, 256, 512}

// ServiceDemandScheduleID names a fixed formal schedule.
type ServiceDemandScheduleID string

const (
	ServiceDemandScheduleR1 ServiceDemandScheduleID = "R1"
	ServiceDemandScheduleR2 ServiceDemandScheduleID = "R2"
	ServiceDemandScheduleR3 ServiceDemandScheduleID = "provenance gate"
)

// ServiceDemandFormalSchedules are the exact three formal rep orders (Day-0 AC).
// Rep 1→R1, rep 2→R2, rep 3→provenance gate. Pilot rep 0 may reuse R1 and is excluded from
// formal classifier input.
var ServiceDemandFormalSchedules = map[ServiceDemandScheduleID][]int{
	ServiceDemandScheduleR1: {8, 32, 64, 128, 256, 512},
	ServiceDemandScheduleR2: {512, 256, 128, 64, 32, 8},
	ServiceDemandScheduleR3: {64, 256, 8, 512, 32, 128},
}

// FormalScheduleForRep maps formal rep index (1..3) to schedule ID.
func FormalScheduleForRep(rep int) (ServiceDemandScheduleID, error) {
	switch rep {
	case 1:
		return ServiceDemandScheduleR1, nil
	case 2:
		return ServiceDemandScheduleR2, nil
	case 3:
		return ServiceDemandScheduleR3, nil
	default:
		return "", fmt.Errorf("formal service-demand rep must be 1..3, got %d", rep)
	}
}

// ServiceDemandArmConfig configures one timed arm.
type ServiceDemandArmConfig struct {
	// StorePath is the APFS (or other quorum medium) checkpoint path.
	StorePath string
	// Submitters is fixed concurrent mutation callers.
	Submitters int
	// Units is the number of terminal units (each = 3 admissions). 0 → formal quantum.
	Units int
	// MinAdmissions overrides the formal work quantum when > 0 (tests only).
	MinAdmissions int64
	// ElideRawExecSavepoints enables experimental Phase A elision for this arm.
	ElideRawExecSavepoints bool
}

// ServiceDemandArmReport is a sterile arm result (no paths in JSON export helpers).
type ServiceDemandArmReport struct {
	Submitters           int                                `json:"submitters"`
	Units                int                                `json:"units"`
	AdmissionsTarget     int64                              `json:"admissions_target"`
	WallSeconds          float64                            `json:"wall_seconds"`
	MutationsPerSec      float64                            `json:"mutations_per_sec"`
	TransactionsPerSec   float64                            `json:"transactions_per_sec"`
	WriterUtilization    float64                            `json:"writer_utilization"`
	BarrierWaitLoad      float64                            `json:"barrier_wait_load"`
	BarrierWaitMeanNanos float64                            `json:"barrier_wait_mean_nanos"`
	BarrierWaitMaxNanos  int64                              `json:"barrier_wait_max_nanos"`
	AdmissionsPerBatch   float64                            `json:"admissions_per_batch"`
	ReachableOccupancy   float64                            `json:"reachable_occupancy"`
	AdmissionWaitMaxNs   int64                              `json:"admission_wait_max_nanos"`
	AdmissionBlocked     int64                              `json:"admission_blocked"`
	Honest               bool                               `json:"honest"`
	HonestyMessage       string                             `json:"honesty_message,omitempty"`
	WriterStats          reflow.CheckpointWriterStatsRecord `json:"writer_stats"`
}

// serviceDemandUnitKeys holds pre-formatted keys for one terminal unit.
// Keys are built outside the timed region so formatting cost is not measured.
type serviceDemandUnitKeys struct {
	src string
	dst string
	dk  string
}

// RunServiceDemandArm opens a fresh store, runs the terminal mutation mix under
// fixed submitter concurrency, and returns sterile metrics from WriterStats.
//
// Timed region: MarkDestKeyObserved → NoteDestKeySource → UpsertItem(complete)
// only. Key formatting is outside the timed loop (Day-0 mix-order AC).
func RunServiceDemandArm(ctx context.Context, cfg ServiceDemandArmConfig) (ServiceDemandArmReport, error) {
	if cfg.Submitters < 1 {
		return ServiceDemandArmReport{}, fmt.Errorf("submitters must be >= 1")
	}
	if cfg.StorePath == "" {
		return ServiceDemandArmReport{}, fmt.Errorf("store path required")
	}
	minAdm := cfg.MinAdmissions
	if minAdm < 1 {
		minAdm = ServiceDemandAdmissionsPerArm
	}
	units := cfg.Units
	if units < 1 {
		units = int(minAdm / ServiceDemandAdmissionsPerUnit)
		if units < 1 {
			units = 1
		}
	}
	target := int64(units) * ServiceDemandAdmissionsPerUnit

	// Pre-format all unit keys outside the timed region.
	keys := make([]serviceDemandUnitKeys, units)
	for i := 0; i < units; i++ {
		keys[i] = serviceDemandUnitKeys{
			src: fmt.Sprintf("s3://bench-source/obj/%d", i),
			dst: fmt.Sprintf("s3://bench-dest/obj/%d", i),
			dk:  fmt.Sprintf("dest-key/%d", i),
		}
	}

	if err := os.MkdirAll(filepath.Dir(cfg.StorePath), 0o755); err != nil {
		return ServiceDemandArmReport{}, err
	}
	// Fresh unique DB: refuse any existing main/WAL/SHM (do not silently delete).
	if err := refuseExistingStoreFiles(cfg.StorePath); err != nil {
		return ServiceDemandArmReport{}, err
	}

	store, err := reflowstate.Open(ctx, reflowstate.Config{
		Path:                   cfg.StorePath,
		ElideRawExecSavepoints: cfg.ElideRawExecSavepoints,
	})
	if err != nil {
		return ServiceDemandArmReport{}, fmt.Errorf("open store: %w", err)
	}

	work := make(chan int, cfg.Submitters*2)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	worker := func() {
		defer wg.Done()
		for i := range work {
			if err := serviceDemandUnit(runCtx, store, keys[i]); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				errMu.Unlock()
				return
			}
		}
	}
	wg.Add(cfg.Submitters)
	for s := 0; s < cfg.Submitters; s++ {
		go worker()
	}

	start := time.Now()
	for i := 0; i < units; i++ {
		select {
		case work <- i:
		case <-runCtx.Done():
			i = units
		}
	}
	close(work)
	wg.Wait()
	wall := time.Since(start)

	errMu.Lock()
	ferr := firstErr
	errMu.Unlock()
	if ferr != nil {
		_ = store.Close()
		return ServiceDemandArmReport{}, ferr
	}

	st := store.WriterStats()
	if err := store.Close(); err != nil {
		return ServiceDemandArmReport{}, fmt.Errorf("close store: %w", err)
	}
	rec := writerStatsToRecord(st)
	rep := ServiceDemandArmReport{
		Submitters:       cfg.Submitters,
		Units:            units,
		AdmissionsTarget: target,
		WallSeconds:      wall.Seconds(),
		WriterStats:      rec,
	}
	wallNs := wall.Nanoseconds()
	if wallNs < 1 {
		wallNs = 1
	}
	if st.BarrierOK > 0 && wall.Seconds() > 0 {
		rep.MutationsPerSec = float64(st.BarrierOK) / wall.Seconds()
	}
	if st.Batches > 0 && wall.Seconds() > 0 {
		rep.TransactionsPerSec = float64(st.Batches) / wall.Seconds()
	}
	rep.WriterUtilization = float64(st.BatchDurationNanos) / float64(wallNs)
	rep.BarrierWaitLoad = float64(st.BarrierWaitNanos) / float64(wallNs)
	if st.Admissions > 0 {
		rep.BarrierWaitMeanNanos = float64(st.BarrierWaitNanos) / float64(st.Admissions)
	}
	rep.BarrierWaitMaxNanos = st.BarrierWaitMaxNanos
	if st.Batches > 0 {
		rep.AdmissionsPerBatch = float64(st.BatchSizeSum) / float64(st.Batches)
	}
	cap := st.MaxBatch
	if cfg.Submitters < cap {
		cap = cfg.Submitters
	}
	if cap < 1 {
		cap = 1
	}
	rep.ReachableOccupancy = float64(st.QueueDepthPeak) / float64(cap)
	rep.AdmissionWaitMaxNs = st.AdmissionWaitMaxNanos
	rep.AdmissionBlocked = st.AdmissionBlocked

	honest, msg := serviceDemandHonest(st, target)
	rep.Honest = honest
	rep.HonestyMessage = msg
	if !honest {
		return rep, fmt.Errorf("dishonest arm: %s", msg)
	}
	return rep, nil
}

// refuseExistingStoreFiles fails if the SQLite main DB or -wal/-shm siblings exist.
// Formal arms must start from a clean path; silent Remove is not allowed.
func refuseExistingStoreFiles(storePath string) error {
	for _, p := range []string{storePath, storePath + "-wal", storePath + "-shm"} {
		fi, err := os.Stat(p)
		if err == nil {
			return fmt.Errorf("store path not fresh: %s exists (size=%d); refuse silent reuse", p, fi.Size())
		}
		if !os.IsNotExist(err) {
			return fmt.Errorf("store path inspect %s: %w", p, err)
		}
	}
	return nil
}

// serviceDemandUnit is the three-admission terminal mix (Day-0 order):
// MarkDestKeyObserved → NoteDestKeySource → UpsertItem(complete).
// Upsert is last so the timed region matches retained reflow terminal ordering.
func serviceDemandUnit(ctx context.Context, store *reflowstate.Store, k serviceDemandUnitKeys) error {
	if err := store.MarkDestKeyObserved(ctx, k.dk); err != nil {
		return err
	}
	if err := store.NoteDestKeySource(ctx, k.dk, k.src, "e", 1); err != nil {
		return err
	}
	if err := store.UpsertItem(ctx, reflowstate.UpsertItemParams{
		SourceURI: k.src, DestURI: k.dst, DestKey: k.dk,
		Status: "complete", Bytes: 1, SourceSize: 1, SourceETag: "e",
	}); err != nil {
		return err
	}
	return nil
}

// serviceDemandHonest enforces Day-0 strict honesty on one arm.
func serviceDemandHonest(st reflowstate.WriterStats, target int64) (bool, string) {
	if st.CommitFatals != 0 {
		return false, fmt.Sprintf("CommitFatals=%d", st.CommitFatals)
	}
	if st.BarrierWriterFailed != 0 || st.BarrierWriterClosed != 0 || st.BarrierCanceled != 0 {
		return false, fmt.Sprintf("lifecycle barriers failed=%d closed=%d canceled=%d",
			st.BarrierWriterFailed, st.BarrierWriterClosed, st.BarrierCanceled)
	}
	if st.RequestRefusals != 0 {
		return false, fmt.Sprintf("RequestRefusals=%d", st.RequestRefusals)
	}
	if st.BarrierRefusal != 0 {
		return false, fmt.Sprintf("BarrierRefusal=%d", st.BarrierRefusal)
	}
	if st.MaxBatch != ServiceDemandProductMaxBatch {
		return false, fmt.Sprintf("MaxBatch=%d want %d", st.MaxBatch, ServiceDemandProductMaxBatch)
	}
	if st.Admissions != target {
		return false, fmt.Sprintf("Admissions=%d want exact target %d", st.Admissions, target)
	}
	if st.Barriers != st.Admissions || st.BarrierOK != st.Admissions {
		return false, fmt.Sprintf("Barriers=%d BarrierOK=%d Admissions=%d (must equal)",
			st.Barriers, st.BarrierOK, st.Admissions)
	}
	if st.BatchSizeSum != st.Admissions {
		return false, fmt.Sprintf("BatchSizeSum=%d Admissions=%d", st.BatchSizeSum, st.Admissions)
	}
	return true, ""
}

func writerStatsToRecord(st reflowstate.WriterStats) reflow.CheckpointWriterStatsRecord {
	return reflow.CheckpointWriterStatsRecord{
		MaxBatch:              st.MaxBatch,
		QueueDepthSamples:     st.QueueDepthSamples,
		QueueDepthSum:         st.QueueDepthSum,
		QueueDepthPeak:        st.QueueDepthPeak,
		Admissions:            st.Admissions,
		AdmissionWaitNanos:    st.AdmissionWaitNanos,
		AdmissionWaitMaxNanos: st.AdmissionWaitMaxNanos,
		AdmissionBlocked:      st.AdmissionBlocked,
		Barriers:              st.Barriers,
		BarrierWaitNanos:      st.BarrierWaitNanos,
		BarrierWaitMaxNanos:   st.BarrierWaitMaxNanos,
		BarrierOK:             st.BarrierOK,
		BarrierRefusal:        st.BarrierRefusal,
		BarrierWriterFailed:   st.BarrierWriterFailed,
		BarrierWriterClosed:   st.BarrierWriterClosed,
		BarrierCanceled:       st.BarrierCanceled,
		Batches:               st.Batches,
		BatchSizeSum:          st.BatchSizeSum,
		BatchSizeMax:          st.BatchSizeMax,
		BatchSize1:            st.BatchSize1,
		BatchSize2To8:         st.BatchSize2To8,
		BatchSize9To32:        st.BatchSize9To32,
		BatchSize33To128:      st.BatchSize33To128,
		BatchSize129Plus:      st.BatchSize129Plus,
		Commits:               st.Commits,
		BatchDurationNanos:    st.BatchDurationNanos,
		BatchDurationMaxNanos: st.BatchDurationMaxNanos,
		CommitFatals:          st.CommitFatals,
		RequestRefusals:       st.RequestRefusals,
		SavepointsCreated:     st.SavepointsCreated,
		SavepointsElided:      st.SavepointsElided,
	}
}

// ServiceDemandCell is one arm inside a multi-rep set (sterile; no store path).
type ServiceDemandCell struct {
	Rep        int                     `json:"rep"`
	Ordinal    int                     `json:"ordinal"`
	ScheduleID ServiceDemandScheduleID `json:"schedule_id"`
	Submitters int                     `json:"submitters"`
	ServiceDemandArmReport
}

// ServiceDemandSetConfig configures RunServiceDemandSet.
type ServiceDemandSetConfig struct {
	// RootDir holds per-arm SQLite files (fresh unique DB per arm).
	RootDir string
	// FormalReps is the list of formal rep indices (default 1,2,3). Pilot 0 excluded.
	FormalReps []int
	// Units overrides formal quantum for tests (0 → formal 100k units).
	Units int
	// MinAdmissions overrides formal quantum for tests.
	MinAdmissions int64
	// WorktreeCommitOverride is optional instrument commit override (tests).
	// When set, the set report is always non-formal (test-only identity).
	WorktreeCommitOverride string
	// MediumOverride labels reduced/non-formal structural runs (e.g. "test_tmp").
	// Empty + formal path → verified APFS medium (or set fails formal admission).
	MediumOverride string
	// SkipAPFSCheck allows structural tests on non-APFS roots; forces non-formal.
	SkipAPFSCheck bool
	// ElideRawExecSavepoints enables Phase A elision for every arm in the set.
	// Sets with elision are never formal baseline authority (wantFormalShape false).
	ElideRawExecSavepoints bool
}

// ServiceDemandSetReport is the versioned sterile multi-rep report.
type ServiceDemandSetReport struct {
	SchemaVersion    string `json:"schema_version"`
	Medium           string `json:"medium"`
	MixID            string `json:"mix_id"`
	AdmissionsPerArm int64  `json:"admissions_per_arm"`
	// Formal is true only for the default Day-0 shape: reps {1,2,3}, full
	// submitter grid, exact ServiceDemandAdmissionsPerArm on every cell.
	// Reduced Units/MinAdmissions/FormalReps sets are non-formal (structure only).
	Formal                 bool                     `json:"formal"`
	InstrumentSHABefore    string                   `json:"instrument_sha_before"`
	InstrumentCommitBefore string                   `json:"instrument_commit_before"`
	InstrumentDirtyBefore  bool                     `json:"instrument_dirty_before"`
	InstrumentSHAAfter     string                   `json:"instrument_sha_after"`
	InstrumentCommitAfter  string                   `json:"instrument_commit_after"`
	InstrumentDirtyAfter   bool                     `json:"instrument_dirty_after"`
	Cells                  []ServiceDemandCell      `json:"cells"`
	Disposition            ServiceDemandDisposition `json:"disposition"`
	DispositionMessage     string                   `json:"disposition_message,omitempty"`
}

// RunServiceDemandSet runs formal reps under fixed schedules (18 cells for 3×6).
// Captures instrument identity before/after; refuses set if instrument drifts.
func RunServiceDemandSet(ctx context.Context, cfg ServiceDemandSetConfig) (ServiceDemandSetReport, error) {
	if cfg.RootDir == "" {
		return ServiceDemandSetReport{}, fmt.Errorf("root dir required")
	}
	if err := os.MkdirAll(cfg.RootDir, 0o755); err != nil {
		return ServiceDemandSetReport{}, err
	}
	reps := cfg.FormalReps
	if len(reps) == 0 {
		reps = []int{1, 2, 3}
	}

	// Medium: formal path requires verified APFS unless explicitly non-formal.
	// Phase A elision sets are candidates, never formal baseline authority.
	wantFormalShape := len(reps) == 3 && reps[0] == 1 && reps[1] == 2 && reps[2] == 3 &&
		cfg.Units == 0 && cfg.MinAdmissions == 0 && cfg.WorktreeCommitOverride == "" &&
		cfg.MediumOverride == "" && !cfg.SkipAPFSCheck && !cfg.ElideRawExecSavepoints
	medium := cfg.MediumOverride
	if medium == "" {
		medium = ServiceDemandMedium
	}
	if wantFormalShape {
		if err := requireAPFSRoot(cfg.RootDir); err != nil {
			return ServiceDemandSetReport{}, fmt.Errorf("formal medium: %w", err)
		}
		medium = ServiceDemandMedium
	} else if cfg.MediumOverride == "" {
		// Reduced/structural: do not claim APFS quorum without verification.
		if cfg.SkipAPFSCheck || cfg.Units > 0 || cfg.MinAdmissions > 0 || cfg.WorktreeCommitOverride != "" {
			medium = "non_formal"
		} else if err := requireAPFSRoot(cfg.RootDir); err != nil {
			medium = "non_formal"
		} else {
			medium = ServiceDemandMedium
		}
	}

	shaB, commitB, dirtyB, err := captureInstrumentIdentity(cfg.WorktreeCommitOverride)
	if err != nil {
		return ServiceDemandSetReport{}, fmt.Errorf("instrument before: %w", err)
	}

	rep := ServiceDemandSetReport{
		SchemaVersion:          ServiceDemandReportSchema,
		Medium:                 medium,
		MixID:                  ServiceDemandMixID,
		AdmissionsPerArm:       ServiceDemandAdmissionsPerArm,
		InstrumentSHABefore:    shaB,
		InstrumentCommitBefore: commitB,
		InstrumentDirtyBefore:  dirtyB,
	}
	// Realized admissions/arm matches arm honesty target (units*3), not a raw
	// MinAdmissions that is not divisible by the mix width.
	if cfg.Units > 0 {
		rep.AdmissionsPerArm = int64(cfg.Units) * ServiceDemandAdmissionsPerUnit
	} else if cfg.MinAdmissions > 0 {
		u := cfg.MinAdmissions / ServiceDemandAdmissionsPerUnit
		if u < 1 {
			u = 1
		}
		rep.AdmissionsPerArm = u * ServiceDemandAdmissionsPerUnit
	}

	cells := make([]ServiceDemandCell, 0, len(reps)*len(ServiceDemandSubmitterGrid))
	for _, r := range reps {
		schedID, err := FormalScheduleForRep(r)
		if err != nil {
			return ServiceDemandSetReport{}, err
		}
		order := ServiceDemandFormalSchedules[schedID]
		for ord, n := range order {
			storePath := filepath.Join(cfg.RootDir, fmt.Sprintf("rep%d-ord%d-n%d.db", r, ord, n))
			arm, err := RunServiceDemandArm(ctx, ServiceDemandArmConfig{
				StorePath:              storePath,
				Submitters:             n,
				Units:                  cfg.Units,
				MinAdmissions:          cfg.MinAdmissions,
				ElideRawExecSavepoints: cfg.ElideRawExecSavepoints,
			})
			if err != nil {
				return ServiceDemandSetReport{}, fmt.Errorf("rep=%d ordinal=%d submitters=%d: %w", r, ord, n, err)
			}
			cells = append(cells, ServiceDemandCell{
				Rep:                    r,
				Ordinal:                ord,
				ScheduleID:             schedID,
				Submitters:             n,
				ServiceDemandArmReport: arm,
			})
		}
	}
	rep.Cells = cells
	// Formal authority requires the full formal-shape gate (incl. APFS check run,
	// no medium/commit overrides) plus pure schedule validator and verified medium.
	// MediumOverride:"apfs_disk" must not skip requireAPFSRoot and still set Formal.
	rep.Formal = wantFormalShape &&
		ValidateServiceDemandFormalSet(cells) == nil &&
		medium == ServiceDemandMedium
	if err := assertInstrumentIdentityUnchanged(cfg.WorktreeCommitOverride, shaB, commitB, dirtyB); err != nil {
		return ServiceDemandSetReport{}, err
	}
	shaA, commitA, dirtyA, err := captureInstrumentIdentity(cfg.WorktreeCommitOverride)
	if err != nil {
		return ServiceDemandSetReport{}, fmt.Errorf("instrument after: %w", err)
	}
	rep.InstrumentSHAAfter = shaA
	rep.InstrumentCommitAfter = commitA
	rep.InstrumentDirtyAfter = dirtyA

	disp, msg := ClassifyServiceDemandV2(cells)
	// Classifier formal gate is pure on cells; demote authority if set envelope is non-formal.
	if !rep.Formal && (disp == ServiceDemandMechanismBinds || disp == ServiceDemandMechanismDoesNot) {
		disp = ServiceDemandInconclusive
		msg = "metrics pattern matched but set envelope is non-formal (medium/identity/quantum): " + msg
	}
	rep.Disposition = disp
	rep.DispositionMessage = msg
	return rep, nil
}

// ValidateServiceDemandFormalSet is the pure retained-set schedule/honesty validator (E1).
// A malformed set must never be treated as formal.
func ValidateServiceDemandFormalSet(cells []ServiceDemandCell) error {
	wantLen := 3 * len(ServiceDemandSubmitterGrid)
	if len(cells) != wantLen {
		return fmt.Errorf("cell count %d want %d", len(cells), wantLen)
	}
	type key struct{ rep, n int }
	seen := map[key]bool{}
	for _, c := range cells {
		if c.Rep < 1 || c.Rep > 3 {
			return fmt.Errorf("rep %d out of formal range", c.Rep)
		}
		schedID, err := FormalScheduleForRep(c.Rep)
		if err != nil {
			return err
		}
		if c.ScheduleID != schedID {
			return fmt.Errorf("rep %d schedule_id=%s want %s", c.Rep, c.ScheduleID, schedID)
		}
		order := ServiceDemandFormalSchedules[schedID]
		if c.Ordinal < 0 || c.Ordinal >= len(order) {
			return fmt.Errorf("rep %d ordinal %d out of range", c.Rep, c.Ordinal)
		}
		wantN := order[c.Ordinal]
		if c.Submitters != wantN {
			return fmt.Errorf("rep %d ordinal %d submitters=%d want %d", c.Rep, c.Ordinal, c.Submitters, wantN)
		}
		if c.ServiceDemandArmReport.Submitters != c.Submitters {
			return fmt.Errorf("rep %d n=%d arm submitters mismatch %d vs %d",
				c.Rep, c.Submitters, c.ServiceDemandArmReport.Submitters, c.Submitters)
		}
		if c.AdmissionsTarget != ServiceDemandAdmissionsPerArm {
			return fmt.Errorf("rep %d n=%d admissions_target=%d want %d",
				c.Rep, c.Submitters, c.AdmissionsTarget, ServiceDemandAdmissionsPerArm)
		}
		// Re-derive honesty from retained WriterStats (do not trust boolean alone).
		st := writerStatsFromRecord(c.WriterStats)
		honest, msg := serviceDemandHonest(st, c.AdmissionsTarget)
		if !honest {
			return fmt.Errorf("rep %d n=%d re-derived dishonest: %s", c.Rep, c.Submitters, msg)
		}
		if !c.Honest {
			return fmt.Errorf("rep %d n=%d stored Honest=false", c.Rep, c.Submitters)
		}
		k := key{c.Rep, c.Submitters}
		if seen[k] {
			return fmt.Errorf("duplicate cell rep=%d n=%d", c.Rep, c.Submitters)
		}
		seen[k] = true
	}
	// Full grid: every rep × every N present exactly once.
	for rep := 1; rep <= 3; rep++ {
		for _, n := range ServiceDemandSubmitterGrid {
			if !seen[key{rep, n}] {
				return fmt.Errorf("missing cell rep=%d n=%d", rep, n)
			}
		}
	}
	return nil
}

func writerStatsFromRecord(r reflow.CheckpointWriterStatsRecord) reflowstate.WriterStats {
	return reflowstate.WriterStats{
		MaxBatch:              r.MaxBatch,
		QueueDepthSamples:     r.QueueDepthSamples,
		QueueDepthSum:         r.QueueDepthSum,
		QueueDepthPeak:        r.QueueDepthPeak,
		Admissions:            r.Admissions,
		AdmissionWaitNanos:    r.AdmissionWaitNanos,
		AdmissionWaitMaxNanos: r.AdmissionWaitMaxNanos,
		AdmissionBlocked:      r.AdmissionBlocked,
		Barriers:              r.Barriers,
		BarrierWaitNanos:      r.BarrierWaitNanos,
		BarrierWaitMaxNanos:   r.BarrierWaitMaxNanos,
		BarrierOK:             r.BarrierOK,
		BarrierRefusal:        r.BarrierRefusal,
		BarrierWriterFailed:   r.BarrierWriterFailed,
		BarrierWriterClosed:   r.BarrierWriterClosed,
		BarrierCanceled:       r.BarrierCanceled,
		Batches:               r.Batches,
		BatchSizeSum:          r.BatchSizeSum,
		BatchSizeMax:          r.BatchSizeMax,
		BatchSize1:            r.BatchSize1,
		BatchSize2To8:         r.BatchSize2To8,
		BatchSize9To32:        r.BatchSize9To32,
		BatchSize33To128:      r.BatchSize33To128,
		BatchSize129Plus:      r.BatchSize129Plus,
		Commits:               r.Commits,
		BatchDurationNanos:    r.BatchDurationNanos,
		BatchDurationMaxNanos: r.BatchDurationMaxNanos,
		CommitFatals:          r.CommitFatals,
		RequestRefusals:       r.RequestRefusals,
	}
}

// RunFormalServiceDemandBaseline is the gated formal-run entry: default 3×6×300k
// under RootDir, writes the sterile set report to reportPath (required, no overwrite).
func RunFormalServiceDemandBaseline(ctx context.Context, rootDir, reportPath string) (ServiceDemandSetReport, error) {
	if reportPath == "" {
		return ServiceDemandSetReport{}, fmt.Errorf("formal baseline requires non-empty report path")
	}
	set, err := RunServiceDemandSet(ctx, ServiceDemandSetConfig{RootDir: rootDir})
	if err != nil {
		return ServiceDemandSetReport{}, err
	}
	if !set.Formal {
		return set, fmt.Errorf("formal baseline envelope refused (formal=false medium=%s): %s", set.Medium, set.DispositionMessage)
	}
	if err := WriteServiceDemandSetReport(reportPath, set); err != nil {
		return set, fmt.Errorf("write report: %w", err)
	}
	return set, nil
}

// WriteServiceDemandSetReport writes the sterile set report as JSON.
// Uses O_CREATE|O_EXCL|O_WRONLY so the final path cannot replace existing
// retained evidence (POSIX rename is replace-capable and is not used here).
// Non-blocking nit: a crash mid-write may leave a partial exclusive file that
// blocks re-run until the operator removes it.
func WriteServiceDemandSetReport(path string, rep ServiceDemandSetReport) error {
	if path == "" {
		return fmt.Errorf("report path required")
	}
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644) // #nosec G302 -- harness report under operator root
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("report path exists (refuse overwrite): %s", path)
		}
		return err
	}
	_, werr := f.Write(b)
	cerr := f.Close()
	if werr != nil {
		return werr
	}
	return cerr
}

// ServiceDemandDisposition is the pure classifier outcome for a formal set.
type ServiceDemandDisposition string

const (
	ServiceDemandMechanismBinds   ServiceDemandDisposition = "mechanism_binds"
	ServiceDemandMechanismDoesNot ServiceDemandDisposition = "mechanism_does_not_bind"
	ServiceDemandInconclusive     ServiceDemandDisposition = "inconclusive"
)

// service-demand classifier constants (formal service-demand plan).
const (
	ServiceDemandUtilMinHighN   = 0.85 // util median floor at each of 64..512
	ServiceDemandPlateauRatio   = 1.05 // mut/s@512 ≤ 1.05 × mut/s@128
	ServiceDemandBarrierMeanMul = 2.0  // mean barrier-wait/admission @512 ≥ 2× @64
	ServiceDemandMutsCVMax      = 0.10 // mut/s CV ceiling on high-N cells
	ServiceDemandDoesNotUtilMax = 0.50 // util median < 0.50 at 256 and 512
	ServiceDemandDoesNotScale   = 1.5  // mut/s increases ≥1.5× from 64→256
)

// ClassifyServiceDemandV2 is a pure classifier over formal set cells.
//
// mechanism_binds requires formal shape (3 reps × full grid × exact 300k
// admissions on every cell) plus all of:
//  1. util median ≥ 0.85 at each of {64,128,256,512}
//  2. mut/s@512 ≤ 1.05 × mut/s@128 (plateau) OR decline across two adjacent of {128,256,512}
//  3. mean barrier-wait/admission @512 ≥ 2 × mean @64
//  4. mut/s CV ≤ 0.10 at each high-N point with n≥3 samples (else inconclusive)
//
// mechanism_does_not_bind: same formal shape; util median < 0.50 at 256 and 512,
// and mut/s increases ≥1.5× from 64→256.
// Otherwise: inconclusive.
func ClassifyServiceDemandV2(cells []ServiceDemandCell) (ServiceDemandDisposition, string) {
	if len(cells) == 0 {
		return ServiceDemandInconclusive, "no cells"
	}
	for _, c := range cells {
		if !c.Honest {
			return ServiceDemandInconclusive, fmt.Sprintf("dishonest cell rep=%d n=%d: %s", c.Rep, c.Submitters, c.HonestyMessage)
		}
	}
	formalErr := ValidateServiceDemandFormalSet(cells)
	formal := formalErr == nil

	highN := []int{64, 128, 256, 512}
	// Per-N mut/s series for CV and medians of util / barrier mean.
	mutsByN := map[int][]float64{}
	utilByN := map[int][]float64{}
	barMeanByN := map[int][]float64{}
	for _, c := range cells {
		mutsByN[c.Submitters] = append(mutsByN[c.Submitters], c.MutationsPerSec)
		utilByN[c.Submitters] = append(utilByN[c.Submitters], c.WriterUtilization)
		barMeanByN[c.Submitters] = append(barMeanByN[c.Submitters], c.BarrierWaitMeanNanos)
	}

	// CV gate on high-N: require 3-rep confidence (n≥3); n=1 must not green CV=0.
	for _, n := range highN {
		series := mutsByN[n]
		if len(series) == 0 {
			return ServiceDemandInconclusive, fmt.Sprintf("missing high-N=%d cells", n)
		}
		if len(series) < 3 {
			return ServiceDemandInconclusive, fmt.Sprintf("high-N=%d has %d samples; need ≥3 for CV confidence", n, len(series))
		}
		cv := coeffOfVariation(series)
		if math.IsNaN(cv) || cv > ServiceDemandMutsCVMax {
			return ServiceDemandInconclusive, fmt.Sprintf("mut/s CV@%d=%.4f exceeds %.2f (rerun high-N or stabilize host)", n, cv, ServiceDemandMutsCVMax)
		}
	}

	// util median ≥ 0.85 at each high-N.
	utilOK := true
	for _, n := range highN {
		med := medianFloat(utilByN[n])
		if med < ServiceDemandUtilMinHighN {
			utilOK = false
			break
		}
	}

	// Plateau / decline (Day-0 plateau plus freeze-v2 decline OR).
	mut128 := medianFloat(mutsByN[128])
	mut256 := medianFloat(mutsByN[256])
	mut512 := medianFloat(mutsByN[512])
	mut64 := medianFloat(mutsByN[64])
	plateau := mut512 <= ServiceDemandPlateauRatio*mut128
	decline := (mut256 < mut128 && mut512 < mut256) || (mut512 < mut256 && mut256 <= mut128)
	plateauOK := plateau || decline

	// Barrier mean rising: 512 ≥ 2× 64.
	bar64 := medianFloat(barMeanByN[64])
	bar512 := medianFloat(barMeanByN[512])
	barrierOK := bar64 > 0 && bar512 >= ServiceDemandBarrierMeanMul*bar64

	if utilOK && plateauOK && barrierOK {
		if !formal {
			return ServiceDemandInconclusive, "metrics match mechanism_binds pattern but set is non-formal (need 3×6×300k honest cells)"
		}
		return ServiceDemandMechanismBinds, "formal set: high-N util, plateau/decline, and barrier-mean rise hold with CV gate"
	}

	// does_not_bind path (also formal-only for authority).
	util256 := medianFloat(utilByN[256])
	util512 := medianFloat(utilByN[512])
	if util256 < ServiceDemandDoesNotUtilMax && util512 < ServiceDemandDoesNotUtilMax &&
		mut64 > 0 && mut256 >= ServiceDemandDoesNotScale*mut64 {
		if !formal {
			return ServiceDemandInconclusive, "metrics match does_not_bind pattern but set is non-formal (need 3×6×300k honest cells)"
		}
		return ServiceDemandMechanismDoesNot, "formal set: low util at 256/512 with healthy mut/s scale-up 64→256"
	}

	return ServiceDemandInconclusive, fmt.Sprintf(
		"formal=%v utilOK=%v plateauOK=%v barrierOK=%v (util med 64/128/256/512=%.3f/%.3f/%.3f/%.3f mut med 64/128/256/512=%.0f/%.0f/%.0f/%.0f barMean 64/512=%.0f/%.0f)",
		formal, utilOK, plateauOK, barrierOK,
		medianFloat(utilByN[64]), medianFloat(utilByN[128]), util256, util512,
		mut64, mut128, mut256, mut512, bar64, bar512,
	)
}

func medianFloat(xs []float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	cp := append([]float64(nil), xs...)
	// insertion sort — n≤3 formal reps
	for i := 1; i < len(cp); i++ {
		j := i
		for j > 0 && cp[j-1] > cp[j] {
			cp[j-1], cp[j] = cp[j], cp[j-1]
			j--
		}
	}
	mid := len(cp) / 2
	if len(cp)%2 == 1 {
		return cp[mid]
	}
	return (cp[mid-1] + cp[mid]) / 2
}

func coeffOfVariation(xs []float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	if len(xs) == 1 {
		return 0
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	mean := sum / float64(len(xs))
	if mean == 0 {
		return math.NaN()
	}
	var ss float64
	for _, x := range xs {
		d := x - mean
		ss += d * d
	}
	// Population CV over formal reps (n small); sample would use n-1.
	sd := math.Sqrt(ss / float64(len(xs)))
	return sd / mean
}
