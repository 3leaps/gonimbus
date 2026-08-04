package reflowthroughput

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/reflowstate"
)

// Provider-free checkpoint service-demand grid (freeze v2 / entarch 2026-08-04).
// No object copy or provider in the timed region.

const (
	// ServiceDemandSubmitters is the fixed submitter grid.
	// Order is the default run order for rep 0; later reps rotate.
	ServiceDemandAdmissionsPerUnit = 3 // UpsertItem + MarkDestKeyObserved + NoteDestKeySource
	// ServiceDemandMinAdmissions is the preferred work quantum (≥300k admissions).
	ServiceDemandMinAdmissions int64 = 300_000
	// ServiceDemandMinWall is the preferred minimum arm wall when work finishes early.
	ServiceDemandMinWall = 20 * time.Second
)

// ServiceDemandSubmitterGrid is the freeze-v2 concurrency steps.
var ServiceDemandSubmitterGrid = []int{8, 32, 64, 128, 256, 512}

// ServiceDemandArmConfig configures one timed arm.
type ServiceDemandArmConfig struct {
	// StorePath is the APFS (or other quorum medium) checkpoint path.
	StorePath string
	// Submitters is fixed concurrent mutation callers.
	Submitters int
	// Units is the number of terminal units (each = 3 admissions). 0 → derive from MinAdmissions.
	Units int
	// MinAdmissions overrides the default work quantum when > 0.
	MinAdmissions int64
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

// RunServiceDemandArm opens a fresh store, runs the terminal mutation mix under
// fixed submitter concurrency, and returns sterile metrics from WriterStats.
func RunServiceDemandArm(ctx context.Context, cfg ServiceDemandArmConfig) (ServiceDemandArmReport, error) {
	if cfg.Submitters < 1 {
		return ServiceDemandArmReport{}, fmt.Errorf("submitters must be >= 1")
	}
	if cfg.StorePath == "" {
		return ServiceDemandArmReport{}, fmt.Errorf("store path required")
	}
	minAdm := cfg.MinAdmissions
	if minAdm < 1 {
		minAdm = ServiceDemandMinAdmissions
	}
	units := cfg.Units
	if units < 1 {
		units = int(minAdm / ServiceDemandAdmissionsPerUnit)
		if units < 1 {
			units = 1
		}
	}
	if err := os.MkdirAll(filepath.Dir(cfg.StorePath), 0o755); err != nil {
		return ServiceDemandArmReport{}, err
	}
	_ = os.Remove(cfg.StorePath)

	store, err := reflowstate.Open(ctx, reflowstate.Config{Path: cfg.StorePath})
	if err != nil {
		return ServiceDemandArmReport{}, fmt.Errorf("open store: %w", err)
	}
	defer func() { _ = store.Close() }()

	work := make(chan int, cfg.Submitters*2)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	worker := func() {
		defer wg.Done()
		for i := range work {
			if err := serviceDemandUnit(runCtx, store, i); err != nil {
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
			// stop dispatch
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
		return ServiceDemandArmReport{}, ferr
	}

	st := store.WriterStats()
	rec := writerStatsToRecord(st)
	rep := ServiceDemandArmReport{
		Submitters:       cfg.Submitters,
		Units:            units,
		AdmissionsTarget: int64(units) * ServiceDemandAdmissionsPerUnit,
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

	honest, msg := serviceDemandHonest(st)
	rep.Honest = honest
	rep.HonestyMessage = msg
	if !honest {
		return rep, fmt.Errorf("dishonest arm: %s", msg)
	}
	return rep, nil
}

// serviceDemandUnit is the three-admission terminal mix used in writer stress tests
// (UpsertItem + MarkDestKeyObserved + NoteDestKeySource).
func serviceDemandUnit(ctx context.Context, store *reflowstate.Store, i int) error {
	src := fmt.Sprintf("s3://bench-source/obj/%d", i)
	dst := fmt.Sprintf("s3://bench-dest/obj/%d", i)
	dk := fmt.Sprintf("dest-key/%d", i)
	if err := store.UpsertItem(ctx, reflowstate.UpsertItemParams{
		SourceURI: src, DestURI: dst, DestKey: dk,
		Status: "complete", Bytes: 1, SourceSize: 1, SourceETag: "e",
	}); err != nil {
		return err
	}
	if err := store.MarkDestKeyObserved(ctx, dk); err != nil {
		return err
	}
	if err := store.NoteDestKeySource(ctx, dk, src, "e", 1); err != nil {
		return err
	}
	return nil
}

func serviceDemandHonest(st reflowstate.WriterStats) (bool, string) {
	if st.CommitFatals != 0 {
		return false, fmt.Sprintf("CommitFatals=%d", st.CommitFatals)
	}
	if st.BarrierWriterFailed != 0 || st.BarrierWriterClosed != 0 || st.BarrierCanceled != 0 {
		return false, fmt.Sprintf("lifecycle barriers failed=%d closed=%d canceled=%d",
			st.BarrierWriterFailed, st.BarrierWriterClosed, st.BarrierCanceled)
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
	}
}

// RotateSubmitterGrid returns the submitter order for rep index (counterbalanced).
func RotateSubmitterGrid(rep int) []int {
	g := append([]int(nil), ServiceDemandSubmitterGrid...)
	if rep <= 0 {
		return g
	}
	n := len(g)
	off := rep % n
	out := make([]int, 0, n)
	out = append(out, g[off:]...)
	out = append(out, g[:off]...)
	return out
}
