package reflowstate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

// TestWriterStatsSnapshotOnly pins C1 measure-first: Open with no callback
// surface still works, structural mutations succeed, and always-on aggregates
// populate from the sole-writer path alone.
func TestWriterStatsSnapshotOnly(t *testing.T) {
	ctx := context.Background()
	store := mustOpen(t)
	defer func() { _ = store.Close() }()

	zero := store.WriterStats()
	if zero.MaxBatch != defaultMaxBatch {
		t.Fatalf("MaxBatch = %d, want %d", zero.MaxBatch, defaultMaxBatch)
	}
	if zero.Admissions != 0 || zero.Batches != 0 || zero.Commits != 0 {
		t.Fatalf("fresh stats not zero: %+v", zero)
	}

	if err := store.UpsertItem(ctx, UpsertItemParams{
		SourceURI: "s3://source-bucket/parity", DestURI: "s3://dest-bucket/parity",
		Status: "complete", Bytes: 1,
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	done, status, err := store.ItemDone(ctx, "s3://source-bucket/parity", "s3://dest-bucket/parity")
	if err != nil || !done || status != "complete" {
		t.Fatalf("item done: done=%v status=%q err=%v", done, status, err)
	}

	st := store.WriterStats()
	if st.Admissions < 1 {
		t.Fatalf("Admissions = %d, want >= 1 after mutation", st.Admissions)
	}
	if st.Batches < 1 || st.Commits < 1 {
		t.Fatalf("Batches=%d Commits=%d, want both >= 1", st.Batches, st.Commits)
	}
	if st.BarrierOK < 1 {
		t.Fatalf("BarrierOK = %d, want >= 1", st.BarrierOK)
	}
	if st.CommitFatals != 0 || st.RequestRefusals != 0 {
		t.Fatalf("unexpected fatals/refusals: fatals=%d refusals=%d", st.CommitFatals, st.RequestRefusals)
	}
	// Reads must not inflate barrier counters (mutation-scoped only).
	if st.Barriers != st.Admissions {
		t.Fatalf("Barriers=%d Admissions=%d; query path must not add barriers", st.Barriers, st.Admissions)
	}
}

// TestWriterStatsPopulateUnderLoad drives a saturated producer mix and asserts
// that queue samples, admissions, batches, barriers, and batch timings move.
func TestWriterStatsPopulateUnderLoad(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, Config{
		Path: filepath.Join(t.TempDir(), "state.db"),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = store.Close() }()

	const workers = defaultMaxBatch + 32
	const perWorker = 4
	var wg sync.WaitGroup
	errCh := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				src := fmt.Sprintf("s3://source-bucket/load/%d/%d", w, i)
				if err := store.UpsertItem(ctx, UpsertItemParams{
					SourceURI: src, DestURI: "s3://dest-bucket/load",
					Status: "complete", Bytes: 1,
				}); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(60 * time.Second):
		t.Fatal("load mix timed out")
	}
	close(errCh)
	for err := range errCh {
		t.Fatalf("upsert: %v", err)
	}

	st := store.WriterStats()
	wantAdmissions := int64(workers * perWorker)
	if st.Admissions != wantAdmissions {
		t.Fatalf("Admissions = %d, want %d", st.Admissions, wantAdmissions)
	}
	if st.QueueDepthSamples != wantAdmissions {
		t.Fatalf("QueueDepthSamples = %d, want %d", st.QueueDepthSamples, wantAdmissions)
	}
	if st.Batches < 1 || st.BatchSizeSum < wantAdmissions {
		t.Fatalf("Batches=%d BatchSizeSum=%d, want batches>=1 and size sum >= admissions", st.Batches, st.BatchSizeSum)
	}
	if st.BarrierOK != wantAdmissions {
		t.Fatalf("BarrierOK = %d, want %d", st.BarrierOK, wantAdmissions)
	}
	if st.Commits != st.Batches {
		t.Fatalf("Commits=%d Batches=%d", st.Commits, st.Batches)
	}
	if st.BatchDurationNanos <= 0 {
		t.Fatalf("BatchDurationNanos = %d, want > 0", st.BatchDurationNanos)
	}
	// Saturated producers larger than the queue should see some full-queue samples.
	if st.QueueDepthPeak < 1 {
		t.Fatalf("QueueDepthPeak = %d, want >= 1", st.QueueDepthPeak)
	}
	partition := st.BarrierOK + st.BarrierRefusal + st.BarrierWriterFailed + st.BarrierWriterClosed + st.BarrierCanceled
	if partition != st.Barriers {
		t.Fatalf("barrier partition sum = %d, Barriers = %d", partition, st.Barriers)
	}
}

// TestWriterStatsAdmissionBlockedOnFullQueue pins the blocked heuristic:
// pre-send depth == MaxBatch (channel full) increments AdmissionBlocked.
// A silent > vs >= inversion must fail this control.
func TestWriterStatsAdmissionBlockedOnFullQueue(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, Config{
		Path: filepath.Join(t.TempDir(), "state.db"),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = store.Close() }()

	// Stall the sole writer on the first mutation so subsequent submits fill
	// the admission channel and experience true backpressure.
	entered := make(chan struct{})
	release := make(chan struct{})
	var firstStarted sync.Once
	go func() {
		_ = store.writer.execTx(ctx, func(context.Context, *sql.Tx) error {
			firstStarted.Do(func() { close(entered) })
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not enter stalled mutation")
	}

	// Fill the exec channel to capacity with non-blocking admits, then force
	// one more admit that must block until the writer drains.
	fill := defaultMaxBatch
	var wg sync.WaitGroup
	for i := 0; i < fill; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = store.UpsertItem(ctx, UpsertItemParams{
				SourceURI: fmt.Sprintf("s3://source-bucket/block/%d", i),
				DestURI:   "s3://dest-bucket/block",
				Status:    "complete", Bytes: 1,
			})
		}(i)
	}
	// Give fillers time to occupy the buffered channel.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(store.writer.execCh) >= defaultMaxBatch {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(store.writer.execCh) < defaultMaxBatch {
		close(release)
		wg.Wait()
		t.Fatalf("execCh depth = %d, want full %d before blocked admit", len(store.writer.execCh), defaultMaxBatch)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = store.UpsertItem(ctx, UpsertItemParams{
			SourceURI: "s3://source-bucket/block/overflow",
			DestURI:   "s3://dest-bucket/block",
			Status:    "complete", Bytes: 1,
		})
	}()
	// Let the overflow submitter enter the admission select while full.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	st := store.WriterStats()
	if st.AdmissionBlocked < 1 {
		t.Fatalf("AdmissionBlocked = %d, want >= 1 after full-queue admit", st.AdmissionBlocked)
	}
}

// TestWriterStatsRefusalVsFatalSeparate counts per-request logical refusals
// separately from batch-fatal commit failures.
func TestWriterStatsRefusalVsFatalSeparate(t *testing.T) {
	ctx := context.Background()

	// --- refusal path: request-local savepoint rollback, writer stays healthy ---
	path := filepath.Join(t.TempDir(), "refusal.db")
	seed, err := Open(ctx, Config{Path: path})
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}

	db, err := indexstore.Open(ctx, indexstore.Config{Path: path, SynchronousFull: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db.Close() }()

	c := newCoordinator(db)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin: %v", err)
	}
	if err := indexstore.ConfigureDurableConn(ctx, conn); err != nil {
		t.Fatalf("configure: %v", err)
	}
	c.conn = conn
	c.baseCtx = context.Background()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	refusal := errors.New("designed refusal")
	refused := &writeRequest{
		kind: reqExec,
		exec: func(ctx context.Context, tx *sql.Tx) error {
			return refuseRequest(refusal)
		},
		done: make(chan error, 1),
	}
	accepted := &writeRequest{
		kind: reqExec,
		exec: func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `
				INSERT INTO reflow_items (source_uri, dest_uri, status, updated_at)
				VALUES (?, ?, ?, ?)
			`, "s3://source-bucket/accepted", "s3://dest-bucket/item", "complete", now)
			return err
		},
		done: make(chan error, 1),
	}
	c.execCh <- accepted
	if ok := c.serveBatch(refused); !ok {
		t.Fatal("refusal must not fail the writer")
	}
	<-refused.done
	<-accepted.done

	st := c.statsSnapshot()
	if st.RequestRefusals != 1 {
		t.Fatalf("RequestRefusals = %d, want 1", st.RequestRefusals)
	}
	if st.CommitFatals != 0 {
		t.Fatalf("CommitFatals = %d after refusal, want 0", st.CommitFatals)
	}
	if st.Batches != 1 || st.BatchSizeSum != 2 {
		t.Fatalf("Batches=%d BatchSizeSum=%d, want 1 and 2", st.Batches, st.BatchSizeSum)
	}
	if st.BatchSize2To8 != 1 {
		t.Fatalf("BatchSize2To8 = %d, want 1", st.BatchSize2To8)
	}

	// --- fatal path: injected commit failure ---
	fatalStore, err := openStore(ctx, Config{Path: filepath.Join(t.TempDir(), "fatal.db")}, func(c *coordinator) {
		c.injectCommit = func() error { return errors.New("simulated commit failure") }
	})
	if err != nil {
		t.Fatalf("open fatal store: %v", err)
	}
	if err := fatalStore.UpsertItem(ctx, UpsertItemParams{
		SourceURI: "s3://source-bucket/fatal", DestURI: "d", Status: "complete",
	}); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("fatal upsert = %v, want ErrWriterFailed", err)
	}
	fst := fatalStore.WriterStats()
	if fst.CommitFatals < 1 {
		t.Fatalf("CommitFatals = %d, want >= 1", fst.CommitFatals)
	}
	if fst.RequestRefusals != 0 {
		t.Fatalf("RequestRefusals = %d on fatal path, want 0", fst.RequestRefusals)
	}
	if fst.BarrierWriterFailed < 1 {
		t.Fatalf("BarrierWriterFailed = %d, want >= 1", fst.BarrierWriterFailed)
	}
	_ = fatalStore.Close()
}

// TestWriterStatsMultiRequestBatchSize records batch size for a genuine
// multi-request batch assembled via pre-loaded exec channel (same determinism
// pattern as the mid-batch structural gate).
func TestWriterStatsMultiRequestBatchSize(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "batch.db")
	seed, err := Open(ctx, Config{Path: path})
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}

	db, err := indexstore.Open(ctx, indexstore.Config{Path: path, SynchronousFull: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db.Close() }()

	c := newCoordinator(db)
	var hookSize int
	c.onBatchAssembled = func(n int) { hookSize = n }
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin: %v", err)
	}
	if err := indexstore.ConfigureDurableConn(ctx, conn); err != nil {
		t.Fatalf("configure: %v", err)
	}
	c.conn = conn
	c.baseCtx = context.Background()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	validInsert := `INSERT INTO reflow_items (source_uri, dest_uri, status, updated_at) VALUES (?, ?, ?, ?)`
	mk := func(src string) *writeRequest {
		return &writeRequest{
			kind: reqExec,
			stmt: validInsert,
			args: []any{src, "d", "complete", now},
			done: make(chan error, 1),
		}
	}
	a, b, d := mk("s3://source-bucket/a"), mk("s3://source-bucket/b"), mk("s3://source-bucket/d")
	c.execCh <- b
	c.execCh <- d
	if ok := c.serveBatch(a); !ok {
		t.Fatal("serveBatch failed a valid multi-request batch")
	}
	for _, req := range []*writeRequest{a, b, d} {
		if err := <-req.done; err != nil {
			t.Fatalf("waiter error: %v", err)
		}
	}

	if hookSize != 3 {
		t.Fatalf("onBatchAssembled hook size = %d, want 3", hookSize)
	}
	st := c.statsSnapshot()
	if st.Batches != 1 || st.BatchSizeMax != 3 || st.BatchSizeSum != 3 {
		t.Fatalf("stats Batches=%d Max=%d Sum=%d, want 1/3/3", st.Batches, st.BatchSizeMax, st.BatchSizeSum)
	}
	if st.BatchSize2To8 != 1 {
		t.Fatalf("BatchSize2To8 = %d, want 1", st.BatchSize2To8)
	}
	if st.CommitFatals != 0 || st.RequestRefusals != 0 {
		t.Fatalf("unexpected fatals/refusals: %+v", st)
	}
	if st.BatchDurationNanos <= 0 {
		t.Fatalf("BatchDurationNanos = %d, want > 0", st.BatchDurationNanos)
	}
}

func TestBarrierOutcomeString(t *testing.T) {
	cases := []struct {
		o BarrierOutcome
		s string
	}{
		{BarrierOK, "ok"},
		{BarrierRefusal, "refusal"},
		{BarrierWriterFailed, "writer_failed"},
		{BarrierWriterClosed, "writer_closed"},
		{BarrierCanceled, "canceled"},
		{BarrierOutcome(99), "unknown"},
	}
	for _, tc := range cases {
		if got := tc.o.String(); got != tc.s {
			t.Fatalf("%v.String() = %q, want %q", tc.o, got, tc.s)
		}
	}
}

func TestWriterStatsNilStore(t *testing.T) {
	var s *Store
	if got := s.WriterStats(); got.MaxBatch != 0 {
		t.Fatalf("nil store stats = %+v", got)
	}
}

// TestRawExecSavepointElisionOffDefault: flag off creates savepoints for Mark/Note
// (raw exec) and Upsert (execTx); elided stays 0.
func TestRawExecSavepointElisionOffDefault(t *testing.T) {
	ctx := context.Background()
	store := mustOpen(t)
	defer func() { _ = store.Close() }()

	if err := store.MarkDestKeyObserved(ctx, "k1"); err != nil {
		t.Fatal(err)
	}
	if err := store.NoteDestKeySource(ctx, "k1", "s3://src/1", "e", 1); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertItem(ctx, UpsertItemParams{
		SourceURI: "s3://src/1", DestURI: "s3://dst/1", DestKey: "k1",
		Status: "complete", Bytes: 1, SourceSize: 1, SourceETag: "e",
	}); err != nil {
		t.Fatal(err)
	}
	st := store.WriterStats()
	if st.SavepointsElided != 0 {
		t.Fatalf("elided=%d want 0 with flag off", st.SavepointsElided)
	}
	if st.SavepointsCreated < 3 {
		t.Fatalf("created=%d want >= 3 (mark+note+upsert)", st.SavepointsCreated)
	}
}

// TestRawExecSavepointElisionOn: raw Mark/Note elide; Upsert (execTx) still creates.
func TestRawExecSavepointElisionOn(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, Config{
		Path:                   filepath.Join(t.TempDir(), "state.db"),
		ElideRawExecSavepoints: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.MarkDestKeyObserved(ctx, "k1"); err != nil {
		t.Fatal(err)
	}
	if err := store.NoteDestKeySource(ctx, "k1", "s3://src/1", "e", 1); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertItem(ctx, UpsertItemParams{
		SourceURI: "s3://src/1", DestURI: "s3://dst/1", DestKey: "k1",
		Status: "complete", Bytes: 1, SourceSize: 1, SourceETag: "e",
	}); err != nil {
		t.Fatal(err)
	}
	st := store.WriterStats()
	if st.SavepointsElided < 2 {
		t.Fatalf("elided=%d want >= 2 (mark+note)", st.SavepointsElided)
	}
	if st.SavepointsCreated < 1 {
		t.Fatalf("created=%d want >= 1 (upsert execTx)", st.SavepointsCreated)
	}
	// Designed refusal still works on execTx with savepoints.
	// Identity collision via second upsert with different source is product path;
	// structural gate already covers refusal savepoint rollback elsewhere.
}

// TestRawExecSQLFatalStillFailsWriterWithElision: bad SQL on raw path is batch-fatal.
func TestRawExecSQLFatalStillFailsWriterWithElision(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, Config{
		Path:                   filepath.Join(t.TempDir(), "state.db"),
		ElideRawExecSavepoints: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	err = store.writer.exec(ctx, "INSERT INTO reflow_no_such_table (x) VALUES (?)", 1)
	if err == nil {
		t.Fatal("want error")
	}
	if !errors.Is(err, ErrWriterFailed) {
		// exec wraps; may be bare SQL error from await — check coordinator failed
		if store.writer.err() == nil {
			t.Fatalf("writer not failed after bad SQL: %v", err)
		}
	}
}
