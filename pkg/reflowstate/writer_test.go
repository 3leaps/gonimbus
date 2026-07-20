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

func mustOpen(t *testing.T) *Store {
	t.Helper()
	store, err := Open(context.Background(), Config{Path: filepath.Join(t.TempDir(), "state.db")})
	if err != nil {
		t.Fatalf("open reflow state: %v", err)
	}
	return store
}

func readSynchronous(t *testing.T, store *Store) int {
	t.Helper()
	var level int
	if err := store.writer.query(context.Background(), func(ctx context.Context, conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&level)
	}); err != nil {
		t.Fatalf("read synchronous on writer connection: %v", err)
	}
	return level
}

// TestCoordinatorSaturatedMixNoDeadlock is the E-A3-I2 (C6) structural gate: a
// producer set larger than the admission queue concurrently drives strict
// terminal upserts alongside destination observations, source notes, collision
// notes, and resume reads. The single pinned writer must service the whole mix
// through one connection without deadlock or read starvation, every operation
// must succeed, and synchronous=FULL must hold on the exact writer connection.
func TestCoordinatorSaturatedMixNoDeadlock(t *testing.T) {
	ctx := context.Background()
	store := mustOpen(t)
	defer func() { _ = store.Close() }()

	// More producers than the admission queue is deep (defaultMaxBatch) so
	// submitters genuinely block on admission and must be released by the
	// draining writer — the saturated-queue condition the gate requires.
	const workers = defaultMaxBatch + 64
	const perWorker = 8

	var wg sync.WaitGroup
	errCh := make(chan error, workers*perWorker*6)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				src := fmt.Sprintf("s3://source-bucket/obj/%d/%d", w, i)
				dst := fmt.Sprintf("s3://dest-bucket/data/%d/%d", w, i)
				dk := fmt.Sprintf("data/%d/%d", w, i)
				etag := fmt.Sprintf("etag-%d-%d", w, i)

				if err := store.UpsertItem(ctx, UpsertItemParams{
					SourceURI: src, DestURI: dst, SourceKey: fmt.Sprintf("obj/%d/%d", w, i),
					DestKey: dk, SourceETag: etag, SourceSize: 10, Status: "complete", Bytes: 10,
				}); err != nil {
					errCh <- fmt.Errorf("upsert: %w", err)
					return
				}
				if err := store.MarkDestKeyObserved(ctx, dk); err != nil {
					errCh <- fmt.Errorf("mark observed: %w", err)
					return
				}
				if err := store.NoteDestKeySource(ctx, dk, src, etag, 10); err != nil {
					errCh <- fmt.Errorf("note source: %w", err)
					return
				}
				if err := store.NoteCollision(ctx, dk, CollisionDuplicate, src, etag, 10, "dest-etag", 10); err != nil {
					errCh <- fmt.Errorf("note collision: %w", err)
					return
				}
				// Resume reads interleaved with the write saturation: these must
				// not starve behind the batched writes.
				done, status, err := store.ItemDone(ctx, src, dst)
				if err != nil {
					errCh <- fmt.Errorf("item done: %w", err)
					return
				}
				if !done || status != "complete" {
					errCh <- fmt.Errorf("item %s not durably complete: done=%v status=%q", src, done, status)
					return
				}
				if observed, err := store.DestKeyObserved(ctx, dk); err != nil {
					errCh <- fmt.Errorf("dest observed: %w", err)
					return
				} else if !observed {
					errCh <- fmt.Errorf("dest key %s not observed after mark", dk)
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
		t.Fatal("saturated mix did not complete: possible deadlock or read starvation")
	}
	close(errCh)
	for err := range errCh {
		t.Fatalf("mixed operation failed: %v", err)
	}

	if level := readSynchronous(t, store); level != 2 {
		t.Fatalf("synchronous = %d on writer connection, want 2 (FULL)", level)
	}
}

// TestCoordinatorInjectedFailureUnblocksAllWaiters is the E-A3-I2 (C6)
// injected-writer-failure gate: when the writer cannot durably commit, every
// blocked mutation caller must unblock with the typed ErrWriterFailed rather
// than hang, no success is acknowledged, and Close must surface the failure
// instead of discarding it.
func TestCoordinatorInjectedFailureUnblocksAllWaiters(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	injected := errors.New("simulated durable-commit failure")

	store, err := openStore(ctx, Config{Path: path}, func(c *coordinator) {
		c.injectCommit = func() error { return injected }
	})
	if err != nil {
		t.Fatalf("open reflow state: %v", err)
	}

	const callers = 200
	var wg sync.WaitGroup
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results <- store.UpsertItem(ctx, UpsertItemParams{
				SourceURI: fmt.Sprintf("s3://source-bucket/obj/%d", i),
				DestURI:   "s3://dest-bucket/data/obj",
				Status:    "complete",
				Bytes:     1,
			})
		}(i)
	}

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatal("injected writer failure did not unblock every caller: deadlock")
	}
	close(results)

	unblocked := 0
	for err := range results {
		unblocked++
		if !errors.Is(err, ErrWriterFailed) {
			t.Fatalf("caller error = %v, want it to wrap ErrWriterFailed", err)
		}
	}
	if unblocked != callers {
		t.Fatalf("unblocked %d callers, want %d", unblocked, callers)
	}

	// A submission attempted after the writer has failed must also fail fast,
	// never block.
	lateDone := make(chan error, 1)
	go func() {
		lateDone <- store.UpsertItem(ctx, UpsertItemParams{SourceURI: "s3://source-bucket/obj/late", DestURI: "d", Status: "complete"})
	}()
	select {
	case err := <-lateDone:
		if !errors.Is(err, ErrWriterFailed) {
			t.Fatalf("post-failure submission error = %v, want ErrWriterFailed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("post-failure submission blocked instead of failing fast")
	}

	// Close must surface the writer failure rather than a nil or close-only error.
	if cerr := store.Close(); !errors.Is(cerr, ErrWriterFailed) {
		t.Fatalf("Close error = %v, want it to wrap ErrWriterFailed", cerr)
	}
}

// TestCoordinatorRealStatementFailureRollsBackAndFailsClosed exercises the REAL
// mid-batch rollback path (commit -> tx.ExecContext error -> Rollback), which the
// injectCommit control deliberately bypasses. A genuine statement failure must
// roll the batch back, unblock the caller with the typed error, and put the
// coordinator into its terminal failed state so every subsequent call fails
// closed and Close surfaces it.
func TestCoordinatorRealStatementFailureRollsBackAndFailsClosed(t *testing.T) {
	ctx := context.Background()
	store := mustOpen(t)

	// BeginTx accepts this; ExecContext rejects it at run time (missing table),
	// driving the real Rollback path rather than an injected commit result.
	err := store.writer.exec(ctx, "INSERT INTO reflow_no_such_table (x) VALUES (?)", 1)
	if !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("real statement failure error = %v, want ErrWriterFailed", err)
	}

	// The coordinator is now terminally failed: a well-formed mutation must also
	// fail closed rather than succeed.
	if err := store.UpsertItem(ctx, UpsertItemParams{SourceURI: "s3://source-bucket/obj", DestURI: "d", Status: "complete"}); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("post-failure upsert error = %v, want ErrWriterFailed", err)
	}

	if cerr := store.Close(); !errors.Is(cerr, ErrWriterFailed) {
		t.Fatalf("Close error = %v, want ErrWriterFailed", cerr)
	}
}

// TestCoordinatorMidBatchStatementFailureCommitsNoSubset closes the E-A3-I4 proof
// gap: TestCoordinatorRealStatementFailureRollsBackAndFailsClosed drives a
// one-request batch, so it proves the real rollback path but cannot prove that a
// failure in the MIDDLE of a multi-request batch commits no valid subset. Here a
// genuine three-request batch (valid A, invalid B, valid C) is assembled
// deterministically and driven through the real serveBatch -> commit -> BeginTx ->
// Exec(A) -> Exec(B fails) -> Rollback path. The batch must fail atomically:
// neither A nor C may survive, and all three co-batched waiters must observe the
// typed failure. Determinism comes from pre-loading the exec channel and invoking
// serveBatch directly (the run loop only supplies the first request and returns on
// a false result, which this test asserts explicitly), so batch composition never
// depends on scheduler timing.
func TestCoordinatorMidBatchStatementFailureCommitsNoSubset(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")

	// Create the schema via a normal store, then close it so the durable file
	// retains the schema. The batch under test is then driven by hand on a fresh
	// connection with no writer goroutine competing for the exec channel.
	seed, err := Open(ctx, Config{Path: path})
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}

	db, err := indexstore.Open(ctx, indexstore.Config{Path: path, SynchronousFull: true})
	if err != nil {
		t.Fatalf("reopen durable db: %v", err)
	}
	defer func() { _ = db.Close() }()

	c := newCoordinator(db)
	var assembled int
	c.onBatchAssembled = func(n int) { assembled = n }
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin conn: %v", err)
	}
	if err := indexstore.ConfigureDurableConn(ctx, conn); err != nil {
		t.Fatalf("configure durable conn: %v", err)
	}
	c.conn = conn
	c.baseCtx = context.Background()

	const (
		srcA = "s3://source-bucket/mid-batch/A"
		srcC = "s3://source-bucket/mid-batch/C"
	)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	validInsert := `INSERT INTO reflow_items (source_uri, dest_uri, status, updated_at) VALUES (?, ?, ?, ?)`
	reqA := &writeRequest{kind: reqExec, stmt: validInsert, args: []any{srcA, "d", "complete", now}, done: make(chan error, 1)}
	reqB := &writeRequest{kind: reqExec, stmt: "INSERT INTO reflow_no_such_table (x) VALUES (?)", args: []any{1}, done: make(chan error, 1)}
	reqC := &writeRequest{kind: reqExec, stmt: validInsert, args: []any{srcC, "d", "complete", now}, done: make(chan error, 1)}

	// Pre-load B and C so serveBatch's greedy coalescing pulls them behind A into
	// one transaction with the invalid statement in the middle.
	c.execCh <- reqB
	c.execCh <- reqC
	if ok := c.serveBatch(reqA); ok {
		t.Fatal("serveBatch returned true; a mid-batch statement failure must fail the batch")
	}

	if assembled != 3 {
		t.Fatalf("batch coalesced %d requests, want a genuine 3-request batch", assembled)
	}
	for name, req := range map[string]*writeRequest{"A": reqA, "B": reqB, "C": reqC} {
		if werr := <-req.done; !errors.Is(werr, ErrWriterFailed) {
			t.Fatalf("waiter %s error = %v, want it to wrap ErrWriterFailed", name, werr)
		}
	}
	if cerr := c.err(); !errors.Is(cerr, ErrWriterFailed) {
		t.Fatalf("coordinator error = %v, want ErrWriterFailed after mid-batch failure", cerr)
	}
	select {
	case <-c.failed:
	default:
		t.Fatal("failed channel not closed after mid-batch statement failure")
	}

	// Independent reopen: neither valid statement may have committed a subset.
	if err := conn.Close(); err != nil {
		t.Fatalf("close writer conn: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close writer db: %v", err)
	}
	verify, err := indexstore.Open(ctx, indexstore.Config{Path: path})
	if err != nil {
		t.Fatalf("verify open: %v", err)
	}
	defer func() { _ = verify.Close() }()
	var landed int
	if err := verify.QueryRowContext(ctx, `SELECT COUNT(*) FROM reflow_items WHERE source_uri IN (?, ?)`, srcA, srcC).Scan(&landed); err != nil {
		t.Fatalf("count committed subset: %v", err)
	}
	if landed != 0 {
		t.Fatalf("mid-batch failure committed a subset: %d of {A,C} rows present, want 0", landed)
	}
}

// TestCoordinatorPublishedResultNotMaskedByLifecycle closes the E-A3-I5 masking
// race: after a batch commits and publishes request A's success on req.done, a
// LATER batch may fail (or a graceful close may begin) and signal c.failed /
// c.closed before A's caller is scheduled. With both the published result and the
// lifecycle signal ready, an equal-priority select would let Go pick the lifecycle
// error and rewrite A's durable success into a failure. awaitResult must always
// return the published result; only a request that was admitted but never serviced
// (empty req.done) may return the lifecycle error. The loop defeats select
// pseudo-randomness — a single unlucky pick would flake without the fix.
func TestCoordinatorPublishedResultNotMaskedByLifecycle(t *testing.T) {
	ctx := context.Background()

	// Writer-failure side: a committed success must survive c.failed being closed.
	failed := newCoordinator(nil)
	failed.fail(fmt.Errorf("%w: a later batch failed to commit", ErrWriterFailed))
	for i := 0; i < 2000; i++ {
		committed := &writeRequest{done: make(chan error, 1)}
		committed.done <- nil
		if err := failed.awaitResult(ctx, committed); err != nil {
			t.Fatalf("iteration %d: committed success masked by writer failure, got %v want nil", i, err)
		}
	}
	// A request admitted but never serviced has no published result and must
	// surface the typed writer failure.
	unserviced := &writeRequest{done: make(chan error, 1)}
	if err := failed.awaitResult(ctx, unserviced); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("unserviced request under writer failure = %v, want ErrWriterFailed", err)
	}

	// Graceful-close side: a committed success must survive c.closed being closed.
	closed := newCoordinator(nil)
	close(closed.closed)
	for i := 0; i < 2000; i++ {
		committed := &writeRequest{done: make(chan error, 1)}
		committed.done <- nil
		if err := closed.awaitResult(ctx, committed); err != nil {
			t.Fatalf("iteration %d: committed success masked by close, got %v want nil", i, err)
		}
	}
	unadmitted := &writeRequest{done: make(chan error, 1)}
	if err := closed.awaitResult(ctx, unadmitted); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("unserviced request under close = %v, want ErrWriterClosed", err)
	}
}

// TestCoordinatorSubmitAfterCloseFailsFast pins the graceful-close symmetry: a
// submission after Close must fail fast with ErrWriterClosed rather than block on
// a departed writer goroutine.
func TestCoordinatorSubmitAfterCloseFailsFast(t *testing.T) {
	ctx := context.Background()
	store := mustOpen(t)
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- store.UpsertItem(ctx, UpsertItemParams{SourceURI: "s3://source-bucket/late", DestURI: "d", Status: "complete"})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrWriterClosed) {
			t.Fatalf("post-close submission error = %v, want ErrWriterClosed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("post-close submission blocked instead of failing fast")
	}
}

// TestCoordinatorReadsNotStarvedUnderWriteSaturation upgrades the C6 "no
// starvation" claim from liveness to fairness: an independent, continuously
// looping reader set must keep making progress while a large writer set saturates
// the admission queue. Reads serialized to near-zero behind the write flood fail
// this test; the earlier saturated-mix test only proves no deadlock.
func TestCoordinatorReadsNotStarvedUnderWriteSaturation(t *testing.T) {
	ctx := context.Background()
	store := mustOpen(t)
	defer func() { _ = store.Close() }()

	const seedKeys = 32
	for i := 0; i < seedKeys; i++ {
		src := fmt.Sprintf("s3://source-bucket/seed/%d", i)
		if err := store.UpsertItem(ctx, UpsertItemParams{SourceURI: src, DestURI: "s3://dest-bucket/seed", Status: "complete", Bytes: 1}); err != nil {
			t.Fatalf("seed upsert: %v", err)
		}
	}

	const writers = defaultMaxBatch // saturate the admission queue
	const writesPerWorker = 40
	stop := make(chan struct{})

	var writeWG sync.WaitGroup
	writeErr := make(chan error, writers)
	for w := 0; w < writers; w++ {
		writeWG.Add(1)
		go func(w int) {
			defer writeWG.Done()
			for i := 0; i < writesPerWorker; i++ {
				if err := store.UpsertItem(ctx, UpsertItemParams{
					SourceURI: fmt.Sprintf("s3://source-bucket/w/%d/%d", w, i),
					DestURI:   "s3://dest-bucket/w", Status: "complete", Bytes: 1,
				}); err != nil {
					writeErr <- err
					return
				}
			}
		}(w)
	}

	const readers = 8
	var readWG sync.WaitGroup
	readCounts := make([]int, readers) // each goroutine touches only its own index
	readErr := make(chan error, readers)
	for r := 0; r < readers; r++ {
		readWG.Add(1)
		go func(r int) {
			defer readWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				src := fmt.Sprintf("s3://source-bucket/seed/%d", readCounts[r]%seedKeys)
				done, _, err := store.ItemDone(ctx, src, "s3://dest-bucket/seed")
				if err != nil {
					readErr <- err
					return
				}
				if !done {
					readErr <- fmt.Errorf("seed key %s not durable-complete", src)
					return
				}
				readCounts[r]++
			}
		}(r)
	}

	writeDone := make(chan struct{})
	go func() { writeWG.Wait(); close(writeDone) }()
	select {
	case <-writeDone:
	case <-time.After(60 * time.Second):
		close(stop)
		t.Fatal("writers did not complete under saturation: possible deadlock")
	}
	close(stop)
	readWG.Wait()
	close(writeErr)
	close(readErr)
	for err := range writeErr {
		t.Fatalf("writer failed: %v", err)
	}
	for err := range readErr {
		t.Fatalf("reader failed: %v", err)
	}

	total := 0
	for r, c := range readCounts {
		if c == 0 {
			t.Fatalf("reader %d completed zero reads while writers saturated the queue: starved", r)
		}
		total += c
	}
	if minTotal := readers * 5; total < minTotal {
		t.Fatalf("readers completed %d reads total, want >= %d: reads starved under write saturation", total, minTotal)
	}
}
