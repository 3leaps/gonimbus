package reflowstate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

// ErrWriterFailed marks a terminal failure of the checkpoint write coordinator.
// It is resumable: a run that observes it must abort without treating any
// pending terminal as durably acknowledged, and a later resume re-drives the
// affected items. Every mutation waiter blocked on a failing writer unblocks
// with an error that wraps this sentinel.
var ErrWriterFailed = errors.New("reflow checkpoint writer failed")

// ErrWriterClosed is returned to a caller that submits after the coordinator has
// been gracefully closed. Graceful close is symmetric with the failure path: a
// late or racing submitter unblocks with this typed error rather than blocking
// forever on a writer goroutine that has already exited. Well-behaved callers do
// not submit after Close; this is a fail-fast guard, not a supported ordering.
var ErrWriterClosed = errors.New("reflow checkpoint writer closed")

// defaultMaxBatch bounds how many pending mutations one write transaction
// coalesces. It also bounds the admission queue depth so a saturated producer
// set applies backpressure rather than growing memory without limit.
const defaultMaxBatch = 256

type reqKind uint8

const (
	reqExec reqKind = iota
	reqQuery
)

// writeRequest is one unit of work handed to the coordinator: either a single
// mutation statement coalesced into a batched transaction (reqExec), or a read
// executed on the pinned connection between batches (reqQuery).
type writeRequest struct {
	kind  reqKind
	stmt  string
	args  []any
	query func(ctx context.Context, conn *sql.Conn) error
	done  chan error
}

// coordinator owns the single state.db write connection and is the sole
// mutation authority for the reflow checkpoint store. Every store mutation and
// read is serviced on one pinned *sql.Conn: mutations are coalesced into batched
// transactions whose waiters are released only after COMMIT (the per-outcome
// post-COMMIT barrier), and reads run on the same connection so no implicit
// second writer is ever created. A commit or statement failure is fatal to the
// coordinator: it enters a failed state, unblocks every waiting caller with a
// typed error, and refuses further work, so a durability loss can never be
// papered over by continuing to acknowledge successes.
type coordinator struct {
	db   *sql.DB
	conn *sql.Conn

	// baseCtx scopes the pinned connection's DB operations to the writer's own
	// lifetime. It is deliberately independent of the caller context that opened
	// the store (which may be cancelled once Open returns) and is not cancelled
	// on close: close is graceful (drain, then stop), and a stalled commit is
	// already bounded by busy_timeout, so a cancelable context here would buy no
	// real interruption. Forced cancellation of in-flight work is a separate,
	// later shutdown concern.
	baseCtx context.Context

	execCh  chan *writeRequest
	queryCh chan *writeRequest

	quit    chan struct{}
	stopped chan struct{}

	// closed is closed by close() after the writer goroutine has drained and
	// exited, so a late/racing submitter fails fast with ErrWriterClosed instead
	// of blocking on a goroutine that is gone. It mirrors failed on the graceful
	// path.
	closed    chan struct{}
	closeOnce sync.Once
	closeErr  error

	failOnce sync.Once
	failed   chan struct{}

	mu      sync.Mutex
	failErr error

	maxBatch int

	// injectCommit, when non-nil, replaces the real batched commit with its
	// result. It exists to drive the injected-writer-failure gate deterministically
	// and is set only by in-package tests before the writer goroutine starts.
	injectCommit func() error

	// onBatchAssembled, when non-nil, is called with the coalesced batch size just
	// before the batch commits. It is control-flow-neutral and exists only so an
	// in-package test can assert a genuine multi-request batch was assembled rather
	// than inferring size from behavior. Set only by in-package tests.
	onBatchAssembled func(n int)
}

func newCoordinator(db *sql.DB) *coordinator {
	return &coordinator{
		db:       db,
		execCh:   make(chan *writeRequest, defaultMaxBatch),
		queryCh:  make(chan *writeRequest, defaultMaxBatch),
		quit:     make(chan struct{}),
		stopped:  make(chan struct{}),
		closed:   make(chan struct{}),
		failed:   make(chan struct{}),
		maxBatch: defaultMaxBatch,
	}
}

// start pins the sole write connection, asserts WAL+FULL durability on that
// exact connection, and launches the writer goroutine. It runs synchronously up
// to a verified connection so Open can surface a durability failure to its
// caller rather than deferring it to the first write.
func (c *coordinator) start(ctx context.Context) error {
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("pin reflow writer connection: %w", err)
	}
	if err := indexstore.ConfigureDurableConn(ctx, conn); err != nil {
		_ = conn.Close()
		return err
	}
	c.conn = conn
	c.baseCtx = context.Background()
	go c.run()
	return nil
}

// run is the single writer goroutine. It is the only goroutine that touches the
// pinned connection, which is what makes that connection the sole mutation
// authority. Select is fair across the query and exec channels so reads are not
// starved by a saturated write queue.
func (c *coordinator) run() {
	defer close(c.stopped)
	for {
		select {
		case <-c.quit:
			c.drain()
			return
		case req := <-c.queryCh:
			c.serveQuery(req)
		case req := <-c.execCh:
			if !c.serveBatch(req) {
				return
			}
		}
	}
}

// drain flushes everything still queued at graceful shutdown so Close joins and
// flushes admitted work rather than dropping it. Callers must not submit new
// work concurrently with Close.
func (c *coordinator) drain() {
	for {
		select {
		case req := <-c.queryCh:
			c.serveQuery(req)
		case req := <-c.execCh:
			if !c.serveBatch(req) {
				return
			}
		default:
			return
		}
	}
}

// serveBatch coalesces the first mutation with any immediately-available
// mutations into one transaction, then releases every waiter only after the
// batch has durably COMMITted. It returns false when the batch failed and the
// coordinator has entered its terminal failed state.
func (c *coordinator) serveBatch(first *writeRequest) bool {
	batch := make([]*writeRequest, 0, c.maxBatch)
	batch = append(batch, first)
	for len(batch) < c.maxBatch {
		select {
		case req := <-c.execCh:
			batch = append(batch, req)
		default:
			goto flush
		}
	}
flush:
	if c.onBatchAssembled != nil {
		c.onBatchAssembled(len(batch))
	}
	if err := c.commit(batch); err != nil {
		wrapped := fmt.Errorf("%w: %v", ErrWriterFailed, err)
		for _, req := range batch {
			req.done <- wrapped
		}
		c.fail(wrapped)
		return false
	}
	for _, req := range batch {
		req.done <- nil
	}
	return true
}

// serveQuery runs a read on the pinned connection. A read error is returned to
// its caller but does not fail the coordinator; only a write/commit failure is
// durability-fatal.
func (c *coordinator) serveQuery(req *writeRequest) {
	req.done <- req.query(c.baseCtx, c.conn)
}

// commit executes the batch inside a single transaction on the pinned
// connection. A statement error rolls the whole batch back (no subset commits)
// and is reported to every batched caller as a failure.
func (c *coordinator) commit(batch []*writeRequest) error {
	if c.injectCommit != nil {
		if err := c.injectCommit(); err != nil {
			return err
		}
	}
	tx, err := c.conn.BeginTx(c.baseCtx, nil)
	if err != nil {
		return err
	}
	for _, req := range batch {
		if _, err := tx.ExecContext(c.baseCtx, req.stmt, req.args...); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// fail records the terminal writer error once and closes the failed channel so
// every current and future waiter unblocks with the typed failure.
func (c *coordinator) fail(err error) {
	c.failOnce.Do(func() {
		c.mu.Lock()
		if c.failErr == nil {
			c.failErr = err
		}
		c.mu.Unlock()
		close(c.failed)
	})
}

func (c *coordinator) err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failErr
}

// exec submits one mutation and blocks until the batch that persisted it has
// COMMITted (the post-COMMIT barrier), or until the writer fails or the caller's
// context is cancelled. On writer failure it returns the typed error; queue
// admission and the wait both observe the failed channel so a failed writer can
// never deadlock a caller.
func (c *coordinator) exec(ctx context.Context, stmt string, args ...any) error {
	req := &writeRequest{kind: reqExec, stmt: stmt, args: args, done: make(chan error, 1)}
	select {
	case c.execCh <- req:
	case <-c.failed:
		return c.err()
	case <-c.closed:
		return ErrWriterClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	// A caller whose context cancels here returns ctx.Err(), but the request may
	// already be admitted and will still be committed under baseCtx — context
	// cancellation does not abort the write. That leaves a "durably written yet
	// caller observed an error" state, which is safe for D1 (the barrier only
	// guards the nil/success direction) and is reconciled on resume; the §3
	// recovery matrix governs it for non-idempotent collision modes.
	return c.awaitResult(ctx, req)
}

// awaitResult blocks for an admitted request's authoritative outcome. A published
// result on req.done always wins over the global failed/closed lifecycle signals
// (see resolve) so a durably-committed success is never masked into a failure by a
// later batch's failure or a graceful close. Caller-context cancellation stays a
// separate case: a cancelled caller may return ctx.Err() even though its write
// later commits under baseCtx.
func (c *coordinator) awaitResult(ctx context.Context, req *writeRequest) error {
	select {
	case err := <-req.done:
		return err
	case <-c.failed:
		return c.resolve(req, c.err())
	case <-c.closed:
		return c.resolve(req, ErrWriterClosed)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// resolve returns an admitted request's own published result in preference to a
// global lifecycle error (writer failure or graceful close). The writer goroutine
// is single-threaded and publishes every result of a batch before it can fail or
// close on any *later* batch, so once req.done carries a value it is the single
// authoritative outcome for that request; a concurrently-signalled failed/closed
// state must not mask a result that was already published — most importantly, a
// durably-committed success must never be rewritten into ErrWriterFailed. A
// request that was admitted but never serviced has no published result and
// correctly returns the lifecycle error. Caller-context cancellation is
// deliberately handled as a separate case by the callers (not here): a cancelled
// caller may return ctx.Err() even though its write later commits.
func (c *coordinator) resolve(req *writeRequest, lifecycleErr error) error {
	select {
	case err := <-req.done:
		return err
	default:
		return lifecycleErr
	}
}

// query submits a read serviced on the pinned connection and blocks for its
// result. Reads observe all mutations that completed before submission because
// every mutation's caller has already passed its post-COMMIT barrier.
func (c *coordinator) query(ctx context.Context, fn func(ctx context.Context, conn *sql.Conn) error) error {
	req := &writeRequest{kind: reqQuery, query: fn, done: make(chan error, 1)}
	select {
	case c.queryCh <- req:
	case <-c.failed:
		return c.err()
	case <-c.closed:
		return ErrWriterClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	return c.awaitResult(ctx, req)
}

// close stops the writer, flushes admitted work, releases the pinned connection,
// and closes the underlying database. It surfaces the terminal writer failure
// (if any) in preference to a close error, so a durability failure is never
// discarded. Callers must ensure no submissions race Close.
func (c *coordinator) close() error {
	c.closeOnce.Do(func() {
		close(c.quit)
		<-c.stopped
		// Release any late/racing submitter before tearing down the connection,
		// mirroring the failure path so no caller blocks on a departed writer.
		close(c.closed)
		var connErr error
		if c.conn != nil {
			connErr = c.conn.Close()
		}
		dbErr := c.db.Close()
		switch {
		case c.err() != nil:
			c.closeErr = c.err()
		case connErr != nil:
			c.closeErr = connErr
		default:
			c.closeErr = dbErr
		}
	})
	return c.closeErr
}
