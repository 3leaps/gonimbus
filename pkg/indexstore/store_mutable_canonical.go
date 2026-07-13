package indexstore

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"

	sqlite "modernc.org/sqlite"
)

// exactEpochRemovalHooks are test/injection points around capture and
// exact-object destruction of reserved SQLite transaction artifacts.
type exactEpochRemovalHooks struct {
	afterCapture func(path, quarantine string) error
	afterAttest  func(path, quarantine string) error
}

type mutableCanonicalConnector struct {
	driver         *sqlite.Driver
	dsn            string
	path           string
	bound          *os.File
	beforeMutation func() error
	sidecars       canonicalSQLiteSidecarExpectation
	hooks          mutableCanonicalOpenHooks
	lastState      *canonicalSQLiteVFSState
}

func newMutableCanonicalConnector(dsn, path string, bound *os.File, beforeMutation func() error, hooks mutableCanonicalOpenHooks) *mutableCanonicalConnector {
	return &mutableCanonicalConnector{
		driver:         &sqlite.Driver{},
		dsn:            dsn,
		path:           path,
		bound:          bound,
		beforeMutation: beforeMutation,
		sidecars:       canonicalSQLiteSidecarsMustBeAbsent,
		hooks:          hooks,
	}
}

func (c *mutableCanonicalConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.hooks.beforeDriverOpen != nil {
		if err := c.hooks.beforeDriverOpen(); err != nil {
			return nil, fmt.Errorf("before canonical SQLite driver open: %w", err)
		}
	}
	guard, err := registerCanonicalSQLiteVFS(c.path, c.bound, c.beforeMutation, c.sidecars, c.hooks)
	if err != nil {
		return nil, err
	}
	c.lastState = guard.state
	if c.hooks.afterVFSRegistrationBeforeDriverOpen != nil {
		if err := c.hooks.afterVFSRegistrationBeforeDriverOpen(); err != nil {
			return nil, errors.Join(fmt.Errorf("after canonical SQLite VFS registration: %w", err), guard.Close())
		}
	}
	conn, err := c.driver.Open(withSQLiteVFS(c.dsn, guard.name))
	if err != nil {
		return nil, errors.Join(err, guard.state.failure(), guard.Close())
	}
	if c.hooks.afterAuthorityCheckBeforeSQLite != nil {
		if err := c.hooks.afterAuthorityCheckBeforeSQLite(); err != nil {
			return nil, errors.Join(fmt.Errorf("after canonical SQLite authority check: %w", err), conn.Close(), guard.Close())
		}
	}
	return &mutableCanonicalConn{Conn: conn, guard: guard}, nil
}

func (c *mutableCanonicalConnector) Driver() driver.Driver { return c.driver }

func (c *mutableCanonicalConnector) failure() error {
	if c == nil || c.lastState == nil {
		return nil
	}
	return c.lastState.failure()
}

// mutableCanonicalConn keeps the per-connection VFS registered until SQLite
// has closed every main, journal, WAL, and shared-memory handle.
type mutableCanonicalConn struct {
	driver.Conn
	guard *canonicalSQLiteVFS
}

func (c *mutableCanonicalConn) Close() error {
	if c == nil {
		return nil
	}
	var errs []error
	if c.Conn != nil {
		errs = append(errs, c.Conn.Close())
		c.Conn = nil
	}
	if c.guard != nil {
		errs = append(errs, c.guard.Close())
		c.guard = nil
	}
	return errors.Join(errs...)
}

func (c *mutableCanonicalConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if v, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return v.PrepareContext(ctx, query)
	}
	return c.Prepare(query)
}

func (c *mutableCanonicalConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if v, ok := c.Conn.(driver.ConnBeginTx); ok {
		return v.BeginTx(ctx, opts)
	}
	return nil, driver.ErrSkip
}

func (c *mutableCanonicalConn) Ping(ctx context.Context) error {
	if v, ok := c.Conn.(driver.Pinger); ok {
		return v.Ping(ctx)
	}
	return driver.ErrSkip
}

func (c *mutableCanonicalConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if v, ok := c.Conn.(driver.ExecerContext); ok {
		return v.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (c *mutableCanonicalConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if v, ok := c.Conn.(driver.QueryerContext); ok {
		return v.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (c *mutableCanonicalConn) CheckNamedValue(value *driver.NamedValue) error {
	if v, ok := c.Conn.(driver.NamedValueChecker); ok {
		return v.CheckNamedValue(value)
	}
	return driver.ErrSkip
}

func (c *mutableCanonicalConn) ResetSession(ctx context.Context) error {
	if v, ok := c.Conn.(driver.SessionResetter); ok {
		return v.ResetSession(ctx)
	}
	return nil
}

func (c *mutableCanonicalConn) IsValid() bool {
	if v, ok := c.Conn.(driver.Validator); ok {
		return v.IsValid()
	}
	return true
}
