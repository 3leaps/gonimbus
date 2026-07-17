package indexreader

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

// ErrVerificationProjectionTarget reports that a run-scoped verification
// projection path could not be created or retained safely under the
// authority-bound segment-set root.
var ErrVerificationProjectionTarget = errors.New("refusing unsafe verification projection target")

var validVerificationAttemptName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)

// SQLiteVerificationTargetOptions identifies one run-scoped parity-verification
// projection attempt under the authority-bound segment-set root.
type SQLiteVerificationTargetOptions struct {
	SegmentSetRoot string
	IndexSetID     string
	Authority      *indexcoord.Lease
	// AttemptName is the per-run attempt directory name (for example
	// run_<nano>). Each attempt must be new: a pre-existing attempt directory
	// is refused, never adopted.
	AttemptName string
}

// SQLiteVerificationTarget owns the run-scoped SQLite parity-verification
// database of a dual-format build. The projection carries no canonical trust
// and is never a reader-selectable consumer artifact; this type only
// guarantees that the projection is created exclusively on a fresh owner-only
// path under the bound segment-set root (prefix possession is not authority),
// and that the exact created file is retained through close so a pathname
// substitution refuses instead of mutating or blessing foreign state.
type SQLiteVerificationTarget struct {
	db        *sql.DB
	opts      SQLiteVerificationTargetOptions
	path      string
	bound     *os.File
	boundInfo os.FileInfo
	closed    bool
}

// OpenSQLiteVerificationTarget exclusively creates the attempt directory and
// database file for one verification projection, then opens the SQLite
// connection through the created pathname. Every intermediate is validated
// no-follow: a symlinked or non-directory segment-set root, `verification`
// intermediate, or database name refuses before any SQLite mutation, as does a
// pre-existing attempt directory or database file.
func OpenSQLiteVerificationTarget(ctx context.Context, opts SQLiteVerificationTargetOptions) (*SQLiteVerificationTarget, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	opts.SegmentSetRoot = strings.TrimSpace(opts.SegmentSetRoot)
	opts.IndexSetID = strings.TrimSpace(opts.IndexSetID)
	opts.AttemptName = strings.TrimSpace(opts.AttemptName)
	if opts.SegmentSetRoot == "" {
		return nil, fmt.Errorf("%w: segment set root is required", ErrVerificationProjectionTarget)
	}
	if opts.IndexSetID == "" {
		return nil, fmt.Errorf("%w: index set id is required", ErrVerificationProjectionTarget)
	}
	if !validVerificationAttemptName.MatchString(opts.AttemptName) {
		return nil, fmt.Errorf("%w: attempt name %q is not a plain directory name", ErrVerificationProjectionTarget, opts.AttemptName)
	}
	if err := opts.Authority.AssertHeldFor(opts.IndexSetID, opts.SegmentSetRoot); err != nil {
		return nil, fmt.Errorf("verification projection authority: %w", err)
	}

	// A first build may dispatch before the durable engine creates the segment
	// tree; create-if-absent like every other set-root writer, then validate
	// the final component no-follow.
	if _, err := os.Lstat(opts.SegmentSetRoot); os.IsNotExist(err) {
		if mkErr := os.MkdirAll(opts.SegmentSetRoot, 0o700); mkErr != nil {
			return nil, fmt.Errorf("%w: create segment set root: %v", ErrVerificationProjectionTarget, mkErr)
		}
	}
	if err := requireRealDirectoryNoFollow(opts.SegmentSetRoot, "segment set root"); err != nil {
		return nil, err
	}
	verificationDir := filepath.Join(opts.SegmentSetRoot, "verification")
	if err := ensureRealDirectoryNoFollow(verificationDir, "verification intermediate"); err != nil {
		return nil, err
	}
	attemptDir := filepath.Join(verificationDir, opts.AttemptName)
	// The attempt directory must be created by this call. EEXIST covers both a
	// concurrent honest writer (attempt names are per-run unique) and a planted
	// directory or symlink waiting to be adopted.
	if err := os.Mkdir(attemptDir, 0o700); err != nil {
		return nil, fmt.Errorf("%w: create verification attempt directory: %v", ErrVerificationProjectionTarget, err)
	}
	if err := requireRealDirectoryNoFollow(attemptDir, "verification attempt directory"); err != nil {
		return nil, err
	}

	dbPath := filepath.Join(attemptDir, "index.db")
	bound, err := openSQLiteWriteBinding(dbPath, true)
	if err != nil {
		return nil, fmt.Errorf("%w: create verification database exclusively: %v", ErrVerificationProjectionTarget, err)
	}
	boundInfo, err := bound.Stat()
	if err != nil || !boundInfo.Mode().IsRegular() {
		_ = bound.Close()
		return nil, errors.Join(fmt.Errorf("%w: retained verification database proof is not a regular file", ErrVerificationProjectionTarget), err)
	}

	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	if err != nil {
		_ = bound.Close()
		return nil, fmt.Errorf("open verification database: %w", err)
	}
	target := &SQLiteVerificationTarget{db: db, opts: opts, path: dbPath, bound: bound, boundInfo: boundInfo}
	if err := target.Check(); err != nil {
		return nil, errors.Join(err, target.Close())
	}
	return target, nil
}

// DB returns the mutable verification connection.
func (t *SQLiteVerificationTarget) DB() *sql.DB {
	if t == nil {
		return nil
	}
	return t.db
}

// Path returns the created verification database pathname.
func (t *SQLiteVerificationTarget) Path() string {
	if t == nil {
		return ""
	}
	return t.path
}

// Check revalidates authority and that the verification pathname still names
// the exclusively created database file.
func (t *SQLiteVerificationTarget) Check() error {
	if t == nil || t.closed || t.bound == nil || t.boundInfo == nil {
		return fmt.Errorf("verification projection target is closed")
	}
	if err := t.opts.Authority.AssertHeldFor(t.opts.IndexSetID, t.opts.SegmentSetRoot); err != nil {
		return fmt.Errorf("verification projection authority: %w", err)
	}
	boundInfo, err := t.bound.Stat()
	if err != nil || !boundInfo.Mode().IsRegular() || !os.SameFile(t.boundInfo, boundInfo) {
		return fmt.Errorf("%w: retained verification database binding changed", ErrVerificationProjectionTarget)
	}
	namedInfo, err := os.Lstat(t.path)
	if err != nil || namedInfo.Mode()&os.ModeSymlink != 0 || !namedInfo.Mode().IsRegular() || !os.SameFile(t.boundInfo, namedInfo) {
		return fmt.Errorf("%w: verification database pathname changed", ErrVerificationProjectionTarget)
	}
	return nil
}

// Close closes the verification connection after revalidating that the
// pathname still names the created file, and verifies live SQLite transaction
// sidecars are gone on a clean close. A failed Close means the projection is
// not bindable evidence: callers must not emit a successful dual-format
// terminal result. Close never deletes projection files.
func (t *SQLiteVerificationTarget) Close() error {
	if t == nil || t.closed {
		return nil
	}
	var errs []error
	checkErr := t.Check()
	if checkErr != nil {
		errs = append(errs, checkErr)
	}
	dbCloseOK := true
	if t.db != nil {
		if err := t.db.Close(); err != nil {
			errs = append(errs, err)
			dbCloseOK = false
		}
		t.db = nil
	}
	if dbCloseOK && checkErr == nil {
		if namedInfo, err := os.Lstat(t.path); err != nil || namedInfo.Mode()&os.ModeSymlink != 0 || !namedInfo.Mode().IsRegular() || !os.SameFile(t.boundInfo, namedInfo) {
			errs = append(errs, fmt.Errorf("%w: verification database pathname changed during close", ErrVerificationProjectionTarget))
		} else if err := indexstore.RejectLiveSQLiteTransactionSidecars(t.path); err != nil {
			errs = append(errs, fmt.Errorf("verification SQLite transaction state remains after close: %w", err))
		}
	}
	if t.bound != nil {
		errs = append(errs, t.bound.Close())
		t.bound = nil
	}
	t.closed = true
	return errors.Join(errs...)
}

// requireRealDirectoryNoFollow refuses paths whose final component is absent,
// a symlink, or anything other than a real directory.
func requireRealDirectoryNoFollow(path, label string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("%w: inspect %s: %v", ErrVerificationProjectionTarget, label, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%w: %s is not a real directory", ErrVerificationProjectionTarget, label)
	}
	return nil
}

// ensureRealDirectoryNoFollow creates the directory when absent and refuses a
// pre-existing symlink or non-directory in its place, before and after the
// create.
func ensureRealDirectoryNoFollow(path, label string) error {
	info, err := os.Lstat(path)
	switch {
	case err == nil:
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("%w: %s is not a real directory", ErrVerificationProjectionTarget, label)
		}
	case os.IsNotExist(err):
		if mkErr := os.Mkdir(path, 0o700); mkErr != nil && !os.IsExist(mkErr) {
			return fmt.Errorf("%w: create %s: %v", ErrVerificationProjectionTarget, label, mkErr)
		}
	default:
		return fmt.Errorf("%w: inspect %s: %v", ErrVerificationProjectionTarget, label, err)
	}
	return requireRealDirectoryNoFollow(path, label)
}
