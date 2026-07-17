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

// verificationDirBinding retains the identity of one authority-bearing
// directory component so Check/Close can re-attest that the named path still
// resolves to the exact directory that was validated at creation time. The
// held handle pins the inode against reuse for the target's lifetime.
type verificationDirBinding struct {
	path   string
	label  string
	handle *os.File
	info   os.FileInfo
}

// SQLiteVerificationTarget owns the run-scoped SQLite parity-verification
// database of a dual-format build. The projection carries no canonical trust
// and is never a reader-selectable consumer artifact; this type guarantees
// that the projection is created exclusively on a fresh owner-only path under
// the bound segment-set root (prefix possession is not authority), that the
// SQLite connection is bound to the exclusively created file through the
// attested-VFS open, and that every authority-bearing directory component
// (segment-set root, verification intermediate, attempt directory) plus the
// exact database binding is re-attested at every mutation boundary and through
// close — so a substituted parent namespace refuses instead of blessing a
// projection through a foreign tree, even when the final pathname still names
// the originally created inode.
type SQLiteVerificationTarget struct {
	db        *sql.DB
	opts      SQLiteVerificationTargetOptions
	path      string
	bound     *os.File
	boundInfo os.FileInfo
	dirs      []verificationDirBinding
	closed    bool
}

// OpenSQLiteVerificationTarget exclusively creates the attempt directory and
// database file for one verification projection, then opens the SQLite
// connection bound to the created file with Check as the before-mutation
// attestation. Every intermediate is validated no-follow and retained: a
// symlinked or non-directory segment-set root, `verification` intermediate, or
// database name refuses before any SQLite mutation, as does a pre-existing
// attempt directory or database file.
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

	target := &SQLiteVerificationTarget{opts: opts}
	fail := func(err error) (*SQLiteVerificationTarget, error) {
		target.releaseHandles()
		return nil, err
	}

	// A first build may dispatch before the durable engine creates the segment
	// tree; create-if-absent like every other set-root writer, then validate
	// and retain the final component no-follow.
	if _, err := os.Lstat(opts.SegmentSetRoot); os.IsNotExist(err) {
		if mkErr := os.MkdirAll(opts.SegmentSetRoot, 0o700); mkErr != nil {
			return fail(fmt.Errorf("%w: create segment set root: %v", ErrVerificationProjectionTarget, mkErr))
		}
	}
	rootBinding, err := bindRealDirectoryNoFollow(opts.SegmentSetRoot, "segment set root")
	if err != nil {
		return fail(err)
	}
	target.dirs = append(target.dirs, rootBinding)

	verificationDir := filepath.Join(opts.SegmentSetRoot, "verification")
	if _, err := os.Lstat(verificationDir); os.IsNotExist(err) {
		// Mkdir cannot traverse or replace a symlink at the final component; a
		// racing plant surfaces as EEXIST-then-binding-failure below.
		if mkErr := os.Mkdir(verificationDir, 0o700); mkErr != nil && !os.IsExist(mkErr) {
			return fail(fmt.Errorf("%w: create verification intermediate: %v", ErrVerificationProjectionTarget, mkErr))
		}
	}
	verificationBinding, err := bindRealDirectoryNoFollow(verificationDir, "verification intermediate")
	if err != nil {
		return fail(err)
	}
	target.dirs = append(target.dirs, verificationBinding)

	attemptDir := filepath.Join(verificationDir, opts.AttemptName)
	// The attempt directory must be created by this call. EEXIST covers both a
	// concurrent honest writer (attempt names are per-run unique) and a planted
	// directory or symlink waiting to be adopted.
	if err := os.Mkdir(attemptDir, 0o700); err != nil {
		return fail(fmt.Errorf("%w: create verification attempt directory: %v", ErrVerificationProjectionTarget, err))
	}
	attemptBinding, err := bindRealDirectoryNoFollow(attemptDir, "verification attempt directory")
	if err != nil {
		return fail(err)
	}
	target.dirs = append(target.dirs, attemptBinding)

	dbPath := filepath.Join(attemptDir, "index.db")
	bound, err := openSQLiteWriteBinding(dbPath, true)
	if err != nil {
		return fail(fmt.Errorf("%w: create verification database exclusively: %v", ErrVerificationProjectionTarget, err))
	}
	target.bound = bound
	boundInfo, err := bound.Stat()
	if err != nil || !boundInfo.Mode().IsRegular() {
		return fail(errors.Join(fmt.Errorf("%w: retained verification database proof is not a regular file", ErrVerificationProjectionTarget), err))
	}
	target.boundInfo = boundInfo
	target.path = dbPath
	if err := target.Check(); err != nil {
		return fail(err)
	}

	// Bind the connection to the exclusively created file through the attested
	// VFS: SQLite's exact main-file handle is matched against bound, sidecars
	// are reserved through a retained no-follow directory binding, and Check
	// (authority + full parent-namespace + database identity) runs before
	// mutation boundaries — never a generic pathname open.
	db, err := indexstore.OpenLocalMutableCanonical(ctx, dbPath, bound, target.Check)
	if err != nil {
		return fail(fmt.Errorf("open bound verification database: %w", err))
	}
	target.db = db
	if err := target.Check(); err != nil {
		return nil, errors.Join(err, target.Close())
	}
	return target, nil
}

// DB returns the bound mutable verification connection.
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

// AttemptName returns the per-run attempt identifier for this projection.
func (t *SQLiteVerificationTarget) AttemptName() string {
	if t == nil {
		return ""
	}
	return t.opts.AttemptName
}

// Locator returns the stable set-relative locator of the projection database
// (never a host-absolute path).
func (t *SQLiteVerificationTarget) Locator() string {
	if t == nil {
		return ""
	}
	return "verification/" + t.opts.AttemptName + "/index.db"
}

// Check revalidates caller authority, every retained authority-bearing
// directory component, and that the verification pathname still names the
// exclusively created database file. A rename, symlink/replacement, or
// hardlink alias through a foreign parent refuses even when the final inode is
// unchanged.
func (t *SQLiteVerificationTarget) Check() error {
	if t == nil || t.closed || t.bound == nil || t.boundInfo == nil {
		return fmt.Errorf("verification projection target is closed")
	}
	if err := t.opts.Authority.AssertHeldFor(t.opts.IndexSetID, t.opts.SegmentSetRoot); err != nil {
		return fmt.Errorf("verification projection authority: %w", err)
	}
	for _, d := range t.dirs {
		info, err := os.Lstat(d.path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || !os.SameFile(d.info, info) {
			return fmt.Errorf("%w: %s identity changed", ErrVerificationProjectionTarget, d.label)
		}
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

// Close closes the verification connection after re-attesting the retained
// parent namespace and database binding, and verifies live SQLite transaction
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
		postClose := func() error {
			for _, d := range t.dirs {
				info, err := os.Lstat(d.path)
				if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || !os.SameFile(d.info, info) {
					return fmt.Errorf("%w: %s identity changed during close", ErrVerificationProjectionTarget, d.label)
				}
			}
			if namedInfo, err := os.Lstat(t.path); err != nil || namedInfo.Mode()&os.ModeSymlink != 0 || !namedInfo.Mode().IsRegular() || !os.SameFile(t.boundInfo, namedInfo) {
				return fmt.Errorf("%w: verification database pathname changed during close", ErrVerificationProjectionTarget)
			}
			return nil
		}
		if err := postClose(); err != nil {
			errs = append(errs, err)
		} else if err := indexstore.RejectLiveSQLiteTransactionSidecars(t.path); err != nil {
			errs = append(errs, fmt.Errorf("verification SQLite transaction state remains after close: %w", err))
		}
	}
	if t.bound != nil {
		errs = append(errs, t.bound.Close())
		t.bound = nil
	}
	t.releaseHandles()
	t.closed = true
	return errors.Join(errs...)
}

func (t *SQLiteVerificationTarget) releaseHandles() {
	for i := range t.dirs {
		if t.dirs[i].handle != nil {
			_ = t.dirs[i].handle.Close()
			t.dirs[i].handle = nil
		}
	}
}

// bindRealDirectoryNoFollow refuses a path whose final component is absent, a
// symlink, or anything other than a real directory, then retains an open
// handle whose identity matches the validated component (a symlink swapped in
// between the validation and the open is caught by the identity comparison).
func bindRealDirectoryNoFollow(path, label string) (verificationDirBinding, error) {
	lstatInfo, err := os.Lstat(path)
	if err != nil {
		return verificationDirBinding{}, fmt.Errorf("%w: inspect %s: %v", ErrVerificationProjectionTarget, label, err)
	}
	if lstatInfo.Mode()&os.ModeSymlink != 0 || !lstatInfo.IsDir() {
		return verificationDirBinding{}, fmt.Errorf("%w: %s is not a real directory", ErrVerificationProjectionTarget, label)
	}
	handle, err := os.Open(path) // #nosec G304 -- path was validated no-follow above and identity is re-attested below
	if err != nil {
		return verificationDirBinding{}, fmt.Errorf("%w: retain %s: %v", ErrVerificationProjectionTarget, label, err)
	}
	info, err := handle.Stat()
	if err != nil || !info.IsDir() || !os.SameFile(lstatInfo, info) {
		_ = handle.Close()
		return verificationDirBinding{}, errors.Join(fmt.Errorf("%w: retained %s does not name the validated directory", ErrVerificationProjectionTarget, label), err)
	}
	return verificationDirBinding{path: path, label: label, handle: handle, info: info}, nil
}
