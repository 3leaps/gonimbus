package indexreader

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/manifest"
)

// ErrCanonicalSQLiteAdoption reports that a writer found an existing canonical
// database whose trusted identity marker cannot authorize reuse.
var ErrCanonicalSQLiteAdoption = errors.New("refusing to adopt untrusted existing canonical SQLite database")

// SQLiteWriteTargetOptions identifies a canonical SQLite target before a
// writer creates a marker, opens the database, or performs migration.
type SQLiteWriteTargetOptions struct {
	Path           string
	IdentityPath   string
	SegmentSetRoot string
	IndexSetID     string
	Authority      *indexcoord.Lease
	MaxMarkerBytes int64
}

// SQLiteWriteTarget owns a mutable connection opened through the canonical
// index.db pathname while retaining a no-follow handle to the exact file that
// passed identity validation. Keeping SQLite on the canonical pathname gives
// every writer one lock/WAL namespace; the retained handle detects pathname
// substitution before mutation and across the target lifetime. The
// caller-held authority remains caller-owned.
type SQLiteWriteTarget struct {
	db                *sql.DB
	opts              SQLiteWriteTargetOptions
	bound             *os.File
	boundInfo         os.FileInfo
	created           bool
	opened            bool
	closed            bool
	quarantineResidue []string // set on live-clean close when residue remains (never deleted)
}

// SQLiteIdentityPublicationGuard retains the proof used to decide whether
// canonical identity.json may be published without adopting an unvalidated
// index.db. Existing databases stay bound by a no-follow handle; an absent
// database must remain absent at the publication boundary.
type SQLiteIdentityPublicationGuard struct {
	opts      SQLiteWriteTargetOptions
	bound     *os.File
	boundInfo os.FileInfo
	absent    bool
	closed    bool
}

// OpenSQLiteIdentityPublicationGuard validates the canonical SQLite sibling
// and retains that proof until PublishIdentity or Close. It does not create an
// absent database or publish metadata.
func OpenSQLiteIdentityPublicationGuard(ctx context.Context, opts SQLiteWriteTargetOptions) (*SQLiteIdentityPublicationGuard, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var err error
	opts, err = normalizeSQLiteWriteTargetOptions(opts)
	if err != nil {
		return nil, err
	}
	before, err := os.Lstat(opts.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return &SQLiteIdentityPublicationGuard{opts: opts, absent: true}, nil
		}
		return nil, fmt.Errorf("inspect canonical SQLite write target: %w", err)
	}
	if err := validateSQLiteWriteTargetWithoutBinding(ctx, opts, before); err != nil {
		return nil, err
	}
	bound, err := openSQLiteIdentityBinding(opts.Path)
	if err != nil {
		return nil, fmt.Errorf("%w: retain canonical index.db identity proof: %v", ErrCanonicalSQLiteAdoption, err)
	}
	boundInfo, err := bound.Stat()
	if err != nil || !boundInfo.Mode().IsRegular() || !os.SameFile(before, boundInfo) {
		_ = bound.Close()
		return nil, errors.Join(fmt.Errorf("%w: retained identity proof does not name validated index.db", ErrCanonicalSQLiteAdoption), err)
	}
	guard := &SQLiteIdentityPublicationGuard{opts: opts, bound: bound, boundInfo: boundInfo}
	if err := guard.Check(); err != nil {
		return nil, errors.Join(err, guard.Close())
	}
	return guard, nil
}

// Check revalidates the retained database/absence proof and caller authority.
func (g *SQLiteIdentityPublicationGuard) Check() error {
	if g == nil || g.closed {
		return fmt.Errorf("canonical SQLite identity publication guard is closed")
	}
	if err := g.opts.Authority.AssertHeldFor(g.opts.IndexSetID, g.opts.SegmentSetRoot); err != nil {
		return fmt.Errorf("canonical SQLite identity publication authority: %w", err)
	}
	if g.absent {
		if _, err := os.Lstat(g.opts.Path); err == nil || !os.IsNotExist(err) {
			return fmt.Errorf("%w: canonical index.db appeared before identity publication", ErrCanonicalSQLiteAdoption)
		}
		return nil
	}
	boundInfo, err := g.bound.Stat()
	if err != nil || !boundInfo.Mode().IsRegular() || !os.SameFile(g.boundInfo, boundInfo) {
		return errors.Join(fmt.Errorf("%w: retained index.db identity proof changed", ErrCanonicalSQLiteAdoption), err)
	}
	namedInfo, err := os.Lstat(g.opts.Path)
	if err != nil || namedInfo.Mode()&os.ModeSymlink != 0 || !namedInfo.Mode().IsRegular() || !os.SameFile(g.boundInfo, namedInfo) {
		return errors.Join(fmt.Errorf("%w: canonical index.db changed before identity publication", ErrCanonicalSQLiteAdoption), err)
	}
	return nil
}

// PublishIdentity durably replaces canonical identity.json through the bound
// directory without following an existing marker symlink.
func (g *SQLiteIdentityPublicationGuard) PublishIdentity(identity *indexstore.IndexSetIdentityResult) error {
	if g == nil || g.closed || identity == nil || identity.IndexSetID != g.opts.IndexSetID {
		return fmt.Errorf("identity publication requires the guarded index_set_id")
	}
	data := []byte(identity.CanonicalJSON + "\n")
	return publishCanonicalMetadata(filepath.Dir(g.opts.Path), canonicalIdentityName, data, g.Check)
}

// PublishManifest durably replaces the informational canonical manifest under
// the same retained proof and whole-set authority as identity publication.
func (g *SQLiteIdentityPublicationGuard) PublishManifest(doc *manifest.IndexManifest) error {
	if g == nil || g.closed {
		return fmt.Errorf("manifest publication requires an open canonical SQLite identity publication guard")
	}
	data, err := canonicalManifestData(doc)
	if err != nil {
		return err
	}
	return publishCanonicalMetadata(filepath.Dir(g.opts.Path), canonicalManifestName, data, g.Check)
}

// Close releases the retained proof. The caller-owned authority is unchanged.
func (g *SQLiteIdentityPublicationGuard) Close() error {
	if g == nil || g.closed {
		return nil
	}
	g.closed = true
	if g.bound == nil {
		return nil
	}
	err := g.bound.Close()
	g.bound = nil
	return err
}

// OpenSQLiteWriteTarget validates an existing canonical database or atomically
// reserves an absent target, binds that exact file, and opens the bound file for
// mutation. Callers must use DB rather than reopening Options.Path themselves.
func OpenSQLiteWriteTarget(ctx context.Context, opts SQLiteWriteTargetOptions) (*SQLiteWriteTarget, error) {
	return openSQLiteWriteTarget(ctx, opts, nil)
}

// openSQLiteWriteTarget accepts a package-private interposition hook so tests
// can exercise pathname replacement at the former check-to-open boundary.
func openSQLiteWriteTarget(ctx context.Context, opts SQLiteWriteTargetOptions, afterValidation func() error) (*SQLiteWriteTarget, error) {
	target, err := prepareSQLiteWriteTarget(ctx, opts, true)
	if err != nil {
		return nil, err
	}
	if afterValidation != nil {
		if err := afterValidation(); err != nil {
			return nil, errors.Join(fmt.Errorf("%w: canonical index.db interposition was denied: %v", ErrCanonicalSQLiteAdoption, err), target.abandon())
		}
	}
	if err := target.Check(); err != nil {
		return nil, errors.Join(err, target.abandon())
	}
	// From this point onward SQLite may create or recover canonical transaction
	// state. Abandon must never remove the database after an open attempt, even
	// when the driver later reports a close/checkpoint/configuration error.
	target.opened = true
	db, err := indexstore.OpenLocalMutableCanonical(ctx, target.opts.Path, target.bound, target.Check)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open bound canonical SQLite write target: %w", err), target.abandon())
	}
	target.db = db
	if err := target.Check(); err != nil {
		return nil, errors.Join(err, target.abandon())
	}
	return target, nil
}

// DB returns the bound mutable connection owned by the target.
func (t *SQLiteWriteTarget) DB() *sql.DB {
	if t == nil || t.closed {
		return nil
	}
	return t.db
}

// PublishIdentity publishes canonical identity metadata through the same
// retained database binding and authority that own this mutable connection.
func (t *SQLiteWriteTarget) PublishIdentity(identity *indexstore.IndexSetIdentityResult) error {
	if t == nil || t.closed || identity == nil || identity.IndexSetID != t.opts.IndexSetID {
		return fmt.Errorf("identity publication requires the bound index_set_id")
	}
	return publishCanonicalMetadata(filepath.Dir(t.opts.Path), canonicalIdentityName, []byte(identity.CanonicalJSON+"\n"), t.Check)
}

// PublishManifest publishes canonical manifest metadata without following an
// existing destination symlink.
func (t *SQLiteWriteTarget) PublishManifest(doc *manifest.IndexManifest) error {
	if t == nil || t.closed {
		return fmt.Errorf("manifest publication requires an open canonical SQLite write target")
	}
	data, err := canonicalManifestData(doc)
	if err != nil {
		return err
	}
	return publishCanonicalMetadata(filepath.Dir(t.opts.Path), canonicalManifestName, data, t.Check)
}

// Check revalidates the caller's authority and confirms that the retained
// no-follow binding and canonical pathname still name the selected file.
func (t *SQLiteWriteTarget) Check() error {
	if t == nil || t.closed || t.bound == nil || t.boundInfo == nil {
		return fmt.Errorf("canonical SQLite write target is closed")
	}
	if err := t.opts.Authority.AssertHeldFor(t.opts.IndexSetID, t.opts.SegmentSetRoot); err != nil {
		return fmt.Errorf("canonical SQLite write authority: %w", err)
	}
	boundInfo, err := t.bound.Stat()
	if err != nil || !boundInfo.Mode().IsRegular() || !os.SameFile(t.boundInfo, boundInfo) {
		return fmt.Errorf("%w: retained index.db binding changed before mutation", ErrCanonicalSQLiteAdoption)
	}
	namedInfo, err := os.Lstat(t.opts.Path)
	if err != nil || namedInfo.Mode()&os.ModeSymlink != 0 || !namedInfo.Mode().IsRegular() || !os.SameFile(t.boundInfo, namedInfo) {
		return fmt.Errorf("%w: canonical index.db pathname changed before mutation", ErrCanonicalSQLiteAdoption)
	}
	return nil
}

// Close closes the mutable connection and verifies that live SQLite
// transaction sidecars (WAL/SHM/journal) were checkpointed away. It never
// deletes transaction or quarantine files. Exact-epoch cleanup may leave
// discoverable quarantine residue after truncating the reserved inode; when
// live sidecars are clean that residue is recorded via QuarantineResidue and
// inventory without failing Close. Unrecovered quarantine residue blocks later
// canonical readers and mutable writers until a receipt-backed recovery
// transaction removes exact-bound captures. Whole-set authority alone never
// authorizes prefix-wide quarantine deletion. Live-sidecar checks run only when
// db close, authority, and main-file binding all succeed; failures skip those
// checks and never delete residue.
func (t *SQLiteWriteTarget) Close() error {
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
	authorityErr := t.opts.Authority.AssertHeldFor(t.opts.IndexSetID, t.opts.SegmentSetRoot)
	if authorityErr != nil {
		errs = append(errs, fmt.Errorf("canonical SQLite write authority at close: %w", authorityErr))
	}
	var bindingErr error
	if namedInfo, err := os.Lstat(t.opts.Path); err != nil || namedInfo.Mode()&os.ModeSymlink != 0 || !namedInfo.Mode().IsRegular() || !os.SameFile(t.boundInfo, namedInfo) {
		bindingErr = fmt.Errorf("%w: canonical index.db pathname changed during mutation", ErrCanonicalSQLiteAdoption)
		errs = append(errs, bindingErr)
	}
	// Only when the connection closed cleanly and authority plus main-file
	// binding still hold: verify live sidecars are gone and record any
	// quarantine residue. Never delete quarantine names here — not by prefix,
	// emptiness, or whole-set authority alone.
	if dbCloseOK && checkErr == nil && authorityErr == nil && bindingErr == nil {
		if err := indexstore.RejectLiveSQLiteTransactionSidecars(t.opts.Path); err != nil {
			errs = append(errs, fmt.Errorf("canonical SQLite transaction state remains after close: %w", err))
		}
		if residue, err := indexstore.QuarantineSQLiteTransactionResidue(t.opts.Path); err != nil {
			errs = append(errs, fmt.Errorf("inspect SQLite quarantine residue after close: %w", err))
		} else {
			t.quarantineResidue = append([]string(nil), residue...)
		}
	}
	if t.bound != nil {
		errs = append(errs, t.bound.Close())
		t.bound = nil
	}
	t.closed = true
	return errors.Join(errs...)
}

// QuarantineResidue returns discoverable quarantine-prefix names observed at
// the last live-clean Close, without implying any entry was deleted.
func (t *SQLiteWriteTarget) QuarantineResidue() []string {
	if t == nil || len(t.quarantineResidue) == 0 {
		return nil
	}
	return append([]string(nil), t.quarantineResidue...)
}

// ValidateSQLiteWriteTarget is the non-mutating companion used by workflows
// such as durable-only publication that write canonical identity metadata but
// do not open or create index.db. An absent database remains absent.
func ValidateSQLiteWriteTarget(ctx context.Context, opts SQLiteWriteTargetOptions) error {
	guard, err := OpenSQLiteIdentityPublicationGuard(ctx, opts)
	if err != nil {
		return err
	}
	return guard.Close()
}

func prepareSQLiteWriteTarget(ctx context.Context, opts SQLiteWriteTargetOptions, create bool) (*SQLiteWriteTarget, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	var err error
	opts, err = normalizeSQLiteWriteTargetOptions(opts)
	if err != nil {
		return nil, err
	}

	before, statErr := os.Lstat(opts.Path)
	created := false
	var bound *os.File
	if statErr != nil {
		if !os.IsNotExist(statErr) {
			return nil, fmt.Errorf("inspect canonical SQLite write target: %w", statErr)
		}
		if !create {
			return nil, nil
		}
		if err := os.MkdirAll(filepath.Dir(opts.Path), 0o700); err != nil {
			return nil, fmt.Errorf("create canonical SQLite target directory: %w", err)
		}
		file, err := openSQLiteWriteBinding(opts.Path, true)
		if err != nil {
			if os.IsExist(err) {
				return nil, fmt.Errorf("%w: canonical index.db appeared before atomic reservation", ErrCanonicalSQLiteAdoption)
			}
			return nil, fmt.Errorf("reserve canonical SQLite write target: %w", err)
		}
		before, err = file.Stat()
		if err != nil {
			_ = file.Close()
			_ = os.Remove(opts.Path)
			return nil, fmt.Errorf("inspect reserved canonical SQLite target: %w", err)
		}
		bound = file
		created = true
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: existing index.db is not a regular file", ErrCanonicalSQLiteAdoption)
	}
	if !created {
		// Reject missing/invalid identity and unsafe database state before
		// retaining a writable handle.
		if err := validateSQLiteWriteTargetWithoutBinding(ctx, opts, before); err != nil {
			return nil, err
		}
		file, err := openSQLiteWriteBinding(opts.Path, false)
		if err != nil {
			return nil, fmt.Errorf("%w: bind canonical index.db: %v", ErrCanonicalSQLiteAdoption, err)
		}
		bound = file
	}
	boundInfo, err := bound.Stat()
	if err != nil || !boundInfo.Mode().IsRegular() || !os.SameFile(before, boundInfo) {
		_ = bound.Close()
		if created {
			_ = os.Remove(opts.Path)
		}
		return nil, errors.Join(fmt.Errorf("%w: retained binding does not name the validated index.db", ErrCanonicalSQLiteAdoption), err)
	}
	target := &SQLiteWriteTarget{opts: opts, bound: bound, boundInfo: boundInfo, created: created}
	if err := target.Check(); err != nil {
		return nil, errors.Join(err, target.abandon())
	}
	return target, nil
}

func normalizeSQLiteWriteTargetOptions(opts SQLiteWriteTargetOptions) (SQLiteWriteTargetOptions, error) {
	opts.Path = strings.TrimSpace(opts.Path)
	opts.IdentityPath = strings.TrimSpace(opts.IdentityPath)
	opts.SegmentSetRoot = strings.TrimSpace(opts.SegmentSetRoot)
	opts.IndexSetID = strings.TrimSpace(opts.IndexSetID)
	if opts.Path == "" || opts.SegmentSetRoot == "" || !isFullIndexSetID(opts.IndexSetID) {
		return opts, fmt.Errorf("canonical SQLite write target path, segment root, and full index_set_id are required")
	}
	if opts.Authority == nil {
		return opts, fmt.Errorf("canonical SQLite write target authority is required")
	}
	if err := opts.Authority.AssertHeldFor(opts.IndexSetID, opts.SegmentSetRoot); err != nil {
		return opts, fmt.Errorf("validate canonical SQLite write authority: %w", err)
	}
	if opts.IdentityPath == "" {
		opts.IdentityPath = filepath.Join(filepath.Dir(opts.Path), "identity.json")
	}
	pathAbs, err := filepath.Abs(filepath.Clean(opts.Path))
	if err != nil {
		return opts, fmt.Errorf("resolve canonical SQLite write target: %w", err)
	}
	identityAbs, err := filepath.Abs(filepath.Clean(opts.IdentityPath))
	if err != nil {
		return opts, fmt.Errorf("resolve canonical SQLite identity path: %w", err)
	}
	expectedIdentity := filepath.Join(filepath.Dir(pathAbs), canonicalIdentityName)
	if identityAbs != expectedIdentity {
		return opts, fmt.Errorf("canonical SQLite identity path must be the exact identity.json sibling of index.db")
	}
	opts.Path = pathAbs
	opts.IdentityPath = identityAbs
	return opts, nil
}

func validateSQLiteWriteTargetWithoutBinding(ctx context.Context, opts SQLiteWriteTargetOptions, before os.FileInfo) error {
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return fmt.Errorf("%w: existing index.db is not a regular file", ErrCanonicalSQLiteAdoption)
	}
	identity, err := ReadLocalIdentityFile(opts.IdentityPath, opts.MaxMarkerBytes)
	if err != nil {
		return fmt.Errorf("%w: existing index.db requires a valid identity.json: %v", ErrCanonicalSQLiteAdoption, err)
	}
	if identity.IndexSetID != opts.IndexSetID {
		return fmt.Errorf("%w: identity index_set_id %q does not match requested %q", ErrCanonicalSQLiteAdoption, identity.IndexSetID, opts.IndexSetID)
	}
	if !identityMatchesCanonicalDir(identity.IndexSetID, filepath.Base(filepath.Dir(opts.Path))) {
		return fmt.Errorf("%w: identity.json does not match the canonical directory", ErrCanonicalSQLiteAdoption)
	}
	if err := opts.Authority.AssertHeldFor(opts.IndexSetID, opts.SegmentSetRoot); err != nil {
		return fmt.Errorf("validate canonical SQLite write authority after identity read: %w", err)
	}
	after, err := os.Lstat(opts.Path)
	if err != nil || after.Mode()&os.ModeSymlink != 0 || !after.Mode().IsRegular() || !os.SameFile(before, after) {
		return fmt.Errorf("%w: index.db binding changed during identity validation", ErrCanonicalSQLiteAdoption)
	}
	if err := indexstore.RejectSQLiteTransactionSidecars(opts.Path); err != nil {
		return fmt.Errorf("%w: %v", ErrCanonicalSQLiteAdoption, err)
	}
	db, err := indexstore.OpenLocalReadOnly(ctx, opts.Path)
	if err != nil {
		return fmt.Errorf("%w: open existing index.db read-only: %v", ErrCanonicalSQLiteAdoption, err)
	}
	ids, queryErr := readSQLiteWriteTargetIDs(ctx, db)
	closeErr := db.Close()
	postErr := indexstore.RejectSQLiteTransactionSidecars(opts.Path)
	authorityErr := opts.Authority.AssertHeldFor(opts.IndexSetID, opts.SegmentSetRoot)
	finalInfo, finalErr := os.Lstat(opts.Path)
	if finalErr == nil && (finalInfo.Mode()&os.ModeSymlink != 0 || !finalInfo.Mode().IsRegular() || !os.SameFile(before, finalInfo)) {
		finalErr = fmt.Errorf("index.db binding changed during database validation")
	}
	if err := errors.Join(queryErr, closeErr, postErr, authorityErr, finalErr); err != nil {
		return fmt.Errorf("%w: validate existing database identity: %v", ErrCanonicalSQLiteAdoption, err)
	}
	if len(ids) != 1 || ids[0] != opts.IndexSetID {
		return fmt.Errorf("%w: existing database contains %d matching-scope rows and does not contain exactly the requested index_set_id", ErrCanonicalSQLiteAdoption, len(ids))
	}
	return nil
}

func readSQLiteWriteTargetIDs(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT index_set_id FROM index_sets ORDER BY index_set_id LIMIT 2`)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return ids, errors.Join(err, rows.Close())
		}
		ids = append(ids, id)
	}
	return ids, errors.Join(rows.Err(), rows.Close())
}

func (t *SQLiteWriteTarget) abandon() error {
	if t == nil {
		return nil
	}
	var errs []error
	if t.db != nil {
		if err := t.db.Close(); err != nil {
			errs = append(errs, err)
		}
		t.db = nil
	}
	if t.created && !t.opened && t.boundInfo != nil {
		if info, err := os.Lstat(t.opts.Path); err == nil && info.Mode().IsRegular() && os.SameFile(t.boundInfo, info) {
			if err := os.Remove(t.opts.Path); err != nil && !os.IsNotExist(err) {
				errs = append(errs, err)
			}
		}
	}
	if t.bound != nil {
		errs = append(errs, t.bound.Close())
		t.bound = nil
	}
	t.closed = true
	return errors.Join(errs...)
}
