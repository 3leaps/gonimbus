package indexstore

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpenLocalMutableCanonicalRefusesSidecarInsertedBeforeDelegatedOpen(t *testing.T) {
	tests := []struct {
		name    string
		suffix  string
		trigger func(context.Context, *sql.DB) error
	}{
		{
			name:   "wal",
			suffix: "-wal",
			trigger: func(ctx context.Context, db *sql.DB) error {
				_, err := db.ExecContext(ctx, `UPDATE marker SET value = 'must-not-commit'`)
				return err
			},
		},
		{
			name:   "rollback-journal",
			suffix: "-journal",
			trigger: func(ctx context.Context, db *sql.DB) error {
				if _, err := db.ExecContext(ctx, `PRAGMA journal_mode=DELETE`); err != nil {
					return err
				}
				_, err := db.ExecContext(ctx, `UPDATE marker SET value = 'must-not-commit'`)
				return err
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testCanonicalSQLiteInsertedSidecarRefusal(t, tc.suffix, tc.trigger)
		})
	}
}

func TestOpenLocalMutableCanonicalRefusesSHMInsertedBeforeDelegatedMap(t *testing.T) {
	testCanonicalSQLiteInsertedSidecarRefusal(t, "-shm", func(ctx context.Context, db *sql.DB) error {
		_, err := db.ExecContext(ctx, `UPDATE marker SET value = 'must-not-commit'`)
		return err
	})
}

func TestOpenLocalMutableCanonicalOwnsWALCreateEpoch(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "index.db")
	seedMutableCanonicalTestDB(t, canonical, "canonical")
	bound, err := os.OpenFile(canonical, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = bound.Close() }()

	var reservedWAL string
	hooks := mutableCanonicalOpenHooks{beforeSidecarReservation: func(path string) error {
		if strings.HasSuffix(path, "-wal") {
			reservedWAL = filepath.Clean(path)
		}
		return nil
	}}
	ctx := context.Background()
	db, err := openLocalMutableCanonical(ctx, canonical, bound, func() error { return nil }, hooks)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	resolved, err := resolveCanonicalSQLitePath(canonical)
	require.NoError(t, err)
	require.Equal(t, filepath.Clean(resolved+"-wal"), reservedWAL, "WAL create must reserve the connection-owned main path + -wal")
	walInfo, err := os.Lstat(reservedWAL)
	require.NoError(t, err)
	require.True(t, walInfo.Mode().IsRegular())
	// modernc may fchmod a successfully opened WAL to the main DB mode after
	// open; ownership is proven by the exclusive reservation path and the
	// independent pre-reservation refusal fixtures, not by final mode bits.
	_, err = db.ExecContext(ctx, `UPDATE marker SET value = 'owned-wal'`)
	require.NoError(t, err)
}

func TestSQLiteTransactionSidecarsReportsUnprovenEmptyQuarantine(t *testing.T) {
	// Inventory must be read-only: a quarantine-prefix name with no receipt or
	// binding is still reported as blocking residue and must not be deleted.
	dir := t.TempDir()
	canonical := filepath.Join(dir, "index.db")
	seedMutableCanonicalTestDB(t, canonical, "canonical")
	unproven := filepath.Join(dir, canonicalSQLiteQuarantinePrefix+"unproven-empty")
	require.NoError(t, os.WriteFile(unproven, nil, 0o600))
	before := captureReservationFileState(t, unproven)

	found, err := SQLiteTransactionSidecars(canonical)
	require.NoError(t, err)
	require.Contains(t, found, filepath.Base(unproven), "inventory must report unproven quarantine residue")
	requireReservationFileState(t, unproven, before)
	// A second inventory call must still see the same unproven entry.
	foundAgain, err := SQLiteTransactionSidecars(canonical)
	require.NoError(t, err)
	require.Contains(t, foundAgain, filepath.Base(unproven))
	requireReservationFileState(t, unproven, before)
}

func TestOpenLocalMutableCanonicalRetainsEmptyQuarantineAfterExactTruncate(t *testing.T) {
	// After attested capture, content is destroyed only via fd truncate. The
	// quarantine directory entry remains discoverable blocking residue; no
	// pathname unlink is performed.
	dir := t.TempDir()
	canonical := filepath.Join(dir, "index.db")
	seedMutableCanonicalTestDB(t, canonical, "canonical")
	bound, err := os.OpenFile(canonical, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = bound.Close() }()

	resolved, err := resolveCanonicalSQLitePath(canonical)
	require.NoError(t, err)
	var quarantinePath string
	hooks := mutableCanonicalOpenHooks{afterExactEpochAttest: func(path, quarantine string) error {
		if strings.HasSuffix(path, "-wal") && quarantinePath == "" {
			quarantinePath = quarantine
		}
		return nil
	}}

	ctx := context.Background()
	db, err := openLocalMutableCanonical(ctx, canonical, bound, func() error { return nil }, hooks)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE marker SET value = 'force-wal'`)
	require.NoError(t, err)
	_, _ = db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	require.NoError(t, db.Close())

	require.NotEmpty(t, quarantinePath, "fixture did not observe WAL capture")
	info, err := os.Lstat(quarantinePath)
	require.NoError(t, err, "empty capture must remain as discoverable residue")
	require.True(t, info.Mode().IsRegular())
	require.Equal(t, int64(0), info.Size(), "reserved epoch content must be truncated via open fd")
	found, err := SQLiteTransactionSidecars(resolved)
	require.NoError(t, err)
	require.Contains(t, found, filepath.Base(quarantinePath))

	// Ordinary re-open must refuse while unreclaimed quarantine residue exists.
	bound2, err := os.OpenFile(canonical, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = bound2.Close() }()
	_, err = openLocalMutableCanonical(ctx, canonical, bound2, func() error { return nil }, mutableCanonicalOpenHooks{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "sidecar")
}

func TestOpenLocalMutableCanonicalRefusesExactEpochRemovalSubstitution(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "index.db")
	seedMutableCanonicalTestDB(t, canonical, "canonical")
	bound, err := os.OpenFile(canonical, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = bound.Close() }()

	resolved, err := resolveCanonicalSQLitePath(canonical)
	require.NoError(t, err)
	target := resolved + "-wal"
	payload := []byte("substituted transaction epoch before exact removal")
	var injected reservationFileState
	injectedOnce := false
	hooks := mutableCanonicalOpenHooks{beforeExactEpochRemoval: func(path string) error {
		if filepath.Clean(path) != filepath.Clean(target) || injectedOnce {
			return nil
		}
		// Replace the live name after the bound epoch is known but before the
		// atomic capture/rename. Create the substitute while the live epoch
		// still exists so it cannot recycle that inode number (Linux tmpfs
		// reuse would otherwise make the plant look exact-bound). The
		// substitute must be preserved, not deleted.
		plant := target + ".plant-mismatch"
		if err := writeReservationPayload(plant, payload, 0o640, time.Unix(1_700_000_100, 456_000_000)); err != nil {
			return err
		}
		if err := os.Rename(plant, target); err != nil {
			_ = os.Remove(plant)
			return err
		}
		injected = captureReservationFileState(t, target)
		injectedOnce = true
		return nil
	}}

	ctx := context.Background()
	db, err := openLocalMutableCanonical(ctx, canonical, bound, func() error { return nil }, hooks)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE marker SET value = 'force-wal'`)
	require.NoError(t, err)
	// Checkpoint + truncate should drive xDelete of the owned WAL epoch.
	_, _ = db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	closeErr := db.Close()
	require.True(t, injectedOnce, "fixture did not reach exact-epoch removal; close=%v", closeErr)
	requireReservationFileState(t, target, injected)
}

func TestOpenLocalMutableCanonicalRefusesExactEpochRestoreOverLiveName(t *testing.T) {
	// After capture of a mismatched epoch, recreating the live name must not be
	// overwritten by restore: both the capture and the new live epoch remain.
	dir := t.TempDir()
	canonical := filepath.Join(dir, "index.db")
	seedMutableCanonicalTestDB(t, canonical, "canonical")
	bound, err := os.OpenFile(canonical, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = bound.Close() }()

	resolved, err := resolveCanonicalSQLitePath(canonical)
	require.NoError(t, err)
	target := resolved + "-wal"
	capturePayload := []byte("captured mismatched transaction epoch")
	livePayload := []byte("recreated live transaction epoch after capture")
	var captureInjected, liveInjected reservationFileState
	var quarantinePath string
	captured := false
	hooks := mutableCanonicalOpenHooks{
		beforeExactEpochRemoval: func(path string) error {
			if filepath.Clean(path) != filepath.Clean(target) {
				return nil
			}
			// Create the substitute while the live epoch still occupies the
			// name so the plant must receive a distinct inode. Remove+recreate
			// can recycle the just-freed inode number on Linux (especially
			// tmpfs), which would falsely take the exact-epoch match path and
			// truncate the plant — defeating this mismatch fixture.
			plant := target + ".plant-mismatch"
			if err := writeReservationPayload(plant, capturePayload, 0o640, time.Unix(1_700_000_200, 0)); err != nil {
				return err
			}
			if err := os.Rename(plant, target); err != nil {
				_ = os.Remove(plant)
				return err
			}
			captureInjected = captureReservationFileState(t, target)
			return nil
		},
		afterExactEpochCapture: func(path, quarantine string) error {
			if filepath.Clean(path) != filepath.Clean(target) || captured {
				return nil
			}
			captured = true
			quarantinePath = quarantine
			// Capture already moved the mismatched epoch aside. Recreate a new
			// live object under the canonical name before mismatch restore.
			if err := writeReservationPayload(target, livePayload, 0o640, time.Unix(1_700_000_201, 0)); err != nil {
				return err
			}
			liveInjected = captureReservationFileState(t, target)
			return nil
		},
	}

	ctx := context.Background()
	db, err := openLocalMutableCanonical(ctx, canonical, bound, func() error { return nil }, hooks)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE marker SET value = 'force-wal'`)
	require.NoError(t, err)
	_, _ = db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	_ = db.Close()

	require.True(t, captured, "fixture did not reach after-capture boundary")
	requireReservationFileState(t, target, liveInjected)
	require.NotEmpty(t, quarantinePath)
	requireReservationFileState(t, quarantinePath, captureInjected)
	// Retained capture must be discoverable transaction residue.
	found, err := SQLiteTransactionSidecars(resolved)
	require.NoError(t, err)
	require.Contains(t, found, filepath.Base(quarantinePath))
}

func TestOpenLocalMutableCanonicalRefusesExactEpochDestroyNameSubstitution(t *testing.T) {
	// After attestation of the captured reserved epoch, replacing the
	// quarantine directory entry must not destroy the substitute.
	dir := t.TempDir()
	canonical := filepath.Join(dir, "index.db")
	seedMutableCanonicalTestDB(t, canonical, "canonical")
	bound, err := os.OpenFile(canonical, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = bound.Close() }()

	resolved, err := resolveCanonicalSQLitePath(canonical)
	require.NoError(t, err)
	target := resolved + "-wal"
	payload := []byte("substituted quarantine epoch after attestation")
	var injected reservationFileState
	var quarantinePath string
	attested := false
	hooks := mutableCanonicalOpenHooks{afterExactEpochAttest: func(path, quarantine string) error {
		if filepath.Clean(path) != filepath.Clean(target) || attested {
			return nil
		}
		attested = true
		quarantinePath = quarantine
		if err := os.Remove(quarantine); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := writeReservationPayload(quarantine, payload, 0o640, time.Unix(1_700_000_300, 0)); err != nil {
			return err
		}
		injected = captureReservationFileState(t, quarantine)
		return nil
	}}

	ctx := context.Background()
	db, err := openLocalMutableCanonical(ctx, canonical, bound, func() error { return nil }, hooks)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE marker SET value = 'force-wal'`)
	require.NoError(t, err)
	_, _ = db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	_ = db.Close()

	require.True(t, attested, "fixture did not reach after-attest boundary")
	require.NotEmpty(t, quarantinePath)
	requireReservationFileState(t, quarantinePath, injected)
}

func writeReservationPayload(path string, payload []byte, mode os.FileMode, when time.Time) error {
	if err := os.WriteFile(path, payload, mode); err != nil {
		return err
	}
	if err := os.Chmod(path, mode); err != nil {
		return err
	}
	return os.Chtimes(path, when, when)
}

func testCanonicalSQLiteInsertedSidecarRefusal(t *testing.T, suffix string, trigger func(context.Context, *sql.DB) error) {
	t.Helper()
	dir := t.TempDir()
	canonical := filepath.Join(dir, "index.db")
	seedMutableCanonicalTestDB(t, canonical, "canonical")
	bound, err := os.OpenFile(canonical, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = bound.Close() }()
	boundInfo, err := bound.Stat()
	require.NoError(t, err)

	// Reservation keys use the concrete resolved main path + suffix.
	resolved, err := resolveCanonicalSQLitePath(canonical)
	require.NoError(t, err)
	target := resolved + suffix
	payload := []byte("uncoordinated transaction epoch: " + suffix)
	var injected reservationFileState
	injectedOnce := false
	var reservationPaths []string
	hooks := mutableCanonicalOpenHooks{beforeSidecarReservation: func(path string) error {
		reservationPaths = append(reservationPaths, filepath.Clean(path))
		if filepath.Clean(path) != filepath.Clean(target) || injectedOnce {
			return nil
		}
		if err := os.WriteFile(target, payload, 0o640); err != nil {
			return err
		}
		if err := os.Chmod(target, 0o640); err != nil {
			return err
		}
		fixedTime := time.Unix(1_700_000_000, 123_000_000)
		if err := os.Chtimes(target, fixedTime, fixedTime); err != nil {
			return err
		}
		injected = captureReservationFileState(t, target)
		injectedOnce = true
		return nil
	}}

	ctx := context.Background()
	db, refusal := openLocalMutableCanonical(ctx, canonical, bound, func() error { return nil }, hooks)
	if refusal == nil {
		refusal = trigger(ctx, db)
	}
	if db != nil {
		_ = db.Close()
	}
	require.True(t, injectedOnce, "fixture did not reach the requested sidecar reservation boundary; refusal=%v reservations=%v", refusal, reservationPaths)
	require.Error(t, refusal)
	requireReservationFileState(t, target, injected)
	namedInfo, err := os.Lstat(canonical)
	require.NoError(t, err)
	require.True(t, namedInfo.Mode().IsRegular())
	require.True(t, os.SameFile(boundInfo, namedInfo), "canonical main-file binding changed")
	// Refused writes must not commit.
	if suffix == "-wal" || suffix == "-journal" {
		data, err := os.ReadFile(canonical)
		require.NoError(t, err)
		require.NotContains(t, string(data), "must-not-commit")
	}
}

type reservationFileState struct {
	data  []byte
	mode  os.FileMode
	mtime int64
}

func captureReservationFileState(t *testing.T, path string) reservationFileState {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	info, err := os.Stat(path)
	require.NoError(t, err)
	return reservationFileState{data: data, mode: info.Mode(), mtime: info.ModTime().UnixNano()}
}

func requireReservationFileState(t *testing.T, path string, want reservationFileState) {
	t.Helper()
	require.Equal(t, want, captureReservationFileState(t, path))
}
