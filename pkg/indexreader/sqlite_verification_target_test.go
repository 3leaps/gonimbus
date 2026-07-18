package indexreader

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

type verificationTargetEnv struct {
	segmentRoot string
	indexSetID  string
	authority   *indexcoord.Lease
}

func newVerificationTargetEnv(t *testing.T) verificationTargetEnv {
	t.Helper()
	segmentRoot := filepath.Join(t.TempDir(), "segments", "idx_set")
	require.NoError(t, os.MkdirAll(segmentRoot, 0o700))
	indexSetID := "idx_verification_target_test"
	authority, err := indexcoord.Acquire(context.Background(), segmentRoot, indexSetID, "verification-test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = authority.Release() })
	return verificationTargetEnv{segmentRoot: segmentRoot, indexSetID: indexSetID, authority: authority}
}

func openVerificationTarget(env verificationTargetEnv, attempt string) (*SQLiteVerificationTarget, error) {
	return OpenSQLiteVerificationTarget(context.Background(), SQLiteVerificationTargetOptions{
		SegmentSetRoot: env.segmentRoot,
		IndexSetID:     env.indexSetID,
		Authority:      env.authority,
		AttemptName:    attempt,
	})
}

func TestVerificationTargetHonestSuccessiveAttempts(t *testing.T) {
	env := newVerificationTargetEnv(t)

	for _, attempt := range []string{"run_1", "run_2"} {
		target, err := openVerificationTarget(env, attempt)
		require.NoError(t, err)
		require.NoError(t, indexstore.Migrate(context.Background(), target.DB()))
		require.NoError(t, target.Check())

		info, err := os.Lstat(target.Path())
		require.NoError(t, err)
		require.True(t, info.Mode().IsRegular())
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm(), "projection must be owner-only")
		dirInfo, err := os.Lstat(filepath.Dir(target.Path()))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm(), "attempt directory must be owner-only")

		require.NoError(t, target.Close())
		require.NoError(t, target.Close(), "close is idempotent")
	}

	first := filepath.Join(env.segmentRoot, "verification", "run_1", "index.db")
	second := filepath.Join(env.segmentRoot, "verification", "run_2", "index.db")
	require.FileExists(t, first)
	require.FileExists(t, second)
}

func TestVerificationTargetRefusesSymlinkedIntermediate(t *testing.T) {
	env := newVerificationTargetEnv(t)

	// Plant <root>/verification as a symlink to an outside directory holding a
	// sentinel. The open must refuse before any SQLite mutation and leave the
	// outside directory byte-identical.
	outside := filepath.Join(t.TempDir(), "outside")
	require.NoError(t, os.MkdirAll(outside, 0o700))
	sentinelPath := filepath.Join(outside, "sentinel.txt")
	require.NoError(t, os.WriteFile(sentinelPath, []byte("untouched\n"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(env.segmentRoot, "verification")))

	target, err := openVerificationTarget(env, "run_1")
	require.ErrorIs(t, err, ErrVerificationProjectionTarget)
	require.Nil(t, target)

	entries, err := os.ReadDir(outside)
	require.NoError(t, err)
	require.Len(t, entries, 1, "no files may be created through the planted symlink")
	require.Equal(t, []byte("untouched\n"), mustReadVerificationFile(t, sentinelPath))
}

func TestVerificationTargetRefusesSymlinkedSegmentRoot(t *testing.T) {
	env := newVerificationTargetEnv(t)
	realRoot := filepath.Join(t.TempDir(), "real-root")
	require.NoError(t, os.MkdirAll(realRoot, 0o700))
	linkRoot := filepath.Join(t.TempDir(), "link-root")
	require.NoError(t, os.Symlink(realRoot, linkRoot))

	_, err := OpenSQLiteVerificationTarget(context.Background(), SQLiteVerificationTargetOptions{
		SegmentSetRoot: linkRoot,
		IndexSetID:     env.indexSetID,
		Authority:      env.authority,
		AttemptName:    "run_1",
	})
	require.Error(t, err)
	entries, readErr := os.ReadDir(realRoot)
	require.NoError(t, readErr)
	require.Empty(t, entries, "nothing may be created through a symlinked set root")
}

func TestVerificationTargetRefusesPreExistingAttempt(t *testing.T) {
	env := newVerificationTargetEnv(t)
	attemptDir := filepath.Join(env.segmentRoot, "verification", "run_1")
	require.NoError(t, os.MkdirAll(attemptDir, 0o700))

	_, err := openVerificationTarget(env, "run_1")
	require.ErrorIs(t, err, ErrVerificationProjectionTarget)
}

func TestVerificationTargetRefusesPlantedDatabaseSymlink(t *testing.T) {
	env := newVerificationTargetEnv(t)

	// A pre-existing attempt directory is refused outright, so a planted DB
	// symlink can only matter if the attempt directory itself is racing; the
	// exclusive no-follow create still refuses an existing name.
	outsideDB := filepath.Join(t.TempDir(), "outside.db")
	require.NoError(t, os.WriteFile(outsideDB, []byte("foreign"), 0o600))
	attemptDir := filepath.Join(env.segmentRoot, "verification", "run_1")
	require.NoError(t, os.MkdirAll(attemptDir, 0o700))
	require.NoError(t, os.Symlink(outsideDB, filepath.Join(attemptDir, "index.db")))

	_, err := openVerificationTarget(env, "run_1")
	require.ErrorIs(t, err, ErrVerificationProjectionTarget)
	require.Equal(t, []byte("foreign"), mustReadVerificationFile(t, outsideDB), "outside database must stay byte-identical")
}

func TestVerificationTargetRefusesSubstitutionBeforeClose(t *testing.T) {
	env := newVerificationTargetEnv(t)
	target, err := openVerificationTarget(env, "run_1")
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(context.Background(), target.DB()))

	// Substitute the created file behind the pathname after open.
	require.NoError(t, os.Remove(target.Path()))
	require.NoError(t, os.WriteFile(target.Path(), []byte("substitute"), 0o600))

	require.Error(t, target.Check())
	require.Error(t, target.Close(), "substituted pathname must fail close so no success receipt can bind it")
}

func TestVerificationTargetRefusesAttemptDirSubstitutionAfterOpen(t *testing.T) {
	// Rename the attempt directory away and symlink the original name back to
	// it: the final pathname still names the originally bound DB inode, but the
	// containing authority path changed. Check and Close must refuse; the moved
	// database must be preserved.
	env := newVerificationTargetEnv(t)
	target, err := openVerificationTarget(env, "run_1")
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(context.Background(), target.DB()))

	attemptDir := filepath.Join(env.segmentRoot, "verification", "run_1")
	moved := filepath.Join(t.TempDir(), "moved-attempt")
	require.NoError(t, os.Rename(attemptDir, moved))
	require.NoError(t, os.Symlink(moved, attemptDir))

	require.ErrorIs(t, target.Check(), ErrVerificationProjectionTarget)
	require.ErrorIs(t, target.Close(), ErrVerificationProjectionTarget)
	require.FileExists(t, filepath.Join(moved, "index.db"), "moved projection must be preserved")
}

func TestVerificationTargetRefusesVerificationDirSubstitutionWithHardlinkAlias(t *testing.T) {
	// Rename the legitimate verification directory, replace it with a symlink
	// to an outside tree, and expose the originally created DB inode there via
	// a hardlink: SameFile on the final pathname passes, so only parent
	// re-attestation can refuse. Check and Close must both refuse.
	env := newVerificationTargetEnv(t)
	target, err := openVerificationTarget(env, "run_1")
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(context.Background(), target.DB()))

	verificationDir := filepath.Join(env.segmentRoot, "verification")
	moved := filepath.Join(t.TempDir(), "moved-verification")
	require.NoError(t, os.Rename(verificationDir, moved))
	outside := filepath.Join(t.TempDir(), "outside")
	require.NoError(t, os.MkdirAll(filepath.Join(outside, "run_1"), 0o700))
	require.NoError(t, os.Link(filepath.Join(moved, "run_1", "index.db"), filepath.Join(outside, "run_1", "index.db")))
	require.NoError(t, os.Symlink(outside, verificationDir))

	require.ErrorIs(t, target.Check(), ErrVerificationProjectionTarget)
	require.ErrorIs(t, target.Close(), ErrVerificationProjectionTarget)
}

func TestVerificationTargetRefusesRealDirectoryReplacement(t *testing.T) {
	// Replace the attempt directory with a different real directory carrying a
	// hardlink to the bound inode: no symlink anywhere, final SameFile passes,
	// but the retained directory identity differs. Check and Close must refuse.
	env := newVerificationTargetEnv(t)
	target, err := openVerificationTarget(env, "run_1")
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(context.Background(), target.DB()))

	attemptDir := filepath.Join(env.segmentRoot, "verification", "run_1")
	moved := filepath.Join(t.TempDir(), "moved-attempt")
	require.NoError(t, os.Rename(attemptDir, moved))
	require.NoError(t, os.Mkdir(attemptDir, 0o700))
	require.NoError(t, os.Link(filepath.Join(moved, "index.db"), filepath.Join(attemptDir, "index.db")))

	require.ErrorIs(t, target.Check(), ErrVerificationProjectionTarget)
	require.ErrorIs(t, target.Close(), ErrVerificationProjectionTarget)
}

func TestVerificationTargetRefusesInvalidAttemptNames(t *testing.T) {
	env := newVerificationTargetEnv(t)
	for _, attempt := range []string{"", "..", "a/b", "../escape", ".hidden"} {
		_, err := openVerificationTarget(env, attempt)
		require.ErrorIs(t, err, ErrVerificationProjectionTarget, "attempt %q", attempt)
	}
}

// substitutionOutside prepares an outside tree carrying a sentinel whose bytes
// and directory inventory must survive any refused open untouched.
type substitutionOutside struct {
	dir      string
	sentinel string
}

func newSubstitutionOutside(t *testing.T) substitutionOutside {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "outside")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	sentinel := filepath.Join(dir, "sentinel.txt")
	require.NoError(t, os.WriteFile(sentinel, []byte("untouched\n"), 0o600))
	return substitutionOutside{dir: dir, sentinel: sentinel}
}

func (o substitutionOutside) assertUntouched(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(o.dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "no verification component may be created in the substituted tree")
	require.Equal(t, "sentinel.txt", entries[0].Name())
	require.Equal(t, []byte("untouched\n"), mustReadVerificationFile(t, o.sentinel))
}

// substituteWithSymlink / substituteWithRealDirectory rename the bound
// directory away and replace its name — with a symlink to the outside tree,
// or with a different real directory. Both return the moved original path.
func substituteWithSymlink(t *testing.T, boundPath, outsideDir string) string {
	t.Helper()
	moved := filepath.Join(t.TempDir(), "moved")
	require.NoError(t, os.Rename(boundPath, moved))
	require.NoError(t, os.Symlink(outsideDir, boundPath))
	return moved
}

func substituteWithRealDirectory(t *testing.T, boundPath string) string {
	t.Helper()
	moved := filepath.Join(t.TempDir(), "moved")
	require.NoError(t, os.Rename(boundPath, moved))
	require.NoError(t, os.Mkdir(boundPath, 0o700))
	return moved
}

func TestVerificationTargetRefusesRootSubstitutionBeforeVerificationCreate(t *testing.T) {
	for _, variant := range []string{"symlink", "realdir"} {
		t.Run(variant, func(t *testing.T) {
			env := newVerificationTargetEnv(t)
			outside := newSubstitutionOutside(t)
			var moved string
			hooks := verificationTargetOpenHooks{afterRootBind: func(*os.File) error {
				if variant == "symlink" {
					moved = substituteWithSymlink(t, env.segmentRoot, outside.dir)
				} else {
					moved = substituteWithRealDirectory(t, env.segmentRoot)
				}
				return nil
			}}
			target, err := openSQLiteVerificationTarget(context.Background(), SQLiteVerificationTargetOptions{
				SegmentSetRoot: env.segmentRoot,
				IndexSetID:     env.indexSetID,
				Authority:      env.authority,
				AttemptName:    "run_1",
			}, hooks)
			require.ErrorIs(t, err, ErrVerificationProjectionTarget)
			require.Nil(t, target)
			outside.assertUntouched(t)
			require.NoFileExists(t, filepath.Join(env.segmentRoot, "verification"))
			// Anything created relative to the retained root handle may live
			// only in the moved original tree, never the substituted one.
			if entries, readErr := os.ReadDir(filepath.Join(moved, "verification")); readErr == nil {
				for _, entry := range entries {
					require.NoDirExists(t, filepath.Join(env.segmentRoot, "verification", entry.Name()))
				}
			}
		})
	}
}

func TestVerificationTargetRefusesVerificationSubstitutionBeforeAttemptCreate(t *testing.T) {
	for _, variant := range []string{"symlink", "realdir"} {
		t.Run(variant, func(t *testing.T) {
			env := newVerificationTargetEnv(t)
			outside := newSubstitutionOutside(t)
			verificationDir := filepath.Join(env.segmentRoot, "verification")
			hooks := verificationTargetOpenHooks{afterVerificationBind: func(*os.File) error {
				if variant == "symlink" {
					substituteWithSymlink(t, verificationDir, outside.dir)
				} else {
					substituteWithRealDirectory(t, verificationDir)
				}
				return nil
			}}
			target, err := openSQLiteVerificationTarget(context.Background(), SQLiteVerificationTargetOptions{
				SegmentSetRoot: env.segmentRoot,
				IndexSetID:     env.indexSetID,
				Authority:      env.authority,
				AttemptName:    "run_1",
			}, hooks)
			require.ErrorIs(t, err, ErrVerificationProjectionTarget)
			require.Nil(t, target)
			outside.assertUntouched(t)
			require.NoDirExists(t, filepath.Join(outside.dir, "run_1"))
			require.NoFileExists(t, filepath.Join(verificationDir, "run_1", "index.db"))
		})
	}
}

func TestVerificationTargetRefusesAttemptSubstitutionBeforeDBCreate(t *testing.T) {
	for _, variant := range []string{"symlink", "realdir"} {
		t.Run(variant, func(t *testing.T) {
			env := newVerificationTargetEnv(t)
			outside := newSubstitutionOutside(t)
			attemptDir := filepath.Join(env.segmentRoot, "verification", "run_1")
			hooks := verificationTargetOpenHooks{afterAttemptBind: func(*os.File) error {
				if variant == "symlink" {
					substituteWithSymlink(t, attemptDir, outside.dir)
				} else {
					substituteWithRealDirectory(t, attemptDir)
				}
				return nil
			}}
			target, err := openSQLiteVerificationTarget(context.Background(), SQLiteVerificationTargetOptions{
				SegmentSetRoot: env.segmentRoot,
				IndexSetID:     env.indexSetID,
				Authority:      env.authority,
				AttemptName:    "run_1",
			}, hooks)
			require.ErrorIs(t, err, ErrVerificationProjectionTarget)
			require.Nil(t, target)
			outside.assertUntouched(t)
			require.NoFileExists(t, filepath.Join(outside.dir, "index.db"))
			require.NoFileExists(t, filepath.Join(attemptDir, "index.db"))
		})
	}
}

func TestVerificationTargetParentSwapBeforeSQLiteOpenPreservesForeignDB(t *testing.T) {
	// Substitute the attempt directory after the exclusive database
	// reservation but before SQLite's first open: the foreign database exposed
	// at the reserved pathname must be neither opened nor mutated, and no
	// target is returned.
	env := newVerificationTargetEnv(t)
	outside := newSubstitutionOutside(t)
	foreignDB := filepath.Join(outside.dir, "index.db")
	require.NoError(t, os.WriteFile(foreignDB, []byte("foreign-db"), 0o600))
	attemptDir := filepath.Join(env.segmentRoot, "verification", "run_1")
	var moved string
	hooks := verificationTargetOpenHooks{afterDBReserve: func(*os.File) error {
		moved = substituteWithSymlink(t, attemptDir, outside.dir)
		return nil
	}}
	target, err := openSQLiteVerificationTarget(context.Background(), SQLiteVerificationTargetOptions{
		SegmentSetRoot: env.segmentRoot,
		IndexSetID:     env.indexSetID,
		Authority:      env.authority,
		AttemptName:    "run_1",
	}, hooks)
	require.ErrorIs(t, err, ErrVerificationProjectionTarget)
	require.Nil(t, target)
	require.Equal(t, []byte("foreign-db"), mustReadVerificationFile(t, foreignDB), "foreign database must stay byte-identical")
	require.FileExists(t, filepath.Join(moved, "index.db"), "reserved database must be preserved in the moved original tree")
	require.Equal(t, []byte("untouched\n"), mustReadVerificationFile(t, outside.sentinel))
}

func TestVerificationTargetFailedOpenClosesAllBindings(t *testing.T) {
	env := newVerificationTargetEnv(t)
	var root, verification, attempt, db *os.File
	sentinelErr := errors.New("interposition sentinel")
	hooks := verificationTargetOpenHooks{
		afterRootBind:         func(h *os.File) error { root = h; return nil },
		afterVerificationBind: func(h *os.File) error { verification = h; return nil },
		afterAttemptBind:      func(h *os.File) error { attempt = h; return nil },
		afterDBReserve:        func(h *os.File) error { db = h; return sentinelErr },
	}
	target, err := openSQLiteVerificationTarget(context.Background(), SQLiteVerificationTargetOptions{
		SegmentSetRoot: env.segmentRoot,
		IndexSetID:     env.indexSetID,
		Authority:      env.authority,
		AttemptName:    "run_1",
	}, hooks)
	require.ErrorIs(t, err, ErrVerificationProjectionTarget)
	require.ErrorContains(t, err, "interposition sentinel")
	require.Nil(t, target)
	for name, handle := range map[string]*os.File{
		"root": root, "verification": verification, "attempt": attempt, "db": db,
	} {
		require.NotNil(t, handle, name)
		_, statErr := handle.Stat()
		require.ErrorIs(t, statErr, os.ErrClosed, "%s binding must be closed after a failed open", name)
	}
}

func mustReadVerificationFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
