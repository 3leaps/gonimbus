package indexreader

import (
	"context"
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

func TestVerificationTargetRefusesInvalidAttemptNames(t *testing.T) {
	env := newVerificationTargetEnv(t)
	for _, attempt := range []string{"", "..", "a/b", "../escape", ".hidden"} {
		_, err := openVerificationTarget(env, attempt)
		require.ErrorIs(t, err, ErrVerificationProjectionTarget, "attempt %q", attempt)
	}
}

func mustReadVerificationFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
