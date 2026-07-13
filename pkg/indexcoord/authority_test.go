package indexcoord

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthorityRemainsBoundAfterSegmentRootQuarantine(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "segments")
	id := "idx_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	canonical := filepath.Join(parent, id)
	quarantine := filepath.Join(parent, ".quarantine-"+id)
	require.NoError(t, os.MkdirAll(canonical, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(canonical, "latest.json"), []byte("keep\n"), 0o600))

	held, err := Acquire(context.Background(), canonical, id, "gc")
	require.NoError(t, err)
	defer func() { _ = held.Release() }()
	require.NoError(t, os.Rename(canonical, quarantine))

	contender, err := Acquire(context.Background(), canonical, id, "library-writer")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrHeld))
	require.Nil(t, contender)
	require.NoDirExists(t, canonical, "authority acquisition must not recreate the quarantined canonical root")
	require.FileExists(t, filepath.Join(quarantine, "latest.json"))

	require.NoError(t, held.Release())
	retry, err := Acquire(context.Background(), canonical, id, "retry")
	require.NoError(t, err)
	require.NoDirExists(t, canonical, "stable authority remains outside the canonical set root")
	require.NoError(t, retry.Release())
}

func TestAuthorityDetectsCanonicalLockPathReplacement(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "segments")
	id := "idx_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	canonical := filepath.Join(parent, id)
	require.NoError(t, os.MkdirAll(canonical, 0o700))

	held, err := Acquire(context.Background(), canonical, id, "writer")
	require.NoError(t, err)
	defer func() { _ = held.Release() }()
	authorityRoot, err := AuthorityRoot(canonical)
	require.NoError(t, err)
	lockPath := filepath.Join(authorityRoot, id+".lock")
	require.NoError(t, os.Remove(lockPath))
	require.NoError(t, os.WriteFile(lockPath, []byte("replacement\n"), 0o600))

	require.ErrorIs(t, held.AssertHeld(), ErrLost)
}
