package opcheckpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResolveRootRejectsForbiddenWorktree(t *testing.T) {
	repo := t.TempDir()
	_, err := ResolveRoot(Config{
		RootDir:        filepath.Join(repo, "checkpoints"),
		ForbiddenRoots: []string{repo},
	})
	require.ErrorIs(t, err, ErrPathInsideForbiddenRoot)

	root, err := ResolveRoot(Config{
		AppDataDir:     filepath.Join(t.TempDir(), "gonimbus"),
		ForbiddenRoots: []string{repo},
	})
	require.NoError(t, err)
	require.NotContains(t, root, repo)
	require.Equal(t, filepath.Join(filepath.Dir(root), defaultRootName), root)
}

func TestResolveRootRejectsSymlinkIntoForbiddenWorktree(t *testing.T) {
	repo := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(outside, "linked-repo")
	require.NoError(t, os.Symlink(repo, link))

	_, err := ResolveRoot(Config{
		RootDir:        filepath.Join(link, "checkpoints"),
		ForbiddenRoots: []string{repo},
	})
	require.ErrorIs(t, err, ErrPathInsideForbiddenRoot)
}

func TestWriteCheckpointCreatesSecureDirsAndFile(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	fingerprint := testFingerprint(t, map[string]any{"source": "s3://example/prefix/", "scope": []string{"a", "b"}})

	err := store.WriteCheckpoint(ctx, Envelope{
		Operation:         "index-build",
		RunID:             "run_123",
		ConfigFingerprint: fingerprint,
		Status:            StatusFailedResumable,
		ErrorClass:        ErrorClassInterrupted,
		Progress:          map[string]int64{"objects_ingested": 12},
		Payload:           json.RawMessage(`{"frontier":["p1","p2"]}`),
	})
	require.NoError(t, err)

	path, err := store.CheckpointPath("index-build", "run_123")
	require.NoError(t, err)
	requireFileMode(t, filepath.Dir(filepath.Dir(path)), 0o700)
	requireFileMode(t, filepath.Dir(path), 0o700)
	requireFileMode(t, path, 0o600)

	env, err := store.ReadCheckpoint(ctx, "index-build", "run_123")
	require.NoError(t, err)
	require.Equal(t, SchemaVersion, env.SchemaVersion)
	require.Equal(t, StatusFailedResumable, env.Status)
	require.Equal(t, int64(12), env.Progress["objects_ingested"])
	require.NotEmpty(t, env.CheckpointID)
	require.NoError(t, store.ValidateIdentity(env, Identity{
		Operation:         "index-build",
		RunID:             "run_123",
		ConfigFingerprint: fingerprint,
	}))
}

func TestValidateIdentityFailsClosedOnMismatch(t *testing.T) {
	store := newTestStore(t)
	env := &Envelope{
		SchemaVersion:     SchemaVersion,
		Operation:         "transfer-reflow",
		RunID:             "run_abc",
		ConfigFingerprint: "fp1",
		Status:            StatusFailedResumable,
	}
	err := store.ValidateIdentity(env, Identity{
		Operation:         "transfer-reflow",
		RunID:             "run_abc",
		ConfigFingerprint: "fp2",
	})
	require.ErrorIs(t, err, ErrIdentityMismatch)
}

func TestWriteCheckpointRejectsCredentialMaterial(t *testing.T) {
	store := newTestStore(t)
	cases := []struct {
		name    string
		payload string
	}{
		{
			name:    "credential key",
			payload: `{"access_key_id":"AKIA...","frontier":["a"]}`,
		},
		{
			name:    "aws signed url under neutral key",
			payload: `{"source_uri":"https://bucket.s3.amazonaws.com/object?X-Amz-Signature=abc"}`,
		},
		{
			name:    "gcs signed url under neutral key",
			payload: `{"source_uri":"https://storage.googleapis.com/bucket/object?X-Goog-Credential=svc&X-Goog-Signature=abc"}`,
		},
		{
			name:    "azure sas url under neutral key",
			payload: `{"source_uri":"https://account.blob.core.windows.net/container/object?sv=2024-11-04&sig=abc"}`,
		},
		{
			name:    "auth bearing dsn",
			payload: `{"source_uri":"postgres://user:password@example.invalid/db"}`,
		},
		{
			name:    "username only url userinfo",
			payload: `{"source_uri":"https://token@example.invalid/object"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := store.WriteCheckpoint(context.Background(), Envelope{
				Operation:         "index-build",
				RunID:             "run_secret_" + fmt.Sprint(len(tc.name)),
				ConfigFingerprint: "fp",
				Status:            StatusFailedResumable,
				Payload:           json.RawMessage(tc.payload),
			})
			require.ErrorIs(t, err, ErrCredentialMaterial)
		})
	}
}

func TestFingerprintConfigIsStableAndRejectsSecrets(t *testing.T) {
	a, err := FingerprintConfig(map[string]any{
		"dest":   "s3://dest/out/",
		"source": "s3://source/in/",
		"filters": map[string]any{
			"prefix": "raw/",
			"after":  "2026-01-01",
		},
	})
	require.NoError(t, err)
	b, err := FingerprintConfig(map[string]any{
		"filters": map[string]any{
			"after":  "2026-01-01",
			"prefix": "raw/",
		},
		"source": "s3://source/in/",
		"dest":   "s3://dest/out/",
	})
	require.NoError(t, err)
	require.Equal(t, a, b)

	_, err = FingerprintConfig(map[string]any{"authToken": "secret"})
	require.ErrorIs(t, err, ErrCredentialMaterial)
}

func TestClaimLeaseIsExclusiveAndStaleLeaseCanBeReclaimed(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	first, err := store.ClaimLease(ctx, "index-build", "run_lease", "holder-1", time.Hour)
	require.NoError(t, err)
	require.Equal(t, "holder-1", first.HolderID)

	_, err = store.ClaimLease(ctx, "index-build", "run_lease", "holder-2", time.Hour)
	require.ErrorIs(t, err, ErrLeaseHeld)
	wrong := *first
	wrong.HolderID = "holder-2"
	require.ErrorIs(t, store.ReleaseLease("index-build", wrong), ErrLeaseHeld)
	require.NoError(t, store.ReleaseLease("index-build", *first))

	second, err := store.ClaimLease(ctx, "index-build", "run_lease", "holder-2", time.Hour)
	require.NoError(t, err)
	require.Equal(t, "holder-2", second.HolderID)

	leasePath := filepath.Join(store.RootDir(), "index-build", "run_lease", leaseFileName)
	stale := Lease{
		RunID:     "run_lease",
		HolderID:  "stale",
		ClaimedAt: time.Now().Add(-2 * time.Hour).UTC(),
		ExpiresAt: time.Now().Add(-time.Hour).UTC(),
	}
	data, err := json.Marshal(stale)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(leasePath, data, 0o600))
	third, err := store.ClaimLease(ctx, "index-build", "run_lease", "holder-3", time.Hour)
	require.NoError(t, err)
	require.Equal(t, "holder-3", third.HolderID)
}

func TestReleaseLeaseDoesNotDeleteSuccessorAfterStaleTakeover(t *testing.T) {
	store := newTestStore(t)
	runID := "run_release_stale"
	dir, err := store.ensureRunDir("index-build", runID)
	require.NoError(t, err)
	leasePath := filepath.Join(dir, leaseFileName)
	stale := Lease{
		RunID:     runID,
		HolderID:  "holder-a",
		ClaimedAt: time.Now().Add(-2 * time.Hour).UTC(),
		ExpiresAt: time.Now().Add(-time.Hour).UTC(),
	}
	writeLeaseFile(t, leasePath, stale)

	successor, err := store.ClaimLease(context.Background(), "index-build", runID, "holder-b", time.Hour)
	require.NoError(t, err)

	err = store.ReleaseLease("index-build", stale)
	require.ErrorIs(t, err, ErrLeaseHeld)
	current, err := readLease(leasePath)
	require.NoError(t, err)
	require.True(t, sameLease(current, successor))
}

func TestClaimLeaseCleansExpiredReclaimLock(t *testing.T) {
	store := newTestStore(t)
	runID := "run_expired_reclaim_lock"
	dir, err := store.ensureRunDir("index-build", runID)
	require.NoError(t, err)

	writeLeaseFile(t, filepath.Join(dir, leaseFileName), Lease{
		RunID:     runID,
		HolderID:  "stale",
		ClaimedAt: time.Now().Add(-2 * time.Hour).UTC(),
		ExpiresAt: time.Now().Add(-time.Hour).UTC(),
	})
	writeReclaimLockFile(t, filepath.Join(dir, reclaimLockName), reclaimLock{
		HolderID:  "dead-reclaimer",
		ClaimedAt: time.Now().Add(-2 * reclaimLockTTL).UTC(),
		ExpiresAt: time.Now().Add(-reclaimLockTTL).UTC(),
	})

	lease, err := store.ClaimLease(context.Background(), "index-build", runID, "holder-retry", time.Hour)
	require.NoError(t, err)
	require.Equal(t, "holder-retry", lease.HolderID)
}

func TestClaimLeaseFailsClosedOnLiveReclaimLock(t *testing.T) {
	store := newTestStore(t)
	runID := "run_live_reclaim_lock"
	dir, err := store.ensureRunDir("index-build", runID)
	require.NoError(t, err)

	writeLeaseFile(t, filepath.Join(dir, leaseFileName), Lease{
		RunID:     runID,
		HolderID:  "stale",
		ClaimedAt: time.Now().Add(-2 * time.Hour).UTC(),
		ExpiresAt: time.Now().Add(-time.Hour).UTC(),
	})
	writeReclaimLockFile(t, filepath.Join(dir, reclaimLockName), reclaimLock{
		HolderID:  "active-reclaimer",
		ClaimedAt: time.Now().UTC(),
		ExpiresAt: time.Now().Add(reclaimLockTTL).UTC(),
	})

	_, err = store.ClaimLease(context.Background(), "index-build", runID, "holder-blocked", time.Hour)
	require.ErrorIs(t, err, ErrLeaseHeld)
}

func TestConcurrentClaimLeaseAllowsOnlyOneWinner(t *testing.T) {
	store := newTestStore(t)
	const contenders = 12
	var wg sync.WaitGroup
	results := make(chan error, contenders)
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := store.ClaimLease(context.Background(), "transfer-reflow", "run_concurrent", string(rune('a'+i)), time.Hour)
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)

	var winners int
	var held int
	for err := range results {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, ErrLeaseHeld):
			held++
		default:
			t.Fatalf("unexpected lease error: %v", err)
		}
	}
	require.Equal(t, 1, winners)
	require.Equal(t, contenders-1, held)
}

func TestConcurrentStaleLeaseReclaimAllowsOnlyOneWinner(t *testing.T) {
	store := newTestStore(t)
	const contenders = 16
	runID := "run_stale_concurrent"
	dir, err := store.ensureRunDir("transfer-reflow", runID)
	require.NoError(t, err)
	stale := Lease{
		RunID:     runID,
		HolderID:  "stale",
		ClaimedAt: time.Now().Add(-2 * time.Hour).UTC(),
		ExpiresAt: time.Now().Add(-time.Hour).UTC(),
	}
	data, err := json.Marshal(stale)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, leaseFileName), data, 0o600))

	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan error, contenders)
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := store.ClaimLease(context.Background(), "transfer-reflow", runID, fmt.Sprintf("holder_%02d", i), time.Hour)
			results <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)

	var winners int
	var held int
	for err := range results {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, ErrLeaseHeld):
			held++
		default:
			t.Fatalf("unexpected lease error: %v", err)
		}
	}
	require.Equal(t, 1, winners)
	require.Equal(t, contenders-1, held)
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(context.Background(), Config{AppDataDir: filepath.Join(t.TempDir(), "data")})
	require.NoError(t, err)
	return store
}

func testFingerprint(t *testing.T, v any) string {
	t.Helper()
	fp, err := FingerprintConfig(v)
	require.NoError(t, err)
	return fp
}

func requireFileMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, want, info.Mode().Perm(), path)
}

func writeLeaseFile(t *testing.T, path string, lease Lease) {
	t.Helper()
	data, err := json.Marshal(lease)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
}

func writeReclaimLockFile(t *testing.T, path string, lock reclaimLock) {
	t.Helper()
	data, err := json.Marshal(lock)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
}
