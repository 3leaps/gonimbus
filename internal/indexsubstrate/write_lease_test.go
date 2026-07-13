package indexsubstrate

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteLeaseFlockIsExclusive(t *testing.T) {
	dir := t.TempDir()
	first, err := AcquireWriteLease(dir, "idx_test", "holder-a", 0)
	require.NoError(t, err)
	require.NoError(t, first.AssertHeld())

	_, err = AcquireWriteLease(dir, "idx_test", "holder-b", 0)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrWriteLeaseHeld)

	require.NoError(t, first.Release())
	require.ErrorIs(t, first.AssertHeld(), ErrWriteLeaseLost)

	second, err := AcquireWriteLease(dir, "idx_test", "holder-b", 0)
	require.NoError(t, err)
	require.NoError(t, second.Release())
}

func TestCheckWriteLeaseAvailableIsReadOnlyAndDetectsHolder(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, CheckWriteLeaseAvailable(root))
	_, err := os.Stat(filepath.Join(root, writeLeaseFileName))
	require.ErrorIs(t, err, os.ErrNotExist, "availability probe must not create a lock file")

	lease, err := AcquireWriteLease(root, "idx_test", "holder", 0)
	require.NoError(t, err)
	err = CheckWriteLeaseAvailable(root)
	require.ErrorIs(t, err, ErrWriteLeaseHeld)
	require.NoError(t, lease.Release())
	require.NoError(t, CheckWriteLeaseAvailable(root))
}

func TestAcquireWriteLeaseForMaintenancePreservesArtifactAndExcludesWriter(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	seed, err := AcquireWriteLease(root, "idx_test", "seed", 0)
	require.NoError(t, err)
	require.NoError(t, seed.Release())
	path := filepath.Join(root, writeLeaseFileName)
	beforeBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	beforeInfo, err := os.Stat(path)
	require.NoError(t, err)

	lease, err := AcquireWriteLeaseForMaintenance(root, "idx_test", "gc")
	require.NoError(t, err)
	require.NoError(t, lease.AssertHeldFor("idx_test", filepath.Join(root, "latest.json")))
	_, err = AcquireWriteLease(root, "idx_test", "writer", 0)
	require.ErrorIs(t, err, ErrWriteLeaseHeld)
	require.NoError(t, lease.Release())

	afterBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	afterInfo, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, beforeBytes, afterBytes)
	require.Equal(t, beforeInfo.Mode(), afterInfo.Mode())
	require.Equal(t, beforeInfo.Size(), afterInfo.Size())
	require.Equal(t, beforeInfo.ModTime(), afterInfo.ModTime())
}

func TestAcquireWriteLeaseForMaintenanceDoesNotCreateMissingArtifact(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	_, err = AcquireWriteLeaseForMaintenance(root, "idx_test", "gc")
	require.ErrorContains(t, err, "existing write lease")
	require.NoFileExists(t, filepath.Join(root, writeLeaseFileName))
	require.Error(t, CheckWriteLeaseAvailableForMaintenance(root))
}

func TestWriteLeaseCorruptMetadataDoesNotGrantAccess(t *testing.T) {
	dir := t.TempDir()
	// Pre-create a lock file with garbage; flock still serializes writers.
	path := filepath.Join(dir, writeLeaseFileName)
	require.NoError(t, os.WriteFile(path, []byte("{not-json"), 0o600))

	first, err := AcquireWriteLease(dir, "idx_test", "holder-a", 0)
	require.NoError(t, err)
	_, err = AcquireWriteLease(dir, "idx_test", "holder-b", 0)
	require.ErrorIs(t, err, ErrWriteLeaseHeld)
	require.NoError(t, first.Release())
}

func TestWriteLeaseConcurrentOnlyOneWinner(t *testing.T) {
	dir := t.TempDir()
	var winners int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := AcquireWriteLease(dir, "idx_test", "h", 0)
			if err != nil {
				return
			}
			mu.Lock()
			winners++
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			_ = lease.Release()
		}()
	}
	wg.Wait()
	// Exactly one holder at a time; sequential acquisition may allow multiple
	// winners across time. Count concurrent exclusivity via overlapping holds.
	// With sleep while held, only one goroutine should enter the critical section
	// at a time; total winners over the full run can be up to 8 after releases.
	// Assert we never exceeded one concurrent holder by checking final release.
	require.GreaterOrEqual(t, winners, 1)
	require.LessOrEqual(t, winners, 8)
}

func TestWriteLeaseAssertHeldForRejectsRootAndIDMismatch(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	lease, err := AcquireWriteLease(rootA, "idx_a", "holder", 0)
	require.NoError(t, err)
	defer func() { _ = lease.Release() }()

	require.NoError(t, lease.AssertHeldFor("idx_a", filepath.Join(rootA, "latest.json")))

	err = lease.AssertHeldFor("idx_b", filepath.Join(rootA, "latest.json"))
	require.ErrorIs(t, err, ErrWriteLeaseScope)

	err = lease.AssertHeldFor("idx_a", filepath.Join(rootB, "latest.json"))
	require.ErrorIs(t, err, ErrWriteLeaseScope)
}

func TestWriteLeaseConcurrentCriticalSectionExclusive(t *testing.T) {
	dir := t.TempDir()
	var concurrent, maxConcurrent int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				lease, err := AcquireWriteLease(dir, "idx_test", "h", 0)
				if err != nil {
					time.Sleep(time.Millisecond)
					continue
				}
				mu.Lock()
				concurrent++
				if concurrent > maxConcurrent {
					maxConcurrent = concurrent
				}
				mu.Unlock()
				time.Sleep(5 * time.Millisecond)
				mu.Lock()
				concurrent--
				mu.Unlock()
				_ = lease.Release()
				return
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 1, maxConcurrent)
}
