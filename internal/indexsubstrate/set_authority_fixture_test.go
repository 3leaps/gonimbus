package indexsubstrate

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Environment contract for the re-exec authority-holder child.
const (
	envHoldAuthority = "GONIMBUS_HELPER_HOLD_AUTHORITY"
	envSegmentRoot   = "GONIMBUS_HELPER_SEGMENT_ROOT"
	envIndexSetID    = "GONIMBUS_HELPER_INDEX_SET_ID"
	envReadyFile     = "GONIMBUS_HELPER_READY_FILE"
)

// fixtureIndexSetID returns a canonical idx_<64 hex> ID keyed by a single-hex
// seed, so tests can mint several distinct valid lease IDs deterministically.
func fixtureIndexSetID(seed rune) string {
	return "idx_" + strings.Repeat(string(seed), 64)
}

// authorityHolder is a real child process that genuinely holds a set-authority
// flock. Killing it (SIGKILL, no graceful release) reproduces the exact
// held->unheld residue the leak leaves in the field, without depending on live
// field artifacts.
type authorityHolder struct {
	t   *testing.T
	cmd *exec.Cmd
}

// spawnAuthorityHolder re-execs this test binary as a child that acquires the
// set authority for indexSetID under segmentSetRoot and blocks while holding it.
// It returns once the child has signalled that the lock is really held.
func spawnAuthorityHolder(t *testing.T, segmentSetRoot, indexSetID string) *authorityHolder {
	t.Helper()
	readyFile := filepath.Join(t.TempDir(), "holder-ready")
	cmd := exec.Command(os.Args[0], "-test.run=TestSetAuthorityHolderHelperProcess", "-test.timeout=120s") // #nosec G204 -- os.Args[0] is this test binary
	cmd.Env = append(os.Environ(),
		envHoldAuthority+"=1",
		envSegmentRoot+"="+segmentSetRoot,
		envIndexSetID+"="+indexSetID,
		envReadyFile+"="+readyFile,
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	h := &authorityHolder{t: t, cmd: cmd}
	t.Cleanup(h.kill)

	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(readyFile); err == nil {
			return h
		}
		if time.Now().After(deadline) {
			h.kill()
			t.Fatalf("authority holder child did not acquire the lock within the deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// kill forcibly terminates the holder (SIGKILL): the OS drops the advisory lock
// with no cleanup handler running, exactly like a lost supervisor. Idempotent.
func (h *authorityHolder) kill() {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		return
	}
	_ = h.cmd.Process.Kill()
	_, _ = h.cmd.Process.Wait()
	h.cmd = nil
}

// killAndWaitUnheld kills the holder and waits until the OS has actually
// released the advisory lock, so a follow-on probe deterministically observes
// unheld residue rather than racing the kernel's lock teardown.
func (h *authorityHolder) killAndWaitUnheld(authorityRoot, indexSetID string) {
	h.t.Helper()
	h.kill()
	deadline := time.Now().Add(15 * time.Second)
	for {
		lease, err := ProbeSetAuthorityLease(authorityRoot, indexSetID)
		require.NoError(h.t, err)
		if lease.Verdict == LeaseUnheld {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("authority lock still %q after holder kill", lease.Verdict)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSetAuthorityHolderHelperProcess is the re-exec entry point. Under normal
// `go test` it is a no-op; only the spawned child (envHoldAuthority set) acquires
// and holds the lock until the parent kills it.
func TestSetAuthorityHolderHelperProcess(t *testing.T) {
	if os.Getenv(envHoldAuthority) != "1" {
		t.Skip("helper process entry point; only runs as a spawned child")
	}
	segmentRoot := os.Getenv(envSegmentRoot)
	indexSetID := os.Getenv(envIndexSetID)
	readyFile := os.Getenv(envReadyFile)

	auth, err := AcquireSetAuthority(context.Background(), segmentRoot, indexSetID, "index-build-fixture-holder")
	if err != nil {
		t.Fatalf("child could not acquire set authority: %v", err)
	}
	// Signal the parent only after the lock is truly held.
	if err := os.WriteFile(readyFile, []byte("held"), 0o600); err != nil {
		t.Fatalf("child could not write ready file: %v", err)
	}
	// Hold the lock until the parent kills us. A timer keeps a goroutine runnable
	// so the runtime never mistakes this for a deadlock.
	<-time.After(110 * time.Second)
	_ = auth.Release()
}

// writeInvalidLease creates a lock file with an unparseable authority doc for a
// valid index-set ID, reproducing corrupt residue.
func writeInvalidLease(t *testing.T, authorityRoot, indexSetID string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(authorityRoot, 0o700))
	path := filepath.Join(authorityRoot, indexSetID+".lock")
	require.NoError(t, os.WriteFile(path, []byte("{not-json"), 0o600))
	return path
}

// The wrong-type and oversized artifact classes are planted by
// internal/leasefixture, which every layer's matrix drives — keeping one copy of
// each class rather than a per-package variant that can drift.

// writeLeaseWithDocID creates a well-formed authority doc for fileID whose
// embedded index_set_id is docIndexSetID, reproducing a correctly-named lock
// that claims a different set identity.
func writeLeaseWithDocID(t *testing.T, authorityRoot, fileID, docIndexSetID string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(authorityRoot, 0o700))
	path := filepath.Join(authorityRoot, fileID+".lock")
	doc := setAuthorityDoc{
		Type:       setAuthorityDocType,
		IndexSetID: docIndexSetID,
		Holder:     "index-build-mismatch",
		AcquiredAt: time.Now().UTC(),
	}
	data, err := json.Marshal(doc)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

// heldHolderAttribution returns the holder string a probe can recover while the
// lease is genuinely held.
//
// Unix advisory locks (flock) leave the doc readable by other processes, so
// attribution survives while a holder is live. Windows LockFileEx is MANDATORY:
// the held range is unreadable by anyone else, so attribution is unavailable
// until the holder exits. The verdict is unaffected on either platform — the
// lock alone decides held, and attribution never authorizes anything — so this
// is a reporting difference, not a safety one.
func heldHolderAttribution(want string) string {
	if mandatoryFileLocks {
		return ""
	}
	return want
}

// lockedRangeUnreadable reports whether err is the platform's specific refusal
// to touch or rebind a range another handle holds locked — not merely any error
// on a platform where such refusals exist.
func lockedRangeUnreadable(err error) bool {
	return err != nil && isLockedRangeError(err)
}

// docSnapshot captures the content, size, mode, and mtime of a lock file so a
// test can prove a probe left every byte untouched. Content is captured only
// when the file is readable: under a mandatory lock the bytes cannot be read at
// all, so metadata carries the zero-mutation proof there.
type docSnapshot struct {
	content         []byte
	contentReadable bool
	size            int64
	mode            os.FileMode
	modTime         time.Time
}

func snapshotLockDoc(t *testing.T, path string) docSnapshot {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	snap := docSnapshot{size: info.Size(), mode: info.Mode(), modTime: info.ModTime()}
	content, readErr := os.ReadFile(path) // #nosec G304 -- test-owned temp path
	if readErr == nil {
		snap.content = content
		snap.contentReadable = true
		return snap
	}
	// A mandatory lock blocking the read is expected on Windows; any other read
	// failure is a real fixture problem.
	require.True(t, lockedRangeUnreadable(readErr), "unexpected lock doc read failure: %v", readErr)
	return snap
}

func (s docSnapshot) assertUnchanged(t *testing.T, path string) {
	t.Helper()
	after := snapshotLockDoc(t, path)
	if s.contentReadable && after.contentReadable {
		require.Equal(t, s.content, after.content, "lock doc content must be byte-identical after a read-only probe")
	}
	require.Equal(t, s.size, after.size, "lock doc size must be unchanged")
	require.Equal(t, s.mode, after.mode, "lock doc mode must be unchanged")
	require.Equal(t, s.modTime, after.modTime, "lock doc mtime must be unchanged")
}
