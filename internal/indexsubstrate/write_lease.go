package indexsubstrate

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ErrWriteLeaseHeld is returned when another durable writer holds the set lock.
var ErrWriteLeaseHeld = errors.New("durable write lease held")

// ErrWriteLeaseLost is returned when a held guard is no longer valid at the
// commit boundary (released or never acquired).
var ErrWriteLeaseLost = errors.New("durable write lease not held")

// ErrWriteLeaseScope is returned when a held lease does not authorize the
// publish target (index set ID or segment-set root mismatch).
var ErrWriteLeaseScope = errors.New("durable write lease does not authorize target")

const writeLeaseFileName = ".durable-write.lock"

// WriteLease is an index-set-scoped exclusive lock for durable latest writers.
// Mutual exclusion is provided by an OS advisory lock held on an open FD for
// the lifetime of the critical section. Process crash releases the lock.
//
// The lease is bound at acquisition to a canonical segment-set root and
// index-set ID. PublishSnapshot must assert that binding against the latest
// path it is about to advance.
//
// Metadata in the lock file is diagnostic only and is never used for reclaim.
type WriteLease struct {
	f              *os.File
	path           string
	segmentSetRoot string // canonical absolute clean root
	indexSetID     string
	holder         string
	token          string
	released       bool
}

type writeLeaseDoc struct {
	Type     string    `json:"type"`
	IndexSet string    `json:"index_set_id"`
	Holder   string    `json:"holder"`
	Token    string    `json:"token"`
	Acquired time.Time `json:"acquired_at"`
}

// canonicalizeSegmentSetRoot returns an absolute, cleaned segment-set root.
// Symlinks in the path are not resolved; both acquire and assert use the same
// Abs+Clean policy so matching roots compare equal.
func canonicalizeSegmentSetRoot(segmentSetRoot string) (string, error) {
	segmentSetRoot = strings.TrimSpace(segmentSetRoot)
	if segmentSetRoot == "" {
		return "", fmt.Errorf("segment set root is required")
	}
	abs, err := filepath.Abs(segmentSetRoot)
	if err != nil {
		return "", fmt.Errorf("resolve segment set root: %w", err)
	}
	return filepath.Clean(abs), nil
}

// AcquireWriteLease takes an exclusive cross-process write lock under
// segmentSetRoot (cache/segments/<index_set_id>/). The ttl parameter is ignored;
// exclusivity is OS-lock-based for the process lifetime of the returned lease.
// The lease is bound to the canonical root and indexSetID for later AssertHeldFor.
func AcquireWriteLease(segmentSetRoot, indexSetID, holder string, _ time.Duration) (*WriteLease, error) {
	root, err := canonicalizeSegmentSetRoot(segmentSetRoot)
	if err != nil {
		return nil, err
	}
	indexSetID = strings.TrimSpace(indexSetID)
	holder = strings.TrimSpace(holder)
	if indexSetID == "" {
		return nil, fmt.Errorf("index_set_id is required")
	}
	if holder == "" {
		return nil, fmt.Errorf("lease holder is required")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create segment set root: %w", err)
	}
	path := filepath.Join(root, writeLeaseFileName)
	// Open (create) then lock — do not delete foreign/corrupt content to "reclaim".
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600) // #nosec G304 -- operator data root
	if err != nil {
		return nil, fmt.Errorf("open write lease: %w", err)
	}
	if err := lockFileExclusive(f); err != nil {
		_ = f.Close()
		if errors.Is(err, errLockWouldBlock) {
			return nil, fmt.Errorf("%w by concurrent durable writer", ErrWriteLeaseHeld)
		}
		return nil, fmt.Errorf("lock write lease: %w", err)
	}
	now := time.Now().UTC()
	token := fmt.Sprintf("%d", now.UnixNano())
	doc := writeLeaseDoc{
		Type:     "gonimbus.index.durable_write_lease.v1",
		IndexSet: indexSetID,
		Holder:   holder,
		Token:    token,
		Acquired: now,
	}
	// Best-effort diagnostic metadata under the held lock.
	_ = f.Truncate(0)
	_, _ = f.Seek(0, 0)
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	_ = enc.Encode(doc)
	_ = f.Sync()

	return &WriteLease{
		f:              f,
		path:           path,
		segmentSetRoot: root,
		indexSetID:     indexSetID,
		holder:         holder,
		token:          token,
	}, nil
}

// CheckWriteLeaseAvailable probes an existing durable writer lock without
// creating or rewriting operator state. It is suitable for read-only planning;
// callers that mutate must still acquire and hold a WriteLease.
func CheckWriteLeaseAvailable(segmentSetRoot string) error {
	root, err := canonicalizeSegmentSetRoot(segmentSetRoot)
	if err != nil {
		return err
	}
	path := filepath.Join(root, writeLeaseFileName)
	f, err := os.OpenFile(path, os.O_RDWR, 0) // #nosec G304 -- package-owned lock under the bound segment root
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open write lease for availability probe: %w", err)
	}
	defer func() { _ = f.Close() }()
	if err := lockFileExclusive(f); err != nil {
		if errors.Is(err, errLockWouldBlock) {
			return fmt.Errorf("%w by concurrent durable writer", ErrWriteLeaseHeld)
		}
		return fmt.Errorf("probe write lease: %w", err)
	}
	return unlockFile(f)
}

// AssertHeld verifies the package-owned write lease is still held (FD live).
func (l *WriteLease) AssertHeld() error {
	if l == nil || l.released || l.f == nil {
		return ErrWriteLeaseLost
	}
	if _, err := l.f.Stat(); err != nil {
		return fmt.Errorf("%w: %v", ErrWriteLeaseLost, err)
	}
	return nil
}

// AssertHeldFor verifies the lease is held and authorizes advancing latestPath
// for indexSetID. latestPath's parent directory must be the lease segment-set root.
func (l *WriteLease) AssertHeldFor(indexSetID, latestPath string) error {
	if err := l.AssertHeld(); err != nil {
		return err
	}
	indexSetID = strings.TrimSpace(indexSetID)
	if indexSetID == "" || indexSetID != l.indexSetID {
		return fmt.Errorf("%w: lease index_set_id %q does not match publish %q", ErrWriteLeaseScope, l.indexSetID, indexSetID)
	}
	latestPath = strings.TrimSpace(latestPath)
	if latestPath == "" {
		return fmt.Errorf("%w: latest path is required", ErrWriteLeaseScope)
	}
	absLatest, err := filepath.Abs(latestPath)
	if err != nil {
		return fmt.Errorf("%w: resolve latest path: %v", ErrWriteLeaseScope, err)
	}
	latestRoot := filepath.Clean(filepath.Dir(absLatest))
	if latestRoot != l.segmentSetRoot {
		return fmt.Errorf("%w: lease root %q does not authorize latest under %q", ErrWriteLeaseScope, l.segmentSetRoot, latestRoot)
	}
	return nil
}

// SegmentSetRoot returns the canonical segment-set root bound at acquisition.
func (l *WriteLease) SegmentSetRoot() string {
	if l == nil {
		return ""
	}
	return l.segmentSetRoot
}

// IndexSetID returns the index-set ID bound at acquisition.
func (l *WriteLease) IndexSetID() string {
	if l == nil {
		return ""
	}
	return l.indexSetID
}

// Token returns the diagnostic ownership token for this lease instance.
func (l *WriteLease) Token() string {
	if l == nil {
		return ""
	}
	return l.token
}

// Holder returns the holder string supplied at acquire time.
func (l *WriteLease) Holder() string {
	if l == nil {
		return ""
	}
	return l.holder
}

// Release unlocks and closes the lease FD. Idempotent.
func (l *WriteLease) Release() error {
	if l == nil || l.released {
		return nil
	}
	l.released = true
	var first error
	if l.f != nil {
		if err := unlockFile(l.f); err != nil && first == nil {
			first = err
		}
		if err := l.f.Close(); err != nil && first == nil {
			first = err
		}
		l.f = nil
	}
	return first
}

// Renew is a no-op for OS-lock leases (hold is FD lifetime, not wall-clock TTL).
func (l *WriteLease) Renew(_ time.Duration) error {
	return l.AssertHeld()
}
