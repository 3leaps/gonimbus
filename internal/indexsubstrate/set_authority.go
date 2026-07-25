package indexsubstrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// SetAuthorityDirectoryName is the package-owned stable authority directory
	// beside set-specific segment roots. GC must never treat it as a set target.
	SetAuthorityDirectoryName = ".gonimbus-set-authority"
	setAuthorityDocType       = "gonimbus.index.set_authority.v1"
	// setAuthorityAcquireAttempts bounds how many times acquisition re-attempts a
	// pathname whose binding changed under the lock. Each departing owner can
	// invalidate at most one attempt, so a small bound absorbs the completion
	// race while still failing closed against a pathname being churned
	// continuously by something else.
	setAuthorityAcquireAttempts = 3
)

var (
	ErrSetAuthorityHeld  = errors.New("index set authority held")
	ErrSetAuthorityLost  = errors.New("index set authority not held")
	ErrSetAuthorityScope = errors.New("index set authority does not authorize target")
	// errSetAuthorityBindingChanged reports that the lock file's pathname no
	// longer named the locked inode when revalidated under the lock. It is
	// retryable: the common cause is a departing owner removing its own lease.
	errSetAuthorityBindingChanged = errors.New("set authority binding changed after lock")
)

// acquireAfterLockHook is a test-only seam invoked immediately after an
// acquisition attempt takes the OS file lock and before it revalidates the
// pathname binding. It lets a test open the exact window in which a departing
// owner unlinks the pathname, without racing two real processes. Nil in
// production.
var acquireAfterLockHook func()

// SetAuthority is the stable, whole-set cross-process exclusion primitive.
// Its lock file is outside the set-specific segment root, so quarantining that
// root cannot detach the held lock from the canonical writer pathname.
type SetAuthority struct {
	f    *os.File
	path string
	// authorityRoot and lockName are the resolved directory and the entry name
	// bound at acquisition. Release re-opens the directory (never the lock file)
	// so it can unlink through a rooted handle; see Release.
	authorityRoot  string
	lockName       string
	segmentSetRoot string
	indexSetID     string
	holder         string
	released       bool
}

type setAuthorityDoc struct {
	Type       string    `json:"type"`
	IndexSetID string    `json:"index_set_id"`
	Holder     string    `json:"holder"`
	AcquiredAt time.Time `json:"acquired_at"`
}

// SetAuthorityRootForSegmentSet returns the stable authority root shared by
// every set beneath the same segment-cache parent.
func SetAuthorityRootForSegmentSet(segmentSetRoot string) (string, error) {
	root, err := canonicalizeSegmentSetRoot(segmentSetRoot)
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(root), SetAuthorityDirectoryName), nil
}

// AcquireSetAuthority takes whole-set authority before any canonical substrate
// is opened or mutated. Acquisition is non-blocking; process exit releases the
// OS lock automatically.
func AcquireSetAuthority(ctx context.Context, segmentSetRoot, indexSetID, holder string) (*SetAuthority, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	canonicalRoot, err := canonicalizeSegmentSetRoot(segmentSetRoot)
	if err != nil {
		return nil, err
	}
	indexSetID = strings.TrimSpace(indexSetID)
	holder = strings.TrimSpace(holder)
	if err := validateSetAuthorityPart(indexSetID, "index_set_id"); err != nil {
		return nil, err
	}
	if holder == "" {
		return nil, fmt.Errorf("authority holder is required")
	}
	authorityRoot, err := SetAuthorityRootForSegmentSet(canonicalRoot)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(authorityRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create set authority root: %w", err)
	}
	rootInfo, err := os.Lstat(authorityRoot)
	if err != nil || rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, fmt.Errorf("set authority root must be a real directory")
	}
	resolved, err := filepath.EvalSymlinks(authorityRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve set authority root: %w", err)
	}
	// Parent path aliases (notably /var -> /private/var on macOS) converge on
	// the same physical authority. The final authority directory itself was
	// already Lstat-verified as a real directory above.
	authorityRoot = filepath.Clean(resolved)
	root, err := os.OpenRoot(authorityRoot)
	if err != nil {
		return nil, fmt.Errorf("open set authority root: %w", err)
	}
	defer func() { _ = root.Close() }()
	name := indexSetID + ".lock"
	// Retry a binding change. A departing owner removes its lock file under the
	// held lock (see SetAuthority.Release), so an acquirer can lock an inode whose
	// pathname is already gone. That is a completion race, not contention: the
	// right outcome is a fresh acquisition, or ErrSetAuthorityHeld if a live holder
	// is there. Each attempt reopens through the rooted handle and re-locks, so it
	// is a complete, independently validated acquisition.
	//
	// Exhausting the bound returns no authority and reports contention, the same
	// public outcome as a live holder, with the binding-change cause retained
	// inside the error.
	var f *os.File
	for attempt := 1; ; attempt++ {
		var acquireErr error
		f, acquireErr = openAndLockSetAuthority(root, name)
		if acquireErr == nil {
			break
		}
		if errors.Is(acquireErr, errSetAuthorityBindingChanged) {
			if attempt < setAuthorityAcquireAttempts {
				continue
			}
			return nil, fmt.Errorf("%w: set authority pathname changed under each of %d acquisition attempts: %w",
				ErrSetAuthorityHeld, setAuthorityAcquireAttempts, acquireErr)
		}
		return nil, acquireErr
	}
	_ = f.Truncate(0)
	_, _ = f.Seek(0, 0)
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	_ = enc.Encode(setAuthorityDoc{Type: setAuthorityDocType, IndexSetID: indexSetID, Holder: holder, AcquiredAt: time.Now().UTC()})
	_ = f.Sync()
	return &SetAuthority{
		f:              f,
		path:           filepath.Join(authorityRoot, name),
		authorityRoot:  authorityRoot,
		lockName:       name,
		segmentSetRoot: canonicalRoot,
		indexSetID:     indexSetID,
		holder:         holder,
	}, nil
}

// openAndLockSetAuthority performs one complete acquisition attempt inside an
// already-rooted authority directory: open-or-create the lock file, verify it is
// a regular file, take the OS file lock non-blocking, then revalidate under the
// lock that the pathname still names the locked inode. It returns the locked,
// bound descriptor, or an error after closing whatever it opened.
//
// The post-lock revalidation is the anti-split gate: locking an inode proves no
// other process holds THAT inode, not that our pathname still refers to it. If
// the entry was removed or replaced in the window, the caller must not treat the
// lock as authority over the pathname — it retries instead.
func openAndLockSetAuthority(root *os.Root, name string) (*os.File, error) {
	f, err := root.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open set authority: %w", err)
	}
	bound, err := f.Stat()
	if err != nil || !bound.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("set authority must be a regular file")
	}
	if err := lockFileExclusive(f); err != nil {
		_ = f.Close()
		if errors.Is(err, errLockWouldBlock) {
			return nil, fmt.Errorf("%w by concurrent set operation", ErrSetAuthorityHeld)
		}
		return nil, fmt.Errorf("lock set authority: %w", err)
	}
	if acquireAfterLockHook != nil {
		acquireAfterLockHook()
	}
	named, err := root.Lstat(name)
	if err != nil || named.Mode()&os.ModeSymlink != 0 || !named.Mode().IsRegular() || !os.SameFile(bound, named) {
		_ = unlockFile(f)
		_ = f.Close()
		return nil, errSetAuthorityBindingChanged
	}
	return f, nil
}

func validateSetAuthorityPart(value, label string) error {
	if value == "" || value == "." || value == ".." || strings.ContainsAny(value, `/\\`) {
		return fmt.Errorf("%s is invalid", label)
	}
	return nil
}

func (a *SetAuthority) AssertHeld() error {
	if a == nil || a.released || a.f == nil {
		return ErrSetAuthorityLost
	}
	bound, err := a.f.Stat()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSetAuthorityLost, err)
	}
	named, err := os.Lstat(a.path)
	if err != nil || named.Mode()&os.ModeSymlink != 0 || !named.Mode().IsRegular() || !os.SameFile(bound, named) {
		return fmt.Errorf("%w: authority pathname no longer names the held lock", ErrSetAuthorityLost)
	}
	return nil
}

func (a *SetAuthority) AssertHeldFor(indexSetID, segmentSetRoot string) error {
	if err := a.AssertHeld(); err != nil {
		return err
	}
	root, err := canonicalizeSegmentSetRoot(segmentSetRoot)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSetAuthorityScope, err)
	}
	if strings.TrimSpace(indexSetID) != a.indexSetID || root != a.segmentSetRoot {
		return fmt.Errorf("%w: held set/root does not match requested authority", ErrSetAuthorityScope)
	}
	return nil
}

// Release drops whole-set authority and removes the holder's own lock file, so a
// completed run leaves no set-authority artifact behind. It is idempotent.
//
// Cleanup goes through unlinkUnderHeldLock on the descriptor this SetAuthority
// already holds:
//
//   - The descriptor is never reopened and the lock never reacquired. An OS file
//     lock belongs to an open-file description, so a second open would contend
//     with this holder.
//   - The unlink happens under the held lock, never after releasing it, so no
//     successor's pathname can be removed.
//
// If the pathname no longer names the held inode, removal is refused and
// reported. The lock is released on every path, including that refusal, so a
// refused cleanup never strands authority. Removal is authorized by possession of
// the held descriptor and the path-to-inode identity check; the holder is
// removing its own artifact, so the document's schema is not re-proved here.
//
// A returned error means the lock was released but the artifact remains. It stays
// visible through the lease surface, with the verdict its content warrants: an
// intact original reports unheld and is reclaimable, a swapped-in replacement
// reports invalid. Untrappable termination bypasses this path and leaves the
// unheld kind.
func (a *SetAuthority) Release() error {
	if a == nil || a.released {
		return nil
	}
	a.released = true
	f := a.f
	a.f = nil
	if f == nil {
		return nil
	}
	// Open the authority DIRECTORY (not the lock file) so the unlink runs through
	// a rooted handle. Opening the directory takes no file lock and so cannot
	// contend with the descriptor we hold.
	root, err := os.OpenRoot(a.authorityRoot)
	if err != nil {
		return errors.Join(releaseHeldDescriptor(f), fmt.Errorf("open set authority root for cleanup: %w", err))
	}
	defer func() { _ = root.Close() }()
	removed, unlinkErr := unlinkUnderHeldLock(root, a.lockName, f)
	if !removed {
		return fmt.Errorf("release set authority: %w", unlinkErr)
	}
	// Removed: cleanup succeeded even if a best-effort unlock/close step warned.
	return unlinkErr
}

// releaseHeldDescriptor unlocks and closes a held authority descriptor without
// removing its pathname. It is the fallback for the one path that cannot reach
// unlinkUnderHeldLock — an unusable authority directory — and exists so the lock
// is dropped even when cleanup is impossible.
func releaseHeldDescriptor(f *os.File) error {
	var first error
	if err := unlockFile(f); err != nil {
		first = err
	}
	if err := f.Close(); err != nil && first == nil {
		first = err
	}
	return first
}

func (a *SetAuthority) SegmentSetRoot() string {
	if a == nil {
		return ""
	}
	return a.segmentSetRoot
}

func (a *SetAuthority) IndexSetID() string {
	if a == nil {
		return ""
	}
	return a.indexSetID
}

func (a *SetAuthority) Path() string {
	if a == nil {
		return ""
	}
	return a.path
}
