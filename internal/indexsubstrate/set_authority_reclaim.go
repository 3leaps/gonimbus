package indexsubstrate

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// reclaimAfterLockHook is a test-only seam invoked immediately after the reclaim
// path acquires the advisory lock and before it re-reads the doc for validation.
// It lets a test prove the decisive read/validation happens under the lock:
// mutating the doc here must change the outcome, which it can only do if the
// validation runs after acquisition. Nil in production.
var reclaimAfterLockHook func()

// ReclaimResult reports the outcome of a reclaim attempt for one set-authority
// lock file.
type ReclaimResult struct {
	IndexSetID string
	Path       string
	Verdict    LeaseVerdict
	// Reclaimed is true only when a provably-unheld lock file was removed.
	Reclaimed bool
	// Holder is attribution copied from the on-disk doc before any action.
	Holder string
}

// unlinkUnderHeldLock removes name while the caller's descriptor still holds the
// advisory lock, then unlocks and closes that descriptor. It is the single
// post-acquisition boundary shared by the reclaim path and (in a later change)
// completion-path owner-cleanup.
//
// Two invariants make it safe:
//   - It unlinks *under* the held lock, never after releasing it. Unlinking
//     after release races a successor's acquire: the successor could recreate
//     and lock the same pathname before the unlink lands, and the unlink would
//     then delete the successor's live pathname — splitting authority across
//     inodes. Holding the lock across the unlink makes that window impossible.
//   - It revalidates the rooted path<->inode binding under the lock before
//     removing. If the pathname no longer names the locked inode (it was
//     swapped), it refuses to remove — it will not delete some other file that
//     happens to occupy the name now — and still releases its own descriptor.
//
// Cross-platform note: on Unix the directory entry disappears at Remove. On
// Windows, Go opens the descriptor with FILE_SHARE_DELETE, so Remove succeeds
// while the handle is open but the name may linger in delete-pending state until
// last close. The anti-split guarantee does not depend on the exact moment the
// entry vanishes — it rests on the lock still being held across the unlink, which
// blocks any successor from acquiring until this descriptor closes and the name
// is gone. A successor then recreates a fresh inode, never a split.
//
// The caller owns root and must close it; unlinkUnderHeldLock always consumes
// lockedFd (unlocks and closes it) on every path.
// It returns removed=true once the directory entry is gone. A non-nil error with
// removed=true means the unlink succeeded but a best-effort unlock/close step
// warned — the removal still stands (and the fd's lock is dropped when it
// closes). removed=false means nothing was removed.
func unlinkUnderHeldLock(root *os.Root, name string, lockedFd *os.File) (removed bool, err error) {
	release := func() {
		_ = unlockFile(lockedFd)
		_ = lockedFd.Close()
	}

	named, err := root.Lstat(name)
	if err != nil || named.Mode()&os.ModeSymlink != 0 || !named.Mode().IsRegular() {
		release()
		return false, fmt.Errorf("refuse to unlink: set authority pathname no longer names a regular file: %v", err)
	}
	bound, err := lockedFd.Stat()
	if err != nil || !os.SameFile(named, bound) {
		release()
		return false, fmt.Errorf("refuse to unlink: set authority pathname no longer names the held lock")
	}
	// Unlink while still holding the lock.
	if err := root.Remove(name); err != nil {
		release()
		return false, fmt.Errorf("remove set authority lock: %w", err)
	}
	// The directory entry is gone; now drop the lock on the (unlinked) inode and
	// close. A successor acquiring the pathname creates a fresh inode — no split.
	// These are best-effort cleanup: the removal already succeeded.
	var cleanupErr error
	if err := unlockFile(lockedFd); err != nil {
		cleanupErr = fmt.Errorf("unlock removed set authority lock: %w", err)
	}
	if err := lockedFd.Close(); err != nil && cleanupErr == nil {
		cleanupErr = fmt.Errorf("close removed set authority lock: %w", err)
	}
	return true, cleanupErr
}

// ReclaimUnheldSetAuthorityLease removes a provably-unheld set-authority lock
// file for indexSetID. It opens the existing file (never creating it), takes the
// advisory lock non-blocking, and only if that succeeds — proving no live holder
// exists — unlinks the file under the held lock. A held lease is refused with
// ErrSetAuthorityHeld and its holder reported; the file is never touched. A
// missing lease is reported as not-reclaimed with a nil error, so reclaim is
// idempotent.
//
// The held/unheld decision is made solely by the lock probe. The on-disk doc,
// any job record, and PID liveness are attribution only and never authorize a
// removal.
//
// Refusal contract (ADR-0007 typed lock-state), identical to the probe's: an
// artifact-grounded refusal — non-canonical target name, non-regular lock
// pathname, or a doc failing the schema/scope gate — returns LeaseInvalid with
// its error. An infrastructure failure — unusable authorityRoot, an unexpected
// (non-would-block) lock error, or a failed unlink — returns an error claiming
// NO artifact state. A held lease returns LeaseHeld with ErrSetAuthorityHeld.
func ReclaimUnheldSetAuthorityLease(authorityRoot, indexSetID string) (ReclaimResult, error) {
	indexSetID = strings.TrimSpace(indexSetID)
	// Both target-name rejections below carry LeaseInvalid rather than a
	// zero-value verdict: the probe judges a non-canonical target invalid, so the
	// mutating path must report the same typed state or the two boundaries
	// disagree about the same artifact. Infrastructure failures further down
	// (unusable root, unexpected lock error, failed unlink) claim no state — see
	// the doc comment above.
	if err := validateSetAuthorityPart(indexSetID, "index_set_id"); err != nil {
		return ReclaimResult{IndexSetID: indexSetID, Verdict: LeaseInvalid}, err
	}
	// The mutating path only ever acts on well-formed canonical lease names. A
	// foreign artifact that happens to sit unheld in the authority dir is
	// operator-attention residue, never a reclaim target — the removal authority
	// stops at names we own.
	if !isFullIndexSetID(indexSetID) {
		return ReclaimResult{IndexSetID: indexSetID, Verdict: LeaseInvalid}, fmt.Errorf("refuse to reclaim non-canonical set authority id %q", indexSetID)
	}
	name := indexSetID + ".lock"
	result := ReclaimResult{IndexSetID: indexSetID}

	resolved, err := resolveAuthorityRootForRead(authorityRoot)
	if errors.Is(err, os.ErrNotExist) {
		result.Verdict = LeaseMissing
		return result, nil
	}
	if err != nil {
		return ReclaimResult{}, err
	}
	result.Path = filepath.Join(resolved, name)

	root, f, _, err := openBoundSetAuthority(resolved, name)
	if errors.Is(err, os.ErrNotExist) {
		result.Verdict = LeaseMissing
		return result, nil
	}
	if err != nil {
		result.Verdict = LeaseInvalid
		return result, fmt.Errorf("reclaim set authority lease: %w", err)
	}
	defer func() { _ = root.Close() }()

	// Holder attribution, best-effort. Where locks are advisory the doc stays
	// readable while another process holds the lock, so a refused reclaim can
	// name the holder. Where they are mandatory the held range is unreadable and
	// attribution is simply absent until the holder exits. Either way this never
	// touches the verdict — the lock decides that alone.
	if doc, docErr := readSetAuthorityDoc(f); docErr == nil {
		result.Holder = doc.Holder
	}

	lockErr := lockFileExclusive(f)
	if lockErr != nil {
		_ = f.Close()
		if errors.Is(lockErr, errLockWouldBlock) {
			result.Verdict = LeaseHeld
			return result, fmt.Errorf("%w: holder %q", ErrSetAuthorityHeld, result.Holder)
		}
		return result, fmt.Errorf("lock set authority for reclaim: %w", lockErr)
	}

	// Lock acquired => no live holder. Re-read and validate the doc UNDER the held
	// lock: the artifact may have changed since enumeration, and the lock proves
	// only that no process holds it — not that it carries the expected identity.
	// Only an exact, well-formed record is reaped; corrupt/type-/scope-mismatched
	// residue is invalid and released WITHOUT unlink, for operator attention. This
	// is the same schema/scope gate the probe applies, so the library and CLI
	// never diverge on invalid-vs-unheld.
	if reclaimAfterLockHook != nil {
		reclaimAfterLockHook()
	}
	doc, docErr := readSetAuthorityDoc(f)
	if doc.Holder != "" {
		result.Holder = doc.Holder
	}
	if ok, detail := validSetAuthorityDoc(doc, docErr, indexSetID); !ok {
		_ = unlockFile(f)
		_ = f.Close()
		result.Verdict = LeaseInvalid
		return result, fmt.Errorf("refuse to reclaim invalid set authority lease: %s", detail)
	}

	// Exact, valid, unheld: unlink under the held lock (consumes f).
	removed, err := unlinkUnderHeldLock(root, name, f)
	if !removed {
		return result, fmt.Errorf("reclaim set authority lease: %w", err)
	}
	// Removed: the reclaim succeeded even if a best-effort cleanup step warned.
	result.Verdict = LeaseUnheld
	result.Reclaimed = true
	return result, nil
}
