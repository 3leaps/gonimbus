package indexsubstrate

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// MaxSetAuthorityDocBytes bounds a well-formed authority doc, which is tiny. A
// file beyond this size is treated as invalid rather than parsed from a
// truncated prefix. It is exported so a fixture can plant a provably-oversized
// artifact against the real bound instead of a drifting copy of it.
const MaxSetAuthorityDocBytes = 1 << 16

// LeaseVerdict is the non-mutating lock-state verdict for a set-authority lock
// file. It is decided solely by a non-blocking lock probe on the existing file
// (never by a job record, PID, or holder string).
type LeaseVerdict string

const (
	// LeaseHeld means a live process holds the advisory lock right now: a
	// non-blocking exclusive lock attempt would block. Never reclaim a held lease.
	LeaseHeld LeaseVerdict = "held"
	// LeaseUnheld means the lock file exists with a well-formed authority doc but
	// no process holds the advisory lock: dead-holder residue, safe to reclaim.
	LeaseUnheld LeaseVerdict = "unheld"
	// LeaseMissing means no lock file exists for the index set.
	LeaseMissing LeaseVerdict = "missing"
	// LeaseInvalid is the indeterminate bucket: the artifact is not a regular file,
	// its binding changed under the probe, or its authority doc failed to parse.
	// Recovery treats it as needing operator attention, not blind removal.
	LeaseInvalid LeaseVerdict = "invalid"
)

// SetAuthorityLease is the read-only report for one set-authority lock file.
// Holder/AcquiredAt are attribution copied from the on-disk doc for operator
// clarity; they are never inputs to the held/unheld verdict.
type SetAuthorityLease struct {
	IndexSetID string
	Path       string
	Verdict    LeaseVerdict
	Holder     string
	AcquiredAt time.Time
	DocType    string
	ModTime    time.Time
	// Detail carries the reason for an invalid/indeterminate verdict, or an
	// attribution note. It never affects the verdict.
	Detail string
}

// isFullIndexSetID reports whether id is a canonical index-set ID: the literal
// prefix "idx_" followed by exactly 64 lowercase hex digits. Canonical Gonimbus
// IDs are lowercase, so uppercase hex is rejected — this is the single shared
// name gate applied identically at the probe, enumerate, and reclaim boundaries
// so those layers never disagree on what is a lease artifact.
func isFullIndexSetID(id string) bool {
	if !strings.HasPrefix(id, "idx_") || len(id) != len("idx_")+64 {
		return false
	}
	for _, c := range id[len("idx_"):] {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// resolveAuthorityRootForRead resolves an authority root for a read-only or
// maintenance operation without creating it. A missing root returns os.ErrNotExist
// so callers can map it to a "missing" verdict; a symlinked or non-directory root
// fails closed.
func resolveAuthorityRootForRead(authorityRoot string) (string, error) {
	authorityRoot = strings.TrimSpace(authorityRoot)
	if authorityRoot == "" {
		return "", fmt.Errorf("authority root is required")
	}
	info, err := os.Lstat(authorityRoot)
	if err != nil {
		return "", err // os.ErrNotExist flows through untouched
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("set authority root must be a real directory")
	}
	resolved, err := filepath.EvalSymlinks(authorityRoot)
	if err != nil {
		return "", fmt.Errorf("resolve set authority root: %w", err)
	}
	return filepath.Clean(resolved), nil
}

// openBoundSetAuthority opens an existing set-authority lock file inside a
// freshly resolved, rooted authority directory and binds the returned fd to the
// named inode. It opens read-only (no O_CREATE, no O_TRUNC, no write access):
// advisory locking needs only read access, and reclaim unlinks through the root
// handle rather than the fd, so no path here ever writes the lock file. The
// caller decides whether to lock, read, unlink, or simply close. On success the
// caller owns both the *os.Root and the *os.File and must close them. A missing
// lock file returns os.ErrNotExist.
//
// This is the shared open+bind seam under both the read-only probe and the
// mutating reclaim path; owner-cleanup instead calls unlinkUnderHeldLock on the
// descriptor it already holds.
func openBoundSetAuthority(resolvedRoot, name string) (root *os.Root, f *os.File, named os.FileInfo, err error) {
	root, err = os.OpenRoot(resolvedRoot)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open set authority root: %w", err)
	}
	named, err = root.Lstat(name)
	if err != nil {
		_ = root.Close()
		return nil, nil, nil, err // os.ErrNotExist flows through
	}
	if named.Mode()&os.ModeSymlink != 0 || !named.Mode().IsRegular() {
		_ = root.Close()
		return nil, nil, nil, fmt.Errorf("set authority lock must be a regular non-symlink file")
	}
	f, err = root.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		_ = root.Close()
		return nil, nil, nil, fmt.Errorf("open set authority lock: %w", err)
	}
	bound, err := f.Stat()
	if err != nil || !bound.Mode().IsRegular() || !os.SameFile(named, bound) {
		_ = f.Close()
		_ = root.Close()
		return nil, nil, nil, fmt.Errorf("set authority lock binding changed during open")
	}
	return root, f, named, nil
}

// readSetAuthorityDoc reads and parses the on-disk authority doc for attribution
// only. Read failures and malformed docs are reported to the caller; they never
// change the lock verdict.
func readSetAuthorityDoc(f *os.File) (setAuthorityDoc, error) {
	var doc setAuthorityDoc
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return doc, err
	}
	// Read one byte past the bound so an oversized file is detected rather than
	// silently truncated: a valid JSON prefix followed by garbage beyond the cap
	// must not be accepted as a well-formed doc.
	data, err := io.ReadAll(io.LimitReader(f, MaxSetAuthorityDocBytes+1))
	if err != nil {
		return doc, err
	}
	if len(data) > MaxSetAuthorityDocBytes {
		return doc, fmt.Errorf("authority doc exceeds %d bytes", MaxSetAuthorityDocBytes)
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return doc, err
	}
	return doc, nil
}

// ProbeSetAuthorityLease returns the non-mutating lock-state verdict for one
// index set's authority lock within authorityRoot. It opens the existing file
// read-only and never truncates or rewrites it: the probe must never call
// AcquireSetAuthority, which rewrites the holder doc and would destroy the very
// provenance being reported. The lock file and every byte of its doc are
// identical before and after the probe.
//
// Refusal contract (ADR-0007 typed lock-state): a refusal grounded in the
// artifact — a malformed or non-canonical target name, a lock pathname that is
// not a regular file, or a doc failing the schema/scope gate — returns
// LeaseInvalid, because the artifact was examined and judged. A failure of the
// surrounding infrastructure — an unusable authorityRoot, or an unexpected
// (non-would-block) lock error — returns an error with NO verdict claimed: no
// judgement was reached, and inventing one would be indistinguishable from an
// observed verdict. Callers must therefore check the error before trusting the
// verdict on the infrastructure paths.
func ProbeSetAuthorityLease(authorityRoot, indexSetID string) (SetAuthorityLease, error) {
	indexSetID = strings.TrimSpace(indexSetID)
	if err := validateSetAuthorityPart(indexSetID, "index_set_id"); err != nil {
		// An artifact-grounded refusal reports its verdict: we judged the target
		// itself, so the caller gets that judgement rather than a zero value.
		// Infrastructure failures below (unusable root, unexpected lock error) do
		// the opposite and claim no state at all — see the doc comment above.
		return SetAuthorityLease{IndexSetID: indexSetID, Verdict: LeaseInvalid, Detail: err.Error()}, err
	}
	// A path-safe but non-canonical name is not a lease artifact: report it as
	// invalid rather than probing it as one, so the direct library, enumerate,
	// and CLI paths all agree.
	if !isFullIndexSetID(indexSetID) {
		return SetAuthorityLease{IndexSetID: indexSetID, Verdict: LeaseInvalid, Detail: "non-canonical index set id"}, nil
	}
	name := indexSetID + ".lock"
	lease := SetAuthorityLease{IndexSetID: indexSetID}

	resolved, err := resolveAuthorityRootForRead(authorityRoot)
	if errors.Is(err, os.ErrNotExist) {
		lease.Verdict = LeaseMissing
		return lease, nil
	}
	if err != nil {
		return SetAuthorityLease{}, err
	}
	lease.Path = filepath.Join(resolved, name)

	root, f, named, err := openBoundSetAuthority(resolved, name)
	if errors.Is(err, os.ErrNotExist) {
		lease.Verdict = LeaseMissing
		return lease, nil
	}
	if err != nil {
		lease.Verdict = LeaseInvalid
		lease.Detail = err.Error()
		return lease, nil
	}
	defer func() { _ = root.Close() }()
	defer func() { _ = f.Close() }()
	lease.ModTime = named.ModTime()

	// Attribution only — parsed before any lock attempt, never gates the verdict.
	doc, docErr := readSetAuthorityDoc(f)
	if docErr == nil {
		lease.Holder = doc.Holder
		lease.AcquiredAt = doc.AcquiredAt
		lease.DocType = doc.Type
	}

	lockErr := lockFileExclusive(f)
	if lockErr != nil {
		if errors.Is(lockErr, errLockWouldBlock) {
			lease.Verdict = LeaseHeld
			return lease, nil
		}
		return SetAuthorityLease{}, fmt.Errorf("probe set authority lock: %w", lockErr)
	}
	// We momentarily hold the lock, so no live holder exists. Release at once —
	// the probe must leave no residual hold.
	_ = unlockFile(f)

	// Re-verify the pathname still names the probed inode; a swap under the probe
	// is indeterminate, not "unheld".
	after, err := root.Lstat(name)
	if err != nil || after.Mode()&os.ModeSymlink != 0 || !after.Mode().IsRegular() || !os.SameFile(named, after) {
		lease.Verdict = LeaseInvalid
		lease.Detail = "authority lock binding changed during probe"
		return lease, nil
	}

	if ok, detail := validSetAuthorityDoc(doc, docErr, indexSetID); !ok {
		lease.Verdict = LeaseInvalid
		lease.Detail = detail
		return lease, nil
	}
	lease.Verdict = LeaseUnheld
	return lease, nil
}

// validSetAuthorityDoc reports whether a parsed authority doc is a well-formed,
// exactly-scoped record for indexSetID. The advisory lock decides held vs
// unheld; it is not proof of artifact identity. This is the single schema/scope
// gate shared by the read-only probe and the mutating reclaimer so their
// invalid-vs-unheld verdicts can never diverge: a valid record needs the right
// doc type AND an index_set_id that exactly matches the lease name.
func validSetAuthorityDoc(doc setAuthorityDoc, docErr error, indexSetID string) (bool, string) {
	if docErr != nil {
		return false, fmt.Sprintf("authority doc unreadable: %v", docErr)
	}
	if doc.Type != setAuthorityDocType {
		return false, fmt.Sprintf("unexpected authority doc type %q", doc.Type)
	}
	// Byte-exact comparison — no normalization on persisted authority proof. A
	// whitespace-padded or otherwise non-canonical embedded ID is not the exact
	// value SetAuthority emits and must never authorize a removal.
	if doc.IndexSetID != indexSetID {
		return false, fmt.Sprintf("authority doc index_set_id %q does not match lease name %q", doc.IndexSetID, indexSetID)
	}
	return true, ""
}

// EnumerateSetAuthorityLeases probes every set-authority lock file under
// authorityRoot and returns one report per artifact, sorted by index-set ID for
// deterministic output. A missing authority root is not an error — it yields an
// empty slice. Enumeration is fully non-mutating.
func EnumerateSetAuthorityLeases(authorityRoot string) ([]SetAuthorityLease, error) {
	resolved, err := resolveAuthorityRootForRead(authorityRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(resolved)
	if err != nil {
		return nil, fmt.Errorf("read set authority root: %w", err)
	}
	leases := make([]SetAuthorityLease, 0, len(entries))
	for _, entry := range entries {
		fileName := entry.Name()
		// Classify by artifact class (.lock suffix) first, so a canonical-looking
		// symlink/directory lock is surfaced for operator attention rather than
		// silently dropped by a pre-filter.
		if !strings.HasSuffix(fileName, ".lock") {
			continue
		}
		id := strings.TrimSuffix(fileName, ".lock")
		fullPath := filepath.Join(resolved, fileName)
		invalid := func(detail string) SetAuthorityLease {
			lease := SetAuthorityLease{IndexSetID: id, Path: fullPath, Verdict: LeaseInvalid, Detail: detail}
			if info, statErr := entry.Info(); statErr == nil {
				lease.ModTime = info.ModTime()
			}
			return lease
		}
		// A .lock entry that is not a regular file (symlink, directory, device) is
		// residue we report as invalid WITHOUT following it — never a probe target.
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			leases = append(leases, invalid("set authority lock is not a regular file"))
			continue
		}
		if !isFullIndexSetID(id) {
			// Foreign artifact in a package-owned directory: report honestly rather
			// than probe it as a lease or silently drop it.
			leases = append(leases, invalid("unrecognized set authority artifact name"))
			continue
		}
		lease, err := ProbeSetAuthorityLease(resolved, id)
		if err != nil {
			// A single anomalous lease must not take down the read-only listing:
			// degrade it to an indeterminate entry carrying the probe error.
			lease = invalid(err.Error())
		}
		leases = append(leases, lease)
	}
	sort.Slice(leases, func(i, j int) bool { return leases[i].IndexSetID < leases[j].IndexSetID })
	return leases, nil
}
