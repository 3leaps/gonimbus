// Package leasefixture plants set-authority lease artifacts and snapshots them,
// so every layer that observes a lease drives the SAME invalid-artifact matrix.
//
// The substrate probe/reclaim, the pkg/indexcoord coordination wrapper, and the
// CLI list/reap adapters each classify these artifacts independently. Three
// hand-written copies of the matrix would drift, and a mapping bug in one layer
// could then survive with the other two still green — which is precisely the
// class of gap this package exists to close. The row list here is the single
// source of truth for what "an artifact that must never be reclaimed" means.
//
// Artifacts are written as raw bytes against the on-disk wire contract (the doc
// type string is spelled out rather than imported) so a fixture stays honest if
// a constant is renamed: the file on disk is what every layer actually reads.
//
// Test-support only. It lives under internal/ and is imported solely by _test.go
// files; nothing here is part of the product surface.
package leasefixture

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
)

// DocType is the on-disk set-authority doc type. Spelled out deliberately: the
// fixture pins the wire contract, not whatever an internal constant happens to
// hold.
const DocType = "gonimbus.index.set_authority.v1"

// FullID returns a canonical index-set ID — the literal prefix "idx_" followed
// by 64 lowercase hex digits — keyed by a single hex seed so tests can mint
// several distinct valid IDs deterministically.
func FullID(seed byte) string {
	return "idx_" + strings.Repeat(string(seed), 64)
}

// UppercaseID returns an "idx_" name whose 64 hex digits are uppercase.
// Canonical Gonimbus IDs are lowercase, so this is a non-canonical name that is
// nonetheless path-safe and hex-decodable — the exact shape that slipped past a
// hex-decoding gate.
func UppercaseID() string {
	return "idx_" + strings.Repeat("A", 64)
}

// Planted describes what a row put on disk.
type Planted struct {
	// Path is the lease artifact (file, directory, or symlink). Empty when the
	// row plants no artifact at all.
	Path string
	// External is a file OUTSIDE the lease artifact that must remain untouched —
	// a symlink's target. Empty when the row has none.
	External string
}

// Row is one artifact class that every layer must classify as invalid and must
// never reclaim.
type Row struct {
	// Name is the subtest name; identical across layers so a matrix gap is
	// obvious when comparing test output.
	Name string
	// TargetID is the ID a caller probes/reclaims with. Empty means "the
	// canonical ID the row was planted for".
	TargetID string
	// NeedsSymlink marks rows a platform may not support.
	NeedsSymlink bool
	// Plant writes the artifact under authorityRoot for the canonical id.
	Plant func(authorityRoot, id string) (Planted, error)
}

// Target returns the ID a caller should probe/reclaim with for this row.
func (r Row) Target(canonicalID string) string {
	if r.TargetID != "" {
		return r.TargetID
	}
	return canonicalID
}

// InvalidRows is the canonical matrix: every artifact class that must classify
// invalid (or skipped) and must survive unchanged at every layer.
//
// Rows split into three kinds, all of which must reach the same verdict:
//   - doc-content rows: a correctly-named lock whose doc is wrong
//   - name rows: a target ID that is not a canonical lease name at all
//   - non-regular rows: a lock pathname that is not a regular file
//
// SINGLE DEFECT PER ROW is a hard invariant here. Everything about a row except
// the one property it is named for must be valid, or the row can stay green off
// an unrelated gate and prove nothing — which is exactly how an earlier revision
// of the non-canonical row (planted with a malformed body) went hollow.
//
// The non-regular rows are the deliberate exception to reading a row as evidence
// about one gate: a directory or symlink lock is independently rejected by the
// explicit artifact-type check, by rooted path resolution refusing to traverse a
// symlink, and by the under-lock binding check. That redundancy is defense in
// depth, so those rows assert the outcome rather than any single gate.
func InvalidRows() []Row {
	return []Row{
		{Name: "malformed_json", Plant: plantMalformed},
		{Name: "wrong_doc_type", Plant: plantWrongType},
		{Name: "exact_id_mismatch", Plant: plantMismatchedID},
		{Name: "whitespace_padded_id", Plant: plantWhitespacePaddedID},
		{Name: "oversized_valid_prefix", Plant: plantOversized},
		{Name: "noncanonical_name", TargetID: NonCanonicalName, Plant: plantNonCanonicalName},
		{Name: "uppercase_hex_id", TargetID: UppercaseID(), Plant: plantUppercaseName},
		{Name: "directory_artifact", Plant: plantDirectory},
		{Name: "symlink_artifact", NeedsSymlink: true, Plant: plantSymlink},
	}
}

// PlantValidUnheld writes a well-formed authority doc for id: the positive
// control every layer must still reclaim.
func PlantValidUnheld(authorityRoot, id string) (Planted, error) {
	return writeDoc(authorityRoot, id, docFor(DocType, id, "index-build-valid"))
}

func plantMalformed(authorityRoot, id string) (Planted, error) {
	return writeRaw(authorityRoot, id+".lock", []byte("{not-json"))
}

func plantWrongType(authorityRoot, id string) (Planted, error) {
	return writeDoc(authorityRoot, id, docFor("gonimbus.index.wrong_type.v1", id, "index-build-wrongtype"))
}

func plantMismatchedID(authorityRoot, id string) (Planted, error) {
	other := FullID('b')
	if other == id {
		other = FullID('c')
	}
	return writeDoc(authorityRoot, id, docFor(DocType, other, "index-build-mismatch"))
}

func plantWhitespacePaddedID(authorityRoot, id string) (Planted, error) {
	// Byte-exact identity: a padded ID is not the value SetAuthority emits, so it
	// must never authorize a removal even though it "looks right" once trimmed.
	return writeDoc(authorityRoot, id, docFor(DocType, " "+id+" ", "index-build-padded"))
}

func plantOversized(authorityRoot, id string) (Planted, error) {
	body, err := json.Marshal(docFor(DocType, id, "index-build-oversized"))
	if err != nil {
		return Planted{}, err
	}
	// A valid JSON prefix, padded past the real substrate bound, then garbage: a
	// reader that truncates at the bound would parse the prefix and call it valid.
	padded := append(body, bytes.Repeat([]byte(" "), indexsubstrate.MaxSetAuthorityDocBytes+16)...)
	padded = append(padded, []byte("garbage-past-the-cap")...)
	return writeRaw(authorityRoot, id+".lock", padded)
}

// NonCanonicalName is a path-safe lease name that is not a canonical index-set
// ID. It is the sole defect in its row.
const NonCanonicalName = "not-an-idx"

func plantNonCanonicalName(authorityRoot, _ string) (Planted, error) {
	// SINGLE DEFECT BY CONSTRUCTION: correct doc type, and an embedded ID that
	// exactly equals the filename ID. Every gate except the canonical-name gate
	// passes, so this row proves that gate and nothing else — weaken the name
	// rejection and the artifact classifies unheld, failing the row. (A malformed
	// body here would keep the row green off the JSON gate and prove nothing.)
	return writeDoc(authorityRoot, NonCanonicalName, docFor(DocType, NonCanonicalName, "index-build-noncanonical"))
}

func plantUppercaseName(authorityRoot, _ string) (Planted, error) {
	upper := UppercaseID()
	return writeDoc(authorityRoot, upper, docFor(DocType, upper, "index-build-uppercase"))
}

func plantDirectory(authorityRoot, id string) (Planted, error) {
	if err := os.MkdirAll(authorityRoot, 0o700); err != nil {
		return Planted{}, err
	}
	path := filepath.Join(authorityRoot, id+".lock")
	if err := os.Mkdir(path, 0o700); err != nil {
		return Planted{}, err
	}
	return Planted{Path: path}, nil
}

func plantSymlink(authorityRoot, id string) (Planted, error) {
	if err := os.MkdirAll(authorityRoot, 0o700); err != nil {
		return Planted{}, err
	}
	// The target is a real regular file holding a well-formed doc: if any layer
	// followed the link it would see a reclaimable lease and delete the target.
	external := filepath.Join(authorityRoot, "symlink-target-"+id[:12])
	body, err := json.Marshal(docFor(DocType, id, "index-build-symlink-target"))
	if err != nil {
		return Planted{}, err
	}
	if err := os.WriteFile(external, body, 0o600); err != nil {
		return Planted{}, err
	}
	path := filepath.Join(authorityRoot, id+".lock")
	if err := os.Symlink(external, path); err != nil {
		return Planted{}, err
	}
	return Planted{Path: path, External: external}, nil
}

func docFor(docType, indexSetID, holder string) map[string]any {
	return map[string]any{
		"type":         docType,
		"index_set_id": indexSetID,
		"holder":       holder,
		"acquired_at":  time.Now().UTC().Format(time.RFC3339Nano),
	}
}

func writeDoc(authorityRoot, name string, doc map[string]any) (Planted, error) {
	body, err := json.Marshal(doc)
	if err != nil {
		return Planted{}, err
	}
	return writeRaw(authorityRoot, name+".lock", body)
}

func writeRaw(authorityRoot, name string, body []byte) (Planted, error) {
	if err := os.MkdirAll(authorityRoot, 0o700); err != nil {
		return Planted{}, err
	}
	path := filepath.Join(authorityRoot, name)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		return Planted{}, err
	}
	return Planted{Path: path}, nil
}

// Snapshot records everything about an artifact that a mutation would disturb.
// It uses Lstat throughout, so a symlink is captured as the link itself.
type Snapshot struct {
	Path     string
	Exists   bool
	Mode     os.FileMode
	Size     int64
	ModTime  time.Time
	Content  []byte // regular files only
	LinkDest string // symlinks only
}

// TakeSnapshot captures the artifact at path. A missing path is captured as
// Exists=false, so "it must stay missing" is assertable too.
func TakeSnapshot(path string) (Snapshot, error) {
	snap := Snapshot{Path: path}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return snap, nil
	}
	if err != nil {
		return snap, err
	}
	snap.Exists = true
	snap.Mode = info.Mode()
	snap.Size = info.Size()
	snap.ModTime = info.ModTime()
	switch {
	case info.Mode()&os.ModeSymlink != 0:
		dest, readErr := os.Readlink(path)
		if readErr != nil {
			return snap, readErr
		}
		snap.LinkDest = dest
	case info.Mode().IsRegular():
		content, readErr := os.ReadFile(path) // #nosec G304 -- test fixture path owned by the caller's temp dir
		if readErr != nil {
			return snap, readErr
		}
		snap.Content = content
	}
	return snap, nil
}

// AssertUnchanged reports the first difference between the snapshot and the
// artifact as it stands now, or nil when nothing moved.
func AssertUnchanged(before Snapshot) error {
	after, err := TakeSnapshot(before.Path)
	if err != nil {
		return fmt.Errorf("re-snapshot %s: %w", before.Path, err)
	}
	switch {
	case before.Exists != after.Exists:
		return fmt.Errorf("%s: existence changed (before=%v after=%v)", before.Path, before.Exists, after.Exists)
	case !before.Exists:
		return nil
	case before.Mode != after.Mode:
		return fmt.Errorf("%s: mode changed (%v -> %v)", before.Path, before.Mode, after.Mode)
	case before.Size != after.Size:
		return fmt.Errorf("%s: size changed (%d -> %d)", before.Path, before.Size, after.Size)
	case !before.ModTime.Equal(after.ModTime):
		return fmt.Errorf("%s: mtime changed (%s -> %s)", before.Path, before.ModTime, after.ModTime)
	case before.LinkDest != after.LinkDest:
		return fmt.Errorf("%s: symlink target changed (%q -> %q)", before.Path, before.LinkDest, after.LinkDest)
	case !bytes.Equal(before.Content, after.Content):
		return fmt.Errorf("%s: content changed", before.Path)
	}
	return nil
}
