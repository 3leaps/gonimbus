package reflowthroughput

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// LandedObjectID is a content-identity record for fullpipe A/B parity
// (relative key + size + content digest). Paths stay relative/sanitized.
type LandedObjectID struct {
	RelKey string `json:"rel_key"`
	Size   int64  `json:"size_bytes"`
	Digest string `json:"content_digest"`
}

// SnapshotDestTree walks destRoot and returns a sorted multiset of landed objects.
// Uses ReadDir recursion (not filepath.Walk) so each open is of a directory entry
// under the harness-owned dest root, avoiding Walk callback path races.
func SnapshotDestTree(destRoot string) ([]LandedObjectID, error) {
	var out []LandedObjectID
	if err := snapshotWalk(destRoot, "", &out); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RelKey != out[j].RelKey {
			return out[i].RelKey < out[j].RelKey
		}
		if out[i].Size != out[j].Size {
			return out[i].Size < out[j].Size
		}
		return out[i].Digest < out[j].Digest
	})
	return out, nil
}

func snapshotWalk(absDir, relDir string, out *[]LandedObjectID) error {
	ents, err := os.ReadDir(absDir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		name := e.Name()
		childAbs := filepath.Join(absDir, name)
		childRel := name
		if relDir != "" {
			childRel = filepath.ToSlash(filepath.Join(relDir, name))
		}
		if e.IsDir() {
			if err := snapshotWalk(childAbs, childRel, out); err != nil {
				return err
			}
			continue
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		body, err := os.ReadFile(childAbs) // #nosec G304 -- harness-owned dest under inv root
		if err != nil {
			return err
		}
		*out = append(*out, LandedObjectID{
			RelKey: childRel,
			Size:   info.Size(),
			Digest: ContentDigest(body),
		})
	}
	return nil
}

// CompareLandedMultisets returns nil when the ordered identity sets are equal.
func CompareLandedMultisets(a, b []LandedObjectID) error {
	if len(a) != len(b) {
		return fmt.Errorf("landed count %d vs %d", len(a), len(b))
	}
	var diffs []string
	for i := range a {
		if a[i] != b[i] {
			diffs = append(diffs, fmt.Sprintf("%s!=%s", a[i].RelKey, b[i].RelKey))
			if len(diffs) >= 5 {
				break
			}
		}
	}
	if len(diffs) > 0 {
		return fmt.Errorf("content multiset mismatch: %s", strings.Join(diffs, "; "))
	}
	return nil
}
