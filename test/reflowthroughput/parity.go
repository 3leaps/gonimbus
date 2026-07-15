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
func SnapshotDestTree(destRoot string) ([]LandedObjectID, error) {
	var out []LandedObjectID
	err := filepath.Walk(destRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(destRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		body, err := os.ReadFile(path) // #nosec G304 -- harness-owned dest under inv root
		if err != nil {
			return err
		}
		out = append(out, LandedObjectID{
			RelKey: rel,
			Size:   info.Size(),
			Digest: ContentDigest(body),
		})
		return nil
	})
	if err != nil {
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
