package reflowthroughput

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// ManifestEntry is one object in the ordered corpus identity set.
type ManifestEntry struct {
	RelativeKey   string `json:"relative_key"`
	SizeBytes     int64  `json:"size_bytes"`
	ContentDigest string `json:"content_digest"`
}

// Manifest is the canonical corpus identity bound into every report envelope.
type Manifest struct {
	RecipeVersion string `json:"recipe_version"`
	Seed          int64  `json:"seed"`
	ObjectCount   int    `json:"object_count"`
	// SizeBytes and Partitions are the resolved recipe knobs, recorded so a
	// reader can name the exact corpus shape that produced the evidence.
	// Partitions in particular cannot be reconstructed from the entry digest.
	SizeBytes     int             `json:"size_bytes"`
	Partitions    int             `json:"partitions"`
	TotalBytes    int64           `json:"total_bytes"`
	SizeHistogram map[string]int  `json:"size_histogram"`
	Entries       []ManifestEntry `json:"entries"`
	// Digest is SHA-256 over ordered (relative_key, size, content_digest) lines.
	Digest string `json:"digest"`
}

// BuildManifest constructs and digests a manifest from ordered entries.
func BuildManifest(recipe Recipe, entries []ManifestEntry) (Manifest, error) {
	if err := recipe.Validate(); err != nil {
		return Manifest{}, err
	}
	if len(entries) != recipe.ObjectCount {
		return Manifest{}, fmt.Errorf("entry count %d != recipe object_count %d", len(entries), recipe.ObjectCount)
	}
	hist := map[string]int{}
	var total int64
	for _, e := range entries {
		if e.RelativeKey == "" || e.ContentDigest == "" {
			return Manifest{}, fmt.Errorf("manifest entry missing key or content digest")
		}
		total += e.SizeBytes
		bucket := sizeBucket(e.SizeBytes)
		hist[bucket]++
	}
	m := Manifest{
		RecipeVersion: recipe.Version,
		Seed:          recipe.Seed,
		ObjectCount:   recipe.ObjectCount,
		SizeBytes:     recipe.SizeBytes,
		Partitions:    recipe.Partitions,
		TotalBytes:    total,
		SizeHistogram: hist,
		Entries:       append([]ManifestEntry(nil), entries...),
	}
	m.Digest = DigestManifestEntries(m.Entries)
	return m, nil
}

// DigestManifestEntries returns the ordered entry digest used for envelope identity.
func DigestManifestEntries(entries []ManifestEntry) string {
	h := sha256.New()
	for _, e := range entries {
		_, _ = fmt.Fprintf(h, "%s\t%d\t%s\n", e.RelativeKey, e.SizeBytes, e.ContentDigest)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ContentDigest returns SHA-256 hex of object bytes.
func ContentDigest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func sizeBucket(n int64) string {
	switch {
	case n < 256:
		return "lt_256"
	case n < 1024:
		return "256_1k"
	case n < 4096:
		return "1k_4k"
	case n < 65536:
		return "4k_64k"
	default:
		return "ge_64k"
	}
}

// CompactManifest is the report-safe subset (no path material).
type CompactManifest struct {
	RecipeVersion string         `json:"recipe_version"`
	Seed          int64          `json:"seed"`
	ObjectCount   int            `json:"object_count"`
	SizeBytes     int            `json:"size_bytes"`
	Partitions    int            `json:"partitions"`
	TotalBytes    int64          `json:"total_bytes"`
	SizeHistogram map[string]int `json:"size_histogram"`
	Digest        string         `json:"digest"`
}

// Compact returns the allowlisted report view of the manifest.
func (m Manifest) Compact() CompactManifest {
	hist := make(map[string]int, len(m.SizeHistogram))
	for k, v := range m.SizeHistogram {
		hist[k] = v
	}
	return CompactManifest{
		RecipeVersion: m.RecipeVersion,
		Seed:          m.Seed,
		ObjectCount:   m.ObjectCount,
		SizeBytes:     m.SizeBytes,
		Partitions:    m.Partitions,
		TotalBytes:    m.TotalBytes,
		SizeHistogram: hist,
		Digest:        m.Digest,
	}
}

// SortedHistogramKeys returns histogram keys in stable order (for tests).
func SortedHistogramKeys(h map[string]int) []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// MarshalIndentJSON is a helper for golden-style tests.
func MarshalIndentJSON(v any) (string, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b) + "\n", nil
}

// AssertNoSensitiveTokens fails if s contains obvious path/bucket/credential tells.
// Used by the report sterility sentinel.
func AssertNoSensitiveTokens(s string, forbidden []string) error {
	lower := strings.ToLower(s)
	for _, f := range forbidden {
		if f == "" {
			continue
		}
		if strings.Contains(lower, strings.ToLower(f)) {
			return fmt.Errorf("report contains forbidden token %q", f)
		}
	}
	return nil
}
