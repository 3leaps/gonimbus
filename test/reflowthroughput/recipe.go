package reflowthroughput

import (
	"fmt"
	"path"
	"time"
)

// RecipeVersion identifies the synthetic corpus shape for envelope comparison.
const RecipeVersion = "reflowthroughput-hive-v1"

// Default smoke sizes stay cheap for local credential-free runs.
const (
	DefaultSmokeSeed       int64 = 42
	DefaultSmokeObjects    int   = 32
	DefaultSmokeSizeBytes  int   = 256
	DefaultSmokePartitions int   = 2
)

// Recipe describes a deterministic synthetic hive-shaped corpus.
// Keys are generic (entity/device/date) with no client keyspace identity.
type Recipe struct {
	Version     string `json:"recipe_version"`
	Seed        int64  `json:"seed"`
	ObjectCount int    `json:"object_count"`
	SizeBytes   int    `json:"size_bytes"`
	Partitions  int    `json:"partitions"`
	// FixedDate is a sterile calendar token embedded in keys (not a field identity).
	FixedDate string `json:"fixed_date"`
}

// DefaultSmokeRecipe returns the cheapest credential-free local smoke recipe.
func DefaultSmokeRecipe() Recipe {
	return Recipe{
		Version:     RecipeVersion,
		Seed:        DefaultSmokeSeed,
		ObjectCount: DefaultSmokeObjects,
		SizeBytes:   DefaultSmokeSizeBytes,
		Partitions:  DefaultSmokePartitions,
		FixedDate:   "2026-01-17",
	}
}

// SaturationRecipe returns a larger local recipe for reflow occupancy checks.
// Object count targets roughly 32× a modest effective ceiling of 8 for smoke-scale
// saturation; operators select scale profiles explicitly.
func SaturationRecipe() Recipe {
	r := DefaultSmokeRecipe()
	r.ObjectCount = 256
	r.SizeBytes = 512
	r.Partitions = 4
	return r
}

// Validate checks recipe bounds.
func (r Recipe) Validate() error {
	if r.Version == "" {
		return fmt.Errorf("recipe_version is required")
	}
	if r.ObjectCount < 1 {
		return fmt.Errorf("object_count must be >= 1")
	}
	if r.SizeBytes < 64 {
		return fmt.Errorf("size_bytes must be >= 64 (probeable head)")
	}
	if r.Partitions < 1 {
		return fmt.Errorf("partitions must be >= 1")
	}
	if r.FixedDate == "" {
		return fmt.Errorf("fixed_date is required")
	}
	return nil
}

// RelativeKey returns the sterile relative key for object index i.
// Shape: entity=%04d/device=%04d/date=<fixed>/object-%08d.xml
func (r Recipe) RelativeKey(i int) string {
	part := i % r.Partitions
	device := (i / r.Partitions) % 16
	return path.Join(
		fmt.Sprintf("entity=%04d", part),
		fmt.Sprintf("device=%04d", device),
		fmt.Sprintf("date=%s", r.FixedDate),
		fmt.Sprintf("object-%08d.xml", i),
	)
}

// ObjectContent returns byte-deterministic XML content for object index i.
// A probeable <Marker> field is near the head for full-pipe profiles.
func (r Recipe) ObjectContent(i int) []byte {
	// Seeded LCG for payload fill (deterministic, sterile).
	state := uint64(r.Seed) ^ uint64(i+1)*0x9e3779b97f4a7c15
	marker := fmt.Sprintf("synthetic-marker-%08d", i)
	head := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?><doc><Marker>%s</Marker><seq>%d</seq><payload>`,
		marker, i,
	)
	tail := `</payload></doc>`
	need := r.SizeBytes - len(head) - len(tail)
	if need < 0 {
		need = 0
	}
	buf := make([]byte, 0, r.SizeBytes)
	buf = append(buf, head...)
	for len(buf) < len(head)+need {
		state = state*6364136223846793005 + 1
		buf = append(buf, byte('a'+int(state%26)))
	}
	buf = append(buf, tail...)
	if len(buf) > r.SizeBytes {
		return buf[:r.SizeBytes]
	}
	for len(buf) < r.SizeBytes {
		buf = append(buf, 'x')
	}
	return buf
}

// SyntheticProbeConfigYAML is the sterile probe config matching ObjectContent.
const SyntheticProbeConfigYAML = `read_strategy:
  mode: fixed_window
  max_bytes: "4096"
extract:
  - name: marker
    type: xml_xpath
    xpath: //Marker
    required: true
    on_missing: fail
`

// FileURI builds a file:// URI for an absolute path (unix-style slash form).
// Callers must pass an absolute path; filepath.ToSlash normalization is applied
// at generation call sites via fileURIFromAbs.
func FileURI(absPath string) string {
	return "file://" + absPath
}

// monoNow is a test hook for wall-time injection in unit tests.
var monoNow = time.Now
