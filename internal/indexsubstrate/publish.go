package indexsubstrate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	ErrInvalidCoverage      = errors.New("invalid coverage evidence")
	ErrSnapshotNotPublished = errors.New("snapshot not published")
)

type PublishStep string

const (
	PublishStepJournalsValidated PublishStep = "journal_footers_validated"
	PublishStepCoverageValidated PublishStep = "coverage_validated"
	PublishStepCompacted         PublishStep = "compacted"
	PublishStepSegmentsWritten   PublishStep = "segments_written"
	PublishStepManifestWritten   PublishStep = "manifest_written"
	PublishStepCompleteWritten   PublishStep = "complete_written"
	PublishStepLatestAdvanced    PublishStep = "latest_advanced"
)

type PublishConfig struct {
	IndexSetID           string
	RunID                string
	RunStartedAt         time.Time
	CreatedAt            time.Time
	ParentManifests      []ManifestReference
	PriorRows            []CurrentObjectRow
	JournalPaths         []string
	Coverage             []CoverageAttestation
	SegmentDir           string
	ManifestPath         string
	CompletePath         string
	LatestPath           string
	TargetRowsPerSegment int
	AfterStep            func(PublishStep) error
	// OnSegmentProgress is optional observational segment-write progress
	// (counts only). Outside persisted artifacts; never a publish failure vector.
	OnSegmentProgress OnSegmentProgressFunc
}

type PublishResult struct {
	Journals   []JournalSummary
	Compaction CompactionResult
	Manifest   InternalManifest
}

type publishedCompleteDoc struct {
	Type           string `json:"type"`
	IndexSetID     string `json:"index_set_id"`
	RunID          string `json:"run_id"`
	CompletedAt    string `json:"completed_at"`
	ManifestPath   string `json:"manifest_path"`
	ManifestSHA256 string `json:"manifest_sha256"`
	SegmentDir     string `json:"segment_dir"`
	Segments       int    `json:"segments"`
}

type publishedLatestDoc struct {
	Type         string `json:"type"`
	IndexSetID   string `json:"index_set_id"`
	RunID        string `json:"run_id"`
	UpdatedAt    string `json:"updated_at"`
	CompletePath string `json:"complete_path"`
}

func PublishSnapshot(config PublishConfig) (PublishResult, error) {
	config = normalizePublishConfig(config)
	if err := validatePublishConfig(config); err != nil {
		return PublishResult{}, err
	}

	summaries, err := validateSealedJournalFiles(config)
	if err != nil {
		return PublishResult{}, err
	}
	result := PublishResult{Journals: summaries}
	if err := runPublishHook(config, PublishStepJournalsValidated); err != nil {
		return result, err
	}

	if err := validatePublicationCoverage(config.Coverage); err != nil {
		return result, err
	}
	if err := runPublishHook(config, PublishStepCoverageValidated); err != nil {
		return result, err
	}

	compaction, err := CompactJournalFiles(CompactionInput{
		IndexSetID:   config.IndexSetID,
		RunID:        config.RunID,
		RunStartedAt: config.RunStartedAt,
		PriorRows:    config.PriorRows,
		Coverage:     config.Coverage,
	}, config.JournalPaths)
	if err != nil {
		return result, err
	}
	result.Compaction = compaction
	if err := runPublishHook(config, PublishStepCompacted); err != nil {
		return result, err
	}

	manifest, err := WriteSegmentSet(SegmentWriterConfig{
		Dir:                    config.SegmentDir,
		IndexSetID:             config.IndexSetID,
		RunID:                  config.RunID,
		CreatedAt:              config.CreatedAt,
		TargetRowsPerSegment:   config.TargetRowsPerSegment,
		AllowExistingIdentical: true,
		ParentManifests:        config.ParentManifests,
		Coverage:               config.Coverage,
		OnSegmentProgress:      config.OnSegmentProgress,
	}, compaction.Rows)
	if err != nil {
		return result, err
	}
	result.Manifest = manifest
	if err := runPublishHook(config, PublishStepSegmentsWritten); err != nil {
		return result, err
	}

	if err := writeInternalManifestFile(config.ManifestPath, manifest, true); err != nil {
		return result, err
	}
	if err := runPublishHook(config, PublishStepManifestWritten); err != nil {
		return result, err
	}

	manifestDigest, err := sha256HexFile(config.ManifestPath)
	if err != nil {
		return result, fmt.Errorf("hash manifest: %w", err)
	}
	complete := publishedCompleteDoc{
		Type:           "gonimbus.index.complete.v1",
		IndexSetID:     config.IndexSetID,
		RunID:          config.RunID,
		CompletedAt:    config.CreatedAt.Format(time.RFC3339Nano),
		ManifestPath:   config.ManifestPath,
		ManifestSHA256: manifestDigest,
		SegmentDir:     config.SegmentDir,
		Segments:       len(manifest.Segments),
	}
	if err := writeJSONImmutableOrEqual(config.CompletePath, complete); err != nil {
		return result, fmt.Errorf("write complete marker: %w", err)
	}
	if err := runPublishHook(config, PublishStepCompleteWritten); err != nil {
		return result, err
	}

	latest := publishedLatestDoc{
		Type:         "gonimbus.index.latest.v1",
		IndexSetID:   config.IndexSetID,
		RunID:        config.RunID,
		UpdatedAt:    time.Now().UTC().Format(time.RFC3339Nano),
		CompletePath: config.CompletePath,
	}
	if err := writeLatestPointerFile(config.LatestPath, latest); err != nil {
		return result, fmt.Errorf("advance latest pointer: %w", err)
	}
	if err := runPublishHook(config, PublishStepLatestAdvanced); err != nil {
		return result, err
	}
	return result, nil
}

// PublishedSnapshot is a verified local durable snapshot opened from a latest
// pointer + complete marker + digest-checked manifest. Segments are verified
// per-file when walked.
type PublishedSnapshot struct {
	LatestPath string
	Complete   publishedCompleteDoc
	Manifest   InternalManifest
	SegmentDir string
}

// DefaultMaxPublishedMarkerBytes is the default bound for latest/complete JSON.
const DefaultMaxPublishedMarkerBytes = 1 << 20

// DefaultMaxPublishedManifestBytes is the default bound for internal manifests.
const DefaultMaxPublishedManifestBytes = 64 << 20

// OpenLatestPublishedSnapshot opens and trust-checks the latest durable snapshot
// without materializing rows. Marker and manifest reads are size-bounded.
func OpenLatestPublishedSnapshot(latestPath string) (PublishedSnapshot, error) {
	return OpenLatestPublishedSnapshotBounded(latestPath, DefaultMaxPublishedMarkerBytes, DefaultMaxPublishedManifestBytes)
}

// OpenLatestPublishedSnapshotBounded is OpenLatestPublishedSnapshot with explicit
// marker/manifest size bounds.
func OpenLatestPublishedSnapshotBounded(latestPath string, maxMarkerBytes, maxManifestBytes int64) (PublishedSnapshot, error) {
	latestPath = strings.TrimSpace(latestPath)
	if latestPath == "" {
		return PublishedSnapshot{}, fmt.Errorf("latest path is required")
	}
	if maxMarkerBytes <= 0 {
		maxMarkerBytes = DefaultMaxPublishedMarkerBytes
	}
	if maxManifestBytes <= 0 {
		maxManifestBytes = DefaultMaxPublishedManifestBytes
	}
	data, err := readFileBounded(latestPath, maxMarkerBytes)
	if err != nil {
		if os.IsNotExist(err) {
			return PublishedSnapshot{}, ErrSnapshotNotPublished
		}
		return PublishedSnapshot{}, fmt.Errorf("read latest pointer: %w", err)
	}
	var latest publishedLatestDoc
	if err := json.Unmarshal(data, &latest); err != nil {
		return PublishedSnapshot{}, fmt.Errorf("parse latest pointer: %w", err)
	}
	if strings.TrimSpace(latest.Type) != "gonimbus.index.latest.v1" {
		if strings.TrimSpace(latest.Type) == "" {
			return PublishedSnapshot{}, fmt.Errorf("latest pointer type is required (expected gonimbus.index.latest.v1)")
		}
		return PublishedSnapshot{}, fmt.Errorf("latest pointer type %q is not supported", latest.Type)
	}
	if strings.TrimSpace(latest.CompletePath) == "" {
		return PublishedSnapshot{}, fmt.Errorf("latest pointer complete_path is required")
	}
	complete, err := readCompleteDocFileBounded(latest.CompletePath, maxMarkerBytes)
	if err != nil {
		return PublishedSnapshot{}, err
	}
	if strings.TrimSpace(complete.Type) != "gonimbus.index.complete.v1" {
		if strings.TrimSpace(complete.Type) == "" {
			return PublishedSnapshot{}, fmt.Errorf("complete marker type is required (expected gonimbus.index.complete.v1)")
		}
		return PublishedSnapshot{}, fmt.Errorf("complete marker type %q is not supported", complete.Type)
	}
	if latest.IndexSetID != complete.IndexSetID || latest.RunID != complete.RunID {
		return PublishedSnapshot{}, fmt.Errorf("latest pointer and complete marker disagree")
	}
	// Same-bytes trust: read once, digest those exact bytes, then unmarshal them.
	// Do not hash and parse through separate pathname opens (TOCTOU).
	manifestData, err := readFileBounded(complete.ManifestPath, maxManifestBytes)
	if err != nil {
		return PublishedSnapshot{}, fmt.Errorf("read manifest: %w", err)
	}
	if afterManifestBytesReadForTest != nil {
		afterManifestBytesReadForTest(complete.ManifestPath)
	}
	manifestDigest := sha256HexBytes(manifestData)
	if manifestDigest != complete.ManifestSHA256 {
		return PublishedSnapshot{}, fmt.Errorf("manifest digest mismatch")
	}
	var manifest InternalManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return PublishedSnapshot{}, fmt.Errorf("parse manifest: %w", err)
	}
	if err := validatePublishedManifest(complete, manifest); err != nil {
		return PublishedSnapshot{}, err
	}
	return PublishedSnapshot{
		LatestPath: latestPath,
		Complete:   complete,
		Manifest:   manifest,
		SegmentDir: complete.SegmentDir,
	}, nil
}

// WalkLatestPublishedRows walks the latest published snapshot with streaming
// verify-before-emit semantics (per-segment digest check before that segment's
// rows are visited). Rows are not materialised as a slice.
func WalkLatestPublishedRows(latestPath string, visit func(CurrentObjectRow) error) (InternalManifest, error) {
	snap, err := OpenLatestPublishedSnapshot(latestPath)
	if err != nil {
		return InternalManifest{}, err
	}
	if err := WalkManifestRows(snap.SegmentDir, snap.Manifest, visit); err != nil {
		return InternalManifest{}, err
	}
	return snap.Manifest, nil
}

func ReadLatestPublishedRows(latestPath string) (InternalManifest, []CurrentObjectRow, error) {
	snap, err := OpenLatestPublishedSnapshot(latestPath)
	if err != nil {
		return InternalManifest{}, nil, err
	}
	rows, err := ReadManifestRows(snap.SegmentDir, snap.Manifest)
	if err != nil {
		return InternalManifest{}, nil, err
	}
	return snap.Manifest, rows, nil
}

func normalizePublishConfig(config PublishConfig) PublishConfig {
	config.IndexSetID = strings.TrimSpace(config.IndexSetID)
	config.RunID = strings.TrimSpace(config.RunID)
	config.SegmentDir = strings.TrimSpace(config.SegmentDir)
	config.ManifestPath = strings.TrimSpace(config.ManifestPath)
	config.CompletePath = strings.TrimSpace(config.CompletePath)
	config.LatestPath = strings.TrimSpace(config.LatestPath)
	if !config.RunStartedAt.IsZero() {
		config.RunStartedAt = config.RunStartedAt.UTC()
	}
	if config.CreatedAt.IsZero() {
		config.CreatedAt = config.RunStartedAt
	}
	if !config.CreatedAt.IsZero() {
		config.CreatedAt = config.CreatedAt.UTC()
	}
	if config.TargetRowsPerSegment <= 0 {
		config.TargetRowsPerSegment = DefaultTargetRowsPerSegment
	}
	return config
}

func validatePublishConfig(config PublishConfig) error {
	switch {
	case config.IndexSetID == "":
		return fmt.Errorf("index_set_id is required")
	case config.RunID == "":
		return fmt.Errorf("run_id is required")
	case config.RunStartedAt.IsZero():
		return fmt.Errorf("run_started_at is required")
	case len(config.JournalPaths) == 0:
		return fmt.Errorf("journal paths are required")
	case config.SegmentDir == "":
		return fmt.Errorf("segment directory is required")
	case config.ManifestPath == "":
		return fmt.Errorf("manifest path is required")
	case config.CompletePath == "":
		return fmt.Errorf("complete path is required")
	case config.LatestPath == "":
		return fmt.Errorf("latest path is required")
	default:
		return nil
	}
}

func validateSealedJournalFiles(config PublishConfig) ([]JournalSummary, error) {
	summaries := make([]JournalSummary, 0, len(config.JournalPaths))
	for _, path := range config.JournalPaths {
		summary, err := ValidateJournal(path)
		if err != nil {
			return nil, err
		}
		if summary.Header.IndexSetID != config.IndexSetID {
			return nil, fmt.Errorf("%w: journal index_set_id mismatch", ErrInvalidJournal)
		}
		if summary.Header.RunID != config.RunID {
			return nil, fmt.Errorf("%w: journal run_id mismatch", ErrInvalidJournal)
		}
		summaries = append(summaries, summary)
	}
	return summaries, nil
}

func validatePublicationCoverage(coverage []CoverageAttestation) error {
	if len(coverage) == 0 {
		return fmt.Errorf("%w: at least one confirmed complete scope is required", ErrInvalidCoverage)
	}
	for _, entry := range coverage {
		if entry.Basis != CoverageBasisConfirmed || !entry.Complete {
			return fmt.Errorf("%w: coverage must be confirmed and complete", ErrInvalidCoverage)
		}
		if len(entry.Gaps) != 0 {
			return fmt.Errorf("%w: coverage gaps are not publishable in this slice", ErrInvalidCoverage)
		}
		if !publishableCoverageScope(entry.Scope) {
			return fmt.Errorf("%w: coverage scope must be explicit prefix coverage", ErrInvalidCoverage)
		}
	}
	return nil
}

func publishableCoverageScope(scope *Scope) bool {
	if scope == nil || scope.Window != nil {
		return false
	}
	prefix := cleanCoveragePrefix(scope.Prefix)
	return prefix != ""
}

func runPublishHook(config PublishConfig, step PublishStep) error {
	if config.AfterStep == nil {
		return nil
	}
	if err := config.AfterStep(step); err != nil {
		return fmt.Errorf("%s: %w", step, err)
	}
	return nil
}

func readCompleteDocFile(path string) (publishedCompleteDoc, error) {
	return readCompleteDocFileBounded(path, DefaultMaxPublishedMarkerBytes)
}

func readCompleteDocFileBounded(path string, maxBytes int64) (publishedCompleteDoc, error) {
	data, err := readFileBounded(path, maxBytes)
	if err != nil {
		if os.IsNotExist(err) {
			return publishedCompleteDoc{}, ErrSnapshotNotPublished
		}
		return publishedCompleteDoc{}, fmt.Errorf("read complete marker: %w", err)
	}
	var complete publishedCompleteDoc
	if err := json.Unmarshal(data, &complete); err != nil {
		return publishedCompleteDoc{}, fmt.Errorf("parse complete marker: %w", err)
	}
	return complete, nil
}

// afterManifestBytesReadForTest is an optional test hook invoked after the
// manifest file has been read into memory and before digest check/parse.
// Tests use it to replace the pathname contents and prove the parser never
// re-opens the path for trust decisions.
var afterManifestBytesReadForTest func(path string)

func readFileBounded(path string, maxBytes int64) ([]byte, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("path is required")
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("max bytes must be positive")
	}
	// Single open: size is enforced by LimitReader, not a pre-Stat that can race.
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	limited := io.LimitReader(f, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("file %s size exceeds limit %d", filepath.Base(path), maxBytes)
	}
	return data, nil
}

func sha256HexBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func validatePublishedManifest(complete publishedCompleteDoc, manifest InternalManifest) error {
	switch {
	case manifest.Type != ManifestType:
		return fmt.Errorf("manifest type mismatch")
	case manifest.Render != ManifestRenderType:
		return fmt.Errorf("manifest render mismatch")
	case manifest.IndexSetID != complete.IndexSetID:
		return fmt.Errorf("manifest index_set_id mismatch")
	case manifest.RunID != complete.RunID:
		return fmt.Errorf("manifest run_id mismatch")
	case manifest.IndexSchemaVersion != IndexSchemaVersion:
		return fmt.Errorf("manifest schema version mismatch")
	case len(manifest.Segments) != complete.Segments:
		return fmt.Errorf("manifest segment count mismatch")
	default:
		return nil
	}
}

func writeJSONImmutableOrEqual(path string, value any) error {
	data, err := marshalIndentedJSON(value)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".publish-*.json.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Link(tempPath, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			existing, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			if string(existing) == string(data) {
				return nil
			}
		}
		return err
	}
	cleanup = false
	_ = os.Remove(tempPath)
	return nil
}

func writeLatestPointerFile(path string, latest publishedLatestDoc) error {
	data, err := marshalIndentedJSON(latest)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".latest-*.json.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer func() { _ = os.Remove(tempPath) }()
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
}
