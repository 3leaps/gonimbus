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

func ReadLatestPublishedRows(latestPath string) (InternalManifest, []CurrentObjectRow, error) {
	latestPath = strings.TrimSpace(latestPath)
	if latestPath == "" {
		return InternalManifest{}, nil, fmt.Errorf("latest path is required")
	}
	data, err := os.ReadFile(latestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return InternalManifest{}, nil, ErrSnapshotNotPublished
		}
		return InternalManifest{}, nil, fmt.Errorf("read latest pointer: %w", err)
	}
	var latest publishedLatestDoc
	if err := json.Unmarshal(data, &latest); err != nil {
		return InternalManifest{}, nil, fmt.Errorf("parse latest pointer: %w", err)
	}
	if strings.TrimSpace(latest.CompletePath) == "" {
		return InternalManifest{}, nil, fmt.Errorf("latest pointer complete_path is required")
	}
	complete, err := readCompleteDocFile(latest.CompletePath)
	if err != nil {
		return InternalManifest{}, nil, err
	}
	if latest.IndexSetID != complete.IndexSetID || latest.RunID != complete.RunID {
		return InternalManifest{}, nil, fmt.Errorf("latest pointer and complete marker disagree")
	}
	manifestDigest, err := sha256HexFile(complete.ManifestPath)
	if err != nil {
		return InternalManifest{}, nil, fmt.Errorf("hash manifest: %w", err)
	}
	if manifestDigest != complete.ManifestSHA256 {
		return InternalManifest{}, nil, fmt.Errorf("manifest digest mismatch")
	}
	manifest, err := ReadInternalManifestFile(complete.ManifestPath)
	if err != nil {
		return InternalManifest{}, nil, err
	}
	if err := validatePublishedManifest(complete, manifest); err != nil {
		return InternalManifest{}, nil, err
	}
	rows, err := ReadManifestRows(complete.SegmentDir, manifest)
	if err != nil {
		return InternalManifest{}, nil, err
	}
	return manifest, rows, nil
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
	data, err := os.ReadFile(path)
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
