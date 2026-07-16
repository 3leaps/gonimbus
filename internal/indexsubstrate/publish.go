package indexsubstrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidCoverage      = errors.New("invalid coverage evidence")
	ErrSnapshotNotPublished = errors.New("snapshot not published")
	// ErrStaleParent is returned when ExpectedParent no longer matches the
	// authoritative latest pointer at advance time (lost CAS race).
	ErrStaleParent = errors.New("stale parent: latest advanced since capture")
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

// ExpectedParentToken is the CAS token for latest advance. When set, Publish
// re-reads the authoritative latest immediately before advance and refuses if
// run_id or manifest digest disagree with this capture.
type ExpectedParentToken struct {
	IndexSetID     string
	RunID          string
	ManifestSHA256 string
	// CoverageSHA256 is the digest of the parent's coverage attestation bytes
	// (canonical JSON). Required for enrich-only; child coverage must match.
	CoverageSHA256 string
}

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
	// SpillRoot is the operator-controlled directory under which the streaming
	// current-state merge stages its owner-only, symlink-safe workspace. When
	// empty it defaults to a "spillmerge" directory beside the sealed journals
	// (which are already resolved through operator app-data path classes).
	SpillRoot string
	// SpillBudget bounds the streaming merge's memory, workspace disk, and merge
	// topology. Zero fields fall back to DefaultSpillMergeBudget.
	SpillBudget SpillMergeBudget
	// Mode selects compaction/publication policy (default crawl vs enrich-only).
	Mode PublicationMode
	// ExpectedParent, when non-nil, enforces latest-pointer CAS at advance.
	// First publication of a set may leave this nil. Required for enrich-only.
	ExpectedParent *ExpectedParentToken
	// WriteLease is required for any latest advance. Must be a held package-owned
	// *WriteLease (not an external interface). Nil or released leases fail closed.
	WriteLease *WriteLease
	AfterStep  func(PublishStep) error
	// OnSegmentProgress is optional observational segment-write progress
	// (counts only). Outside persisted artifacts; never a publish failure vector.
	OnSegmentProgress OnSegmentProgressFunc
}

type PublishResult struct {
	Journals   []JournalSummary
	Compaction CompactionResult
	Manifest   InternalManifest
	// ManifestSHA256 is the digest of the committed manifest bytes (same value
	// written into complete.json). Prefer this over re-hashing after publish.
	ManifestSHA256 string
	// LatestAdvanced is true after latest.json was successfully written.
	// Callers must treat this as committed even if a later post-advance hook fails.
	LatestAdvanced bool
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

// PublishSnapshot publishes a durable snapshot with a background context. Prefer
// PublishSnapshotContext so cancellation propagates into the streaming merge.
func PublishSnapshot(config PublishConfig) (PublishResult, error) {
	return PublishSnapshotContext(context.Background(), config)
}

// PublishSnapshotContext compacts the sealed journals against the prior state and
// publishes the resulting current-state snapshot. Compaction and segment writing
// run through the streaming spill/merge current-state source and the streaming
// segment writer so the full current-state row set is never materialized in
// memory. The row/artifact/digest contract is identical to the prior materialized
// Compact -> WriteSegmentSet path.
func PublishSnapshotContext(ctx context.Context, config PublishConfig) (PublishResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Refuse a non-UTC run start on the raw caller value before normalization
	// launders it through .UTC(). The streaming publish seam must not silently
	// accept an offset the lineage/spill contract would reject. Zero remains the
	// concern of validatePublishConfig ("required") for a clearer message.
	if !config.RunStartedAt.IsZero() {
		if err := validateAuthoritativeRunStartedAt(config.RunStartedAt); err != nil {
			return PublishResult{}, err
		}
	}
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

	if err := validatePublicationCoverage(config.Coverage, config.Mode); err != nil {
		return result, err
	}
	if err := runPublishHook(config, PublishStepCoverageValidated); err != nil {
		return result, err
	}

	// Stream parent rows + ordered journals into a sorted current-state source.
	// The streaming segment writer owns the terminal Close of this source on every
	// exit path (success and failure).
	stateSource, err := PrepareCurrentStateSource(ctx, SpillMergeConfig{
		IndexSetID:   config.IndexSetID,
		RunID:        config.RunID,
		RunStartedAt: config.RunStartedAt,
		Parent:       NewSliceParentRows(config.PriorRows),
		JournalPaths: config.JournalPaths,
		Coverage:     config.Coverage,
		Mode:         config.Mode,
		SpillRoot:    config.SpillRoot,
		Budget:       config.SpillBudget,
	})
	if err != nil {
		return result, err
	}
	if err := runPublishHook(config, PublishStepCompacted); err != nil {
		// The streaming writer owns the terminal Close once it starts; here the
		// prepared source is still caller-owned. Close it now and preserve any
		// sticky cleanup failure alongside the hook error so protected spill
		// residue is never silently stranded (both causes stay classifiable).
		if closeErr := stateSource.Close(); closeErr != nil {
			return result, errors.Join(err, closeErr)
		}
		return result, err
	}

	manifest, err := WriteStreamingSegmentSet(ctx, SegmentWriterConfig{
		Dir:                    config.SegmentDir,
		IndexSetID:             config.IndexSetID,
		RunID:                  config.RunID,
		CreatedAt:              config.CreatedAt,
		TargetRowsPerSegment:   config.TargetRowsPerSegment,
		AllowExistingIdentical: true,
		ParentManifests:        config.ParentManifests,
		Coverage:               config.Coverage,
		OnSegmentProgress:      config.OnSegmentProgress,
	}, stateSource)
	if err != nil {
		return result, err
	}
	// Summarize compaction from streaming stats; the full row/tombstone slices are
	// intentionally not materialized on the streaming path. Callers that need row
	// or tombstone counts read result.Manifest.Counts.
	stats := stateSource.Stats()
	result.Compaction = CompactionResult{
		ObservedRecords:   stats.ObservedRecords,
		EnrichmentRecords: stats.EnrichmentRecords,
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
	result.ManifestSHA256 = manifestDigest
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

	if config.WriteLease == nil {
		return result, fmt.Errorf("write lease is required before latest advance")
	}
	// Bind the concrete lease to this publish target: same index set and the
	// segment-set root that owns latest.json. A held lease for another root/set
	// must not authorize this advance.
	if err := config.WriteLease.AssertHeldFor(config.IndexSetID, config.LatestPath); err != nil {
		return result, fmt.Errorf("write lease at latest advance: %w", err)
	}
	if err := assertExpectedParentCAS(config); err != nil {
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
	// Commit boundary: latest is authoritative after a successful write.
	result.LatestAdvanced = true
	if err := runPublishHook(config, PublishStepLatestAdvanced); err != nil {
		// Post-commit observation failure: return result with LatestAdvanced so
		// callers do not report published=false after a successful advance.
		return result, fmt.Errorf("post-latest advance hook: %w", err)
	}
	return result, nil
}

// PublishedSnapshot is a verified local durable snapshot opened from a latest
// pointer + complete marker + digest-checked manifest. Segments are verified
// per-file when walked.
type PublishedSnapshot struct {
	LatestPath string
	// CompletePath is the complete marker path used for this open when known.
	CompletePath string
	Complete     publishedCompleteDoc
	Manifest     InternalManifest
	SegmentDir   string
	// AccountedMarkerBytes / AccountedManifestBytes are the exact on-disk byte
	// lengths of the complete marker and manifest slices that were digested and
	// parsed for this open (same-bytes trust). Used by ancestry aggregate budgets.
	AccountedMarkerBytes   int64
	AccountedManifestBytes int64
}

// AccountedBytes returns marker+manifest bytes charged for this open.
func (s PublishedSnapshot) AccountedBytes() int64 {
	return s.AccountedMarkerBytes + s.AccountedManifestBytes
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
	snap, err := openPublishedCompleteBounded(latest.CompletePath, maxMarkerBytes, maxManifestBytes)
	if err != nil {
		return PublishedSnapshot{}, err
	}
	if latest.IndexSetID != snap.Complete.IndexSetID || latest.RunID != snap.Complete.RunID {
		return PublishedSnapshot{}, fmt.Errorf("latest pointer and complete marker disagree")
	}
	snap.LatestPath = latestPath
	return snap, nil
}

// OpenPublishedRunSnapshot opens a durable snapshot from a complete marker path
// without consulting latest.json. Use this for receipt-pinned set/run selection
// so a later latest advance cannot silently switch the opened snapshot.
//
// When expectedIndexSetID / expectedRunID are non-empty they must match the
// complete marker (and the validated manifest identity).
func OpenPublishedRunSnapshot(completePath, expectedIndexSetID, expectedRunID string) (PublishedSnapshot, error) {
	return OpenPublishedRunSnapshotBounded(completePath, expectedIndexSetID, expectedRunID, DefaultMaxPublishedMarkerBytes, DefaultMaxPublishedManifestBytes)
}

// OpenPublishedRunSnapshotBounded is OpenPublishedRunSnapshot with explicit bounds.
func OpenPublishedRunSnapshotBounded(completePath, expectedIndexSetID, expectedRunID string, maxMarkerBytes, maxManifestBytes int64) (PublishedSnapshot, error) {
	if maxMarkerBytes <= 0 {
		maxMarkerBytes = DefaultMaxPublishedMarkerBytes
	}
	if maxManifestBytes <= 0 {
		maxManifestBytes = DefaultMaxPublishedManifestBytes
	}
	snap, err := openPublishedCompleteBounded(completePath, maxMarkerBytes, maxManifestBytes)
	if err != nil {
		return PublishedSnapshot{}, err
	}
	expectedIndexSetID = strings.TrimSpace(expectedIndexSetID)
	expectedRunID = strings.TrimSpace(expectedRunID)
	if expectedIndexSetID != "" && snap.Complete.IndexSetID != expectedIndexSetID {
		return PublishedSnapshot{}, fmt.Errorf("complete marker index_set_id mismatch")
	}
	if expectedRunID != "" && snap.Complete.RunID != expectedRunID {
		return PublishedSnapshot{}, fmt.Errorf("complete marker run_id mismatch")
	}
	return snap, nil
}

// openPublishedCompleteBounded trust-checks complete → same-bytes manifest
// without reading latest.json. Marker and manifest are each read once; digest
// and parse use those exact slices (TOCTOU-safe). Accounted byte sizes are set.
func openPublishedCompleteBounded(completePath string, maxMarkerBytes, maxManifestBytes int64) (PublishedSnapshot, error) {
	// Unbounded aggregate remainder for ordinary current-state opens.
	const noAggregateCap int64 = 1 << 62
	snap, err := openPublishedCompleteBudgeted(completePath, maxMarkerBytes, maxManifestBytes, noAggregateCap)
	return snap, err
}

// afterFileReadForTest is an optional hook invoked after each bounded file read
// in openPublishedCompleteBudgeted with the path and number of bytes returned.
// Tests use it to prove aggregate-budget caps prevent full over-reads.
// Guarded by afterFileReadForTestMu for -race safety across parallel tests.
var (
	afterFileReadForTestMu sync.Mutex
	afterFileReadForTest   func(path string, bytesRead int)
)

// openPublishedCompleteBudgeted is the same-bytes open with an aggregate
// remaining-byte budget. Marker is read under min(maxMarker, remaining); after
// charging, manifest is read under min(maxManifest, remaining). Files larger
// than the effective cap fail closed without reading the whole file.
func openPublishedCompleteBudgeted(completePath string, maxMarkerBytes, maxManifestBytes, remaining int64) (PublishedSnapshot, error) {
	completePath = strings.TrimSpace(completePath)
	if completePath == "" {
		return PublishedSnapshot{}, fmt.Errorf("complete path is required")
	}
	if remaining <= 0 {
		return PublishedSnapshot{}, fmt.Errorf("aggregate byte budget exhausted")
	}
	if maxMarkerBytes <= 0 {
		maxMarkerBytes = DefaultMaxPublishedMarkerBytes
	}
	if maxManifestBytes <= 0 {
		maxManifestBytes = DefaultMaxPublishedManifestBytes
	}

	markerCap := maxMarkerBytes
	if remaining < markerCap {
		markerCap = remaining
	}
	if markerCap <= 0 {
		return PublishedSnapshot{}, fmt.Errorf("aggregate byte budget exhausted")
	}

	// Same-bytes trust: read complete once, parse those bytes; read manifest once,
	// digest+parse those bytes. Do not re-open paths for trust decisions.
	completeData, err := readFileBounded(completePath, markerCap)
	if err != nil {
		if os.IsNotExist(err) {
			return PublishedSnapshot{}, ErrSnapshotNotPublished
		}
		if strings.Contains(err.Error(), "size exceeds limit") {
			return PublishedSnapshot{}, fmt.Errorf("aggregate byte budget exhausted")
		}
		return PublishedSnapshot{}, fmt.Errorf("read complete marker: %w", err)
	}
	afterFileReadForTestMu.Lock()
	hook := afterFileReadForTest
	afterFileReadForTestMu.Unlock()
	if hook != nil {
		hook(completePath, len(completeData))
	}
	remaining -= int64(len(completeData))
	if remaining < 0 {
		return PublishedSnapshot{}, fmt.Errorf("aggregate byte budget exhausted")
	}

	var complete publishedCompleteDoc
	if err := json.Unmarshal(completeData, &complete); err != nil {
		return PublishedSnapshot{}, fmt.Errorf("parse complete marker: %w", err)
	}
	if strings.TrimSpace(complete.Type) != "gonimbus.index.complete.v1" {
		if strings.TrimSpace(complete.Type) == "" {
			return PublishedSnapshot{}, fmt.Errorf("complete marker type is required (expected gonimbus.index.complete.v1)")
		}
		return PublishedSnapshot{}, fmt.Errorf("complete marker type %q is not supported", complete.Type)
	}

	manifestCap := maxManifestBytes
	if remaining < manifestCap {
		manifestCap = remaining
	}
	if manifestCap <= 0 {
		return PublishedSnapshot{}, fmt.Errorf("aggregate byte budget exhausted")
	}
	manifestData, err := readFileBounded(complete.ManifestPath, manifestCap)
	if err != nil {
		if strings.Contains(err.Error(), "size exceeds limit") {
			return PublishedSnapshot{}, fmt.Errorf("aggregate byte budget exhausted")
		}
		return PublishedSnapshot{}, fmt.Errorf("read manifest: %w", err)
	}
	afterFileReadForTestMu.Lock()
	hook = afterFileReadForTest
	afterFileReadForTestMu.Unlock()
	if hook != nil {
		hook(complete.ManifestPath, len(manifestData))
	}
	if afterManifestBytesReadForTest != nil {
		afterManifestBytesReadForTest(complete.ManifestPath)
	}
	remaining -= int64(len(manifestData))
	if remaining < 0 {
		return PublishedSnapshot{}, fmt.Errorf("aggregate byte budget exhausted")
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
	// Structural lineage checks when optional fields are present. Graph/digest
	// walk is ResolveAncestry — not part of current-state open.
	if err := ValidateManifestLineageStructure(manifest); err != nil {
		return PublishedSnapshot{}, err
	}
	return PublishedSnapshot{
		CompletePath:           completePath,
		Complete:               complete,
		Manifest:               manifest,
		SegmentDir:             complete.SegmentDir,
		AccountedMarkerBytes:   int64(len(completeData)),
		AccountedManifestBytes: int64(len(manifestData)),
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
	config.SpillRoot = strings.TrimSpace(config.SpillRoot)
	if config.SpillRoot == "" && len(config.JournalPaths) > 0 {
		// Co-locate the streaming merge workspace with the sealed journals, which
		// are already resolved through operator-controlled app-data path classes.
		config.SpillRoot = filepath.Join(filepath.Dir(config.JournalPaths[0]), "spillmerge")
	}
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
	switch config.Mode {
	case PublicationModeDefault, PublicationModeEnrichOnly:
		// known modes
	default:
		return fmt.Errorf("unknown publication mode %q", config.Mode)
	}
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
	}
	// Defense in depth for every mode: a parent token must belong to the set being
	// published. Same-set continuity is mandatory; a foreign-set ExpectedParent is
	// refused here even if it digests against some other latest. Library build
	// adapters also bind the parent capture to the requested set before sinks run.
	if config.ExpectedParent != nil {
		if strings.TrimSpace(config.ExpectedParent.IndexSetID) != config.IndexSetID {
			return fmt.Errorf("ExpectedParent index_set_id does not match publication index set")
		}
	}
	if config.Mode == PublicationModeEnrichOnly {
		if config.ExpectedParent == nil {
			return fmt.Errorf("enrich-only mode requires ExpectedParent")
		}
		if len(config.ParentManifests) != 1 {
			return fmt.Errorf("enrich-only mode requires exactly one parent manifest reference")
		}
		parent := config.ParentManifests[0]
		exp := config.ExpectedParent
		if strings.TrimSpace(parent.IndexSetID) != strings.TrimSpace(exp.IndexSetID) ||
			strings.TrimSpace(parent.RunID) != strings.TrimSpace(exp.RunID) ||
			strings.TrimSpace(parent.ManifestSHA256) != strings.TrimSpace(exp.ManifestSHA256) {
			return fmt.Errorf("enrich-only parent manifest reference must equal ExpectedParent token")
		}
		if strings.TrimSpace(exp.IndexSetID) != strings.TrimSpace(config.IndexSetID) {
			return fmt.Errorf("enrich-only ExpectedParent index_set_id must match publish index_set_id")
		}
		if strings.TrimSpace(exp.CoverageSHA256) == "" {
			return fmt.Errorf("enrich-only mode requires ExpectedParent.CoverageSHA256")
		}
		got, err := coverageSHA256(config.Coverage)
		if err != nil {
			return fmt.Errorf("hash enrich coverage: %w", err)
		}
		if got != strings.TrimSpace(exp.CoverageSHA256) {
			return fmt.Errorf("enrich-only coverage must equal captured parent coverage (digest mismatch)")
		}
	}
	return nil
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

func validatePublicationCoverage(coverage []CoverageAttestation, mode PublicationMode) error {
	if mode == PublicationModeEnrichOnly {
		// Enrich-only inherits parent coverage as-is and must not invent
		// observation evidence. Empty inherited coverage is fail-closed:
		// enrichment cannot claim a crawl that never attested coverage.
		if len(coverage) == 0 {
			return fmt.Errorf("%w: enrich-only publish requires inherited parent coverage (refusing empty coverage fabrication)", ErrInvalidCoverage)
		}
		return nil
	}
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

// assertExpectedParentCAS re-reads authoritative latest and refuses advance when
// the captured parent token is stale. When ExpectedParent is nil (first
// publication), latest must still be absent at advance time.
func assertExpectedParentCAS(config PublishConfig) error {
	if config.ExpectedParent == nil {
		// Expected-absent CAS: a concurrent first publisher must not overwrite.
		_, err := OpenLatestPublishedSnapshotBounded(config.LatestPath, DefaultMaxPublishedMarkerBytes, DefaultMaxPublishedManifestBytes)
		if err == nil {
			return fmt.Errorf("%w: latest appeared before first-publish advance", ErrStaleParent)
		}
		if errors.Is(err, ErrSnapshotNotPublished) {
			return nil
		}
		return fmt.Errorf("re-read latest for absent-parent CAS: %w", err)
	}
	expected := *config.ExpectedParent
	expected.IndexSetID = strings.TrimSpace(expected.IndexSetID)
	expected.RunID = strings.TrimSpace(expected.RunID)
	expected.ManifestSHA256 = strings.TrimSpace(expected.ManifestSHA256)
	expected.CoverageSHA256 = strings.TrimSpace(expected.CoverageSHA256)
	if expected.IndexSetID == "" || expected.RunID == "" || expected.ManifestSHA256 == "" {
		return fmt.Errorf("%w: expected parent token is incomplete", ErrStaleParent)
	}
	current, err := OpenLatestPublishedSnapshotBounded(config.LatestPath, DefaultMaxPublishedMarkerBytes, DefaultMaxPublishedManifestBytes)
	if err != nil {
		if errors.Is(err, ErrSnapshotNotPublished) {
			return fmt.Errorf("%w: latest missing at advance (expected parent %s)", ErrStaleParent, expected.RunID)
		}
		return fmt.Errorf("re-read latest for parent CAS: %w", err)
	}
	if current.Complete.IndexSetID != expected.IndexSetID ||
		current.Complete.RunID != expected.RunID ||
		current.Complete.ManifestSHA256 != expected.ManifestSHA256 {
		return fmt.Errorf("%w: expected %s/%s@%s, found %s/%s@%s",
			ErrStaleParent,
			expected.IndexSetID, expected.RunID, shortDigest(expected.ManifestSHA256),
			current.Complete.IndexSetID, current.Complete.RunID, shortDigest(current.Complete.ManifestSHA256),
		)
	}
	// Bind coverage to the live verified parent, not a caller-supplied digest alone.
	// Required when token carries CoverageSHA256 (enrich-only always does).
	if expected.CoverageSHA256 != "" {
		liveCov, covErr := coverageSHA256(current.Manifest.Coverage)
		if covErr != nil {
			return fmt.Errorf("hash live parent coverage for CAS: %w", covErr)
		}
		if liveCov != expected.CoverageSHA256 {
			return fmt.Errorf("%w: parent coverage digest mismatch (token does not match live parent)", ErrStaleParent)
		}
	}
	return nil
}

func shortDigest(d string) string {
	d = strings.TrimSpace(d)
	if len(d) <= 12 {
		return d
	}
	return d[:12]
}

// CoverageSHA256 digests coverage attestations with stable JSON encoding.
func CoverageSHA256(coverage []CoverageAttestation) (string, error) {
	return coverageSHA256(coverage)
}

func coverageSHA256(coverage []CoverageAttestation) (string, error) {
	// Canonical: marshaled JSON of the slice (field order from struct tags).
	data, err := json.Marshal(coverage)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
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
	}
	if err := validateManifestCountBudget(manifest, -1); err != nil {
		return err
	}
	return nil
}

// validateManifestCountBudget ensures nonnegative counts, checked segment sum
// equals manifest.Counts.Rows, and optional maxRows cap. maxRows < 0 means no cap.
func validateManifestCountBudget(manifest InternalManifest, maxRows int) error {
	if manifest.Counts.Rows < 0 || manifest.Counts.ActiveRows < 0 || manifest.Counts.Tombstones < 0 {
		return fmt.Errorf("manifest counts must be non-negative")
	}
	var sum int64
	for i, seg := range manifest.Segments {
		if seg.Rows < 0 {
			return fmt.Errorf("segment %d rows must be non-negative", i)
		}
		next := sum + int64(seg.Rows)
		if next < sum {
			return fmt.Errorf("segment row sum overflow")
		}
		sum = next
	}
	if sum != int64(manifest.Counts.Rows) {
		return fmt.Errorf("manifest counts.rows %d does not equal segment descriptor sum %d", manifest.Counts.Rows, sum)
	}
	if maxRows >= 0 && manifest.Counts.Rows > maxRows {
		return fmt.Errorf("manifest counts.rows %d exceeds limit %d", manifest.Counts.Rows, maxRows)
	}
	return nil
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
