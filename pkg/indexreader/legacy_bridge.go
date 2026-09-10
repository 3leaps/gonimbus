package indexreader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
)

const (
	HubMarkerSchemaV1 = "gonimbus.index.hub_marker.v1"
	HubMarkerSchemaV3 = "gonimbus.index.hub_marker.v3"

	BridgeDispositionCreated          = "created"
	BridgeDispositionAlreadyIdentical = "already_identical"

	BridgeRunStartLegacyAsserted  = "legacy_asserted"
	BridgeRunStartLocallyObserved = "locally_observed"

	BridgeSnapshotTimeExactCommit       = "exact_commit"
	BridgeSnapshotTimeLegacyUnavailable = "legacy_unavailable"

	BridgeConversionType = "gonimbus.index.custody_bridge_conversion.v1"

	maxBridgeJSONCount             = int64(9007199254740991)
	maxBridgeProtocolSegments      = 200_000
	bridgeTerminalReconcileTimeout = 5 * time.Second
)

const (
	BridgeErrorInvalidRequest    = "bridge_invalid_request"
	BridgeErrorSourceRead        = "bridge_source_read_failed"
	BridgeErrorLegacyMarker      = "bridge_legacy_marker_invalid"
	BridgeErrorIdentityInvalid   = "bridge_identity_invalid"
	BridgeErrorEvidenceInvalid   = "bridge_evidence_invalid"
	BridgeErrorManifestInvalid   = "bridge_manifest_invalid"
	BridgeErrorSegmentInvalid    = "bridge_segment_invalid"
	BridgeErrorResourceLimit     = "bridge_resource_limit"
	BridgeErrorWorkspace         = "bridge_workspace_failed"
	BridgeErrorTargetCreate      = "bridge_target_create_failed"
	BridgeErrorTargetRead        = "bridge_target_read_failed"
	BridgeErrorTargetConflict    = "bridge_target_conflict"
	BridgeErrorInternalInvariant = "bridge_internal_invariant"
)

var (
	bridgeSegmentPathRE = regexp.MustCompile(`^segments/[A-Za-z0-9._-]+$`)
	bridgeSegmentIDRE   = regexp.MustCompile(`^seg_[0-9]{6}_[0-9a-f]{16}$`)
	bridgeSHA256RE      = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// BridgeError is a closed, coordinate-free library failure.
type BridgeError struct {
	Code string
}

func (e *BridgeError) Error() string {
	if e == nil {
		return ""
	}
	return e.Code
}

func newBridgeError(code string) error {
	return &BridgeError{Code: code}
}

// IsBridgeError reports whether err carries the given closed bridge code.
func IsBridgeError(err error, code string) bool {
	var target *BridgeError
	return errors.As(err, &target) && target.Code == code
}

// HubConditionalCreatePublisher exposes only exact reads and indivisible
// conditional creates. Created=false with a nil error means an existing object
// won the create; the library then exact-reads and verifies it. Created=true is
// a definitive durable create even if err also reports a post-commit failure;
// implementations must not return it before fully storing the supplied body.
// Implementations must never emulate this operation with check-then-create.
type HubConditionalCreatePublisher interface {
	HubExactObjectReader
	CreateExact(
		ctx context.Context,
		key string,
		body io.Reader,
		sizeBytes int64,
	) (created bool, err error)
}

// BridgeLimits supplies caller ceilings for untrusted input and aggregate
// verified content. Positive values can tighten but never loosen the frozen
// protocol maxima.
type BridgeLimits struct {
	MaxMarkerBytes     int64
	MaxManifestBytes   int64
	MaxIdentityBytes   int64
	MaxEvidenceBytes   int64
	MaxSegmentBytes    int64
	MaxSegments        int
	MaxAggregateBytes  int64
	MaxConversionBytes int64
}

func (limits BridgeLimits) normalize() BridgeLimits {
	limits.MaxMarkerBytes = normalizeBridgeByteLimit(limits.MaxMarkerBytes, 1<<20)
	limits.MaxManifestBytes = normalizeBridgeByteLimit(limits.MaxManifestBytes, 64<<20)
	limits.MaxIdentityBytes = normalizeBridgeByteLimit(limits.MaxIdentityBytes, 1<<20)
	limits.MaxEvidenceBytes = normalizeBridgeByteLimit(limits.MaxEvidenceBytes, 1<<20)
	limits.MaxSegmentBytes = normalizeBridgeByteLimit(limits.MaxSegmentBytes, 1<<40)
	limits.MaxAggregateBytes = normalizeBridgeByteLimit(limits.MaxAggregateBytes, 100<<40)
	limits.MaxConversionBytes = normalizeBridgeByteLimit(limits.MaxConversionBytes, 64<<20)
	if limits.MaxSegments <= 0 || limits.MaxSegments > maxBridgeProtocolSegments {
		limits.MaxSegments = maxBridgeProtocolSegments
	}
	return limits
}

func normalizeBridgeByteLimit(value, defaultValue int64) int64 {
	if value <= 0 {
		value = defaultValue
	}
	if value > maxBridgeJSONCount {
		return maxBridgeJSONCount
	}
	return value
}

// BridgeOptions identifies one exact legacy durable run. Workspace is an
// optional parent for the private verification spool. HubCommitClock is
// injectable for deterministic commit-boundary tests.
type BridgeOptions struct {
	IndexSetID     string
	RunID          string
	Workspace      string
	Limits         BridgeLimits
	ExportedBy     string
	HubCommitClock func() time.Time
}

type BridgeRunStart struct {
	Basis     string `json:"basis"`
	StartedAt string `json:"started_at"`
}

type BridgeSnapshotTime struct {
	Basis          string `json:"basis"`
	CompletedAt    string `json:"completed_at,omitempty"`
	EvidenceType   string `json:"evidence_type,omitempty"`
	EvidenceSHA256 string `json:"evidence_sha256,omitempty"`
}

type BridgeResult struct {
	IndexSetID                string
	RunID                     string
	LegacyMarkerSHA256        string
	IdentitySHA256            string
	IdentitySchema            string
	IdentityProfile           string
	ManifestSHA256            string
	DeclaredRows              int
	DeclaredSegments          int
	SegmentsVerified          int
	BytesRead                 int64
	BytesConditionallyCreated int64
	RunStart                  BridgeRunStart
	SnapshotTime              BridgeSnapshotTime
	ConversionIdentitySHA256  string
	HubCommittedAt            string
	CompleteSHA256            string
	ExportedBy                string
	Disposition               string
}

type bridgeArtifactRef struct {
	Path      string `json:"path"`
	Role      string `json:"role"`
	Required  bool   `json:"required"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
	ETag      string `json:"etag,omitempty"`
}

type legacyHubComplete struct {
	Schema              string               `json:"$schema,omitempty"`
	Version             string               `json:"version"`
	MarkerSchemaVersion string               `json:"marker_schema_version"`
	Format              string               `json:"format"`
	FormatVersion       string               `json:"format_version"`
	IndexSetID          string               `json:"index_set_id"`
	RunID               string               `json:"run_id"`
	CompletedAt         string               `json:"completed_at"`
	ExportedBy          string               `json:"exported_by"`
	Artifacts           legacyHubArtifacts   `json:"artifacts"`
	Durable             bridgeDurableSummary `json:"durable"`
	Source              *legacyHubSource     `json:"source,omitempty"`
	ProviderMeta        map[string]string    `json:"provider_meta,omitempty"`
}

type legacyHubArtifacts struct {
	Manifest bridgeArtifactRef   `json:"manifest"`
	Segments []bridgeArtifactRef `json:"segments"`
}

type legacyHubSource struct {
	BaseURI        string `json:"base_uri,omitempty"`
	Provider       string `json:"provider,omitempty"`
	RunStatus      string `json:"run_status,omitempty"`
	RunStartedAt   string `json:"run_started_at,omitempty"`
	RunEndedAt     string `json:"run_ended_at,omitempty"`
	ObjectCount    int64  `json:"object_count,omitempty"`
	TotalSizeBytes int64  `json:"total_size_bytes,omitempty"`
}

type bridgeDurableSummary struct {
	ManifestType       string `json:"manifest_type"`
	ManifestRender     string `json:"manifest_render"`
	IndexSchemaVersion int    `json:"index_schema_version"`
	SegmentNamespace   string `json:"segment_namespace"`
	Segments           int    `json:"segments"`
	Rows               int    `json:"rows"`
}

type bridgeV3Artifacts struct {
	Identity bridgeArtifactRef   `json:"identity_json"`
	Manifest bridgeArtifactRef   `json:"manifest"`
	Segments []bridgeArtifactRef `json:"segments"`
}

type bridgeHubCompleteV3 struct {
	Version                   string               `json:"version"`
	MarkerSchemaVersion       string               `json:"marker_schema_version"`
	Format                    string               `json:"format"`
	FormatVersion             string               `json:"format_version"`
	IndexSetID                string               `json:"index_set_id"`
	RunID                     string               `json:"run_id"`
	LegacyMarkerSchemaVersion string               `json:"legacy_marker_schema_version"`
	LegacyMarkerSHA256        string               `json:"legacy_marker_sha256"`
	IdentitySchema            string               `json:"identity_schema"`
	IdentityProfile           string               `json:"identity_profile"`
	RunStart                  BridgeRunStart       `json:"run_start"`
	SnapshotTime              BridgeSnapshotTime   `json:"snapshot_time"`
	ConversionIdentitySHA256  string               `json:"conversion_identity_sha256"`
	HubCommittedAt            string               `json:"hub_committed_at"`
	ExportedBy                string               `json:"exported_by"`
	Artifacts                 bridgeV3Artifacts    `json:"artifacts"`
	Durable                   bridgeDurableSummary `json:"durable"`
}

type bridgeConversionSegment struct {
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

type bridgeConversionIdentity struct {
	Type               string                    `json:"type"`
	IndexSetID         string                    `json:"index_set_id"`
	RunID              string                    `json:"run_id"`
	LegacyMarkerSHA256 string                    `json:"legacy_marker_sha256"`
	IdentitySHA256     string                    `json:"identity_sha256"`
	IdentitySchema     string                    `json:"identity_schema"`
	IdentityProfile    string                    `json:"identity_profile"`
	ManifestSHA256     string                    `json:"manifest_sha256"`
	Segments           []bridgeConversionSegment `json:"segments"`
	RunStart           BridgeRunStart            `json:"run_start"`
	SnapshotTime       BridgeSnapshotTime        `json:"snapshot_time"`
}

type completeV2Evidence struct {
	Type                        string `json:"type"`
	IndexSetID                  string `json:"index_set_id"`
	RunID                       string `json:"run_id"`
	SnapshotCompletedAt         string `json:"snapshot_completed_at"`
	SnapshotCompletionSemantics string `json:"snapshot_completion_semantics"`
	ManifestPath                string `json:"manifest_path"`
	ManifestSHA256              string `json:"manifest_sha256"`
	SegmentDir                  string `json:"segment_dir"`
	Segments                    int    `json:"segments"`
}

type verifiedBridgeInput struct {
	legacy        legacyHubComplete
	legacySHA     string
	identity      bridgeIdentity
	manifestBytes []byte
	manifest      indexsubstrate.InternalManifest
	manifestSHA   string
	runStart      BridgeRunStart
	snapshotTime  BridgeSnapshotTime
	segments      []verifiedBridgeSegment
	bytesRead     int64
}

type verifiedBridgeSegment struct {
	ref       bridgeArtifactRef
	spoolPath string
}

// BridgeLegacyDurableRun verifies and republishes one exact legacy durable run.
// It cannot list, select latest, overwrite, delete, or construct live-source
// providers because none of those capabilities are present in its interfaces.
func BridgeLegacyDurableRun(
	ctx context.Context,
	source HubExactObjectReader,
	target HubConditionalCreatePublisher,
	identityAuthority CanonicalIdentityAuthority,
	snapshotCompletionEvidence []byte,
	opts BridgeOptions,
) (BridgeResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return BridgeResult{}, err
	}
	if source == nil || target == nil ||
		!fullAcquiredIndexSetRE.MatchString(opts.IndexSetID) ||
		!acquiredRunIDRE.MatchString(opts.RunID) ||
		strings.TrimSpace(opts.ExportedBy) == "" {
		return BridgeResult{}, newBridgeError(BridgeErrorInvalidRequest)
	}
	limits := opts.Limits.normalize()
	stage, err := os.MkdirTemp(opts.Workspace, ".gonimbus-custody-bridge-")
	if err != nil {
		return BridgeResult{}, newBridgeError(BridgeErrorWorkspace)
	}
	if err := os.Chmod(stage, 0o700); err != nil { // #nosec G302 -- stage is a directory and must remain owner-searchable.
		_ = os.RemoveAll(stage)
		return BridgeResult{}, newBridgeError(BridgeErrorWorkspace)
	}
	defer func() { _ = os.RemoveAll(stage) }()

	verified, err := verifyLegacyBridgeInput(
		ctx,
		source,
		identityAuthority,
		snapshotCompletionEvidence,
		opts.IndexSetID,
		opts.RunID,
		stage,
		limits,
	)
	if err != nil {
		return BridgeResult{}, err
	}
	return publishVerifiedLegacyBridge(ctx, target, verified, opts, limits)
}

func verifyLegacyBridgeInput(
	ctx context.Context,
	source HubExactObjectReader,
	identityAuthority CanonicalIdentityAuthority,
	evidenceBytes []byte,
	indexSetID, runID, stage string,
	limits BridgeLimits,
) (verifiedBridgeInput, error) {
	legacyKey := exactHubKey(indexSetID, runID, "complete.json")
	legacyBytes, err := downloadExactBytes(ctx, source, legacyKey, limits.MaxMarkerBytes)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return verifiedBridgeInput{}, ctxErr
		}
		return verifiedBridgeInput{}, newBridgeError(BridgeErrorSourceRead)
	}
	var legacy legacyHubComplete
	if err := strictDecodeJSON(legacyBytes, &legacy, true); err != nil ||
		validateLegacyHubComplete(legacy, indexSetID, runID, limits) != nil {
		return verifiedBridgeInput{}, newBridgeError(BridgeErrorLegacyMarker)
	}
	legacySHA := sha256HexBridge(legacyBytes)

	identity, err := resolveBridgeIdentity(identityAuthority, indexSetID, limits.MaxIdentityBytes)
	if err != nil {
		return verifiedBridgeInput{}, err
	}

	manifestBytes, err := downloadExactBytes(
		ctx,
		source,
		exactHubKey(indexSetID, runID, "manifest.json"),
		limits.MaxManifestBytes,
	)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return verifiedBridgeInput{}, ctxErr
		}
		return verifiedBridgeInput{}, newBridgeError(BridgeErrorSourceRead)
	}
	if int64(len(manifestBytes)) != legacy.Artifacts.Manifest.SizeBytes ||
		sha256HexBridge(manifestBytes) != legacy.Artifacts.Manifest.SHA256 {
		return verifiedBridgeInput{}, newBridgeError(BridgeErrorManifestInvalid)
	}
	var manifest indexsubstrate.InternalManifest
	if err := strictDecodeJSON(manifestBytes, &manifest, true); err != nil ||
		validateBridgeManifest(manifestBytes, manifest, legacy, indexSetID, runID, limits) != nil {
		return verifiedBridgeInput{}, newBridgeError(BridgeErrorManifestInvalid)
	}
	runStart := BridgeRunStart{
		Basis:     BridgeRunStartLegacyAsserted,
		StartedAt: manifest.RunStartedAt.UTC().Format(time.RFC3339Nano),
	}
	snapshotTime, err := validateBridgeEvidence(
		evidenceBytes,
		indexSetID,
		runID,
		legacy.Artifacts.Manifest.SHA256,
		len(manifest.Segments),
		*manifest.RunStartedAt,
		limits.MaxEvidenceBytes,
	)
	if err != nil {
		return verifiedBridgeInput{}, err
	}

	segments, segmentBytes, err := spoolVerifiedBridgeSegments(
		ctx,
		source,
		stage,
		indexSetID,
		runID,
		manifest,
		legacy,
		limits,
	)
	if err != nil {
		return verifiedBridgeInput{}, err
	}
	bytesRead, ok := checkedBridgeAdd(
		int64(len(legacyBytes)),
		int64(len(manifestBytes)),
		segmentBytes,
	)
	if !ok || bytesRead > maxBridgeJSONCount {
		return verifiedBridgeInput{}, newBridgeError(BridgeErrorResourceLimit)
	}
	return verifiedBridgeInput{
		legacy: legacy, legacySHA: legacySHA,
		identity: identity, manifestBytes: manifestBytes, manifest: manifest,
		manifestSHA: legacy.Artifacts.Manifest.SHA256,
		runStart:    runStart, snapshotTime: snapshotTime,
		segments: segments, bytesRead: bytesRead,
	}, nil
}

func validateLegacyHubComplete(
	legacy legacyHubComplete,
	indexSetID, runID string,
	limits BridgeLimits,
) error {
	if legacy.Version != "1.0" ||
		legacy.MarkerSchemaVersion != HubMarkerSchemaV1 ||
		legacy.Format != "durable-v2" ||
		legacy.FormatVersion != "2" ||
		legacy.IndexSetID != indexSetID ||
		legacy.RunID != runID ||
		strings.TrimSpace(legacy.ExportedBy) == "" {
		return errors.New("contract mismatch")
	}
	if _, err := indexsubstrate.ParseCanonicalUTCTime(legacy.CompletedAt); err != nil {
		return err
	}
	if err := validateBridgeArtifactRef(
		legacy.Artifacts.Manifest,
		"manifest.json",
		"manifest",
		limits.MaxManifestBytes,
	); err != nil {
		return err
	}
	if len(legacy.Artifacts.Segments) > limits.MaxSegments ||
		legacy.Durable.Segments != len(legacy.Artifacts.Segments) ||
		legacy.Durable.Rows < 0 ||
		int64(legacy.Durable.Rows) > maxBridgeJSONCount {
		return errors.New("artifact count")
	}
	if legacy.Source != nil {
		if legacy.Source.ObjectCount < 0 || legacy.Source.TotalSizeBytes < 0 {
			return errors.New("source summary")
		}
		switch legacy.Source.RunStatus {
		case "", "success", "partial", "failed", "failed-resumable":
		default:
			return errors.New("source summary")
		}
		for _, raw := range []string{legacy.Source.RunStartedAt, legacy.Source.RunEndedAt} {
			if raw != "" {
				if _, err := indexsubstrate.ParseCanonicalUTCTime(raw); err != nil {
					return errors.New("source summary")
				}
			}
		}
	}
	seen := make(map[string]struct{}, len(legacy.Artifacts.Segments))
	var aggregate int64
	for _, ref := range legacy.Artifacts.Segments {
		if err := validateBridgeArtifactRef(ref, ref.Path, "segment", limits.MaxSegmentBytes); err != nil ||
			!bridgeSegmentPathRE.MatchString(ref.Path) ||
			path.Clean(ref.Path) != ref.Path ||
			strings.Contains(ref.Path, "%") {
			return errors.New("segment artifact")
		}
		if _, duplicate := seen[ref.Path]; duplicate {
			return errors.New("duplicate segment")
		}
		seen[ref.Path] = struct{}{}
		var ok bool
		aggregate, ok = checkedBridgeAdd(aggregate, ref.SizeBytes)
		if !ok || aggregate > limits.MaxAggregateBytes {
			return errors.New("aggregate")
		}
	}
	return nil
}

func validateBridgeArtifactRef(ref bridgeArtifactRef, expectedPath, expectedRole string, maxBytes int64) error {
	if ref.Path != expectedPath || ref.Role != expectedRole || !ref.Required ||
		len(ref.Path) > 1024 ||
		ref.SizeBytes < 0 || ref.SizeBytes > maxBytes || !bridgeSHA256RE.MatchString(ref.SHA256) {
		return errors.New("artifact contract")
	}
	return nil
}

func validateBridgeManifest(
	raw []byte,
	manifest indexsubstrate.InternalManifest,
	legacy legacyHubComplete,
	indexSetID, runID string,
	limits BridgeLimits,
) error {
	if manifest.Type != indexsubstrate.ManifestType ||
		manifest.Render != indexsubstrate.ManifestRenderType ||
		manifest.IndexSetID != indexSetID ||
		manifest.RunID != runID ||
		manifest.IndexSchemaVersion != indexsubstrate.IndexSchemaVersion ||
		manifest.RunStartedAt == nil ||
		legacy.Durable.ManifestType != manifest.Type ||
		legacy.Durable.ManifestRender != manifest.Render ||
		legacy.Durable.IndexSchemaVersion != manifest.IndexSchemaVersion ||
		legacy.Durable.SegmentNamespace != manifest.Reachability.SegmentNamespace ||
		manifest.Reachability != indexsubstrate.DefaultManifestReachability() ||
		legacy.Durable.Segments != len(manifest.Segments) ||
		legacy.Durable.Rows != manifest.Counts.Rows ||
		len(legacy.Artifacts.Segments) != len(manifest.Segments) ||
		len(manifest.Segments) > limits.MaxSegments {
		return errors.New("manifest contract")
	}
	var timeProbe struct {
		CreatedAt    string  `json:"created_at"`
		RunStartedAt *string `json:"run_started_at"`
	}
	if err := json.Unmarshal(raw, &timeProbe); err != nil || timeProbe.RunStartedAt == nil {
		return errors.New("run start")
	}
	createdAt, err := indexsubstrate.ParseCanonicalUTCTime(timeProbe.CreatedAt)
	if err != nil || !createdAt.Equal(manifest.CreatedAt) {
		return errors.New("created at")
	}
	runStart, err := indexsubstrate.ParseCanonicalUTCTime(*timeProbe.RunStartedAt)
	if err != nil || !runStart.Equal(*manifest.RunStartedAt) {
		return errors.New("run start")
	}
	if err := indexsubstrate.ValidateManifestLineageStructure(manifest); err != nil {
		return err
	}
	if len(manifest.Coverage) == 0 {
		return errors.New("manifest coverage")
	}
	for _, coverage := range manifest.Coverage {
		if coverage.Scope == nil ||
			(coverage.Basis != indexsubstrate.CoverageBasisConfirmed &&
				coverage.Basis != indexsubstrate.CoverageBasisInferred) ||
			!validBridgeScope(*coverage.Scope) {
			return errors.New("manifest coverage")
		}
		for _, gap := range coverage.Gaps {
			if !validBridgeScope(gap) {
				return errors.New("manifest coverage")
			}
		}
	}
	if manifest.Counts.Rows < 0 || manifest.Counts.ActiveRows < 0 ||
		manifest.Counts.Tombstones < 0 || manifest.Counts.DistinctETags < 0 ||
		int64(manifest.Counts.Rows) > maxBridgeJSONCount ||
		int64(manifest.Counts.ActiveRows) > maxBridgeJSONCount ||
		int64(manifest.Counts.Tombstones) > maxBridgeJSONCount ||
		int64(manifest.Counts.DistinctETags) > maxBridgeJSONCount ||
		manifest.SegmentSizing.TargetRowsPerSegment <= 0 ||
		strings.TrimSpace(manifest.SegmentSizing.Rationale) == "" {
		return errors.New("manifest counts")
	}
	parentSeen := make(map[string]struct{}, len(manifest.ParentManifests))
	for _, parent := range manifest.ParentManifests {
		if !fullAcquiredIndexSetRE.MatchString(parent.IndexSetID) ||
			!acquiredRunIDRE.MatchString(parent.RunID) ||
			(parent.ManifestSHA256 != "" && !bridgeSHA256RE.MatchString(parent.ManifestSHA256)) {
			return errors.New("parent manifest")
		}
		key := parent.IndexSetID + "\x00" + parent.RunID
		if _, duplicate := parentSeen[key]; duplicate {
			return errors.New("parent manifest")
		}
		parentSeen[key] = struct{}{}
	}
	var rows int64
	for i, segment := range manifest.Segments {
		ref := legacy.Artifacts.Segments[i]
		if segment.Path == "" ||
			!bridgeSegmentIDRE.MatchString(segment.SegmentID) ||
			segment.Path != segment.SegmentID+".parquet" ||
			ref.Path != "segments/"+segment.Path ||
			ref.SizeBytes != segment.SizeBytes ||
			ref.SHA256 != segment.Digest.Hex ||
			segment.Format != indexsubstrate.SegmentFormatParquet ||
			segment.Digest.Algorithm != "sha256" ||
			!bridgeSHA256RE.MatchString(segment.Digest.Hex) ||
			segment.SizeBytes < 0 || segment.SizeBytes > limits.MaxSegmentBytes ||
			segment.Rows < 0 || segment.Tombstones < 0 || segment.DistinctETags < 0 ||
			int64(segment.Rows) > maxBridgeJSONCount ||
			int64(segment.Tombstones) > maxBridgeJSONCount ||
			int64(segment.DistinctETags) > maxBridgeJSONCount {
			return errors.New("segment binding")
		}
		next, ok := checkedBridgeAdd(rows, int64(segment.Rows))
		if !ok {
			return errors.New("row overflow")
		}
		rows = next
	}
	if rows != int64(manifest.Counts.Rows) {
		return errors.New("row sum")
	}
	return nil
}

func validateBridgeEvidence(
	raw []byte,
	indexSetID, runID, manifestSHA string,
	segments int,
	runStartedAt time.Time,
	maxBytes int64,
) (BridgeSnapshotTime, error) {
	if len(raw) == 0 {
		return BridgeSnapshotTime{Basis: BridgeSnapshotTimeLegacyUnavailable}, nil
	}
	if int64(len(raw)) > maxBytes || bytes.HasPrefix(raw, []byte{0xef, 0xbb, 0xbf}) {
		return BridgeSnapshotTime{}, newBridgeError(BridgeErrorEvidenceInvalid)
	}
	var evidence completeV2Evidence
	if err := strictDecodeJSON(raw, &evidence, true); err != nil ||
		evidence.Type != indexsubstrate.CompleteMarkerTypeV2 ||
		evidence.IndexSetID != indexSetID ||
		evidence.RunID != runID ||
		evidence.ManifestSHA256 != manifestSHA ||
		evidence.Segments != segments ||
		strings.TrimSpace(evidence.ManifestPath) == "" ||
		strings.TrimSpace(evidence.SegmentDir) == "" {
		return BridgeSnapshotTime{}, newBridgeError(BridgeErrorEvidenceInvalid)
	}
	completedAt, err := indexsubstrate.ParseCanonicalUTCTime(evidence.SnapshotCompletedAt)
	if err != nil || indexsubstrate.ValidateExactSnapshotCompletion(
		evidence.SnapshotCompletionSemantics,
		runStartedAt,
		completedAt,
		time.Time{},
	) != nil {
		return BridgeSnapshotTime{}, newBridgeError(BridgeErrorEvidenceInvalid)
	}
	return BridgeSnapshotTime{
		Basis: BridgeSnapshotTimeExactCommit, CompletedAt: evidence.SnapshotCompletedAt,
		EvidenceType: evidence.Type, EvidenceSHA256: sha256HexBridge(raw),
	}, nil
}

func spoolVerifiedBridgeSegments(
	ctx context.Context,
	source HubExactObjectReader,
	stage, indexSetID, runID string,
	manifest indexsubstrate.InternalManifest,
	legacy legacyHubComplete,
	limits BridgeLimits,
) ([]verifiedBridgeSegment, int64, error) {
	segments := make([]verifiedBridgeSegment, 0, len(manifest.Segments))
	var aggregate int64
	for i, descriptor := range manifest.Segments {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		ref := legacy.Artifacts.Segments[i]
		next, ok := checkedBridgeAdd(aggregate, ref.SizeBytes)
		if !ok || next > limits.MaxAggregateBytes {
			return nil, 0, newBridgeError(BridgeErrorResourceLimit)
		}
		aggregate = next
		spoolPath := filepath.Join(stage, fmt.Sprintf("segment-%06d.bin", i))
		if err := spoolExactBridgeObject(
			ctx,
			source,
			exactHubKey(indexSetID, runID, "segments/"+descriptor.Path),
			spoolPath,
			ref,
		); err != nil {
			return nil, 0, err
		}
		segments = append(segments, verifiedBridgeSegment{
			ref:       bridgeV3ArtifactRef(ref),
			spoolPath: spoolPath,
		})
	}
	return segments, aggregate, nil
}

func spoolExactBridgeObject(
	ctx context.Context,
	source HubExactObjectReader,
	key, spoolPath string,
	ref bridgeArtifactRef,
) error {
	body, declared, err := source.OpenExact(ctx, key)
	if err != nil || body == nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return newBridgeError(BridgeErrorSourceRead)
	}
	defer func() { _ = body.Close() }()
	if declared != ref.SizeBytes {
		return newBridgeError(BridgeErrorSegmentInvalid)
	}
	out, err := os.OpenFile(spoolPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G304 -- private stage plus loop index.
	if err != nil {
		return newBridgeError(BridgeErrorWorkspace)
	}
	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(out, hash), io.LimitReader(body, ref.SizeBytes+1))
	closeErr := out.Close()
	if copyErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return newBridgeError(BridgeErrorSourceRead)
	}
	if closeErr != nil {
		return newBridgeError(BridgeErrorWorkspace)
	}
	if n != ref.SizeBytes || hex.EncodeToString(hash.Sum(nil)) != ref.SHA256 {
		return newBridgeError(BridgeErrorSegmentInvalid)
	}
	return nil
}

func checkedBridgeAdd(values ...int64) (int64, bool) {
	var total int64
	for _, value := range values {
		if value < 0 || value > int64(^uint64(0)>>1)-total {
			return 0, false
		}
		total += value
	}
	return total, true
}

func checkedBridgeReceiptAdd(values ...int64) (int64, bool) {
	total, ok := checkedBridgeAdd(values...)
	return total, ok && total <= maxBridgeJSONCount
}

func sha256HexBridge(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func bridgeV3ArtifactRef(ref bridgeArtifactRef) bridgeArtifactRef {
	ref.ETag = ""
	return ref
}

func validBridgeScope(scope indexsubstrate.Scope) bool {
	prefix := strings.TrimSpace(scope.Prefix)
	if prefix == "" || prefix == "/" || strings.HasPrefix(prefix, "/") ||
		strings.Contains(prefix, "\\") || strings.Contains(prefix, "%") {
		return false
	}
	if strings.HasSuffix(prefix, "//") {
		return false
	}
	// Legacy writers emit directory-form coverage with one terminal slash.
	// The stripped candidate decides safety only; original bytes stay
	// authoritative for hashing, binding, copying, and publishing.
	candidate := strings.TrimSuffix(prefix, "/")
	if candidate == "" {
		return false
	}
	if candidate == indexsubstrate.RelativeRootScopePrefix {
		return prefix == indexsubstrate.RelativeRootScopePrefix
	}
	cleaned := path.Clean(candidate)
	if cleaned != candidate || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return false
	}
	return prefix == candidate || prefix == candidate+"/"
}
