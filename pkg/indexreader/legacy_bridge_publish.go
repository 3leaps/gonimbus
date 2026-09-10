package indexreader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
)

func publishVerifiedLegacyBridge(
	ctx context.Context,
	target HubConditionalCreatePublisher,
	verified verifiedBridgeInput,
	opts BridgeOptions,
	limits BridgeLimits,
) (BridgeResult, error) {
	conversionBytes, conversionSHA, err := renderBridgeConversion(verified, limits)
	if err != nil {
		return BridgeResult{}, err
	}
	_ = conversionBytes // The digest is bound; conversion bytes are not a target artifact.

	var bytesCreated int64
	identityKey := path.Join("index-sets", opts.IndexSetID, "identity.json")
	created, err := createOrAdoptBridgeBytes(
		ctx, target, identityKey, verified.identity.bytes, verified.identity.sha256, limits.MaxIdentityBytes,
	)
	if err != nil {
		return BridgeResult{}, err
	}
	if created {
		next, ok := checkedBridgeReceiptAdd(bytesCreated, int64(len(verified.identity.bytes)))
		if !ok {
			return BridgeResult{}, newBridgeError(BridgeErrorResourceLimit)
		}
		bytesCreated = next
	}

	runPrefix := path.Join("index-sets", opts.IndexSetID, "runs", opts.RunID)
	created, err = createOrAdoptBridgeBytes(
		ctx,
		target,
		path.Join(runPrefix, "manifest.json"),
		verified.manifestBytes,
		verified.manifestSHA,
		limits.MaxManifestBytes,
	)
	if err != nil {
		return BridgeResult{}, err
	}
	if created {
		next, ok := checkedBridgeReceiptAdd(bytesCreated, int64(len(verified.manifestBytes)))
		if !ok {
			return BridgeResult{}, newBridgeError(BridgeErrorResourceLimit)
		}
		bytesCreated = next
	}

	for _, segment := range verified.segments {
		created, err = createOrAdoptBridgeFile(
			ctx,
			target,
			path.Join(runPrefix, segment.ref.Path),
			segment.spoolPath,
			segment.ref.SizeBytes,
			segment.ref.SHA256,
		)
		if err != nil {
			return BridgeResult{}, err
		}
		if created {
			next, ok := checkedBridgeReceiptAdd(bytesCreated, segment.ref.SizeBytes)
			if !ok {
				return BridgeResult{}, newBridgeError(BridgeErrorResourceLimit)
			}
			bytesCreated = next
		}
	}

	hubCommittedAt, err := bridgeHubCommitTime(opts.HubCommitClock, verified)
	if err != nil {
		return BridgeResult{}, err
	}
	candidate := buildBridgeV3Marker(verified, conversionSHA, hubCommittedAt, opts.ExportedBy)
	candidateBytes, err := json.MarshalIndent(candidate, "", "  ")
	if err != nil {
		return BridgeResult{}, newBridgeError(BridgeErrorInternalInvariant)
	}
	candidateBytes = append(candidateBytes, '\n')
	if int64(len(candidateBytes)) > limits.MaxMarkerBytes {
		return BridgeResult{}, newBridgeError(BridgeErrorResourceLimit)
	}
	var candidateProbe bridgeHubCompleteV3
	if err := strictDecodeJSON(candidateBytes, &candidateProbe, true); err != nil ||
		validateAdoptedBridgeMarker(candidateProbe, verified, conversionSHA) != nil {
		return BridgeResult{}, newBridgeError(BridgeErrorInternalInvariant)
	}
	bytesCreatedWithMarker, ok := checkedBridgeReceiptAdd(bytesCreated, int64(len(candidateBytes)))
	if !ok {
		return BridgeResult{}, newBridgeError(BridgeErrorResourceLimit)
	}

	completeKey := path.Join(runPrefix, "complete.json")
	created, attempted, createErr := createExactBridgeObject(
		ctx,
		target,
		completeKey,
		bytes.NewReader(candidateBytes),
		int64(len(candidateBytes)),
	)
	if created {
		return bridgeResultFromMarker(
			verified,
			candidate,
			sha256HexBridge(candidateBytes),
			bytesCreatedWithMarker,
			BridgeDispositionCreated,
		), nil
	}
	if !attempted {
		return BridgeResult{}, createErr
	}

	reconcileCtx, cancelReconcile := context.WithTimeout(
		context.WithoutCancel(ctx),
		bridgeTerminalReconcileTimeout,
	)
	defer cancelReconcile()
	existingBytes, err := readExactTargetBytes(
		reconcileCtx,
		target,
		completeKey,
		limits.MaxMarkerBytes,
	)
	if err != nil {
		if createErr != nil {
			return BridgeResult{}, createErr
		}
		return BridgeResult{}, err
	}
	var existing bridgeHubCompleteV3
	if err := strictDecodeJSON(existingBytes, &existing, true); err != nil ||
		validateAdoptedBridgeMarker(existing, verified, conversionSHA) != nil {
		return BridgeResult{}, newBridgeError(BridgeErrorTargetConflict)
	}
	return bridgeResultFromMarker(
		verified,
		existing,
		sha256HexBridge(existingBytes),
		bytesCreated,
		BridgeDispositionAlreadyIdentical,
	), nil
}

func renderBridgeConversion(
	verified verifiedBridgeInput,
	limits BridgeLimits,
) ([]byte, string, error) {
	conversion := bridgeConversionFromVerified(verified)
	data, err := json.Marshal(conversion)
	if err != nil {
		return nil, "", newBridgeError(BridgeErrorInternalInvariant)
	}
	data = append(data, '\n')
	if int64(len(data)) > limits.MaxConversionBytes {
		return nil, "", newBridgeError(BridgeErrorResourceLimit)
	}
	return data, sha256HexBridge(data), nil
}

func bridgeConversionFromVerified(verified verifiedBridgeInput) bridgeConversionIdentity {
	segments := make([]bridgeConversionSegment, 0, len(verified.segments))
	for _, segment := range verified.segments {
		segments = append(segments, bridgeConversionSegment{
			SHA256: segment.ref.SHA256, SizeBytes: segment.ref.SizeBytes,
		})
	}
	return bridgeConversionIdentity{
		Type: BridgeConversionType, IndexSetID: verified.legacy.IndexSetID,
		RunID: verified.legacy.RunID, LegacyMarkerSHA256: verified.legacySHA,
		IdentitySHA256: verified.identity.sha256, IdentitySchema: BridgeIdentitySchema,
		IdentityProfile: BridgeIdentityProfile, ManifestSHA256: verified.manifestSHA,
		Segments: segments, RunStart: verified.runStart, SnapshotTime: verified.snapshotTime,
	}
}

func bridgeConversionFromMarker(marker bridgeHubCompleteV3) bridgeConversionIdentity {
	segments := make([]bridgeConversionSegment, 0, len(marker.Artifacts.Segments))
	for _, segment := range marker.Artifacts.Segments {
		segments = append(segments, bridgeConversionSegment{
			SHA256: segment.SHA256, SizeBytes: segment.SizeBytes,
		})
	}
	return bridgeConversionIdentity{
		Type: BridgeConversionType, IndexSetID: marker.IndexSetID, RunID: marker.RunID,
		LegacyMarkerSHA256: marker.LegacyMarkerSHA256,
		IdentitySHA256:     marker.Artifacts.Identity.SHA256,
		IdentitySchema:     marker.IdentitySchema, IdentityProfile: marker.IdentityProfile,
		ManifestSHA256: marker.Artifacts.Manifest.SHA256, Segments: segments,
		RunStart: marker.RunStart, SnapshotTime: marker.SnapshotTime,
	}
}

func bridgeHubCommitTime(
	clock func() time.Time,
	verified verifiedBridgeInput,
) (time.Time, error) {
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	committedAt := clock()
	if committedAt.IsZero() {
		return time.Time{}, newBridgeError(BridgeErrorInternalInvariant)
	}
	_, offset := committedAt.Zone()
	if offset != 0 {
		return time.Time{}, newBridgeError(BridgeErrorInternalInvariant)
	}
	if committedAt.Before(*verified.manifest.RunStartedAt) {
		return time.Time{}, newBridgeError(BridgeErrorInternalInvariant)
	}
	if verified.snapshotTime.Basis == BridgeSnapshotTimeExactCommit {
		completedAt, err := indexsubstrate.ParseCanonicalUTCTime(verified.snapshotTime.CompletedAt)
		if err != nil || committedAt.Before(completedAt) {
			return time.Time{}, newBridgeError(BridgeErrorInternalInvariant)
		}
	}
	return committedAt.UTC(), nil
}

func buildBridgeV3Marker(
	verified verifiedBridgeInput,
	conversionSHA string,
	hubCommittedAt time.Time,
	exportedBy string,
) bridgeHubCompleteV3 {
	segments := make([]bridgeArtifactRef, 0, len(verified.segments))
	for _, segment := range verified.segments {
		segments = append(segments, segment.ref)
	}
	return bridgeHubCompleteV3{
		Version: "1.0", MarkerSchemaVersion: HubMarkerSchemaV3,
		Format: "durable-v2", FormatVersion: "2",
		IndexSetID: verified.legacy.IndexSetID, RunID: verified.legacy.RunID,
		LegacyMarkerSchemaVersion: HubMarkerSchemaV1,
		LegacyMarkerSHA256:        verified.legacySHA,
		IdentitySchema:            BridgeIdentitySchema, IdentityProfile: BridgeIdentityProfile,
		RunStart: verified.runStart, SnapshotTime: verified.snapshotTime,
		ConversionIdentitySHA256: conversionSHA,
		HubCommittedAt:           hubCommittedAt.Format(time.RFC3339Nano),
		ExportedBy:               strings.TrimSpace(exportedBy),
		Artifacts: bridgeV3Artifacts{
			Identity: bridgeArtifactRef{
				Path: "identity.json", Role: "identity", Required: true,
				SizeBytes: int64(len(verified.identity.bytes)), SHA256: verified.identity.sha256,
			},
			Manifest: bridgeV3ArtifactRef(verified.legacy.Artifacts.Manifest),
			Segments: segments,
		},
		Durable: verified.legacy.Durable,
	}
}

func validateAdoptedBridgeMarker(
	marker bridgeHubCompleteV3,
	verified verifiedBridgeInput,
	expectedConversionSHA string,
) error {
	if marker.Version != "1.0" ||
		marker.MarkerSchemaVersion != HubMarkerSchemaV3 ||
		marker.Format != "durable-v2" ||
		marker.FormatVersion != "2" ||
		marker.IndexSetID != verified.legacy.IndexSetID ||
		marker.RunID != verified.legacy.RunID ||
		marker.LegacyMarkerSchemaVersion != HubMarkerSchemaV1 ||
		marker.LegacyMarkerSHA256 != verified.legacySHA ||
		marker.IdentitySchema != BridgeIdentitySchema ||
		marker.IdentityProfile != BridgeIdentityProfile ||
		marker.RunStart != verified.runStart ||
		marker.SnapshotTime != verified.snapshotTime ||
		marker.ConversionIdentitySHA256 != expectedConversionSHA ||
		strings.TrimSpace(marker.ExportedBy) == "" ||
		marker.Durable != verified.legacy.Durable {
		return errors.New("marker contract")
	}
	hubCommittedAt, err := indexsubstrate.ParseCanonicalUTCTime(marker.HubCommittedAt)
	if err != nil || hubCommittedAt.Before(*verified.manifest.RunStartedAt) {
		return errors.New("marker time")
	}
	if err := validateBridgeSnapshotTime(marker.SnapshotTime, *verified.manifest.RunStartedAt, hubCommittedAt); err != nil {
		return err
	}
	expectedIdentity := bridgeArtifactRef{
		Path: "identity.json", Role: "identity", Required: true,
		SizeBytes: int64(len(verified.identity.bytes)), SHA256: verified.identity.sha256,
	}
	if marker.Artifacts.Identity != expectedIdentity ||
		marker.Artifacts.Manifest != bridgeV3ArtifactRef(verified.legacy.Artifacts.Manifest) ||
		len(marker.Artifacts.Segments) != len(verified.segments) {
		return errors.New("marker artifact")
	}
	for i, segment := range verified.segments {
		if marker.Artifacts.Segments[i] != segment.ref {
			return errors.New("marker segment")
		}
	}
	recomputedBytes, err := json.Marshal(bridgeConversionFromMarker(marker))
	if err != nil {
		return err
	}
	recomputedBytes = append(recomputedBytes, '\n')
	if sha256HexBridge(recomputedBytes) != marker.ConversionIdentitySHA256 {
		return errors.New("conversion identity")
	}
	return nil
}

func validateBridgeSnapshotTime(
	snapshot BridgeSnapshotTime,
	runStartedAt, hubCommittedAt time.Time,
) error {
	switch snapshot.Basis {
	case BridgeSnapshotTimeLegacyUnavailable:
		if snapshot.CompletedAt != "" || snapshot.EvidenceType != "" || snapshot.EvidenceSHA256 != "" {
			return errors.New("legacy snapshot fields")
		}
		return nil
	case BridgeSnapshotTimeExactCommit:
		if snapshot.EvidenceType != indexsubstrate.CompleteMarkerTypeV2 ||
			!bridgeSHA256RE.MatchString(snapshot.EvidenceSHA256) {
			return errors.New("snapshot evidence")
		}
		completedAt, err := indexsubstrate.ParseCanonicalUTCTime(snapshot.CompletedAt)
		if err != nil {
			return err
		}
		return indexsubstrate.ValidateExactSnapshotCompletion(
			indexsubstrate.SnapshotCompletionSemanticsCompleteMarkerCommit,
			runStartedAt,
			completedAt,
			hubCommittedAt,
		)
	default:
		return errors.New("snapshot basis")
	}
}

func createOrAdoptBridgeBytes(
	ctx context.Context,
	target HubConditionalCreatePublisher,
	key string,
	data []byte,
	expectedSHA string,
	maxBytes int64,
) (bool, error) {
	if int64(len(data)) > maxBytes || sha256HexBridge(data) != expectedSHA {
		return false, newBridgeError(BridgeErrorInternalInvariant)
	}
	created, _, err := createExactBridgeObject(
		ctx,
		target,
		key,
		bytes.NewReader(data),
		int64(len(data)),
	)
	if err == nil && created {
		return true, nil
	}
	size, digest, readErr := digestExactTargetObject(ctx, target, key, maxBytes)
	if readErr != nil {
		if err != nil {
			return false, err
		}
		return false, readErr
	}
	if size != int64(len(data)) || digest != expectedSHA {
		return false, newBridgeError(BridgeErrorTargetConflict)
	}
	return false, nil
}

func createOrAdoptBridgeFile(
	ctx context.Context,
	target HubConditionalCreatePublisher,
	key, filename string,
	size int64,
	expectedSHA string,
) (bool, error) {
	file, err := os.Open(filename) // #nosec G304 -- internally generated private-spool path.
	if err != nil {
		return false, newBridgeError(BridgeErrorWorkspace)
	}
	defer func() { _ = file.Close() }()
	created, _, err := createExactBridgeObject(ctx, target, key, file, size)
	if err == nil && created {
		return true, nil
	}
	gotSize, gotSHA, readErr := digestExactTargetObject(ctx, target, key, size)
	if readErr != nil {
		if err != nil {
			return false, err
		}
		return false, readErr
	}
	if gotSize != size || gotSHA != expectedSHA {
		return false, newBridgeError(BridgeErrorTargetConflict)
	}
	return false, nil
}

func createExactBridgeObject(
	ctx context.Context,
	target HubConditionalCreatePublisher,
	key string,
	body io.Reader,
	size int64,
) (created bool, attempted bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, false, err
	}
	counting := &bridgeCountingReader{reader: body}
	created, err = target.CreateExact(ctx, key, counting, size)
	if err != nil {
		if created && counting.read != size {
			return false, true, newBridgeError(BridgeErrorTargetCreate)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return created, true, ctxErr
		}
		return created, true, newBridgeError(BridgeErrorTargetCreate)
	}
	if created && counting.read != size {
		return false, true, newBridgeError(BridgeErrorTargetCreate)
	}
	return created, true, nil
}

type bridgeCountingReader struct {
	reader io.Reader
	read   int64
}

func (reader *bridgeCountingReader) Read(p []byte) (int, error) {
	n, err := reader.reader.Read(p)
	reader.read += int64(n)
	return n, err
}

func digestExactTargetObject(
	ctx context.Context,
	target HubConditionalCreatePublisher,
	key string,
	maxBytes int64,
) (int64, string, error) {
	if err := ctx.Err(); err != nil {
		return 0, "", err
	}
	body, declared, err := target.OpenExact(ctx, key)
	if err != nil || body == nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, "", ctxErr
		}
		return 0, "", newBridgeError(BridgeErrorTargetRead)
	}
	defer func() { _ = body.Close() }()
	if declared < 0 || declared > maxBytes {
		return 0, "", newBridgeError(BridgeErrorTargetConflict)
	}
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(body, maxBytes+1))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, "", ctxErr
		}
		return 0, "", newBridgeError(BridgeErrorTargetRead)
	}
	if n != declared {
		return 0, "", newBridgeError(BridgeErrorTargetConflict)
	}
	return n, hex.EncodeToString(hash.Sum(nil)), nil
}

func readExactTargetBytes(
	ctx context.Context,
	target HubConditionalCreatePublisher,
	key string,
	maxBytes int64,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, declared, err := target.OpenExact(ctx, key)
	if err != nil || body == nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, newBridgeError(BridgeErrorTargetRead)
	}
	defer func() { _ = body.Close() }()
	if declared < 0 || declared > maxBytes {
		return nil, newBridgeError(BridgeErrorTargetConflict)
	}
	data, err := io.ReadAll(io.LimitReader(body, maxBytes+1))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, newBridgeError(BridgeErrorTargetRead)
	}
	if int64(len(data)) != declared {
		return nil, newBridgeError(BridgeErrorTargetConflict)
	}
	return data, nil
}

func bridgeResultFromMarker(
	verified verifiedBridgeInput,
	marker bridgeHubCompleteV3,
	completeSHA string,
	bytesCreated int64,
	disposition string,
) BridgeResult {
	return BridgeResult{
		IndexSetID: verified.legacy.IndexSetID, RunID: verified.legacy.RunID,
		LegacyMarkerSHA256: verified.legacySHA,
		IdentitySHA256:     verified.identity.sha256,
		IdentitySchema:     BridgeIdentitySchema, IdentityProfile: BridgeIdentityProfile,
		ManifestSHA256:   verified.manifestSHA,
		DeclaredRows:     verified.manifest.Counts.Rows,
		DeclaredSegments: len(verified.manifest.Segments),
		SegmentsVerified: len(verified.segments),
		BytesRead:        verified.bytesRead, BytesConditionallyCreated: bytesCreated,
		RunStart: verified.runStart, SnapshotTime: verified.snapshotTime,
		ConversionIdentitySHA256: marker.ConversionIdentitySHA256,
		HubCommittedAt:           marker.HubCommittedAt, CompleteSHA256: completeSHA,
		ExportedBy: marker.ExportedBy, Disposition: disposition,
	}
}
