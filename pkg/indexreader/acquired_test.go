package indexreader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/fulmenhq/gofulmen/schema"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

type memoryExactHub struct {
	objects map[string][]byte
	reads   []string
	errText string
}

func (h *memoryExactHub) OpenExact(_ context.Context, key string) (io.ReadCloser, int64, error) {
	h.reads = append(h.reads, key)
	if h.errText != "" {
		return nil, 0, &testHubError{text: h.errText}
	}
	data, ok := h.objects[key]
	if !ok {
		return nil, 0, &testHubError{text: "missing remote account object: " + key}
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

type testHubError struct{ text string }

func (e *testHubError) Error() string { return e.text }

type cancelingExactHub struct {
	inner  *memoryExactHub
	target string
	cancel context.CancelFunc
}

func (h *cancelingExactHub) OpenExact(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	body, size, err := h.inner.OpenExact(ctx, key)
	if err != nil || key != h.target {
		return body, size, err
	}
	return &cancelingReadCloser{ReadCloser: body, cancel: h.cancel}, size, nil
}

type cancelingReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
	fired  bool
}

func (r *cancelingReadCloser) Read(p []byte) (int, error) {
	if r.fired {
		return 0, context.Canceled
	}
	r.fired = true
	if len(p) > 1 {
		p = p[:1]
	}
	n, err := r.ReadCloser.Read(p)
	r.cancel()
	if err == nil {
		err = context.Canceled
	}
	return n, err
}

type acquiredHubFixture struct {
	hub         *memoryExactHub
	indexSetID  string
	currentRun  string
	baselineRun string
}

func TestAcquireBundle_ExactCurrentOpenAndIdempotent(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	parent := realTempDir(t)
	dest := filepath.Join(parent, "bundle")
	marker, err := AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun, Destination: dest,
	})
	require.NoError(t, err)
	require.Equal(t, AcquiredBundleType, marker.Type)
	require.FileExists(t, filepath.Join(dest, "acquired.json"))

	reader, err := OpenAcquiredBundle(AcquiredOpenOptions{Directory: dest})
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	require.Equal(t, fx.indexSetID, reader.Meta().IndexSetID)
	require.Equal(t, fx.currentRun, reader.Meta().RunID)
	count, err := reader.QueryObjectCount(context.Background(), indexstore.QueryParams{IndexSetID: fx.indexSetID})
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
	verified, err := reader.(VerifiedSnapshotMetadataReader).VerifiedSnapshotMetadata()
	require.NoError(t, err)
	require.Equal(t, SnapshotSourceAcquiredHub, verified.SourceKind)
	require.NotZero(t, verified.SnapshotCompletedAt)
	require.Equal(t, indexsubstrate.SnapshotCompletionSemanticsCompleteMarkerCommit, verified.SnapshotCompletionSemantics)
	require.NotZero(t, verified.HubCommittedAt)
	require.Len(t, verified.HubCompleteSHA256, 64)
	sameRun, err := reader.ResolveSinceRunFilter(context.Background(), fx.currentRun)
	require.NoError(t, err)
	require.True(t, sameRun.SameRun)
	sameRunCount, err := reader.QueryObjectCount(context.Background(), indexstore.QueryParams{SinceRun: sameRun})
	require.NoError(t, err)
	require.Zero(t, sameRunCount)

	before, err := os.ReadFile(filepath.Join(dest, "acquired.json"))
	require.NoError(t, err)
	again, err := AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun, Destination: dest,
	})
	require.NoError(t, err)
	require.Equal(t, marker.AcquiredAt, again.AcquiredAt)
	after, err := os.ReadFile(filepath.Join(dest, "acquired.json"))
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoFileExists(t, filepath.Join(dest, "latest.json"))
}

func TestAcquiredBundleLimitsCannotWidenFrozenProtocol(t *testing.T) {
	wide := AcquiredBundleLimits{
		MaxMarkerBytes:    maxBridgeJSONCount + 1,
		MaxManifestBytes:  maxBridgeJSONCount + 1,
		MaxIdentityBytes:  maxBridgeJSONCount + 1,
		MaxSegmentBytes:   maxBridgeJSONCount + 1,
		MaxSegments:       maxAcquiredSegments + 1,
		MaxLineageNodes:   maxAcquiredLineageNodes + 1,
		MaxAggregateBytes: maxBridgeJSONCount + 1,
		MaxMetadataBytes:  maxBridgeJSONCount + 1,
	}.normalize()
	require.Equal(t, maxBridgeJSONCount, wide.MaxMarkerBytes)
	require.Equal(t, maxBridgeJSONCount, wide.MaxManifestBytes)
	require.Equal(t, maxBridgeJSONCount, wide.MaxIdentityBytes)
	require.Equal(t, maxBridgeJSONCount, wide.MaxSegmentBytes)
	require.Equal(t, maxBridgeJSONCount, wide.MaxAggregateBytes)
	require.Equal(t, maxBridgeJSONCount, wide.MaxMetadataBytes)
	require.Equal(t, maxAcquiredSegments, wide.MaxSegments)
	require.Equal(t, maxAcquiredLineageNodes, wide.MaxLineageNodes)

	lower := AcquiredBundleLimits{
		MaxMarkerBytes: 7, MaxSegments: 8, MaxLineageNodes: 9,
	}.normalize()
	require.Equal(t, int64(7), lower.MaxMarkerBytes)
	require.Equal(t, 8, lower.MaxSegments)
	require.Equal(t, 9, lower.MaxLineageNodes)

	fx := newAcquiredHubFixture(t)
	completeKey := exactHubKey(fx.indexSetID, fx.currentRun, "complete.json")
	var hub acquiredHubComplete
	require.NoError(t, json.Unmarshal(fx.hub.objects[completeKey], &hub))

	tooManySegments := hub
	tooManySegments.Artifacts.Segments = make([]acquiredHubArtifact, maxAcquiredSegments+1)
	tooManySegments.Durable.Segments = len(tooManySegments.Artifacts.Segments)
	require.ErrorContains(t,
		validateAcquiredHubComplete(fx.indexSetID, fx.currentRun, tooManySegments, wide),
		"artifact count exceeds",
	)

	tooLargeArtifact := hub
	tooLargeArtifact.Artifacts.Identity.SizeBytes = maxBridgeJSONCount + 1
	require.ErrorContains(t,
		validateAcquiredHubComplete(fx.indexSetID, fx.currentRun, tooLargeArtifact, wide),
		"artifact contract mismatch",
	)

	tooLargeRowCount := maxBridgeJSONCount
	tooLargeRowCount++
	if int64(int(tooLargeRowCount)) == tooLargeRowCount {
		tooManyRows := hub
		tooManyRows.Durable.Rows = int(tooLargeRowCount)
		require.ErrorContains(t,
			validateAcquiredHubComplete(fx.indexSetID, fx.currentRun, tooManyRows, wide),
			"artifact count exceeds",
		)
	}

	hash := strings.Repeat("a", 64)
	startedAt := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	lineage := make([]AcquiredLineageNode, maxAcquiredLineageNodes+1)
	marker := AcquiredBundleMarker{
		Type: AcquiredBundleTypeV2, Schema: AcquiredBundleSchemaV2,
		IndexSetID: "idx_" + hash, RunID: "run_1",
		SourceIdentitySHA256: hash, SourceIdentitySchema: BridgeIdentitySchema,
		SourceIdentityProfile:  BridgeIdentityProfile,
		HubMarkerSchemaVersion: HubMarkerSchemaV3,
		RunStart: &BridgeRunStart{
			Basis: BridgeRunStartLegacyAsserted, StartedAt: startedAt.Format(time.RFC3339Nano),
		},
		SnapshotTime:             &BridgeSnapshotTime{Basis: BridgeSnapshotTimeLegacyUnavailable},
		ConversionIdentitySHA256: hash,
		HubCommittedAt:           startedAt.Add(time.Minute).Format(time.RFC3339Nano),
		HubCompleteSHA256:        hash, ManifestSHA256: hash,
		AcquiredAt: startedAt.Add(2 * time.Minute).Format(time.RFC3339Nano),
		Lineage:    lineage,
	}
	require.ErrorContains(t, validateAcquiredMarker(marker, wide), "lineage node limit exceeded")

	marker.Lineage = make([]AcquiredLineageNode, maxAcquiredLineageNodes)
	marker.Artifacts = []AcquiredArtifact{{
		Path: "identity.json", Role: acquiredIdentityRole, SizeBytes: 1, SHA256: hash,
	}}
	for i := range marker.Lineage {
		runID := "run_" + strconv.Itoa(i+1)
		marker.Lineage[i] = AcquiredLineageNode{
			IndexSetID: marker.IndexSetID, RunID: runID,
			MarkerSchemaVersion: HubMarkerSchemaV2,
			HubCompleteSHA256:   hash, ManifestSHA256: hash,
		}
		if i == 0 {
			marker.Lineage[i].MarkerSchemaVersion = HubMarkerSchemaV3
			marker.Lineage[i].ConversionIdentitySHA256 = hash
		}
		prefix := "runs/" + runID + "/"
		marker.Artifacts = append(marker.Artifacts,
			AcquiredArtifact{
				Path: prefix + "hub-complete.json", Role: acquiredHubCompleteRole,
				SizeBytes: 1, SHA256: hash,
			},
			AcquiredArtifact{
				Path: prefix + "manifest.json", Role: acquiredManifestRole,
				SizeBytes: 1, SHA256: hash,
			},
		)
	}
	marker.ProofThroughRunID = marker.Lineage[len(marker.Lineage)-1].RunID
	require.NoError(t, validateAcquiredMarker(marker, wide))
}

func TestAcquireBundleV2_PreservesBridgeTimeClassification(t *testing.T) {
	tests := []struct {
		name          string
		evidence      func(*legacyBridgeFixture) []byte
		wantBasis     string
		wantCompleted bool
	}{
		{
			name:      "legacy unavailable",
			wantBasis: BridgeSnapshotTimeLegacyUnavailable,
		},
		{
			name: "exact commit",
			evidence: func(fixture *legacyBridgeFixture) []byte {
				return fixture.completeV2Evidence(t)
			},
			wantBasis:     BridgeSnapshotTimeExactCommit,
			wantCompleted: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newLegacyBridgeFixture(t)
			target := newBridgeMemoryHub()
			var evidence []byte
			if tc.evidence != nil {
				evidence = tc.evidence(fixture)
			}
			_, err := BridgeLegacyDurableRun(
				context.Background(),
				fixture.source,
				target,
				CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
				evidence,
				fixture.options(t),
			)
			require.NoError(t, err)

			dest := filepath.Join(realTempDir(t), "bundle")
			marker, err := AcquireBundle(context.Background(), target, AcquireBundleOptions{
				IndexSetID:  fixture.indexSetID,
				RunID:       fixture.runID,
				Destination: dest,
			})
			require.NoError(t, err)
			require.Equal(t, AcquiredBundleTypeV2, marker.Type)
			require.Equal(t, AcquiredBundleSchemaV2, marker.Schema)
			require.Equal(t, HubMarkerSchemaV3, marker.HubMarkerSchemaVersion)
			require.Equal(t, BridgeIdentitySchema, marker.SourceIdentitySchema)
			require.Equal(t, BridgeIdentityProfile, marker.SourceIdentityProfile)
			require.Equal(t, tc.wantBasis, marker.SnapshotTime.Basis)
			require.NoFileExists(t, filepath.Join(dest, "runs", fixture.runID, "complete.json"))

			data, err := os.ReadFile(filepath.Join(dest, "acquired.json"))
			require.NoError(t, err)
			diagnostics, err := acquiredBundleSchemaValidatorVersion(
				t, "index-acquired-bundle.v2.schema.json",
			).ValidateJSON(data)
			require.NoError(t, err)
			for _, diagnostic := range diagnostics {
				if diagnostic.Severity == schema.SeverityError {
					t.Fatalf("acquired v2 marker failed schema validation: %s: %s", diagnostic.Pointer, diagnostic.Message)
				}
			}

			reader, err := OpenAcquiredBundle(AcquiredOpenOptions{Directory: dest})
			require.NoError(t, err)
			defer func() { require.NoError(t, reader.Close()) }()
			verified, err := reader.(VerifiedSnapshotMetadataReader).VerifiedSnapshotMetadata()
			require.NoError(t, err)
			require.Equal(t, SnapshotSourceAcquiredHub, verified.SourceKind)
			require.Equal(t, BridgeRunStartLegacyAsserted, verified.RunStart.Basis)
			require.Equal(t, tc.wantBasis, verified.SnapshotTime.Basis)
			require.Equal(t, tc.wantCompleted, !verified.SnapshotCompletedAt.IsZero())
			count, err := reader.QueryObjectCount(context.Background(), indexstore.QueryParams{})
			require.NoError(t, err)
			require.Equal(t, int64(1), count)
		})
	}
}

func TestAcquireBundle_RefusesRawLegacyV1Marker(t *testing.T) {
	fixture := newLegacyBridgeFixture(t)
	_, err := AcquireBundle(context.Background(), fixture.source, AcquireBundleOptions{
		IndexSetID:  fixture.indexSetID,
		RunID:       fixture.runID,
		Destination: filepath.Join(realTempDir(t), "bundle"),
	})
	require.ErrorContains(t, err, "durable hub complete marker contract mismatch")
}

func TestAcquireBundleV2_RefusesRewrittenBridgeClassification(t *testing.T) {
	fixture := newLegacyBridgeFixture(t)
	target := newBridgeMemoryHub()
	_, err := BridgeLegacyDurableRun(
		context.Background(),
		fixture.source,
		target,
		CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
		nil,
		fixture.options(t),
	)
	require.NoError(t, err)
	completeKey := exactHubKey(fixture.indexSetID, fixture.runID, "complete.json")
	var marker acquiredHubComplete
	require.NoError(t, json.Unmarshal(target.object(completeKey), &marker))
	marker.ConversionIdentitySHA256 = strings.Repeat("b", 64)
	data, err := json.MarshalIndent(marker, "", "  ")
	require.NoError(t, err)
	target.put(completeKey, append(data, '\n'))

	_, err = AcquireBundle(context.Background(), target, AcquireBundleOptions{
		IndexSetID:  fixture.indexSetID,
		RunID:       fixture.runID,
		Destination: filepath.Join(realTempDir(t), "bundle"),
	})
	require.ErrorContains(t, err, "conversion identity mismatch")
}

func TestAcquireBundle_PreFixV2IsStructurallyValidButTimeIneligible(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	completeKey := exactHubKey(fx.indexSetID, fx.currentRun, "complete.json")
	var complete acquiredHubComplete
	require.NoError(t, json.Unmarshal(fx.hub.objects[completeKey], &complete))
	complete.SnapshotCompletionSemantics = ""
	complete.ExportedBy = "gonimbus/99.0.0"
	require.NotEqual(t, complete.SnapshotCompletedAt, complete.HubCommittedAt)
	require.NoError(t, validateAcquiredHubComplete(
		fx.indexSetID,
		fx.currentRun,
		complete,
		AcquiredBundleLimits{}.normalize(),
	))
	data, err := json.MarshalIndent(complete, "", "  ")
	require.NoError(t, err)
	fx.hub.objects[completeKey] = data

	dest := filepath.Join(realTempDir(t), "bundle")
	_, err = AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID:  fx.indexSetID,
		RunID:       fx.currentRun,
		Destination: dest,
	})
	require.ErrorContains(t, err, "not exact-time eligible")
	require.NoDirExists(t, dest)
}

func TestAcquireBundle_ProofThroughSupportsPinnedDelta(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	dest := filepath.Join(realTempDir(t), "with-proof")
	_, err := AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun,
		ProofThroughRunID: fx.baselineRun, Destination: dest,
	})
	require.NoError(t, err)
	reader, err := OpenAcquiredBundle(AcquiredOpenOptions{Directory: dest})
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	filter, err := reader.ResolveSinceRunFilter(context.Background(), fx.baselineRun)
	require.NoError(t, err)
	require.Equal(t, fx.baselineRun, filter.RunID)
	require.FileExists(t, filepath.Join(dest, "runs", fx.baselineRun, "manifest.json"))
	require.NoDirExists(t, filepath.Join(dest, "runs", fx.baselineRun, "segments"))
	for _, key := range fx.hub.reads {
		require.NotContains(t, key, "latest.json")
	}
}

func TestAcquireBundle_ProofThroughNonAncestorFailsClosed(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	dest := filepath.Join(realTempDir(t), "bad-proof")
	_, err := AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun,
		ProofThroughRunID: "run_1788949900000000000", Destination: dest,
	})
	require.Error(t, err)
	require.NoDirExists(t, dest)
	entries, readErr := os.ReadDir(filepath.Dir(dest))
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestAcquireBundle_DifferentProofConflictsWithoutOverlay(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	dest := filepath.Join(realTempDir(t), "bundle")
	first, err := AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun, Destination: dest,
	})
	require.NoError(t, err)
	before, err := os.ReadFile(filepath.Join(dest, "acquired.json"))
	require.NoError(t, err)

	_, err = AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun,
		ProofThroughRunID: fx.baselineRun, Destination: dest,
	})
	require.ErrorIs(t, err, ErrAcquiredBundleConflict)
	after, err := os.ReadFile(filepath.Join(dest, "acquired.json"))
	require.NoError(t, err)
	require.Equal(t, before, after)
	var retained AcquiredBundleMarker
	require.NoError(t, json.Unmarshal(after, &retained))
	require.Equal(t, first.AcquiredAt, retained.AcquiredAt)
	require.Empty(t, retained.ProofThroughRunID)
}

func TestOpenAcquiredBundle_RemainsValidAfterDirectoryMove(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	parent := realTempDir(t)
	original := filepath.Join(parent, "original")
	_, err := AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun, Destination: original,
	})
	require.NoError(t, err)
	moved := filepath.Join(parent, "moved")
	require.NoError(t, os.Rename(original, moved))

	reader, err := OpenAcquiredBundle(AcquiredOpenOptions{Directory: moved})
	require.NoError(t, err)
	require.Equal(t, fx.currentRun, reader.Meta().RunID)
	require.NoError(t, reader.Close())
}

func TestAcquireBundle_IdentityMismatchAndProviderErrorFailClosed(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	identityKey := exactHubKey(fx.indexSetID, fx.currentRun, "identity.json")
	fx.hub.objects[identityKey] = append(append([]byte(nil), fx.hub.objects[identityKey]...), ' ')
	dest := filepath.Join(realTempDir(t), "bad-identity")
	_, err := AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun, Destination: dest,
	})
	require.Error(t, err)
	require.NoDirExists(t, dest)
	entries, readErr := os.ReadDir(filepath.Dir(dest))
	require.NoError(t, readErr)
	require.Empty(t, entries)

	fx = newAcquiredHubFixture(t)
	fx.hub.errText = "account=secret-account credential=secret-token"
	dest = filepath.Join(realTempDir(t), "provider-failure")
	_, err = AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun, Destination: dest,
	})
	require.ErrorIs(t, err, ErrHubExactRead)
	require.NotContains(t, err.Error(), "secret-account")
	require.NotContains(t, err.Error(), "secret-token")
	require.NoDirExists(t, dest)
}

func TestAcquireBundle_CancellationCleansOnlyStagingPath(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	var complete acquiredHubComplete
	require.NoError(t, json.Unmarshal(
		fx.hub.objects[exactHubKey(fx.indexSetID, fx.currentRun, "complete.json")],
		&complete,
	))
	require.NotEmpty(t, complete.Artifacts.Segments)
	target := exactHubKey(fx.indexSetID, fx.currentRun, complete.Artifacts.Segments[0].Path)
	ctx, cancel := context.WithCancel(context.Background())
	parent := realTempDir(t)
	unrelated := filepath.Join(parent, "unrelated")
	require.NoError(t, os.WriteFile(unrelated, []byte("preserve"), 0o600))
	dest := filepath.Join(parent, "bundle")

	_, err := AcquireBundle(ctx, &cancelingExactHub{
		inner: fx.hub, target: target, cancel: cancel,
	}, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun, Destination: dest,
	})
	require.ErrorIs(t, err, context.Canceled)
	require.NoDirExists(t, dest)
	require.FileExists(t, unrelated)
	entries, readErr := os.ReadDir(parent)
	require.NoError(t, readErr)
	require.Len(t, entries, 1)
	require.Equal(t, "unrelated", entries[0].Name())
}

func TestAcquireBundle_RejectsAncestorSwapBeforeParentBind(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	base := realTempDir(t)
	anchor := filepath.Join(base, "anchor")
	parent := filepath.Join(anchor, "parent")
	outsideAnchor := filepath.Join(base, "outside")
	outsideParent := filepath.Join(outsideAnchor, "parent")
	require.NoError(t, os.MkdirAll(parent, 0o700))
	require.NoError(t, os.MkdirAll(outsideParent, 0o700))

	oldHook := acquiredBeforeDestinationBind
	t.Cleanup(func() { acquiredBeforeDestinationBind = oldHook })
	acquiredBeforeDestinationBind = func(_ string) error {
		require.NoError(t, os.Rename(anchor, anchor+".original"))
		return os.Symlink(outsideAnchor, anchor)
	}

	_, err := AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun,
		Destination: filepath.Join(parent, "bundle"),
	})
	require.Error(t, err)
	entries, readErr := os.ReadDir(outsideParent)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestOpenAcquiredBundle_RejectsRootSwapToValidBundle(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	parent := realTempDir(t)
	target := filepath.Join(parent, "target")
	replacement := filepath.Join(parent, "replacement")
	_, err := AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun, Destination: target,
	})
	require.NoError(t, err)
	_, err = AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.baselineRun, Destination: replacement,
	})
	require.NoError(t, err)

	oldHook := acquiredBeforeOpenRootBind
	t.Cleanup(func() { acquiredBeforeOpenRootBind = oldHook })
	acquiredBeforeOpenRootBind = func(_ string) error {
		require.NoError(t, os.Rename(target, target+".original"))
		return os.Rename(replacement, target)
	}
	_, err = OpenAcquiredBundle(AcquiredOpenOptions{Directory: target})
	require.ErrorIs(t, err, ErrNotAcquiredBundle)

	acquiredBeforeOpenRootBind = nil
	reader, err := OpenAcquiredBundle(AcquiredOpenOptions{Directory: target})
	require.NoError(t, err)
	require.Equal(t, fx.baselineRun, reader.Meta().RunID, "replacement fixture must itself be a valid bundle")
	require.NoError(t, reader.Close())
}

func TestOpenAcquiredBundle_RetainedRootSurvivesNamedReplacement(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	parent := realTempDir(t)
	target := filepath.Join(parent, "target")
	replacement := filepath.Join(parent, "replacement")
	_, err := AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun, Destination: target,
	})
	require.NoError(t, err)
	_, err = AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.baselineRun, Destination: replacement,
	})
	require.NoError(t, err)

	reader, err := OpenAcquiredBundle(AcquiredOpenOptions{Directory: target})
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	require.NoError(t, os.Rename(target, target+".original"))
	require.NoError(t, os.Rename(replacement, target))

	count, err := reader.QueryObjectCount(context.Background(), indexstore.QueryParams{})
	require.NoError(t, err)
	require.Equal(t, int64(2), count, "query must remain pinned to the retained original root")
	replacementReader, err := OpenAcquiredBundle(AcquiredOpenOptions{Directory: target})
	require.NoError(t, err)
	defer func() { require.NoError(t, replacementReader.Close()) }()
	replacementCount, err := replacementReader.QueryObjectCount(context.Background(), indexstore.QueryParams{})
	require.NoError(t, err)
	require.Equal(t, int64(1), replacementCount)
}

func TestAcquireBundle_StageSwapCleanupPreservesSubstitute(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	fx.hub.errText = "forced read failure"
	parent := realTempDir(t)
	dest := filepath.Join(parent, "bundle")
	var substituteFile string
	oldHook := acquiredBeforeStageCleanup
	t.Cleanup(func() { acquiredBeforeStageCleanup = oldHook })
	acquiredBeforeStageCleanup = func(stagePath string) error {
		require.NoError(t, os.Rename(stagePath, stagePath+".original"))
		require.NoError(t, os.Mkdir(stagePath, 0o700))
		substituteFile = filepath.Join(stagePath, "unrelated")
		return os.WriteFile(substituteFile, []byte("preserve"), 0o600)
	}

	_, err := AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun, Destination: dest,
	})
	require.ErrorIs(t, err, ErrHubExactRead)
	require.FileExists(t, substituteFile)
	data, readErr := os.ReadFile(substituteFile)
	require.NoError(t, readErr)
	require.Equal(t, []byte("preserve"), data)
}

func TestAcquireBundle_StageNameSwapCannotSelectLineageRead(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	parent := realTempDir(t)
	dest := filepath.Join(parent, "bundle")
	maliciousRun := "run_1788950200000000000"
	manifestKey := exactHubKey(fx.indexSetID, fx.currentRun, "manifest.json")
	var substitute indexsubstrate.InternalManifest
	require.NoError(t, json.Unmarshal(fx.hub.objects[manifestKey], &substitute))
	require.NotNil(t, substitute.StateParent)
	substitute.StateParent.RunID = maliciousRun
	substituteData, err := json.Marshal(substitute)
	require.NoError(t, err)

	oldHook := acquiredAfterMetadataWrite
	t.Cleanup(func() { acquiredAfterMetadataWrite = oldHook })
	swapped := false
	acquiredAfterMetadataWrite = func(stagePath, relativePath string) error {
		if swapped || relativePath != "runs/"+fx.currentRun+"/manifest.json" {
			return nil
		}
		swapped = true
		require.NoError(t, os.Rename(stagePath, stagePath+".original"))
		substituteDir := filepath.Join(stagePath, "runs", fx.currentRun)
		require.NoError(t, os.MkdirAll(substituteDir, 0o700))
		return os.WriteFile(filepath.Join(substituteDir, "manifest.json"), substituteData, 0o600)
	}

	_, err = AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun,
		ProofThroughRunID: fx.baselineRun, Destination: dest,
	})
	require.Error(t, err)
	require.True(t, swapped)
	require.Contains(t, fx.hub.reads, exactHubKey(fx.indexSetID, fx.baselineRun, "complete.json"))
	require.NotContains(t, fx.hub.reads, exactHubKey(fx.indexSetID, maliciousRun, "complete.json"))
}

func TestAcquireBundle_PostCommitParentSyncFailureWarnsAndIsIdempotent(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	parent := realTempDir(t)
	dest := filepath.Join(parent, "bundle")
	oldSync := acquiredSyncParentDirectory
	t.Cleanup(func() { acquiredSyncParentDirectory = oldSync })
	acquiredSyncParentDirectory = func(*os.File) error {
		return errors.New("injected parent sync failure")
	}
	var warnings []string
	opts := AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun, Destination: dest,
		Warning: func(message string) {
			warnings = append(warnings, message)
		},
	}
	first, err := AcquireBundle(context.Background(), fx.hub, opts)
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0], "parent sync failed")
	reader, err := OpenAcquiredBundle(AcquiredOpenOptions{Directory: dest})
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	again, err := AcquireBundle(context.Background(), fx.hub, opts)
	require.NoError(t, err)
	require.Equal(t, first.AcquiredAt, again.AcquiredAt)
}

func TestOpenAcquiredBundle_MetadataBudgetIsSingleCharge(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	dest := filepath.Join(realTempDir(t), "bundle")
	_, err := AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun,
		ProofThroughRunID: fx.baselineRun, Destination: dest,
	})
	require.NoError(t, err)
	markerData, err := os.ReadFile(filepath.Join(dest, "acquired.json"))
	require.NoError(t, err)
	var marker AcquiredBundleMarker
	require.NoError(t, json.Unmarshal(markerData, &marker))
	metadataBytes := int64(len(markerData))
	for _, artifact := range marker.Artifacts {
		if artifact.Role != acquiredSegmentRole {
			metadataBytes += artifact.SizeBytes
		}
	}
	reader, err := OpenAcquiredBundle(AcquiredOpenOptions{
		Directory: dest, Limits: AcquiredBundleLimits{MaxMetadataBytes: metadataBytes},
	})
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	_, err = OpenAcquiredBundle(AcquiredOpenOptions{
		Directory: dest, Limits: AcquiredBundleLimits{MaxMetadataBytes: metadataBytes - 1},
	})
	require.ErrorIs(t, err, ErrNotAcquiredBundle)
}

func TestAcquireBundle_RejectsSegmentPathAliases(t *testing.T) {
	for _, alias := range []string{
		"segments/../escape.parquet",
		"segments/%2e%2e/escape.parquet",
		"segments//escape.parquet",
		`segments\escape.parquet`,
		"segments/line\nbreak.parquet",
		"segments/unicode-\u00e9.parquet",
	} {
		t.Run(strings.ReplaceAll(alias, "/", "_"), func(t *testing.T) {
			fx := newAcquiredHubFixture(t)
			key := exactHubKey(fx.indexSetID, fx.currentRun, "complete.json")
			var complete acquiredHubComplete
			require.NoError(t, json.Unmarshal(fx.hub.objects[key], &complete))
			require.NotEmpty(t, complete.Artifacts.Segments)
			complete.Artifacts.Segments[0].Path = alias
			data, err := json.Marshal(complete)
			require.NoError(t, err)
			fx.hub.objects[key] = data
			dest := filepath.Join(realTempDir(t), "bundle")

			_, err = AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
				IndexSetID: fx.indexSetID, RunID: fx.currentRun, Destination: dest,
			})
			require.ErrorContains(t, err, "segment path is invalid")
			require.NoDirExists(t, dest)
		})
	}
}

func TestOpenAcquiredBundle_RefusesUnmarkedHydrateAndSymlink(t *testing.T) {
	hydrate := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(hydrate, "complete.json"), []byte("{}\n"), 0o600))
	_, err := OpenAcquiredBundle(AcquiredOpenOptions{Directory: hydrate})
	require.ErrorIs(t, err, ErrNotAcquiredBundle)

	fx := newAcquiredHubFixture(t)
	dest := filepath.Join(realTempDir(t), "bundle")
	_, err = AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun, Destination: dest,
	})
	require.NoError(t, err)
	segmentPath := firstFixtureSegmentPath(t, dest, fx.currentRun)
	real := segmentPath + ".real"
	require.NoError(t, os.Rename(segmentPath, real))
	require.NoError(t, os.Symlink(real, segmentPath))
	_, err = OpenAcquiredBundle(AcquiredOpenOptions{Directory: dest})
	require.ErrorIs(t, err, ErrNotAcquiredBundle)
}

func TestOpenAcquiredBundle_RefusesNoncanonicalAcquisitionTime(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	dest := filepath.Join(realTempDir(t), "bundle")
	_, err := AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun, Destination: dest,
	})
	require.NoError(t, err)
	markerPath := filepath.Join(dest, "acquired.json")
	data, err := os.ReadFile(markerPath)
	require.NoError(t, err)
	var marker AcquiredBundleMarker
	require.NoError(t, json.Unmarshal(data, &marker))
	marker.AcquiredAt = " " + marker.AcquiredAt
	data, err = json.MarshalIndent(marker, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(markerPath, append(data, '\n'), 0o600))

	_, err = OpenAcquiredBundle(AcquiredOpenOptions{Directory: dest})
	require.ErrorIs(t, err, ErrNotAcquiredBundle)
}

func TestAcquireBundleMarker_ConformsToPublicSchema(t *testing.T) {
	fx := newAcquiredHubFixture(t)
	dest := filepath.Join(realTempDir(t), "bundle")
	_, err := AcquireBundle(context.Background(), fx.hub, AcquireBundleOptions{
		IndexSetID: fx.indexSetID, RunID: fx.currentRun,
		ProofThroughRunID: fx.baselineRun, Destination: dest,
	})
	require.NoError(t, err)
	data, err := os.ReadFile(filepath.Join(dest, "acquired.json"))
	require.NoError(t, err)
	validator := acquiredBundleSchemaValidator(t)
	diagnostics, err := validator.ValidateJSON(data)
	require.NoError(t, err)
	for _, diagnostic := range diagnostics {
		if diagnostic.Severity == schema.SeverityError {
			t.Fatalf("acquired marker failed schema validation: %s: %s", diagnostic.Pointer, diagnostic.Message)
		}
	}
}

func acquiredBundleSchemaValidator(t testing.TB) *schema.Validator {
	return acquiredBundleSchemaValidatorVersion(t, "index-acquired-bundle.v1.schema.json")
}

func acquiredBundleSchemaValidatorVersion(t testing.TB, name string) *schema.Validator {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	raw, err := os.ReadFile(filepath.Join(
		root, "schemas", "gonimbus", "v1.0.0", name,
	))
	require.NoError(t, err)
	validator, err := schema.NewValidator(raw)
	require.NoError(t, err)
	return validator
}

func FuzzAcquiredBundleMarkerSchema(f *testing.F) {
	validator := acquiredBundleSchemaValidator(f)
	f.Add([]byte(`{"type":"gonimbus.index.acquired_bundle.v1"}`))
	f.Add([]byte(`null`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		_, _ = validator.ValidateJSON(data)
	})
}

func FuzzNormalizeBundleRelativePath(f *testing.F) {
	for _, seed := range []string{
		"identity.json", "runs/run_1/manifest.json", "../escape", "/absolute",
		`runs\run_1\manifest.json`, "runs/%2e%2e/escape",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 2048 {
			t.Skip()
		}
		normalized, err := normalizeBundleRelativePath(raw, false)
		if err != nil {
			return
		}
		if normalized != raw || normalized == "" || strings.Contains(normalized, "\\") ||
			strings.HasPrefix(normalized, "/") || pathEscapes(normalized) {
			t.Fatalf("unsafe path accepted: %q", raw)
		}
	})
}

func pathEscapes(raw string) bool {
	return raw == "." || raw == ".." || strings.HasPrefix(raw, "../") ||
		filepath.IsAbs(raw)
}

func firstFixtureSegmentPath(t *testing.T, bundle, runID string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(bundle, "runs", runID, "manifest.json"))
	require.NoError(t, err)
	var manifest indexsubstrate.InternalManifest
	require.NoError(t, json.Unmarshal(data, &manifest))
	require.NotEmpty(t, manifest.Segments)
	return filepath.Join(bundle, "runs", runID, "segments", manifest.Segments[0].Path)
}

func realTempDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	return dir
}

func newAcquiredHubFixture(t *testing.T) acquiredHubFixture {
	t.Helper()
	params := indexstore.IndexSetParams{
		BaseURI: "s3://example-bucket/prefix/", Provider: "s3",
		StorageProvider: "aws", CloudProvider: "aws", RegionKind: "explicit", Region: "us-east-1",
		BuildParams: indexstore.BuildParams{
			SourceType: "crawl", SchemaVersion: 1, GonimbusVersion: "0.4.3-dev",
		},
	}
	identity, err := indexstore.ComputeIndexSetID(params)
	require.NoError(t, err)
	identityData := []byte(identity.CanonicalJSON + "\n")
	hub := &memoryExactHub{objects: map[string][]byte{}}
	baseRun := "run_1788950000000000000"
	currentRun := "run_1788950100000000000"
	baseStart := time.Date(2026, 9, 9, 10, 0, 0, 0, time.UTC)
	baseManifest, baseSHA := addFixtureHubRun(t, hub, identity.IndexSetID, baseRun, identityData,
		baseStart, nil, &indexsubstrate.LineageRecord{Version: 1, Generation: 1, Baseline: true},
		[]indexsubstrate.CurrentObjectRow{fixtureRow(identity.IndexSetID, "a.xml", baseRun, baseStart.Add(time.Second))})
	parent := &indexsubstrate.StateParent{
		IndexSetID: identity.IndexSetID, RunID: baseRun, ManifestSHA256: baseSHA,
	}
	_ = baseManifest
	addFixtureHubRun(t, hub, identity.IndexSetID, currentRun, identityData,
		baseStart.Add(time.Hour), parent, &indexsubstrate.LineageRecord{Version: 1, Generation: 2},
		[]indexsubstrate.CurrentObjectRow{
			fixtureRow(identity.IndexSetID, "a.xml", baseRun, baseStart.Add(time.Second)),
			fixtureRow(identity.IndexSetID, "b.xml", currentRun, baseStart.Add(time.Hour+time.Second)),
		})
	return acquiredHubFixture{hub: hub, indexSetID: identity.IndexSetID, currentRun: currentRun, baselineRun: baseRun}
}

func addFixtureHubRun(
	t *testing.T,
	hub *memoryExactHub,
	indexSetID, runID string,
	identityData []byte,
	runStarted time.Time,
	parent *indexsubstrate.StateParent,
	lineage *indexsubstrate.LineageRecord,
	rows []indexsubstrate.CurrentObjectRow,
) (indexsubstrate.InternalManifest, string) {
	t.Helper()
	dir := t.TempDir()
	manifest, err := indexsubstrate.WriteSegmentSet(indexsubstrate.SegmentWriterConfig{
		Dir: dir, IndexSetID: indexSetID, RunID: runID, CreatedAt: runStarted.Add(time.Minute),
		RunStartedAt: &runStarted, StateParent: parent, Lineage: lineage,
		TargetRowsPerSegment: 1,
		Coverage: []indexsubstrate.CoverageAttestation{{
			Scope: &indexsubstrate.Scope{Prefix: indexsubstrate.RelativeRootScopePrefix},
			Basis: indexsubstrate.CoverageBasisConfirmed, Complete: true,
		}},
	}, rows)
	require.NoError(t, err)
	manifestPath := filepath.Join(dir, "manifest.json")
	require.NoError(t, indexsubstrate.WriteInternalManifestFile(manifestPath, manifest))
	manifestData, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	manifestSHA := sha256Hex(manifestData)
	hub.objects[exactHubKey(indexSetID, runID, "manifest.json")] = manifestData
	hub.objects[exactHubKey(indexSetID, runID, "identity.json")] = identityData

	var complete acquiredHubComplete
	complete.Version = "1.0"
	complete.MarkerSchemaVersion = HubMarkerSchemaV2
	complete.Format = acquiredHubFormat
	complete.FormatVersion = "2"
	complete.IndexSetID = indexSetID
	complete.RunID = runID
	complete.SnapshotCompletedAt = runStarted.Add(2 * time.Minute).Format(time.RFC3339Nano)
	complete.SnapshotCompletionSemantics = indexsubstrate.SnapshotCompletionSemanticsCompleteMarkerCommit
	complete.HubCommittedAt = runStarted.Add(3 * time.Minute).Format(time.RFC3339Nano)
	complete.CompletedAt = complete.HubCommittedAt
	complete.ExportedBy = "gonimbus/0.4.3-dev"
	complete.Artifacts.Identity = fixtureHubArtifact("identity.json", "identity", identityData)
	complete.Artifacts.Manifest = fixtureHubArtifact("manifest.json", "manifest", manifestData)
	for _, segment := range manifest.Segments {
		data, err := os.ReadFile(filepath.Join(dir, segment.Path))
		require.NoError(t, err)
		rel := "segments/" + segment.Path
		complete.Artifacts.Segments = append(complete.Artifacts.Segments, fixtureHubArtifact(rel, "segment", data))
		hub.objects[exactHubKey(indexSetID, runID, rel)] = data
	}
	complete.Durable.ManifestType = manifest.Type
	complete.Durable.ManifestRender = manifest.Render
	complete.Durable.IndexSchemaVersion = manifest.IndexSchemaVersion
	complete.Durable.SegmentNamespace = manifest.Reachability.SegmentNamespace
	complete.Durable.Segments = len(manifest.Segments)
	complete.Durable.Rows = manifest.Counts.Rows
	completeData, err := json.MarshalIndent(complete, "", "  ")
	require.NoError(t, err)
	hub.objects[exactHubKey(indexSetID, runID, "complete.json")] = completeData
	return manifest, manifestSHA
}

func fixtureHubArtifact(path, role string, data []byte) acquiredHubArtifact {
	return acquiredHubArtifact{
		Path: path, Role: role, Required: true, SizeBytes: int64(len(data)), SHA256: sha256Hex(data),
	}
}

func fixtureRow(indexSetID, relKey, runID string, at time.Time) indexsubstrate.CurrentObjectRow {
	storageClass := "STANDARD"
	return indexsubstrate.CurrentObjectRow{
		IndexSetID: indexSetID, RelKey: relKey, SizeBytes: 10, ETag: `"` + relKey + `"`,
		StorageClass: &storageClass, FirstSeenRunID: runID, FirstSeenAt: at,
		LastChangedRunID: runID, LastChangedAt: at, LastSeenRunID: runID, LastSeenAt: at,
	}
}
