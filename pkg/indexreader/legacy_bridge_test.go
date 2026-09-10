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
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/fulmenhq/gofulmen/schema"
	"github.com/stretchr/testify/require"
)

func TestBridgeLegacyDurableRunCreatesVerifiedV3Bundle(t *testing.T) {
	fixture := newLegacyBridgeFixture(t)
	target := newBridgeMemoryHub()

	result, err := BridgeLegacyDurableRun(
		context.Background(),
		fixture.source,
		target,
		CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
		nil,
		fixture.options(t),
	)
	require.NoError(t, err)
	require.Equal(t, BridgeDispositionCreated, result.Disposition)
	require.Equal(t, BridgeSnapshotTimeLegacyUnavailable, result.SnapshotTime.Basis)
	require.Equal(t, fixture.indexSetID, result.IndexSetID)
	require.Equal(t, fixture.runID, result.RunID)
	require.Equal(t, fixture.manifestSHA, result.ManifestSHA256)
	require.Equal(t, fixture.identitySHA, result.IdentitySHA256)
	require.Equal(t, len(fixture.manifest.Segments), result.SegmentsVerified)
	require.NotEmpty(t, result.ConversionIdentitySHA256)
	require.NotEmpty(t, result.CompleteSHA256)

	reads := fixture.source.readKeys()
	require.Equal(t, []string{
		exactHubKey(fixture.indexSetID, fixture.runID, "complete.json"),
		exactHubKey(fixture.indexSetID, fixture.runID, "manifest.json"),
		exactHubKey(fixture.indexSetID, fixture.runID, "segments/"+fixture.manifest.Segments[0].Path),
	}, reads)
	for _, key := range append(reads, target.createdKeys()...) {
		require.NotContains(t, key, "latest")
	}

	completeBytes := target.object(exactHubKey(fixture.indexSetID, fixture.runID, "complete.json"))
	var marker bridgeHubCompleteV3
	require.NoError(t, strictDecodeJSON(completeBytes, &marker, true))
	require.Equal(t, HubMarkerSchemaV3, marker.MarkerSchemaVersion)
	require.Equal(t, fixture.legacySHA, marker.LegacyMarkerSHA256)
	require.Equal(t, fixture.identitySHA, marker.Artifacts.Identity.SHA256)
	require.Equal(t, BridgeRunStartLegacyAsserted, marker.RunStart.Basis)
	require.Equal(t, BridgeSnapshotTimeLegacyUnavailable, marker.SnapshotTime.Basis)
	require.NotContains(t, string(completeBytes), `"completed_at"`)
	requireBridgeV3SchemaValid(t, completeBytes)
}

func TestBridgeLegacyDurableRunPublishesExactBytesAndCommitsLast(t *testing.T) {
	fixture := newLegacyBridgeFixture(t)
	var legacy legacyHubComplete
	legacyKey := exactHubKey(fixture.indexSetID, fixture.runID, "complete.json")
	require.NoError(t, json.Unmarshal(fixture.source.object(legacyKey), &legacy))
	legacy.Artifacts.Manifest.ETag = "legacy-manifest-etag"
	legacy.Artifacts.Segments[0].ETag = "legacy-segment-etag"
	legacyBytes, err := json.MarshalIndent(legacy, "", "  ")
	require.NoError(t, err)
	legacyBytes = append(legacyBytes, '\n')
	fixture.source.put(legacyKey, legacyBytes)
	fixture.legacySHA = sha256HexBridge(legacyBytes)

	target := newBridgeMemoryHub()
	manifestKey := exactHubKey(fixture.indexSetID, fixture.runID, "manifest.json")
	segmentKey := exactHubKey(
		fixture.indexSetID,
		fixture.runID,
		"segments/"+fixture.manifest.Segments[0].Path,
	)
	completeKey := exactHubKey(fixture.indexSetID, fixture.runID, "complete.json")
	opts := fixture.options(t)
	clockCalled := false
	opts.HubCommitClock = func() time.Time {
		clockCalled = true
		require.Equal(t, fixture.identityBytes, target.object(pathIdentityKey(fixture.indexSetID)))
		require.Equal(t, fixture.source.object(manifestKey), target.object(manifestKey))
		require.Equal(t, fixture.source.object(segmentKey), target.object(segmentKey))
		require.Nil(t, target.object(completeKey))
		return fixture.startedAt.Add(3 * time.Minute)
	}

	_, err = BridgeLegacyDurableRun(
		context.Background(),
		fixture.source,
		target,
		CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
		nil,
		opts,
	)
	require.NoError(t, err)
	require.True(t, clockCalled)
	require.Equal(t, []string{
		pathIdentityKey(fixture.indexSetID),
		manifestKey,
		segmentKey,
		completeKey,
	}, target.createdKeys())

	var marker bridgeHubCompleteV3
	require.NoError(t, strictDecodeJSON(target.object(completeKey), &marker, true))
	require.Empty(t, marker.Artifacts.Manifest.ETag)
	require.Empty(t, marker.Artifacts.Segments[0].ETag)
}

func TestBridgeLegacyDurableRunConcurrentAttemptsCommitOrAdopt(t *testing.T) {
	fixture := newLegacyBridgeFixture(t)
	target := newBridgeMemoryHub()
	opts := []BridgeOptions{fixture.options(t), fixture.options(t)}
	for i := range opts {
		opts[i].HubCommitClock = func() time.Time {
			return fixture.startedAt.Add(3 * time.Minute)
		}
	}

	type outcome struct {
		result BridgeResult
		err    error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, len(opts))
	for _, options := range opts {
		options := options
		go func() {
			<-start
			result, err := BridgeLegacyDurableRun(
				context.Background(),
				fixture.source,
				target,
				CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
				nil,
				options,
			)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	close(start)

	dispositions := make(map[string]int)
	var completeSHA string
	for range opts {
		got := <-outcomes
		require.NoError(t, got.err)
		dispositions[got.result.Disposition]++
		if completeSHA == "" {
			completeSHA = got.result.CompleteSHA256
		}
		require.Equal(t, completeSHA, got.result.CompleteSHA256)
	}
	require.Equal(t, 1, dispositions[BridgeDispositionCreated])
	require.Equal(t, 1, dispositions[BridgeDispositionAlreadyIdentical])
	require.NotEmpty(t, target.object(exactHubKey(fixture.indexSetID, fixture.runID, "complete.json")))
}

func TestBridgeLegacyDurableRunReconstructsIdentityAndBindsExactEvidence(t *testing.T) {
	fixture := newLegacyBridgeFixture(t)
	target := newBridgeMemoryHub()
	evidence := fixture.completeV2Evidence(t)

	result, err := BridgeLegacyDurableRun(
		context.Background(),
		fixture.source,
		target,
		CanonicalIdentityAuthority{DeclarationJSON: fixture.declarationBytes},
		evidence,
		fixture.options(t),
	)
	require.NoError(t, err)
	require.Equal(t, BridgeSnapshotTimeExactCommit, result.SnapshotTime.Basis)
	require.Equal(t, indexsubstrate.CompleteMarkerTypeV2, result.SnapshotTime.EvidenceType)
	require.Equal(t, sha256HexBridge(evidence), result.SnapshotTime.EvidenceSHA256)
	require.Equal(t, fixture.identityBytes, target.object(pathIdentityKey(fixture.indexSetID)))
	for _, value := range target.objectsSnapshot() {
		require.NotEqual(t, evidence, value, "local evidence bytes must not travel in the target bundle")
	}
}

func TestBridgeIdentityDeclarationMapsThroughComputeIndexSetID(t *testing.T) {
	params := indexstore.IndexSetParams{
		BaseURI:         "s3://example/data",
		Provider:        "s3",
		StorageProvider: " object ",
		CloudProvider:   " example-cloud ",
		RegionKind:      " regional ",
		Region:          " example-1 ",
		EndpointHost:    "API.EXAMPLE.INVALID",
		BuildParams: indexstore.BuildParams{
			SourceType: " crawl ", SchemaVersion: indexsubstrate.IndexSchemaVersion,
			GonimbusVersion: " 0.4.3 ", Includes: []string{"/b/**", "a/**", "/b/**"},
			Excludes: []string{" tmp/** ", ""}, IncludeHidden: true,
			FiltersHash: " filters ", ScopeHash: " scope ",
			PathDateExtraction: &indexstore.PathDateExtraction{
				Method: "segment", SegmentIndex: 0,
			},
		},
	}
	expected, err := indexstore.ComputeIndexSetID(params)
	require.NoError(t, err)
	declaration := map[string]any{
		"type":     "gonimbus.index_set_identity.declaration.v1",
		"base_uri": " s3://example/data ", "provider": " s3 ",
		"storage_provider": " object ", "cloud_provider": " example-cloud ",
		"region_kind": " regional ", "region": " example-1 ",
		"endpoint_host": " API.EXAMPLE.INVALID ",
		"build": map[string]any{
			"source_type": " crawl ", "schema_version": indexsubstrate.IndexSchemaVersion,
			"gonimbus_version": " 0.4.3 ", "includes": []string{"/b/**", "a/**", "/b/**"},
			"excludes": []string{" tmp/** ", ""}, "include_hidden": true,
			"filters_hash": " filters ", "scope_hash": " scope ",
		},
		"path_date": map[string]any{"method": " segment ", "segment_index": 0},
	}
	raw, err := json.Marshal(declaration)
	require.NoError(t, err)
	got, err := reconstructCanonicalIdentity(raw, expected.IndexSetID, 1<<20)
	require.NoError(t, err)
	require.Equal(t, expected.CanonicalJSON+"\n", string(got.bytes))
	require.Equal(t, sha256HexBridge(got.bytes), got.sha256)
}

func TestBridgeCanonicalIdentityFileRejectsAlternateBytes(t *testing.T) {
	fixture := newLegacyBridgeFixture(t)
	var payload any
	require.NoError(t, json.Unmarshal(fixture.identityBytes, &payload))
	pretty, err := json.MarshalIndent(payload, "", "  ")
	require.NoError(t, err)
	otherParams := indexstore.IndexSetParams{
		BaseURI: "file:///different/", Provider: "file",
		BuildParams: indexstore.BuildParams{
			SourceType: "crawl", SchemaVersion: indexsubstrate.IndexSchemaVersion,
			Includes: []string{"**"},
		},
	}
	other, err := indexstore.ComputeIndexSetID(otherParams)
	require.NoError(t, err)
	tests := map[string][]byte{
		"missing LF":        bytes.TrimSuffix(fixture.identityBytes, []byte{'\n'}),
		"extra LF":          append(append([]byte(nil), fixture.identityBytes...), '\n'),
		"pretty whitespace": append(pretty, '\n'),
		"BOM":               append([]byte{0xef, 0xbb, 0xbf}, fixture.identityBytes...),
		"wrong identity":    append([]byte(other.CanonicalJSON), '\n'),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := validateCanonicalIdentityBytes(raw, fixture.indexSetID, 1<<20)
			require.True(t, IsBridgeError(err, BridgeErrorIdentityInvalid))
		})
	}
}

func TestBridgeLegacyDurableRunAdoptsCommittedMarkerByStableConversion(t *testing.T) {
	fixture := newLegacyBridgeFixture(t)
	target := newBridgeMemoryHub()
	firstOpts := fixture.options(t)
	firstOpts.HubCommitClock = func() time.Time { return fixture.startedAt.Add(3 * time.Minute) }
	first, err := BridgeLegacyDurableRun(
		context.Background(),
		fixture.source,
		target,
		CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
		nil,
		firstOpts,
	)
	require.NoError(t, err)

	secondOpts := fixture.options(t)
	secondOpts.HubCommitClock = func() time.Time { return fixture.startedAt.Add(9 * time.Minute) }
	second, err := BridgeLegacyDurableRun(
		context.Background(),
		fixture.source,
		target,
		CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
		nil,
		secondOpts,
	)
	require.NoError(t, err)
	require.Equal(t, BridgeDispositionAlreadyIdentical, second.Disposition)
	require.Equal(t, first.HubCommittedAt, second.HubCommittedAt)
	require.Equal(t, first.CompleteSHA256, second.CompleteSHA256)
	require.Equal(t, first.ExportedBy, second.ExportedBy)
	require.Zero(t, second.BytesConditionallyCreated)
}

func TestBridgeLegacyDurableRunReconcilesCreateSuccessWithReturnedError(t *testing.T) {
	fixture := newLegacyBridgeFixture(t)
	target := newBridgeMemoryHub()
	completeKey := exactHubKey(fixture.indexSetID, fixture.runID, "complete.json")
	target.createErrAfter = map[string]error{
		pathIdentityKey(fixture.indexSetID): errors.New("ambiguous content response"),
		completeKey:                         errors.New("ambiguous terminal response"),
	}
	result, err := BridgeLegacyDurableRun(
		context.Background(),
		fixture.source,
		target,
		CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
		nil,
		fixture.options(t),
	)
	require.NoError(t, err)
	require.Equal(t, BridgeDispositionCreated, result.Disposition)
	require.Equal(t, sha256HexBridge(target.object(completeKey)), result.CompleteSHA256)
}

func TestBridgeLegacyDurableRunResolvesTerminalCreateCancellation(t *testing.T) {
	tests := []struct {
		name        string
		ambiguous   bool
		disposition string
	}{
		{
			name:        "definitive created response",
			disposition: BridgeDispositionCreated,
		},
		{
			name:        "ambiguous response reconciled",
			ambiguous:   true,
			disposition: BridgeDispositionAlreadyIdentical,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLegacyBridgeFixture(t)
			target := newBridgeMemoryHub()
			ctx, cancel := context.WithCancel(context.Background())
			completeKey := exactHubKey(fixture.indexSetID, fixture.runID, "complete.json")
			target.afterCreate = map[string]func(){completeKey: cancel}
			if test.ambiguous {
				target.createAmbiguousErrAfter = map[string]error{
					completeKey: errors.New("ambiguous response after durable create"),
				}
			} else {
				target.createErrAfter = map[string]error{
					completeKey: errors.New("post-commit response failure"),
				}
			}

			result, err := BridgeLegacyDurableRun(
				ctx,
				fixture.source,
				target,
				CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
				nil,
				fixture.options(t),
			)
			require.NoError(t, err)
			require.Equal(t, test.disposition, result.Disposition)
			require.Equal(t, sha256HexBridge(target.object(completeKey)), result.CompleteSHA256)
			require.Equal(t, fixture.startedAt.Add(3*time.Minute).Format(time.RFC3339Nano), result.HubCommittedAt)
		})
	}
}

func TestBridgeLegacyDurableRunPreTerminalCancellationStopsPublication(t *testing.T) {
	fixture := newLegacyBridgeFixture(t)
	target := newBridgeMemoryHub()
	ctx, cancel := context.WithCancel(context.Background())
	manifestKey := exactHubKey(fixture.indexSetID, fixture.runID, "manifest.json")
	target.afterCreate = map[string]func(){manifestKey: cancel}

	_, err := BridgeLegacyDurableRun(
		ctx,
		fixture.source,
		target,
		CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
		nil,
		fixture.options(t),
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []string{
		pathIdentityKey(fixture.indexSetID),
		manifestKey,
	}, target.createdKeys())
	require.Nil(t, target.object(exactHubKey(fixture.indexSetID, fixture.runID, "complete.json")))
}

func TestBridgeLegacyDurableRunRefusesContentAndTerminalConflicts(t *testing.T) {
	fixture := newLegacyBridgeFixture(t)

	t.Run("content", func(t *testing.T) {
		target := newBridgeMemoryHub()
		target.put(pathIdentityKey(fixture.indexSetID), []byte("different"))
		_, err := BridgeLegacyDurableRun(
			context.Background(),
			fixture.source,
			target,
			CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
			nil,
			fixture.options(t),
		)
		require.True(t, IsBridgeError(err, BridgeErrorTargetConflict))
		require.Nil(t, target.object(exactHubKey(fixture.indexSetID, fixture.runID, "complete.json")))
	})

	t.Run("stable conversion", func(t *testing.T) {
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
		var marker map[string]any
		require.NoError(t, json.Unmarshal(target.object(completeKey), &marker))
		marker["conversion_identity_sha256"] = strings.Repeat("f", 64)
		data, err := json.MarshalIndent(marker, "", "  ")
		require.NoError(t, err)
		target.put(completeKey, append(data, '\n'))

		_, err = BridgeLegacyDurableRun(
			context.Background(),
			fixture.source,
			target,
			CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
			nil,
			fixture.options(t),
		)
		require.True(t, IsBridgeError(err, BridgeErrorTargetConflict))
	})
}

func TestBridgeLegacyDurableRunRefusesUnverifiedInputsBeforeTargetMutation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*legacyBridgeFixture)
		auth   func(*legacyBridgeFixture) CanonicalIdentityAuthority
	}{
		{
			name: "neither identity input",
			auth: func(*legacyBridgeFixture) CanonicalIdentityAuthority {
				return CanonicalIdentityAuthority{}
			},
		},
		{
			name: "both identity inputs",
			auth: func(f *legacyBridgeFixture) CanonicalIdentityAuthority {
				return CanonicalIdentityAuthority{
					CanonicalJSONLF: f.identityBytes,
					DeclarationJSON: f.declarationBytes,
				}
			},
		},
		{
			name: "duplicate declaration key",
			auth: func(f *legacyBridgeFixture) CanonicalIdentityAuthority {
				raw := bytes.Replace(
					f.declarationBytes,
					[]byte(`"provider":"file"`),
					[]byte(`"provider":"file","provider":"file"`),
					1,
				)
				return CanonicalIdentityAuthority{DeclarationJSON: raw}
			},
		},
		{
			name: "trailing declaration token",
			auth: func(f *legacyBridgeFixture) CanonicalIdentityAuthority {
				return CanonicalIdentityAuthority{DeclarationJSON: append(f.declarationBytes, []byte(`{}`)...)}
			},
		},
		{
			name: "null declaration field",
			auth: func(f *legacyBridgeFixture) CanonicalIdentityAuthority {
				raw := bytes.Replace(f.declarationBytes, []byte(`"provider":"file"`), []byte(`"provider":null`), 1)
				return CanonicalIdentityAuthority{DeclarationJSON: raw}
			},
		},
		{
			name: "unknown declaration field",
			auth: func(f *legacyBridgeFixture) CanonicalIdentityAuthority {
				raw := bytes.Replace(f.declarationBytes, []byte(`"provider":"file"`), []byte(`"provider":"file","extra":true`), 1)
				return CanonicalIdentityAuthority{DeclarationJSON: raw}
			},
		},
		{
			name: "missing required nested declaration field",
			auth: func(f *legacyBridgeFixture) CanonicalIdentityAuthority {
				var declaration map[string]any
				require.NoError(t, json.Unmarshal(f.declarationBytes, &declaration))
				build, ok := declaration["build"].(map[string]any)
				require.True(t, ok)
				delete(build, "include_hidden")
				raw, err := json.Marshal(declaration)
				require.NoError(t, err)
				return CanonicalIdentityAuthority{DeclarationJSON: raw}
			},
		},
		{
			name: "legacy marker traversal path",
			mutate: func(f *legacyBridgeFixture) {
				key := exactHubKey(f.indexSetID, f.runID, "complete.json")
				var marker legacyHubComplete
				require.NoError(t, json.Unmarshal(f.source.object(key), &marker))
				marker.Artifacts.Segments[0].Path = "segments/../escape.parquet"
				raw, err := json.Marshal(marker)
				require.NoError(t, err)
				f.source.put(key, raw)
			},
			auth: func(f *legacyBridgeFixture) CanonicalIdentityAuthority {
				return CanonicalIdentityAuthority{CanonicalJSONLF: f.identityBytes}
			},
		},
		{
			name: "segment digest mismatch",
			mutate: func(f *legacyBridgeFixture) {
				key := exactHubKey(f.indexSetID, f.runID, "segments/"+f.manifest.Segments[0].Path)
				f.source.put(key, []byte("tampered"))
			},
			auth: func(f *legacyBridgeFixture) CanonicalIdentityAuthority {
				return CanonicalIdentityAuthority{CanonicalJSONLF: f.identityBytes}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLegacyBridgeFixture(t)
			if test.mutate != nil {
				test.mutate(fixture)
			}
			target := newBridgeMemoryHub()
			_, err := BridgeLegacyDurableRun(
				context.Background(),
				fixture.source,
				target,
				test.auth(fixture),
				nil,
				fixture.options(t),
			)
			require.Error(t, err)
			require.Empty(t, target.createdKeys())
		})
	}
}

func TestBridgeLegacyDurableRunEnforcesAggregateBoundBeforeTargetMutation(t *testing.T) {
	fixture := newLegacyBridgeFixture(t)
	target := newBridgeMemoryHub()
	opts := fixture.options(t)
	opts.Limits.MaxAggregateBytes = 1

	_, err := BridgeLegacyDurableRun(
		context.Background(),
		fixture.source,
		target,
		CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
		nil,
		opts,
	)
	require.Error(t, err)
	require.Empty(t, target.createdKeys())
}

func TestBridgeLimitsCannotLoosenFrozenProtocolMaxima(t *testing.T) {
	limits := BridgeLimits{
		MaxMarkerBytes:     maxBridgeJSONCount + 1,
		MaxManifestBytes:   maxBridgeJSONCount + 1,
		MaxIdentityBytes:   maxBridgeJSONCount + 1,
		MaxEvidenceBytes:   maxBridgeJSONCount + 1,
		MaxSegmentBytes:    maxBridgeJSONCount + 1,
		MaxSegments:        maxBridgeProtocolSegments + 1,
		MaxAggregateBytes:  maxBridgeJSONCount + 1,
		MaxConversionBytes: maxBridgeJSONCount + 1,
	}.normalize()
	require.Equal(t, maxBridgeProtocolSegments, limits.MaxSegments)
	require.Equal(t, maxBridgeJSONCount, limits.MaxMarkerBytes)
	require.Equal(t, maxBridgeJSONCount, limits.MaxManifestBytes)
	require.Equal(t, maxBridgeJSONCount, limits.MaxIdentityBytes)
	require.Equal(t, maxBridgeJSONCount, limits.MaxEvidenceBytes)
	require.Equal(t, maxBridgeJSONCount, limits.MaxSegmentBytes)
	require.Equal(t, maxBridgeJSONCount, limits.MaxAggregateBytes)
	require.Equal(t, maxBridgeJSONCount, limits.MaxConversionBytes)
	_, ok := checkedBridgeReceiptAdd(maxBridgeJSONCount, 1)
	require.False(t, ok)

	indexSetID := "idx_" + strings.Repeat("0", 64)
	legacy := legacyHubComplete{
		Version: "1.0", MarkerSchemaVersion: HubMarkerSchemaV1,
		Format: "durable-v2", FormatVersion: "2",
		IndexSetID: indexSetID, RunID: "run_1",
		CompletedAt: "2026-09-10T12:00:00Z", ExportedBy: "gonimbus/test",
		Artifacts: legacyHubArtifacts{
			Manifest: bridgeArtifactRef{
				Path: "manifest.json", Role: "manifest", Required: true,
				SHA256: strings.Repeat("0", 64),
			},
			Segments: make([]bridgeArtifactRef, maxBridgeProtocolSegments+1),
		},
		Durable: bridgeDurableSummary{Segments: maxBridgeProtocolSegments + 1},
	}
	require.Error(t, validateLegacyHubComplete(legacy, indexSetID, "run_1", limits))
}

func TestBridgeLegacyDurableRunEvidenceIsClosedAndBound(t *testing.T) {
	fixture := newLegacyBridgeFixture(t)
	valid := fixture.completeV2Evidence(t)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(valid, &doc))
	tests := map[string]func(map[string]any){
		"unsupported type": func(value map[string]any) {
			value["type"] = "gonimbus.index.complete.caller_defined"
		},
		"wrong set": func(value map[string]any) {
			value["index_set_id"] = "idx_" + strings.Repeat("0", 64)
		},
		"wrong manifest": func(value map[string]any) {
			value["manifest_sha256"] = strings.Repeat("0", 64)
		},
		"offset time": func(value map[string]any) {
			value["snapshot_completed_at"] = "2026-09-10T13:01:00+01:00"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			copyDoc := make(map[string]any, len(doc))
			for key, value := range doc {
				copyDoc[key] = value
			}
			mutate(copyDoc)
			raw, err := json.Marshal(copyDoc)
			require.NoError(t, err)
			target := newBridgeMemoryHub()
			_, err = BridgeLegacyDurableRun(
				context.Background(),
				fixture.source,
				target,
				CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
				raw,
				fixture.options(t),
			)
			require.True(t, IsBridgeError(err, BridgeErrorEvidenceInvalid))
			require.Empty(t, target.createdKeys())
		})
	}
}

func TestBridgeLegacyDurableRunProviderFailuresAreSanitized(t *testing.T) {
	fixture := newLegacyBridgeFixture(t)
	target := newBridgeMemoryHub()
	target.createErr = errors.New("provider leaked bucket and credential details")
	_, err := BridgeLegacyDurableRun(
		context.Background(),
		fixture.source,
		target,
		CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
		nil,
		fixture.options(t),
	)
	require.EqualError(t, err, BridgeErrorTargetCreate)
	require.NotContains(t, err.Error(), "bucket")
	require.NotContains(t, err.Error(), "credential")

	fixture.source.openErr = errors.New("provider leaked source coordinate")
	_, err = BridgeLegacyDurableRun(
		context.Background(),
		fixture.source,
		newBridgeMemoryHub(),
		CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
		nil,
		fixture.options(t),
	)
	require.EqualError(t, err, BridgeErrorSourceRead)
	require.NotContains(t, err.Error(), "coordinate")
}

func TestBridgeLegacyDurableRunCancellationPrecedesPublication(t *testing.T) {
	fixture := newLegacyBridgeFixture(t)
	target := newBridgeMemoryHub()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := BridgeLegacyDurableRun(
		ctx,
		fixture.source,
		target,
		CanonicalIdentityAuthority{CanonicalJSONLF: fixture.identityBytes},
		nil,
		fixture.options(t),
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, target.createdKeys())
}

func TestRenderBridgeConversionMatchesFrozenGoldenVector(t *testing.T) {
	verified := verifiedBridgeInput{
		legacy: legacyHubComplete{
			IndexSetID: "idx_" + strings.Repeat("0", 64),
			RunID:      "run_1",
		},
		legacySHA:   strings.Repeat("1", 64),
		identity:    bridgeIdentity{sha256: strings.Repeat("2", 64)},
		manifestSHA: strings.Repeat("3", 64),
		segments: []verifiedBridgeSegment{{ref: bridgeArtifactRef{
			SHA256: strings.Repeat("4", 64), SizeBytes: 7,
		}}},
		runStart: BridgeRunStart{
			Basis: BridgeRunStartLegacyAsserted, StartedAt: "2026-01-02T03:04:05Z",
		},
		snapshotTime: BridgeSnapshotTime{Basis: BridgeSnapshotTimeLegacyUnavailable},
	}
	data, digest, err := renderBridgeConversion(verified, BridgeLimits{}.normalize())
	require.NoError(t, err)
	const golden = `{"type":"gonimbus.index.custody_bridge_conversion.v1","index_set_id":"idx_0000000000000000000000000000000000000000000000000000000000000000","run_id":"run_1","legacy_marker_sha256":"1111111111111111111111111111111111111111111111111111111111111111","identity_sha256":"2222222222222222222222222222222222222222222222222222222222222222","identity_schema":"gonimbus.index_set_identity.canonical-json-lf.v1","identity_profile":"default","manifest_sha256":"3333333333333333333333333333333333333333333333333333333333333333","segments":[{"sha256":"4444444444444444444444444444444444444444444444444444444444444444","size_bytes":7}],"run_start":{"basis":"legacy_asserted","started_at":"2026-01-02T03:04:05Z"},"snapshot_time":{"basis":"legacy_unavailable"}}` + "\n"
	require.Equal(t, golden, string(data))
	require.Equal(t, "3455886f8d6b6cd84576c7b88327ad2328404ff3a3eeb986ccec2b7f5faa3de2", digest)
}

type legacyBridgeFixture struct {
	source           *bridgeMemoryHub
	indexSetID       string
	runID            string
	startedAt        time.Time
	identityBytes    []byte
	identitySHA      string
	declarationBytes []byte
	manifest         indexsubstrate.InternalManifest
	manifestSHA      string
	legacySHA        string
}

func newLegacyBridgeFixture(t *testing.T) *legacyBridgeFixture {
	t.Helper()
	startedAt := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	params := indexstore.IndexSetParams{
		BaseURI:  "file:///example/source/",
		Provider: "file",
		BuildParams: indexstore.BuildParams{
			SourceType: "crawl", SchemaVersion: indexsubstrate.IndexSchemaVersion,
			Includes: []string{"**"}, IncludeHidden: false,
		},
	}
	identity, err := indexstore.ComputeIndexSetID(params)
	require.NoError(t, err)
	identityBytes := append([]byte(identity.CanonicalJSON), '\n')
	identitySum := sha256.Sum256(identityBytes)
	identitySHA := hex.EncodeToString(identitySum[:])
	runID := "run_1709654400000000000"

	segmentDir := t.TempDir()
	runStartCopy := startedAt
	manifest, err := indexsubstrate.WriteSegmentSet(indexsubstrate.SegmentWriterConfig{
		Dir: segmentDir, IndexSetID: identity.IndexSetID, RunID: runID,
		CreatedAt: startedAt, RunStartedAt: &runStartCopy, TargetRowsPerSegment: 1,
		Coverage: []indexsubstrate.CoverageAttestation{{
			Scope: &indexsubstrate.Scope{Prefix: indexsubstrate.RelativeRootScopePrefix},
			Basis: indexsubstrate.CoverageBasisConfirmed, Complete: true,
		}},
	}, []indexsubstrate.CurrentObjectRow{{
		IndexSetID: identity.IndexSetID, RelKey: "object.txt", SizeBytes: 7,
		LastModified: &startedAt, ETag: "etag",
		FirstSeenRunID: runID, FirstSeenAt: startedAt,
		LastChangedRunID: runID, LastChangedAt: startedAt,
		LastSeenRunID: runID, LastSeenAt: startedAt,
	}})
	require.NoError(t, err)
	manifestPath := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, indexsubstrate.WriteInternalManifestFile(manifestPath, manifest))
	manifestBytes, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	manifestSHA := sha256HexBridge(manifestBytes)

	source := newBridgeMemoryHub()
	source.put(exactHubKey(identity.IndexSetID, runID, "manifest.json"), manifestBytes)
	segmentRefs := make([]bridgeArtifactRef, 0, len(manifest.Segments))
	for _, segment := range manifest.Segments {
		segmentBytes, err := os.ReadFile(filepath.Join(segmentDir, segment.Path))
		require.NoError(t, err)
		source.put(exactHubKey(identity.IndexSetID, runID, "segments/"+segment.Path), segmentBytes)
		segmentRefs = append(segmentRefs, bridgeArtifactRef{
			Path: "segments/" + segment.Path, Role: "segment", Required: true,
			SizeBytes: segment.SizeBytes, SHA256: segment.Digest.Hex,
		})
	}
	legacy := legacyHubComplete{
		Version: "1.0", MarkerSchemaVersion: HubMarkerSchemaV1,
		Format: "durable-v2", FormatVersion: "2",
		IndexSetID: identity.IndexSetID, RunID: runID,
		CompletedAt: startedAt.Add(2 * time.Minute).Format(time.RFC3339Nano),
		ExportedBy:  "gonimbus/legacy",
		Artifacts: legacyHubArtifacts{
			Manifest: bridgeArtifactRef{
				Path: "manifest.json", Role: "manifest", Required: true,
				SizeBytes: int64(len(manifestBytes)), SHA256: manifestSHA,
			},
			Segments: segmentRefs,
		},
		Durable: bridgeDurableSummary{
			ManifestType: manifest.Type, ManifestRender: manifest.Render,
			IndexSchemaVersion: manifest.IndexSchemaVersion,
			SegmentNamespace:   manifest.Reachability.SegmentNamespace,
			Segments:           len(manifest.Segments), Rows: manifest.Counts.Rows,
		},
	}
	legacyBytes, err := json.MarshalIndent(legacy, "", "  ")
	require.NoError(t, err)
	legacyBytes = append(legacyBytes, '\n')
	source.put(exactHubKey(identity.IndexSetID, runID, "complete.json"), legacyBytes)

	declaration := map[string]any{
		"type":     "gonimbus.index_set_identity.declaration.v1",
		"base_uri": params.BaseURI, "provider": params.Provider,
		"build": map[string]any{
			"source_type":    params.BuildParams.SourceType,
			"schema_version": params.BuildParams.SchemaVersion,
			"includes":       params.BuildParams.Includes,
			"include_hidden": params.BuildParams.IncludeHidden,
		},
	}
	declarationBytes, err := json.Marshal(declaration)
	require.NoError(t, err)
	return &legacyBridgeFixture{
		source: source, indexSetID: identity.IndexSetID, runID: runID,
		startedAt: startedAt, identityBytes: identityBytes, identitySHA: identitySHA,
		declarationBytes: declarationBytes, manifest: manifest, manifestSHA: manifestSHA,
		legacySHA: sha256HexBridge(legacyBytes),
	}
}

func (fixture *legacyBridgeFixture) options(t *testing.T) BridgeOptions {
	t.Helper()
	return BridgeOptions{
		IndexSetID: fixture.indexSetID, RunID: fixture.runID,
		Workspace: t.TempDir(), ExportedBy: "gonimbus/test",
		HubCommitClock: func() time.Time {
			return fixture.startedAt.Add(3 * time.Minute)
		},
	}
}

func (fixture *legacyBridgeFixture) completeV2Evidence(t *testing.T) []byte {
	t.Helper()
	doc := completeV2Evidence{
		Type:       indexsubstrate.CompleteMarkerTypeV2,
		IndexSetID: fixture.indexSetID, RunID: fixture.runID,
		SnapshotCompletedAt:         fixture.startedAt.Add(time.Minute).Format(time.RFC3339Nano),
		SnapshotCompletionSemantics: indexsubstrate.SnapshotCompletionSemanticsCompleteMarkerCommit,
		ManifestPath:                "manifest.json", ManifestSHA256: fixture.manifestSHA,
		SegmentDir: "segments", Segments: len(fixture.manifest.Segments),
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	require.NoError(t, err)
	return append(data, '\n')
}

func pathIdentityKey(indexSetID string) string {
	return "index-sets/" + indexSetID + "/identity.json"
}

func requireBridgeV3SchemaValid(t testing.TB, data []byte) {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	rawSchema, err := os.ReadFile(filepath.Join(
		root, "schemas", "gonimbus", "v1.0.0", "index-hub-complete.v3.schema.json",
	))
	require.NoError(t, err)
	validator, err := schema.NewValidator(rawSchema)
	require.NoError(t, err)
	diagnostics, err := validator.ValidateJSON(data)
	require.NoError(t, err)
	for _, diagnostic := range diagnostics {
		if diagnostic.Severity == schema.SeverityError {
			t.Fatalf("v3 marker failed schema validation: %s: %s", diagnostic.Pointer, diagnostic.Message)
		}
	}
}

type bridgeMemoryHub struct {
	mu                      sync.Mutex
	objects                 map[string][]byte
	reads                   []string
	creates                 []string
	openErr                 error
	createErr               error
	createErrAfter          map[string]error
	createAmbiguousErrAfter map[string]error
	afterCreate             map[string]func()
}

func newBridgeMemoryHub() *bridgeMemoryHub {
	return &bridgeMemoryHub{objects: make(map[string][]byte)}
}

func (hub *bridgeMemoryHub) OpenExact(_ context.Context, key string) (io.ReadCloser, int64, error) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	hub.reads = append(hub.reads, key)
	if hub.openErr != nil {
		return nil, 0, hub.openErr
	}
	data, ok := hub.objects[key]
	if !ok {
		return nil, 0, errors.New("not found")
	}
	copyData := append([]byte(nil), data...)
	return io.NopCloser(bytes.NewReader(copyData)), int64(len(copyData)), nil
}

func (hub *bridgeMemoryHub) CreateExact(
	_ context.Context,
	key string,
	body io.Reader,
	sizeBytes int64,
) (bool, error) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	hub.creates = append(hub.creates, key)
	if hub.createErr != nil {
		return false, hub.createErr
	}
	if _, exists := hub.objects[key]; exists {
		return false, nil
	}
	data, err := io.ReadAll(io.LimitReader(body, sizeBytes+1))
	if err != nil {
		return false, err
	}
	if int64(len(data)) != sizeBytes {
		return false, errors.New("size mismatch")
	}
	hub.objects[key] = append([]byte(nil), data...)
	if hook := hub.afterCreate[key]; hook != nil {
		hook()
	}
	if err := hub.createAmbiguousErrAfter[key]; err != nil {
		return false, err
	}
	if err := hub.createErrAfter[key]; err != nil {
		return true, err
	}
	return true, nil
}

func (hub *bridgeMemoryHub) put(key string, data []byte) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	hub.objects[key] = append([]byte(nil), data...)
}

func (hub *bridgeMemoryHub) object(key string) []byte {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return append([]byte(nil), hub.objects[key]...)
}

func (hub *bridgeMemoryHub) objectsSnapshot() map[string][]byte {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	out := make(map[string][]byte, len(hub.objects))
	for key, data := range hub.objects {
		out[key] = append([]byte(nil), data...)
	}
	return out
}

func (hub *bridgeMemoryHub) readKeys() []string {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return append([]string(nil), hub.reads...)
}

func (hub *bridgeMemoryHub) createdKeys() []string {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return append([]string(nil), hub.creates...)
}

func TestValidBridgeScopeLegacyCoveragePrefixCompatibility(t *testing.T) {
	cases := []struct {
		name   string
		prefix string
		want   bool
	}{
		{"canonical safe relative prefix", "a/b/2026-02-01", true},
		{"legacy directory form", "a/b/2026-02-01/", true},
		{"single component", "a", true},
		{"single component directory form", "a/", true},
		{"empty scope", "", false},
		{"exact root sentinel", ".", true},
		{"sentinel with slash", "./", false},
		{"sentinel double slash", ".//", false},
		{"absolute root", "/", false},
		{"double slash", "//", false},
		{"absolute path", "/a/b/2026-02-01/", false},
		{"double terminal slash", "a/b/2026-02-01//", false},
		{"repeated internal separator", "a//b/2026-02-01/", false},
		{"internal dot component", "a/./b/2026-02-01/", false},
		{"traversal", "a/../b/2026-02-01/", false},
		{"parent", "..", false},
		{"parent directory form", "../", false},
		{"parent prefix", "../a", false},
		{"backslash", `a\b\2026-02-01\`, false},
		{"encoded traversal", "a/%2e%2e/b/", false},
		{"encoded separator", "a%2fb/2026-02-01/", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Coverage scopes and coverage gaps share validBridgeScope;
			// one verdict covers both call sites.
			scope := indexsubstrate.Scope{Prefix: tc.prefix}
			require.Equal(t, tc.want, validBridgeScope(scope), "prefix %q", tc.prefix)
		})
	}
}
