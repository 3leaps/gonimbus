package indexsubstrate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/fulmenhq/gofulmen/schema"
	"github.com/stretchr/testify/require"
)

const custodyBridgeConversionGolden = `{"type":"gonimbus.index.custody_bridge_conversion.v1","index_set_id":"idx_0000000000000000000000000000000000000000000000000000000000000000","run_id":"run_1","legacy_marker_sha256":"1111111111111111111111111111111111111111111111111111111111111111","identity_sha256":"2222222222222222222222222222222222222222222222222222222222222222","identity_schema":"gonimbus.index_set_identity.canonical-json-lf.v1","identity_profile":"default","manifest_sha256":"3333333333333333333333333333333333333333333333333333333333333333","segments":[{"sha256":"4444444444444444444444444444444444444444444444444444444444444444","size_bytes":7}],"run_start":{"basis":"legacy_asserted","started_at":"2026-01-02T03:04:05Z"},"snapshot_time":{"basis":"legacy_unavailable"}}` + "\n"

func TestCustodyBridgeConversionGoldenContract(t *testing.T) {
	sum := sha256.Sum256([]byte(custodyBridgeConversionGolden))
	require.Equal(t,
		"3455886f8d6b6cd84576c7b88327ad2328404ff3a3eeb986ccec2b7f5faa3de2",
		hex.EncodeToString(sum[:]),
	)
	validator := publicSchemaValidator(t, "index-custody-bridge-conversion.v1.schema.json")
	requireRawSchemaValid(t, validator, []byte(custodyBridgeConversionGolden))
	var conversion map[string]any
	require.NoError(t, json.Unmarshal([]byte(custodyBridgeConversionGolden), &conversion))
	conversion["snapshot_time"] = exactSnapshotTime(strings.Repeat("5", 64))
	requireSchemaValid(t, validator, conversion)
	conversion["snapshot_time"] = unsupportedExactSnapshotTime(strings.Repeat("5", 64))
	requireSchemaInvalid(t, validator, conversion)
}

func TestCustodyBridgeSchemasFreezeClosedTimeEvidence(t *testing.T) {
	digest := strings.Repeat("a", 64)
	exact := map[string]any{
		"basis":           "exact_commit",
		"completed_at":    "2026-09-10T12:02:00Z",
		"evidence_type":   CompleteMarkerTypeV2,
		"evidence_sha256": digest,
	}
	legacy := map[string]any{"basis": "legacy_unavailable"}

	hubValidator := publicSchemaValidator(t, "index-hub-complete.v3.schema.json")
	hub := validHubV3SchemaDoc(digest, legacy)
	requireSchemaValid(t, hubValidator, hub)
	hub["snapshot_time"] = exact
	requireSchemaValid(t, hubValidator, hub)

	hub["completed_at"] = "2026-09-10T12:03:00Z"
	requireSchemaInvalid(t, hubValidator, hub)
	delete(hub, "completed_at")
	hub["run_start"] = map[string]any{
		"basis":      "locally_observed",
		"started_at": "2026-09-10T12:00:00Z",
	}
	requireSchemaInvalid(t, hubValidator, hub)
	hub["run_start"] = legacyRunStart()
	hub["snapshot_time"] = map[string]any{
		"basis":           "exact_commit",
		"completed_at":    "2026-09-10T12:02:00Z",
		"evidence_type":   "gonimbus.index.complete.caller_defined",
		"evidence_sha256": digest,
	}
	requireSchemaInvalid(t, hubValidator, hub)
	hub["snapshot_time"] = map[string]any{
		"basis":        "legacy_unavailable",
		"completed_at": "2026-09-10T12:02:00Z",
	}
	requireSchemaInvalid(t, hubValidator, hub)
}

func TestCustodyBridgeEnvelopeSchemasAcceptFrozenShapes(t *testing.T) {
	digest := strings.Repeat("b", 64)
	runID := "run_1709654400000000000"
	indexSetID := "idx_" + strings.Repeat("c", 64)
	runStart := legacyRunStart()
	snapshotTime := map[string]any{"basis": "legacy_unavailable"}

	acquired := validAcquiredBundleV2SchemaDoc(indexSetID, runID, digest)
	acquiredValidator := publicSchemaValidator(t, "index-acquired-bundle.v2.schema.json")
	requireSchemaValid(t, acquiredValidator, acquired)
	acquired["snapshot_time"] = exactSnapshotTime(digest)
	requireSchemaValid(t, acquiredValidator, acquired)
	acquired["snapshot_time"] = unsupportedExactSnapshotTime(digest)
	requireSchemaInvalid(t, acquiredValidator, acquired)
	acquired["snapshot_time"] = snapshotTime

	query := validQueryReceiptV2SchemaDoc(indexSetID, runID, digest, runStart, snapshotTime)
	queryValidator := publicSchemaValidator(t, "index-query-receipt.v2.schema.json")
	requireSchemaValid(t, queryValidator, query)
	query["snapshot_time"] = exactSnapshotTime(digest)
	requireSchemaValid(t, queryValidator, query)
	query["snapshot_time"] = unsupportedExactSnapshotTime(digest)
	requireSchemaInvalid(t, queryValidator, query)
	query["snapshot_time"] = snapshotTime
	query["source_identity_profile"] = "gonimbus.index_set_identity.canonical-json-lf.v1"
	requireSchemaInvalid(t, queryValidator, query)
	query["source_identity_schema"] = "gonimbus/v1.0.0/index-hub-identity"
	requireSchemaValid(t, queryValidator, query)

	localQuery := validQueryReceiptV2SchemaDoc(indexSetID, runID, digest, legacyRunStart(), exactSnapshotTime(digest))
	localQuery["source_kind"] = "local_published"
	localQuery["run_start"] = map[string]any{
		"basis":      "locally_observed",
		"started_at": "2026-09-10T12:00:00Z",
	}
	delete(localQuery, "hub_committed_at")
	delete(localQuery, "hub_complete_sha256")
	requireSchemaValid(t, queryValidator, localQuery)
	localQuery["snapshot_time"] = snapshotTime
	requireSchemaInvalid(t, queryValidator, localQuery)

	bridgeReceipt := validBridgeReceiptV1SchemaDoc(indexSetID, runID, digest)
	bridgeValidator := publicSchemaValidator(t, "index-custody-bridge-receipt.v1.schema.json")
	requireSchemaValid(t, bridgeValidator, bridgeReceipt)
	bridgeReceipt["snapshot_time"] = exactSnapshotTime(digest)
	requireSchemaValid(t, bridgeValidator, bridgeReceipt)
	bridgeReceipt["snapshot_time"] = unsupportedExactSnapshotTime(digest)
	requireSchemaInvalid(t, bridgeValidator, bridgeReceipt)
}

func TestCustodyBridgeSchemasRequireCanonicalUTCTimestamps(t *testing.T) {
	digest := strings.Repeat("d", 64)
	indexSetID := "idx_" + strings.Repeat("e", 64)
	runID := "run_1709654400000000000"
	offsetTime := "2026-09-10T13:00:00+01:00"

	var conversion map[string]any
	require.NoError(t, json.Unmarshal([]byte(custodyBridgeConversionGolden), &conversion))
	conversion["run_start"].(map[string]any)["started_at"] = offsetTime
	requireSchemaInvalid(
		t,
		publicSchemaValidator(t, "index-custody-bridge-conversion.v1.schema.json"),
		conversion,
	)

	hub := validHubV3SchemaDoc(digest, map[string]any{"basis": "legacy_unavailable"})
	hub["hub_committed_at"] = offsetTime
	requireSchemaInvalid(t, publicSchemaValidator(t, "index-hub-complete.v3.schema.json"), hub)

	acquired := validAcquiredBundleV2SchemaDoc(indexSetID, runID, digest)
	acquired["acquired_at"] = offsetTime
	requireSchemaInvalid(
		t,
		publicSchemaValidator(t, "index-acquired-bundle.v2.schema.json"),
		acquired,
	)

	query := validQueryReceiptV2SchemaDoc(
		indexSetID,
		runID,
		digest,
		legacyRunStart(),
		map[string]any{"basis": "legacy_unavailable"},
	)
	query["hub_committed_at"] = offsetTime
	requireSchemaInvalid(t, publicSchemaValidator(t, "index-query-receipt.v2.schema.json"), query)

	bridgeReceipt := validBridgeReceiptV1SchemaDoc(indexSetID, runID, digest)
	bridgeReceipt["operation_completed_at"] = offsetTime
	requireSchemaInvalid(
		t,
		publicSchemaValidator(t, "index-custody-bridge-receipt.v1.schema.json"),
		bridgeReceipt,
	)
}

func TestIdentityDeclarationSchemaFreezesTypedShape(t *testing.T) {
	validator := publicSchemaValidator(t, "index-identity-declaration.v1.schema.json")
	valid := map[string]any{
		"type":     "gonimbus.index_set_identity.declaration.v1",
		"base_uri": "s3://example-bucket/data/",
		"provider": "s3",
		"build": map[string]any{
			"source_type": "crawl", "schema_version": 8,
			"includes": []any{"**"}, "include_hidden": false,
		},
		"path_date": map[string]any{"method": "segment", "segment_index": 0},
	}
	requireSchemaValid(t, validator, valid)

	valid["path_date"] = map[string]any{"method": "regex"}
	requireSchemaInvalid(t, validator, valid)
	valid["path_date"] = nil
	requireSchemaInvalid(t, validator, valid)
	valid["path_date"] = map[string]any{"method": "segment", "segment_index": 0, "regex": "extra"}
	requireSchemaInvalid(t, validator, valid)
}

func validHubV3SchemaDoc(digest string, snapshotTime map[string]any) map[string]any {
	return map[string]any{
		"version": "1.0", "marker_schema_version": "gonimbus.index.hub_marker.v3",
		"format": "durable-v2", "format_version": "2",
		"index_set_id":                 "idx_" + strings.Repeat("c", 64),
		"run_id":                       "run_1709654400000000000",
		"legacy_marker_schema_version": "gonimbus.index.hub_marker.v1",
		"legacy_marker_sha256":         digest,
		"identity_schema":              "gonimbus.index_set_identity.canonical-json-lf.v1",
		"identity_profile":             "default",
		"run_start":                    legacyRunStart(), "snapshot_time": snapshotTime,
		"conversion_identity_sha256": digest,
		"hub_committed_at":           "2026-09-10T12:03:00Z",
		"exported_by":                "gonimbus/0.4.3-dev",
		"artifacts": map[string]any{
			"identity_json": map[string]any{
				"path": "identity.json", "role": "identity", "required": true,
				"size_bytes": 10, "sha256": digest,
			},
			"manifest": map[string]any{
				"path": "manifest.json", "role": "manifest", "required": true,
				"size_bytes": 20, "sha256": digest,
			},
			"segments": []any{map[string]any{
				"path": "segments/segment-000001.parquet", "role": "segment", "required": true,
				"size_bytes": 30, "sha256": digest,
			}},
		},
		"durable": map[string]any{
			"manifest_type": "gonimbus.index.manifest.v1", "manifest_render": "internal",
			"index_schema_version": 8, "segment_namespace": "sha256",
			"segments": 1, "rows": 1,
		},
	}
}

func validQueryReceiptV2SchemaDoc(
	indexSetID, runID, digest string,
	runStart, snapshotTime map[string]any,
) map[string]any {
	return map[string]any{
		"type": "gonimbus.index.query_receipt.v2", "schema_version": "2.0.0", "outcome": "success",
		"source_kind": "acquired_hub", "index_set_id": indexSetID, "run_id": runID,
		"run_start": runStart, "snapshot_time": snapshotTime,
		"hub_committed_at": "2026-09-10T12:03:00Z", "hub_complete_sha256": digest,
		"source_identity_sha256":  digest,
		"source_identity_schema":  "gonimbus.index_set_identity.canonical-json-lf.v1",
		"source_identity_profile": "default",
		"manifest_sha256":         digest, "coverage_sha256": digest,
		"coverage": map[string]any{
			"entries": 1, "complete_entries": 1, "confirmed_entries": 1,
			"inferred_entries": 0, "gap_count": 0,
		},
		"declared": map[string]any{
			"rows": 1, "active_rows": 1, "tombstones": 0, "distinct_etags": 1, "segments": 1,
		},
		"query": map[string]any{
			"query_spec_sha256": digest, "result_mode": "count",
			"filter_kinds": []any{}, "filter_count": 0, "storage_class_count": 0,
			"include_alternates": false, "effective_limit": 0,
		},
		"results": map[string]any{
			"examined": 1, "matched": 1, "emitted": 0, "logical_results": 1,
			"truncated": false, "timestamp_parse_anomalies": 0,
		},
		"segments": map[string]any{"declared": 1, "walked": 1, "verified": 1, "manifest_pruned": 0},
		"warnings": []any{}, "errors": []any{},
	}
}

func validAcquiredBundleV2SchemaDoc(indexSetID, runID, digest string) map[string]any {
	return map[string]any{
		"type":                       "gonimbus.index.acquired_bundle.v2",
		"schema":                     "gonimbus/v1.0.0/index-acquired-bundle.v2",
		"index_set_id":               indexSetID,
		"run_id":                     runID,
		"source_identity_sha256":     digest,
		"source_identity_schema":     "gonimbus.index_set_identity.canonical-json-lf.v1",
		"source_identity_profile":    "default",
		"hub_marker_schema_version":  "gonimbus.index.hub_marker.v3",
		"run_start":                  legacyRunStart(),
		"snapshot_time":              map[string]any{"basis": "legacy_unavailable"},
		"conversion_identity_sha256": digest,
		"hub_committed_at":           "2026-09-10T12:03:00Z",
		"hub_complete_sha256":        digest,
		"manifest_sha256":            digest,
		"acquired_at":                "2026-09-10T12:04:00Z",
		"lineage": []any{map[string]any{
			"index_set_id": indexSetID, "run_id": runID,
			"marker_schema_version": "gonimbus.index.hub_marker.v3",
			"hub_complete_sha256":   digest, "manifest_sha256": digest,
			"conversion_identity_sha256": digest,
		}},
		"artifacts": []any{
			map[string]any{"path": "identity.json", "role": "source_identity", "size_bytes": 10, "sha256": digest},
			map[string]any{"path": "runs/" + runID + "/hub-complete.json", "role": "hub_complete", "size_bytes": 20, "sha256": digest},
			map[string]any{"path": "runs/" + runID + "/manifest.json", "role": "manifest", "size_bytes": 30, "sha256": digest},
		},
	}
}

func validBridgeReceiptV1SchemaDoc(indexSetID, runID, digest string) map[string]any {
	return map[string]any{
		"type": "gonimbus.index.custody_bridge_receipt.v1", "schema_version": "1.0.0", "outcome": "success",
		"source_handle_ref": "hnd_" + strings.Repeat("1", 32),
		"target_handle_ref": "hnd_" + strings.Repeat("2", 32),
		"index_set_id":      indexSetID, "run_id": runID,
		"legacy_marker_schema_version": "gonimbus.index.hub_marker.v1",
		"result_marker_schema_version": "gonimbus.index.hub_marker.v3",
		"legacy_marker_sha256":         digest,
		"source_identity_sha256":       digest,
		"source_identity_schema":       "gonimbus.index_set_identity.canonical-json-lf.v1",
		"source_identity_profile":      "default",
		"manifest_sha256":              digest,
		"declared":                     map[string]any{"rows": 1, "segments": 1},
		"verification": map[string]any{
			"segments_verified": 1, "bytes_read": 100, "bytes_conditionally_created": 90,
		},
		"run_start": legacyRunStart(), "snapshot_time": map[string]any{"basis": "legacy_unavailable"},
		"conversion_identity_sha256": digest,
		"result_complete_sha256":     digest,
		"operation_started_at":       "2026-09-10T12:00:00Z",
		"operation_completed_at":     "2026-09-10T12:03:00Z",
		"disposition":                "created",
	}
}

func legacyRunStart() map[string]any {
	return map[string]any{
		"basis":      "legacy_asserted",
		"started_at": "2026-09-10T12:00:00Z",
	}
}

func exactSnapshotTime(digest string) map[string]any {
	return map[string]any{
		"basis":           "exact_commit",
		"completed_at":    "2026-09-10T12:02:00Z",
		"evidence_type":   CompleteMarkerTypeV2,
		"evidence_sha256": digest,
	}
}

func unsupportedExactSnapshotTime(digest string) map[string]any {
	return map[string]any{
		"basis":           "exact_commit",
		"completed_at":    "2026-09-10T12:02:00Z",
		"evidence_type":   "gonimbus.index.complete.caller_defined",
		"evidence_sha256": digest,
	}
}

func publicSchemaValidator(t testing.TB, name string) *schema.Validator {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	raw, err := os.ReadFile(filepath.Join(root, "schemas", "gonimbus", "v1.0.0", name))
	require.NoError(t, err)
	validator, err := schema.NewValidator(raw)
	require.NoError(t, err)
	return validator
}

func requireSchemaInvalid(t *testing.T, validator *schema.Validator, doc map[string]any) {
	t.Helper()
	data, err := json.Marshal(doc)
	require.NoError(t, err)
	diagnostics, err := validator.ValidateJSON(data)
	require.NoError(t, err)
	require.True(t, hasSchemaError(diagnostics), "schema accepted invalid document: %s", data)
}

func requireRawSchemaValid(t *testing.T, validator *schema.Validator, data []byte) {
	t.Helper()
	diagnostics, err := validator.ValidateJSON(data)
	require.NoError(t, err)
	for _, diagnostic := range diagnostics {
		if diagnostic.Severity == schema.SeverityError {
			t.Fatalf("schema validation failed: %s: %s", diagnostic.Pointer, diagnostic.Message)
		}
	}
}
