package reflow

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRecordMarshalCompatibility(t *testing.T) {
	srcLastModified := time.Date(2026, 6, 19, 15, 4, 5, 0, time.UTC)
	destLastModified := time.Date(2026, 6, 19, 15, 5, 6, 0, time.UTC)
	destSize := int64(42)

	assertExactJSON(t, Record{
		SourceURI:    "s3://source-bucket/root/file.xml",
		SourceKey:    "root/file.xml",
		SourceETag:   "src-etag",
		SourceSize:   42,
		DestURI:      "s3://dest-bucket/out/file.xml",
		DestKey:      "out/file.xml",
		Bytes:        42,
		Status:       "complete",
		RoutingClass: "quarantine",
		Collision: &CollisionInfo{
			Kind:                     "overwritten",
			DestETagObserved:         "dest-etag",
			DestSizeObserved:         &destSize,
			SrcLastModified:          &srcLastModified,
			DestLastModifiedObserved: &destLastModified,
			DecisionReason:           "src_newer",
			DecisionPath:             "head_compare_then_conditional_overwrite",
		},
		Provenance: &ProvenanceRef{
			Written: true,
			Key:     "out/file.xml.gnb.json",
			URI:     "s3://dest-bucket/out/file.xml.gnb.json",
		},
		Details: map[string]any{"note": "kept"},
	}, `{"source_uri":"s3://source-bucket/root/file.xml","source_bucket":"source-bucket","source_key":"root/file.xml","source_etag":"src-etag","source_size_bytes":42,"dest_uri":"s3://dest-bucket/out/file.xml","dest_key":"out/file.xml","bytes":42,"status":"complete","routing_class":"quarantine","collision":{"kind":"overwritten","dest_etag_observed":"dest-etag","dest_size_observed":42,"src_last_modified":"2026-06-19T15:04:05Z","dest_last_modified_observed":"2026-06-19T15:05:06Z","decision_reason":"src_newer","decision_path":"head_compare_then_conditional_overwrite"},"provenance":{"written":true,"key":"out/file.xml.gnb.json","uri":"s3://dest-bucket/out/file.xml.gnb.json"},"details":{"note":"kept"}}`)
}

func TestRecordMarshalSourceBucketFallbackCompatibility(t *testing.T) {
	assertExactJSON(t, Record{
		SourceURI: "s3://source-bucket/root/file.xml",
		SourceKey: "root/file.xml",
		DestURI:   "s3://dest-bucket/out/file.xml",
		DestKey:   "out/file.xml",
		Status:    "planned",
	}, `{"source_uri":"s3://source-bucket/root/file.xml","source_bucket":"source-bucket","source_key":"root/file.xml","dest_uri":"s3://dest-bucket/out/file.xml","dest_key":"out/file.xml","status":"planned"}`)

	assertExactJSON(t, Record{
		SourceURI: "file://local/root/file.xml",
		SourceKey: "root/file.xml",
		DestURI:   "file:///tmp/out/file.xml",
		DestKey:   "out/file.xml",
		Status:    "planned",
	}, `{"source_uri":"file://local/root/file.xml","source_bucket":"local","source_key":"root/file.xml","dest_uri":"file:///tmp/out/file.xml","dest_key":"out/file.xml","status":"planned"}`)
}

func TestRunRecordMarshalCompatibility(t *testing.T) {
	assertExactJSON(t, RunRecord{
		DestURI:        "s3://dest-bucket/out/",
		CheckpointPath: "/tmp/reflow.db",
		DryRun:         true,
		Resume:         true,
		Parallel:       8,
		ExecutionPath:  ExecutionPathEngine,
		ConcurrencyStats: ConcurrencyStats{
			AdaptiveEnabled:                   true,
			ConcurrencyFloor:                  1,
			ConcurrencyInitial:                4,
			ConcurrencyCeilingRequested:       8,
			ConcurrencyCeilingEffective:       4,
			ConcurrencyCeilingReason:          "resource_capped:fd",
			ConcurrencyFinal:                  3,
			ConcurrencyThrottleBackoffs:       2,
			ConcurrencyAdditiveIncreases:      1,
			ConcurrencyConnectionErrorFreezes: 1,
			ConcurrencyMaxActive:              4,
			MemoryLimitBytes:                  1 << 30,
			MemoryLimitSource:                 "physical_ram",
			MemoryBudgetRequestedBytes:        128 << 20,
			MemoryBudgetEffectiveBytes:        128 << 20,
			MemoryBudgetSource:                "operator",
		},
		Provenance: &ProvenanceRunConfig{
			Mode:         "sidecar",
			Suffix:       ".gnb.json",
			OnWriteError: "warn",
			Placement:    ProvenancePlacementContext{Mode: "mirrored-root", SidecarRoot: "s3://audit-root/prov/"},
		},
		Metadata: &MetadataRunConfig{
			Policy:                  "merge",
			SetKeys:                 []string{"owner", "team"},
			SourceKeyRuleKeys:       []string{"source-owner"},
			DerivedRuleKeys:         []string{"site"},
			OnMissingSource:         "empty",
			PreserveContentType:     true,
			DestinationStorageClass: "STANDARD",
			MetadataSidecarSuffix:   ".metadata.json",
			Set:                     map[string]string{"owner": "example"},
		},
	}, `{"dest_uri":"s3://dest-bucket/out/","checkpoint_path":"/tmp/reflow.db","dry_run":true,"resume":true,"parallel":8,"execution_path":"engine","adaptive_enabled":true,"concurrency_floor":1,"concurrency_initial":4,"concurrency_ceiling_requested":8,"concurrency_ceiling_effective":4,"concurrency_ceiling_reason":"resource_capped:fd","concurrency_final":3,"concurrency_throttle_backoffs":2,"concurrency_additive_increases":1,"concurrency_connection_error_freezes":1,"concurrency_max_active":4,"memory_limit_bytes":1073741824,"memory_limit_source":"physical_ram","memory_budget_requested_bytes":134217728,"memory_budget_effective_bytes":134217728,"memory_budget_source":"operator","provenance":{"mode":"sidecar","suffix":".gnb.json","on_write_error":"warn","placement":{"mode":"mirrored-root","sidecar_root":"s3://audit-root/prov/"}},"metadata":{"policy":"merge","set_keys":["owner","team"],"source_key_rule_keys":["source-owner"],"derived_rule_keys":["site"],"on_missing_source":"empty","preserve_content_type":true,"destination_storage_class":"STANDARD","metadata_sidecar_suffix":".metadata.json","set":{"owner":"example"}}}`)
}

func TestSummaryRecordMarshalCompatibility(t *testing.T) {
	assertExactJSON(t, SummaryRecord{
		DestURI:       "s3://dest-bucket/out/",
		DryRun:        true,
		OnCollision:   "skip-if-duplicate",
		ExecutionPath: ExecutionPathCLIPool,
		ConcurrencyStats: ConcurrencyStats{
			AdaptiveEnabled:                   true,
			ConcurrencyFloor:                  1,
			ConcurrencyInitial:                4,
			ConcurrencyCeilingRequested:       8,
			ConcurrencyCeilingEffective:       4,
			ConcurrencyCeilingReason:          "resource_capped:fd",
			ConcurrencyFinal:                  3,
			ConcurrencyThrottleBackoffs:       2,
			ConcurrencyAdditiveIncreases:      1,
			ConcurrencyConnectionErrorFreezes: 1,
			ConcurrencyMaxActive:              4,
		},
		FallbackActive:          true,
		IfAbsentFallbackObjects: 7,
		Statuses:                map[string]int64{"complete": 3, "skipped": 1},
		Collisions:              map[string]int64{"duplicate": 1},
		InvalidInputs:           2,
		Errors:                  1,
	}, `{"dest_uri":"s3://dest-bucket/out/","dry_run":true,"on_collision":"skip-if-duplicate","execution_path":"cli-pool","adaptive_enabled":true,"concurrency_floor":1,"concurrency_initial":4,"concurrency_ceiling_requested":8,"concurrency_ceiling_effective":4,"concurrency_ceiling_reason":"resource_capped:fd","concurrency_final":3,"concurrency_throttle_backoffs":2,"concurrency_additive_increases":1,"concurrency_connection_error_freezes":1,"concurrency_max_active":4,"dest_ifabsent_honored":null,"fallback_active":true,"ifabsent_fallback_objects":7,"statuses":{"complete":3,"skipped":1},"collisions":{"duplicate":1},"invalid_inputs":2,"errors":1}`)

	honored := false
	assertExactJSON(t, SummaryRecord{
		DestURI:                 "s3://dest-bucket/out/",
		DryRun:                  false,
		OnCollision:             "fail",
		DestIfAbsentHonored:     &honored,
		DestIfAbsentProbeStatus: "fallback_head_compare",
		Statuses:                map[string]int64{},
		Collisions:              map[string]int64{},
	}, `{"dest_uri":"s3://dest-bucket/out/","dry_run":false,"on_collision":"fail","execution_path":"","adaptive_enabled":false,"concurrency_floor":0,"concurrency_initial":0,"concurrency_ceiling_requested":0,"concurrency_ceiling_effective":0,"concurrency_ceiling_reason":"","concurrency_final":0,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":0,"dest_ifabsent_honored":false,"dest_ifabsent_probe_status":"fallback_head_compare","fallback_active":false,"ifabsent_fallback_objects":0}`)
}

func TestSourceRunRecordMarshalCompatibility(t *testing.T) {
	assertExactJSON(t, SourceRunRecord{
		Provider:   "file",
		Bucket:     "local",
		Root:       "/tmp/source",
		URI:        "file://local/",
		OutputOnly: true,
	}, `{"provider":"file","source_bucket":"local","source_root":"/tmp/source","source_uri":"file://local/","source_uri_output_only":true}`)
}

func TestWarningMarshalCompatibility(t *testing.T) {
	assertExactJSON(t, Warning{
		Code:    "REFLOW_IFABSENT_FALLBACK_ACTIVE",
		Message: "destination IfAbsent support was not verified",
		Key:     "out/file.xml",
		Details: map[string]any{
			"cross_process_atomicity_limited": true,
			"dest_ifabsent_honored":           false,
			"dest_ifabsent_probe_status":      "fallback_head_compare",
			"fallback":                        "head_compare",
			"on_collision":                    "skip-if-duplicate",
		},
	}, `{"code":"REFLOW_IFABSENT_FALLBACK_ACTIVE","message":"destination IfAbsent support was not verified","key":"out/file.xml","details":{"cross_process_atomicity_limited":true,"dest_ifabsent_honored":false,"dest_ifabsent_probe_status":"fallback_head_compare","fallback":"head_compare","on_collision":"skip-if-duplicate"}}`)
}

func assertExactJSON(t *testing.T, value any, want string) {
	t.Helper()
	got, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if string(got) != want {
		t.Fatalf("unexpected JSON:\ngot:  %s\nwant: %s", got, want)
	}
}
