package indexsubstrate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/fulmenhq/gofulmen/schema"
	"github.com/stretchr/testify/require"
)

func TestValidateExactSnapshotCompletion(t *testing.T) {
	startedAt := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	completedAt := startedAt.Add(time.Minute)
	hubCommittedAt := completedAt.Add(time.Minute)

	require.NoError(t, ValidateExactSnapshotCompletion(
		SnapshotCompletionSemanticsCompleteMarkerCommit,
		startedAt,
		completedAt,
		hubCommittedAt,
	))

	tests := []struct {
		name      string
		semantics string
		started   time.Time
		completed time.Time
		hub       time.Time
	}{
		{name: "missing semantics", started: startedAt, completed: completedAt, hub: hubCommittedAt},
		{name: "unknown semantics", semantics: "manifest_write", started: startedAt, completed: completedAt, hub: hubCommittedAt},
		{name: "completion before start", semantics: SnapshotCompletionSemanticsCompleteMarkerCommit, started: startedAt, completed: startedAt.Add(-time.Nanosecond), hub: hubCommittedAt},
		{name: "hub before completion", semantics: SnapshotCompletionSemanticsCompleteMarkerCommit, started: startedAt, completed: completedAt, hub: completedAt.Add(-time.Nanosecond)},
		{name: "non UTC start", semantics: SnapshotCompletionSemanticsCompleteMarkerCommit, started: startedAt.In(time.FixedZone("offset", -4*60*60)), completed: completedAt, hub: hubCommittedAt},
		{name: "non UTC completion", semantics: SnapshotCompletionSemanticsCompleteMarkerCommit, started: startedAt, completed: completedAt.In(time.FixedZone("offset", -4*60*60)), hub: hubCommittedAt},
		{name: "non UTC hub", semantics: SnapshotCompletionSemanticsCompleteMarkerCommit, started: startedAt, completed: completedAt, hub: hubCommittedAt.In(time.FixedZone("offset", -4*60*60))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Error(t, ValidateExactSnapshotCompletion(
				test.semantics,
				test.started,
				test.completed,
				test.hub,
			))
		})
	}
}

func TestCompleteV2PublicSchemaFreezesExactTimeDiscriminator(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	rawSchema, err := os.ReadFile(filepath.Join(
		root,
		"schemas",
		"gonimbus",
		"v1.0.0",
		"index-complete.v2.schema.json",
	))
	require.NoError(t, err)
	validator, err := schema.NewValidator(rawSchema)
	require.NoError(t, err)

	valid := map[string]any{
		"type":                          CompleteMarkerTypeV2,
		"index_set_id":                  "idx_" + strings.Repeat("a", 64),
		"run_id":                        "run_1709654400000000000",
		"snapshot_completed_at":         "2026-09-10T12:01:00Z",
		"snapshot_completion_semantics": SnapshotCompletionSemanticsCompleteMarkerCommit,
		"manifest_path":                 "/app-data/cache/segments/set/runs/run/manifest.json",
		"manifest_sha256":               strings.Repeat("b", 64),
		"segment_dir":                   "/app-data/cache/segments/set/runs/run",
		"segments":                      2,
	}
	requireSchemaValid(t, validator, valid)

	for _, mutate := range []func(map[string]any){
		func(doc map[string]any) { delete(doc, "snapshot_completion_semantics") },
		func(doc map[string]any) { doc["snapshot_completion_semantics"] = "manifest_write" },
		func(doc map[string]any) { doc["type"] = CompleteMarkerTypeV1 },
	} {
		doc := make(map[string]any, len(valid))
		for key, value := range valid {
			doc[key] = value
		}
		mutate(doc)
		data, err := json.Marshal(doc)
		require.NoError(t, err)
		diagnostics, err := validator.ValidateJSON(data)
		require.NoError(t, err)
		require.True(t, hasSchemaError(diagnostics), "schema accepted ineligible marker: %s", data)
	}
}

func requireSchemaValid(t *testing.T, validator *schema.Validator, doc map[string]any) {
	t.Helper()
	data, err := json.Marshal(doc)
	require.NoError(t, err)
	diagnostics, err := validator.ValidateJSON(data)
	require.NoError(t, err)
	for _, diagnostic := range diagnostics {
		if diagnostic.Severity == schema.SeverityError {
			t.Fatalf("schema validation failed: %s: %s", diagnostic.Pointer, diagnostic.Message)
		}
	}
}

func hasSchemaError(diagnostics []schema.Diagnostic) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.Severity == schema.SeverityError {
			return true
		}
	}
	return false
}
