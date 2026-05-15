package atlas

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/fulmenhq/gofulmen/schema"
	"github.com/stretchr/testify/require"
)

func TestAtlasPayloadsConformToSchemas(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)

	header := Header{
		SchemaVersion:    SchemaVersion,
		AtlasID:          "atlas_" + repeatHex("a"),
		CreatedAt:        now,
		SourceIndexSetID: "idx_" + repeatHex("b"),
		SourceRunID:      "run_123",
		BaseURI:          "s3://bucket/prefix/",
		ScopeDigest:      "scope",
		RecipeDigest:     repeatHex("c"),
		HashProfile:      HashProfileSHA256,
		Coverage:         CoverageScoped,
		ShardBy:          []string{"event_date"},
		Dimensions:       []DimensionDeclaration{{Name: "event_date", Kind: DimensionTemporalDay, Classification: ClassificationConfidential}},
		SystemFields:     DefaultSystemFields(),
		Counts:           Counts{ObjectsScanned: 1, RowsWritten: 1},
	}
	validateAgainstSchema(t, "atlas-header.schema.json", header)

	row := ObjectRow{
		SchemaVersion:    SchemaVersion,
		SourceIndexSetID: header.SourceIndexSetID,
		SourceRunID:      header.SourceRunID,
		StorageKey:       "prefix/a.json",
		RelKey:           "a.json",
		SourceURI:        "s3://bucket/prefix/a.json",
		ContentHash:      repeatHex("d"),
		HashProfile:      HashProfileSHA256,
		Dimensions:       map[string]string{"event_date": "2026-05-01"},
		Shard:            map[string]string{"event_date": "2026-05-01"},
		SizeBytes:        32,
		FirstSeenRunID:   header.SourceRunID,
		FirstSeenAt:      now,
	}
	validateAgainstSchema(t, "atlas-object.schema.json", row)

	diag := DiagnosticRow{
		SchemaVersion:    SchemaVersion,
		SourceIndexSetID: header.SourceIndexSetID,
		SourceRunID:      header.SourceRunID,
		StorageKey:       "prefix/missing.json",
		RelKey:           "missing.json",
		Stage:            "read",
		Code:             "get_object_failed",
		Message:          "not found",
		OccurredAt:       now,
	}
	validateAgainstSchema(t, "atlas-diagnostic.schema.json", diag)
}

func validateAgainstSchema(t *testing.T, schemaFile string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	validator, err := schema.NewValidator(loadAtlasSchema(t, schemaFile))
	require.NoError(t, err)
	diags, err := validator.ValidateJSON(data)
	require.NoError(t, err)
	for _, diag := range diags {
		if diag.Severity == schema.SeverityError {
			t.Fatalf("%s failed schema validation: %s: %s", schemaFile, diag.Pointer, diag.Message)
		}
	}
}

func loadAtlasSchema(t *testing.T, name string) []byte {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	data, err := os.ReadFile(filepath.Join(root, "schemas", "gonimbus", "v1.0.0", name))
	require.NoError(t, err)
	return data
}

func repeatHex(s string) string {
	out := ""
	for len(out) < 64 {
		out += s
	}
	return out[:64]
}
