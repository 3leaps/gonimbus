package indexstore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/fulmenhq/gofulmen/schema"
	"github.com/stretchr/testify/require"
)

func TestIndexSetIdentityPayload_ConformsToHubIdentitySchema(t *testing.T) {
	params := IndexSetParams{
		BaseURI:         "s3://bucket/prefix",
		Provider:        "s3",
		StorageProvider: "aws_s3",
		CloudProvider:   "aws",
		RegionKind:      "aws",
		Region:          "us-east-1",
		EndpointHost:    "S3.US-EAST-1.WASABISYS.COM",
		BuildParams: BuildParams{
			SourceType:         "crawl",
			PathDateExtraction: &PathDateExtraction{Method: "segment", SegmentIndex: 2},
			SchemaVersion:      SchemaVersion,
			GonimbusVersion:    "test",
			Includes:           []string{"**", "data/**"},
			Excludes:           []string{"tmp/**"},
			IncludeHidden:      false,
			FiltersHash:        "abc123",
			ScopeHash:          "def456",
		},
	}

	payload := buildIndexSetIdentityPayload(params)
	jsonData, err := json.Marshal(payload)
	require.NoError(t, err)

	schemaBytes := loadHubIdentitySchema(t)
	validator, err := schema.NewValidator(schemaBytes)
	require.NoError(t, err)

	diags, err := validator.ValidateJSON(jsonData)
	require.NoError(t, err)

	for _, d := range diags {
		if d.Severity == schema.SeverityError {
			t.Fatalf("identity payload does not conform to index-hub-identity schema: %s: %s", d.Pointer, d.Message)
		}
	}
}

func loadHubIdentitySchema(t *testing.T) []byte {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)

	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	path := filepath.Join(root, "schemas", "gonimbus", "v1.0.0", "index-hub-identity.schema.json")

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	return data
}
