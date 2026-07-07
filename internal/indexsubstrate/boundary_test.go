package indexsubstrate

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderBoundaryManifestFailsClosedWithoutArtifact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "boundary", "manifest.json")

	err := RenderBoundaryManifestFile(BoundaryRenderConfig{
		OutputPath: path,
		Source: InternalManifest{
			Type:       ManifestType,
			Render:     ManifestRenderType,
			IndexSetID: "idx_test",
			RunID:      "run_test",
		},
		TokenNamespace:    "boundary:test",
		RestrictedColumns: []string{"rel_key"},
	})

	require.ErrorIs(t, err, ErrBoundaryRenderNotImplemented)
	require.NoFileExists(t, path)
	_, statErr := os.Stat(filepath.Dir(path))
	require.True(t, errors.Is(statErr, os.ErrNotExist), "boundary render must not create an artifact directory")

	err = RenderBoundaryManifestFile(BoundaryRenderConfig{})
	require.ErrorIs(t, err, ErrBoundaryRenderNotImplemented)
}

func TestBoundaryManifestSchemaOmitsRestrictedShapeFields(t *testing.T) {
	policies := DefaultBoundaryPolicies()
	require.Contains(t, policies.TrustModel, "does not de-identify row-level keys")
	require.Contains(t, policies.IdentifierPolicy, "not derived from segment_id")
	require.Contains(t, policies.SegmentShapeMetadata, "coarsened or omitted")
	require.Contains(t, policies.RestrictedColumnMetadata, "min/max statistics")
	require.Contains(t, policies.RestrictedColumnMetadata, "bloom filters")
	require.Contains(t, policies.RestrictedColumnMetadata, "dictionary surfaces")

	manifest := BoundaryManifest{
		Type:               BoundaryManifestType,
		Render:             ManifestRenderTypeBoundary,
		IndexSetID:         "idx_test",
		RunID:              "run_test",
		IndexSchemaVersion: IndexSchemaVersion,
		TokenNamespace:     "boundary:test",
		Policies:           policies,
		Segments: []BoundarySegmentDescriptor{{
			Token:       "tok_001",
			Format:      SegmentFormatParquet,
			Compression: "snappy",
			Digest:      SegmentDigest{Algorithm: "sha256", Hex: "abc"},
		}},
	}
	data, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.Contains(t, string(data), `"render":"boundary"`)
	require.NotContains(t, string(data), "distinct_etags")

	segmentType := reflect.TypeOf(BoundarySegmentDescriptor{})
	for _, field := range []string{"SegmentID", "Path", "Rows", "Tombstones", "SizeBytes", "MinRelKey", "MaxRelKey", "DistinctETags"} {
		_, ok := segmentType.FieldByName(field)
		require.False(t, ok, "boundary segment descriptor must not expose %s", field)
	}
}
