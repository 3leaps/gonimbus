package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInspectPairReceiptClaimsDoNotWidenScopeOrVerdict(t *testing.T) {
	line := `{"type":"gonimbus.reflow.v1","data":{"source_uri":"s3://source/key","dest_uri":"s3://outside/key","status":"complete","dest_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","dest_etag":"write-etag","dest_version_id":"9007199254740993","source_last_modified":"2026-01-01T00:00:00Z"}}`
	rec, ok, err := parseInspectPairReflowLine(line)
	require.NoError(t, err)
	require.True(t, ok)
	out, _, head := inspectPairRecordForReflow(rec, []inspectPairScope{{Provider: "s3", Bucket: "allowed"}})
	require.False(t, head)
	require.Equal(t, "invalid_dest", out.Verdict)
	require.Equal(t, rec.DestSHA256, out.DestSHA256)
	require.Equal(t, rec.DestVersionID, out.DestVersionID)
	require.Equal(t, rec.DestETag, out.DestETag)
	require.Equal(t, rec.SourceLastModified, out.SourceLastModified)
	require.Empty(t, out.DestETagObserved)
	rec.DestURI = "s3://allowed/key"
	out, _, head = inspectPairRecordForReflow(rec, []inspectPairScope{{Provider: "s3", Bucket: "allowed"}})
	require.True(t, head)
	require.Empty(t, out.Verdict)
	require.Empty(t, out.DestETagObserved)
	rec.DestETag = "https://example.test?token=secret"
	rec.DestVersionID = "Bearer secret"
	out, _, _ = inspectPairRecordForReflow(rec, nil)
	require.Empty(t, out.DestETag)
	require.Empty(t, out.DestVersionID)
}
