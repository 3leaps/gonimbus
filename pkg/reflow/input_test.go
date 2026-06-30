package reflow

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseReflowInputLineS3(t *testing.T) {
	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"etag-a","source_size_bytes":7,"dest_rel_key":"a/b.xml","vars":{"key":"a/b.xml"}}}`
	in, err := parseReflowInputLine(line)
	require.NoError(t, err)
	require.Equal(t, "s3", in.SourceProvider)
	require.Equal(t, "source-bucket", in.SourceBucket)
	require.Equal(t, "s3://source-bucket/a/b.xml", in.SourceURI)
	require.Equal(t, "a/b.xml", in.SourceKey)
	require.Equal(t, "etag-a", in.SourceETag)
	require.Equal(t, int64(7), in.SourceSize)
	require.Equal(t, "a/b.xml", in.DestRelKey)
	require.Equal(t, "normal", in.RoutingClass)
}

func TestParseReflowInputLineQuarantine(t *testing.T) {
	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://b/k","source_key":"k","routing_class":"quarantine","quarantine_prefix":"quar/"}}`
	in, err := parseReflowInputLine(line)
	require.NoError(t, err)
	require.Equal(t, "quarantine", in.RoutingClass)
	require.Equal(t, "quar", in.QuarantinePrefix)
}

func TestParseReflowInputLineIndexObjectS3(t *testing.T) {
	line := indexObjectInputLineForTest("s3://source-bucket/prefix/", "nested/file.xml", "etag-index", 9)
	in, err := parseReflowInputLine(line)
	require.NoError(t, err)
	require.Equal(t, "s3", in.SourceProvider)
	require.Equal(t, "source-bucket", in.SourceBucket)
	require.Equal(t, "s3://source-bucket/nested/file.xml", in.SourceURI)
	require.Equal(t, "nested/file.xml", in.SourceKey)
	require.Equal(t, "etag-index", in.SourceETag)
	require.Equal(t, int64(9), in.SourceSize)
	require.Empty(t, in.DestRelKey, "index-object stdin records must still flow through rewrite planning")
	require.Equal(t, "normal", in.RoutingClass)
}

func TestParseReflowInputLineRawS3Object(t *testing.T) {
	in, err := parseReflowInputLine("s3://source-bucket/raw/file.xml")
	require.NoError(t, err)
	require.Equal(t, "s3", in.SourceProvider)
	require.Equal(t, "source-bucket", in.SourceBucket)
	require.Equal(t, "s3://source-bucket/raw/file.xml", in.SourceURI)
	require.Equal(t, "raw/file.xml", in.SourceKey)
	require.Empty(t, in.DestRelKey, "raw URI stdin records must still flow through rewrite planning")
	require.Equal(t, "normal", in.RoutingClass)
}

func TestParseReflowInputLineRejects(t *testing.T) {
	cases := map[string]string{
		"raw prefix uri":          "s3://bucket/prefix/",
		"index file source":       indexObjectInputLineForTest("file:///tmp/source/", "a.xml", "etag", 1),
		"non-s3 source":           `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"file:///srcroot/x","source_key":"x","dest_rel_key":"x"}}`,
		"prefix source":           `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://b/p/","source_key":"p","dest_rel_key":"p"}}`,
		"missing source_uri":      `{"type":"gonimbus.reflow.input.v1","data":{"source_key":"k","dest_rel_key":"k"}}`,
		"quarantine no prefix":    `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://b/k","source_key":"k","routing_class":"quarantine"}}`,
		"absolute quarantine pfx": `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://b/k","source_key":"k","routing_class":"quarantine","quarantine_prefix":"/abs"}}`,
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := parseReflowInputLine(line)
			require.Error(t, err)
		})
	}
}

func TestReflowInputRecordSanitizesSourceURI(t *testing.T) {
	in := reflowInput{
		SourceProvider: "s3",
		SourceBucket:   "b",
		SourceURI:      "https://host/key?X-Amz-Signature=secretsig&plain=ok",
		SourceKey:      "key",
	}
	rec := in.record("s3://dest/data/key", "data/key", "complete")
	require.NotContains(t, rec.SourceURI, "secretsig")
	require.Contains(t, rec.SourceURI, "host/key")
}

func indexObjectInputLineForTest(baseURI string, key string, etag string, size int64) string {
	line, err := json.Marshal(map[string]any{
		"type": "gonimbus.index.object.v1",
		"data": map[string]any{
			"base_uri":      baseURI,
			"key":           key,
			"etag":          etag,
			"size_bytes":    size,
			"last_modified": time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC).Format(time.RFC3339),
		},
	})
	if err != nil {
		panic(err)
	}
	return string(line)
}
