package reflow

import (
	"testing"

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

func TestParseReflowInputLineRejects(t *testing.T) {
	cases := map[string]string{
		"bare uri":                "s3://bucket/key",
		"index record type":       `{"type":"gonimbus.index.object.v1","data":{}}`,
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
