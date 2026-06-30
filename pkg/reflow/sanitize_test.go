package reflow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSanitizeSourceURI(t *testing.T) {
	// A plain object URI with no query is unchanged (parity-preserving).
	require.Equal(t, "s3://bucket/a/b.xml", sanitizeSourceURI("s3://bucket/a/b.xml"))
	require.Equal(t, "", sanitizeSourceURI(""))

	// A presigned URL has its signature query redacted, path preserved.
	got := sanitizeSourceURI("https://host/obj?X-Amz-Signature=abc123&X-Amz-Credential=keyid")
	require.NotContains(t, got, "abc123")
	require.NotContains(t, got, "keyid")
	require.Contains(t, got, "host/obj")
}

func TestSanitizeDetailsLeavesBenignVerbatim(t *testing.T) {
	in := map[string]any{
		"on_collision":                    "skip-if-duplicate",
		"fallback":                        "head_compare",
		"dest_ifabsent_honored":           (*bool)(nil),
		"cross_process_atomicity_limited": true,
		"provider":                        "s3",
		"count":                           int64(3),
	}
	out := sanitizeDetails(in)
	require.Equal(t, in, out, "benign details must pass through verbatim for parity")
}

func TestSanitizeDetailsRedactsSensitiveFields(t *testing.T) {
	in := map[string]any{
		"source_uri":    "https://host/obj?X-Amz-Signature=sig",
		"note":          "token=supersecretvalue please ignore",
		"authorization": "bare-token-value",
		"nested":        map[string]any{"dest_uri": "https://h/o?sig=zzz"},
	}
	out := sanitizeDetails(in)
	require.NotContains(t, out["source_uri"].(string), "sig=sig")
	require.NotContains(t, out["note"].(string), "supersecretvalue")
	require.Equal(t, "redacted", out["authorization"])
	require.NotContains(t, out["nested"].(map[string]any)["dest_uri"].(string), "zzz")
}

func TestSanitizeDetailsNil(t *testing.T) {
	require.Nil(t, sanitizeDetails(nil))
}
