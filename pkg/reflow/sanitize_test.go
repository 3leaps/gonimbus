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

// TestSanitizeSourceURIIsLosslessForObjectKeys pins the identity half of the
// sanitizer's contract: an object URI must survive VERBATIM, because the same
// value is the per-item checkpoint authority and the emitted record's source URI.
//
// Every row is a legal S3 key that a generic URL rewrite would damage. `?` is
// key or glob syntax under the gonimbus URI grammar rather than a query
// delimiter, so re-rendering through net/url blanks everything after it — and
// two keys differing only past the `?` would collapse onto one authority. A
// space, a non-ASCII byte, and a literal `%` would be percent-encoded into a
// different key.
func TestSanitizeSourceURIIsLosslessForObjectKeys(t *testing.T) {
	for name, uri := range map[string]string{
		"question mark in key":     "s3://bucket/a/file?version=one",
		"question mark, other key": "s3://bucket/a/file?version=two",
		"question-mark glob":       "s3://bucket/a/file?.xml",
		"asterisk glob":            "s3://bucket/a/file*.xml",
		"brackets":                 "s3://bucket/a/[backup]/f.xml",
		"space":                    "s3://bucket/a/file with space.xml",
		"non-ascii":                "s3://bucket/a/café.xml",
		"percent":                  "s3://bucket/a/100%.xml",
		"hash":                     "s3://bucket/a/file#1.xml",
		"gcs scheme":               "gs://bucket/a/file?version=one",
		"file scheme":              "file://local/tmp/x?y.txt",
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, uri, sanitizeSourceURI(uri),
				"an object URI is identity, not a display string")
		})
	}

	// The distinctness that the collapse would destroy, stated directly.
	require.NotEqual(t,
		sanitizeSourceURI("s3://bucket/a/file?version=one"),
		sanitizeSourceURI("s3://bucket/a/file?version=two"),
		"two distinct listed objects must keep two distinct authorities")
}

// TestSanitizeSourceURIStillRedactsCredentialMaterial is the other half: making
// object URIs lossless must not become a blanket removal of credential
// protection. Presigned material lives on http(s), where a query IS a query, and
// userinfo is stripped on every scheme because it is never part of an object key.
func TestSanitizeSourceURIStillRedactsCredentialMaterial(t *testing.T) {
	t.Run("presigned https query is redacted", func(t *testing.T) {
		got := sanitizeSourceURI("https://host/obj?X-Amz-Signature=SECRETSIG&X-Amz-Credential=SECRETCRED&X-Amz-Security-Token=SECRETTOK")
		require.NotContains(t, got, "SECRETSIG")
		require.NotContains(t, got, "SECRETCRED")
		require.NotContains(t, got, "SECRETTOK")
	})

	t.Run("http userinfo is redacted", func(t *testing.T) {
		got := sanitizeSourceURI("https://user:SECRETPASS@host/obj")
		require.NotContains(t, got, "SECRETPASS")
		require.NotContains(t, got, "user:")
	})

	t.Run("object-uri userinfo is redacted without touching the key", func(t *testing.T) {
		got := sanitizeSourceURI("s3://user:SECRETPASS@bucket/a/file?version=one")
		require.NotContains(t, got, "SECRETPASS")
		require.Equal(t, "s3://<redacted>@bucket/a/file?version=one", got,
			"only the authority is rewritten; the key keeps its exact spelling")
	})

	t.Run("an at-sign inside the key is not authority", func(t *testing.T) {
		uri := "s3://bucket/a/user@example.com.xml"
		require.Equal(t, uri, sanitizeSourceURI(uri),
			"the authority ends at the first slash; a later @ is an ordinary key character")
	})
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
