package s3

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// captureStderr runs fn with os.Stderr redirected and returns what it wrote.
// The SDK's default logger binds to os.Stderr when the config is loaded, so
// the provider must be constructed inside fn.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	orig := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = orig }()

	done := make(chan []byte)
	go func() {
		b, _ := io.ReadAll(r)
		done <- b
	}()
	fn()
	require.NoError(t, w.Close())
	return string(<-done)
}

// A ranged GET carries no full-object checksum. The SDK logs that it skipped
// validation; gonimbus hashes payloads itself, so the line is noise on every
// endpoint, AWS included.
func TestProviderRangedGetLeavesStderrEmpty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 1-3/10")
		w.Header().Set("Content-Length", "3")
		w.Header().Set("ETag", `"etag-1"`)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("bcd"))
	}))
	defer server.Close()

	cases := map[string]Config{
		// No Endpoint in the config: the SDK resolves the AWS endpoint itself,
		// here redirected through its own environment override.
		"native AWS":             {},
		"S3-compatible endpoint": {Endpoint: server.URL},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			// Ambient AWS settings must not hide the log line: when_required
			// skips validation before the SDK would log, so pin when_supported
			// and keep the developer's shared config and profile out.
			empty := t.TempDir()
			t.Setenv("AWS_CONFIG_FILE", filepath.Join(empty, "config"))
			t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(empty, "credentials"))
			t.Setenv("AWS_PROFILE", "")
			t.Setenv("AWS_ENDPOINT_URL", "")
			t.Setenv("AWS_ENDPOINT_URL_S3", "")
			t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_supported")
			if cfg.Endpoint == "" {
				t.Setenv("AWS_ENDPOINT_URL_S3", server.URL)
			}
			cfg.Bucket = "test-bucket"
			cfg.Region = "us-east-1"
			cfg.AccessKeyID = "AKIATEST0000000001"
			cfg.SecretAccessKey = "test-secret"
			cfg.ForcePathStyle = true

			var body []byte
			stderr := captureStderr(t, func() {
				p, err := New(context.Background(), cfg)
				require.NoError(t, err)
				defer func() { _ = p.Close() }()
				rc, _, err := p.GetRange(context.Background(), "object.txt", 1, 3)
				require.NoError(t, err)
				body, err = io.ReadAll(rc)
				require.NoError(t, err)
				require.NoError(t, rc.Close())
			})
			require.True(t, bytes.Equal([]byte("bcd"), body))
			require.Empty(t, stderr)
		})
	}
}
