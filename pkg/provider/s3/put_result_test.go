package s3

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/require"
)

func TestUnconditionalPutResult(t *testing.T) {
	for _, version := range []string{"", "null", "write-version"} {
		t.Run("version-"+version, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				require.Equal(t, http.MethodPut, r.Method)
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Equal(t, "payload", string(body))
				require.Equal(t, "text/plain", r.Header.Get("Content-Type"))
				require.Empty(t, r.Header.Get("If-Match"))
				require.Empty(t, r.Header.Get("If-None-Match"))
				w.Header().Set("ETag", `"written-etag"`)
				if version != "" {
					w.Header().Set("x-amz-version-id", version)
				}
			}))
			defer server.Close()
			p, err := New(context.Background(), Config{Bucket: "test-bucket", Region: "us-east-1", Endpoint: server.URL, ForcePathStyle: true, AccessKeyID: "synthetic-access", SecretAccessKey: "synthetic-secret"})
			require.NoError(t, err)
			result, err := p.PutObjectResultWithOptions(context.Background(), "receipt.txt", strings.NewReader("payload"), 7, provider.PutOptions{ContentType: "text/plain"})
			require.NoError(t, err)
			require.Equal(t, "written-etag", result.ETag)
			require.Equal(t, version, result.Version)
			require.Equal(t, 1, calls)
		})
	}
}
