package gcs

import (
	"strings"
	"testing"

	"golang.org/x/oauth2"

	"github.com/stretchr/testify/require"
)

type staticTokenSource struct {
	token string
}

func (s staticTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: s.token}, nil
}

func TestConfigValidate(t *testing.T) {
	require.NoError(t, (&Config{Bucket: "bucket"}).Validate())

	err := (&Config{}).Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "Bucket")

	err = (&Config{
		Bucket:      "bucket",
		Anonymous:   true,
		TokenSource: staticTokenSource{token: "SECRET-TOKEN"},
	}).Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot be combined with TokenSource")

	err = (&Config{Bucket: "bucket", WriterChunkSizeBytes: -1}).Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "WriterChunkSizeBytes")
}

func TestConfigAuthClientOptions(t *testing.T) {
	require.Empty(t, (Config{Bucket: "bucket"}).AuthClientOptions(), "ADC should remain the SDK default")
	require.Len(t, (Config{Bucket: "bucket", Anonymous: true}).AuthClientOptions(), 1)
	require.Len(t, (Config{Bucket: "bucket", TokenSource: staticTokenSource{token: "SECRET-TOKEN"}}).AuthClientOptions(), 1)
}

func TestConfigStringRedactsTokenSource(t *testing.T) {
	cfg := Config{
		Bucket:               "bucket",
		Project:              "project",
		TokenSource:          staticTokenSource{token: "SECRET-TOKEN"},
		MaxIdleConnsPerHost:  32,
		MaxConnsPerHost:      32,
		WriterChunkSizeBytes: 4 * 1024 * 1024,
	}

	got := cfg.String()
	require.Contains(t, got, "TokenSource:<set:")
	require.NotContains(t, got, "SECRET-TOKEN")
	require.Equal(t, got, cfg.GoString())

	token, err := cfg.TokenSource.Token()
	require.NoError(t, err)
	require.Equal(t, "SECRET-TOKEN", token.AccessToken, "test guard: token source carries the marker that String must not expose")
}

func TestConfigSurfaceHasNoCredentialPathFields(t *testing.T) {
	got := strings.ToLower(Config{Bucket: "bucket"}.String())
	for _, forbidden := range []string{
		"credentialfile",
		"credentialsfile",
		"serviceaccountfile",
		"google_application_credentials",
		"emulator",
		"endpoint",
		"tls",
	} {
		require.NotContains(t, got, forbidden)
	}
}

func TestDefaultScopesUsesStorageReadWrite(t *testing.T) {
	require.Equal(t, []string{"https://www.googleapis.com/auth/devstorage.read_write"}, DefaultScopes())
}

func TestStaticTokenSourceIsTypedHandleNotPath(t *testing.T) {
	_, err := staticTokenSource{token: "SECRET-TOKEN"}.Token()
	require.NoError(t, err)
}
