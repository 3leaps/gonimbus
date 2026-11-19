package indexstore

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenRemoteLibsqlURLFailsClosed(t *testing.T) {
	wd := t.TempDir()
	origWD, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(wd))
	t.Cleanup(func() { _ = os.Chdir(origWD) })

	cases := []struct {
		name string
		cfg  Config
	}{
		{name: "libsql_url", cfg: Config{URL: "libsql://example.invalid/index"}},
		{name: "libsql_url_upper", cfg: Config{URL: "LIBSQL://example.invalid/index"}},
		{name: "https_url", cfg: Config{URL: "https://example.invalid/index"}},
		{name: "http_url", cfg: Config{URL: "http://example.invalid/index"}},
		{name: "wss_url", cfg: Config{URL: "wss://example.invalid/index"}},
		{name: "libsql_path", cfg: Config{Path: "libsql://example.invalid/index"}},
		{name: "https_path", cfg: Config{Path: "https://example.invalid/index"}},
		{name: "http_path", cfg: Config{Path: "http://example.invalid/index"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, openErr := Open(context.Background(), tc.cfg)
			require.Nil(t, db)
			require.EqualError(t, openErr, "remote libsql is not supported in this release")
		})
	}
	entries, err := os.ReadDir(wd)
	require.NoError(t, err)
	require.Empty(t, entries, "remote URL refusal must not create any local DB file")
}
