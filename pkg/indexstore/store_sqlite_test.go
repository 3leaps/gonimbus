//go:build !gonimbus_libsql

package indexstore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenRemoteLibsqlURLRequiresBuildTag(t *testing.T) {
	db, err := Open(context.Background(), Config{URL: "libsql://example.invalid/index"})
	require.Nil(t, db)
	require.EqualError(t, err, "remote libsql URLs require rebuilding with -tags gonimbus_libsql")
}
