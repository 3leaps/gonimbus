package transfer

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
	"github.com/stretchr/testify/require"
)

func TestRawFileCopyDoesNotDependOnMetadataSidecar(t *testing.T) {
	for _, unreadable := range []bool{false, true} {
		for _, receipt := range []bool{false, true} {
			root := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(root, "source"), []byte("payload"), 0o600))
			sidecar := filepath.Join(root, "source"+providerfile.DefaultMetadataSidecarSuffix)
			if unreadable {
				require.NoError(t, os.Mkdir(sidecar, 0o700))
			} else {
				require.NoError(t, os.WriteFile(sidecar, []byte("not-json"), 0o600))
			}
			src, err := providerfile.New(providerfile.Config{BaseDir: root})
			require.NoError(t, err)
			// The richer getter's failure proves this fixture exercises the
			// dependency that raw copies must not acquire.
			_, _, err = src.GetObjectVersioned(context.Background(), "source")
			require.Error(t, err)
			destRoot := t.TempDir()
			dst, err := providerfile.New(providerfile.Config{BaseDir: destRoot})
			require.NoError(t, err)
			if receipt {
				result, copyErr := CopyObjectWithReceipt(context.Background(), src, dst, "source", "dest", 7, CopyReceiptOptions{})
				require.NoError(t, copyErr)
				require.Equal(t, payloadSHA256("payload"), result.SHA256)
				require.True(t, result.SourceLastModified.IsZero())
			} else {
				_, err = CopyObject(context.Background(), src, dst, "source", "dest", 7, 0)
				require.NoError(t, err)
			}
			payload, err := os.ReadFile(filepath.Join(destRoot, "dest"))
			require.NoError(t, err)
			require.Equal(t, "payload", string(payload))
		}
	}
}
