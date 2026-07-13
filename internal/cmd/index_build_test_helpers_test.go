package cmd

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

// writeIndexIdentityFile is deliberately test-only. Production canonical
// metadata must use the library-owned retained publisher.
func writeIndexIdentityFile(indexDir string, identityResult *indexstore.IndexSetIdentityResult) error {
	if indexDir == "" || identityResult == nil {
		return nil
	}
	if err := mkdirAppDataDir(indexDir); err != nil {
		return fmt.Errorf("create index directory: %w", err)
	}
	return os.WriteFile(filepath.Join(indexDir, "identity.json"), []byte(identityResult.CanonicalJSON+"\n"), 0o600)
}

// clearSQLiteQuarantineResidueUnderDataRoot is test-local cleanup for multi-step
// fixtures under t.TempDir() after SQLite builds leave fd-truncated quarantine
// captures. It is not a production recovery API.
func clearSQLiteQuarantineResidueUnderDataRoot(t *testing.T, dataRoot string) {
	t.Helper()
	indexes := filepath.Join(dataRoot, "indexes")
	if _, err := os.Stat(indexes); err != nil {
		return
	}
	require.NoError(t, filepath.WalkDir(indexes, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), indexstore.CanonicalSQLiteQuarantinePrefix) {
			return os.Remove(path)
		}
		return nil
	}))
}
