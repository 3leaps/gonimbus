//go:build !darwin && !linux && !windows

package indexreader

import (
	"fmt"
	"os"
)

func renameDirectoryNoReplace(_, _ *os.File, _, _ string) error {
	return fmt.Errorf("atomic no-replace directory publication is unavailable on this platform")
}

func syncAcquiredDirectory(dir *os.File) error { return dir.Sync() }
