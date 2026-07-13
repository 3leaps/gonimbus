//go:build unix && !linux && !darwin

package indexstore

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func renameCanonicalSQLiteNoReplace(dirfd int, oldName, newName string) error {
	// Without an atomic no-replace primitive, refuse to rename. Callers retain
	// the capture as discoverable residue rather than risk a check-then-rename
	// overwrite of a newly live epoch.
	_ = dirfd
	_ = oldName
	_ = newName
	return fmt.Errorf("atomic no-replace restore is unavailable on this platform: %w", unix.ENOSYS)
}
