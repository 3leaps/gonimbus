//go:build linux

package indexreader

import (
	"os"

	"golang.org/x/sys/unix"
)

func renameDirectoryNoReplace(parent, _ *os.File, oldName, newName string) error {
	fd := int(parent.Fd()) // #nosec G115 -- native descriptors fit int
	return unix.Renameat2(fd, oldName, fd, newName, unix.RENAME_NOREPLACE)
}

func syncAcquiredDirectory(dir *os.File) error { return dir.Sync() }
