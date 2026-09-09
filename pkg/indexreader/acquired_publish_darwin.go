//go:build darwin

package indexreader

import (
	"os"

	"golang.org/x/sys/unix"
)

func renameDirectoryNoReplace(parent, _ *os.File, oldName, newName string) error {
	fd := int(parent.Fd()) // #nosec G115 -- native descriptors fit int
	return unix.RenameatxNp(fd, oldName, fd, newName, unix.RENAME_EXCL)
}

func syncAcquiredDirectory(dir *os.File) error { return dir.Sync() }
