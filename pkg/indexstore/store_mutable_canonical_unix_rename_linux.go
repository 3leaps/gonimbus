//go:build linux

package indexstore

import "golang.org/x/sys/unix"

func renameCanonicalSQLiteNoReplace(dirfd int, oldName, newName string) error {
	return unix.Renameat2(dirfd, oldName, dirfd, newName, unix.RENAME_NOREPLACE)
}
