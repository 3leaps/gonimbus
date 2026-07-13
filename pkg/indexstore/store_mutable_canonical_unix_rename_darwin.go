//go:build darwin

package indexstore

import "golang.org/x/sys/unix"

func renameCanonicalSQLiteNoReplace(dirfd int, oldName, newName string) error {
	return unix.RenameatxNp(dirfd, oldName, dirfd, newName, unix.RENAME_EXCL)
}
