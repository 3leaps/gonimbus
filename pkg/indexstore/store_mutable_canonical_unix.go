//go:build !windows

package indexstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
	sqlite3 "modernc.org/sqlite/lib"
)

var canonicalSQLiteQuarantineSeq atomic.Uint64

type canonicalSQLiteArtifactBinding struct {
	present bool
	dev     uint64
	ino     uint64
}

func bindingFromUnixStat(info unix.Stat_t, present bool) canonicalSQLiteArtifactBinding {
	dev := uint64(info.Dev) // #nosec G115 -- platform device IDs are non-negative identity tokens
	ino := uint64(info.Ino) // #nosec G115 -- platform inode IDs are non-negative identity tokens
	return canonicalSQLiteArtifactBinding{present: present, dev: dev, ino: ino}
}

func openCanonicalSQLiteDirectory(path string) (*os.File, error) {
	clean := filepath.Clean(path)
	fd, err := unix.Open(clean, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	directory := os.NewFile(uintptr(fd), clean) // #nosec G115 -- unix.Open returns a nonnegative descriptor
	if directory == nil {
		_ = unix.Close(fd)
		return nil, os.ErrInvalid
	}
	if err := verifyCanonicalSQLiteDirectory(directory, clean); err != nil {
		_ = directory.Close()
		return nil, err
	}
	return directory, nil
}

func verifyCanonicalSQLiteDirectory(directory *os.File, path string) error {
	if directory == nil {
		return fmt.Errorf("canonical SQLite directory binding is nil")
	}
	var bound unix.Stat_t
	if err := unix.Fstat(int(directory.Fd()), &bound); err != nil { // #nosec G115 -- native descriptors fit int
		return fmt.Errorf("inspect bound canonical SQLite directory: %w", err)
	}
	var named unix.Stat_t
	if err := unix.Fstatat(unix.AT_FDCWD, filepath.Clean(path), &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("inspect named canonical SQLite directory: %w", err)
	}
	if bound.Mode&unix.S_IFMT != unix.S_IFDIR || named.Mode&unix.S_IFMT != unix.S_IFDIR || bound.Dev != named.Dev || bound.Ino != named.Ino {
		return fmt.Errorf("canonical SQLite directory binding changed")
	}
	return nil
}

func reserveCanonicalSQLiteArtifact(directory *os.File, directoryPath, path string) (*os.File, canonicalSQLiteArtifactBinding, error) {
	if err := verifyCanonicalSQLiteDirectory(directory, directoryPath); err != nil {
		return nil, canonicalSQLiteArtifactBinding{}, err
	}
	name := filepath.Base(filepath.Clean(path))
	fd, err := unix.Openat(int(directory.Fd()), name, unix.O_CREAT|unix.O_EXCL|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600) // #nosec G115 -- native descriptors fit int
	if err != nil {
		return nil, canonicalSQLiteArtifactBinding{}, err
	}
	file := os.NewFile(uintptr(fd), name) // #nosec G115 -- unix.Openat returns a nonnegative descriptor
	if file == nil {
		_ = unix.Close(fd)
		return nil, canonicalSQLiteArtifactBinding{}, os.ErrInvalid
	}
	var info unix.Stat_t
	if err := unix.Fstat(fd, &info); err != nil {
		_ = file.Close()
		return nil, canonicalSQLiteArtifactBinding{}, err
	}
	if info.Mode&unix.S_IFMT != unix.S_IFREG {
		_ = file.Close()
		return nil, canonicalSQLiteArtifactBinding{}, fmt.Errorf("reserved canonical SQLite artifact is not regular")
	}
	binding := bindingFromUnixStat(info, true)
	if err := verifyCanonicalSQLiteDirectory(directory, directoryPath); err != nil {
		_ = file.Close()
		_ = removeCanonicalSQLiteArtifact(directory, directoryPath, path, binding)
		return nil, canonicalSQLiteArtifactBinding{}, err
	}
	return file, binding, nil
}

func removeCanonicalSQLiteArtifact(directory *os.File, directoryPath, path string, binding canonicalSQLiteArtifactBinding) error {
	return removeCanonicalSQLiteArtifactWithHooks(directory, directoryPath, path, binding, exactEpochRemovalHooks{})
}

func removeCanonicalSQLiteArtifactWithHooks(directory *os.File, directoryPath, path string, binding canonicalSQLiteArtifactBinding, hooks exactEpochRemovalHooks) error {
	if !binding.present {
		return nil
	}
	if err := verifyCanonicalSQLiteDirectory(directory, directoryPath); err != nil {
		return err
	}
	name := filepath.Base(filepath.Clean(path))
	dirfd := int(directory.Fd()) // #nosec G115 -- native descriptors fit int
	// Capture the live directory entry into a unique quarantine name first.
	// Identity is attested only on an open descriptor for that capture.
	quarantine := fmt.Sprintf(
		"%s%s-%d-%d-%d-%d",
		canonicalSQLiteQuarantinePrefix,
		name,
		binding.dev,
		binding.ino,
		unix.Getpid(),
		canonicalSQLiteQuarantineSeq.Add(1),
	)
	if err := unix.Renameat(dirfd, name, dirfd, quarantine); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil
		}
		return err
	}
	if hooks.afterCapture != nil {
		if err := hooks.afterCapture(path, filepath.Join(directoryPath, quarantine)); err != nil {
			// Best-effort no-replace restore before surfacing the hook error.
			_ = renameCanonicalSQLiteNoReplace(dirfd, quarantine, name)
			return fmt.Errorf("after exact-epoch capture: %w", err)
		}
	}

	fd, err := unix.Openat(dirfd, quarantine, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if restoreErr := renameCanonicalSQLiteNoReplace(dirfd, quarantine, name); restoreErr != nil {
			return fmt.Errorf("open captured canonical SQLite artifact: %w; capture retained as %s: %v", err, quarantine, restoreErr)
		}
		return fmt.Errorf("open captured canonical SQLite artifact: %w", err)
	}
	var opened unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil {
		_ = unix.Close(fd)
		if restoreErr := renameCanonicalSQLiteNoReplace(dirfd, quarantine, name); restoreErr != nil {
			return fmt.Errorf("inspect captured canonical SQLite artifact: %w; capture retained as %s: %v", err, quarantine, restoreErr)
		}
		return fmt.Errorf("inspect captured canonical SQLite artifact: %w", err)
	}
	current := bindingFromUnixStat(opened, opened.Mode&unix.S_IFMT == unix.S_IFREG)
	if current != binding {
		_ = unix.Close(fd)
		if err := renameCanonicalSQLiteNoReplace(dirfd, quarantine, name); err != nil {
			// Never overwrite a newly live epoch. Preserve both objects and
			// leave the capture discoverable under the quarantine name.
			return fmt.Errorf("refusing to remove a different canonical SQLite artifact epoch; capture retained as %s: %w", quarantine, err)
		}
		return fmt.Errorf("refusing to remove a different canonical SQLite artifact epoch")
	}
	if hooks.afterAttest != nil {
		if err := hooks.afterAttest(path, filepath.Join(directoryPath, quarantine)); err != nil {
			_ = unix.Close(fd)
			if restoreErr := renameCanonicalSQLiteNoReplace(dirfd, quarantine, name); restoreErr != nil {
				return fmt.Errorf("after exact-epoch attest: %w; capture retained as %s: %v", err, quarantine, restoreErr)
			}
			return fmt.Errorf("after exact-epoch attest: %w", err)
		}
		// Re-attest the open descriptor after the hook. Pathname substitution
		// cannot change the bound inode held by fd.
		if err := unix.Fstat(fd, &opened); err != nil {
			_ = unix.Close(fd)
			return fmt.Errorf("re-inspect captured canonical SQLite artifact: %w", err)
		}
		current = bindingFromUnixStat(opened, opened.Mode&unix.S_IFMT == unix.S_IFREG)
		if current != binding {
			_ = unix.Close(fd)
			return fmt.Errorf("refusing to destroy a different canonical SQLite artifact epoch; capture retained as %s", quarantine)
		}
	}

	// Exact-object content destruction only: truncate the bound inode via the
	// open descriptor. Do not pathname-unlink the quarantine name — Linux has
	// no supported unlinkat(AT_EMPTY_PATH) for this, and any fstatat-then-
	// unlinkat path retains a substitution race. The empty (or still-named)
	// capture remains discoverable blocking residue until an authorized
	// recovery transaction removes it under a validated binding/receipt.
	if err := unix.Ftruncate(fd, 0); err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("truncate exact canonical SQLite artifact epoch: %w", err)
	}
	_ = unix.Close(fd)
	return nil
}

func captureCanonicalSQLiteArtifactBinding(path string) (canonicalSQLiteArtifactBinding, error) {
	var info unix.Stat_t
	if err := unix.Lstat(filepath.Clean(path), &info); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return canonicalSQLiteArtifactBinding{}, nil
		}
		return canonicalSQLiteArtifactBinding{}, err
	}
	if info.Mode&unix.S_IFMT != unix.S_IFREG {
		return canonicalSQLiteArtifactBinding{}, fmt.Errorf("canonical SQLite artifact is not a regular no-follow file")
	}
	return bindingFromUnixStat(info, true), nil
}

func verifyCanonicalSQLiteArtifactBinding(path string, binding canonicalSQLiteArtifactBinding) error {
	current, err := captureCanonicalSQLiteArtifactBinding(path)
	if err != nil {
		return err
	}
	if current != binding {
		return fmt.Errorf("canonical SQLite artifact epoch changed")
	}
	return nil
}

//nolint:govet // SQLite supplies this exact sqlite3_file pointer to the VFS.
func bindCanonicalSQLiteArtifactFile(pFile uintptr, path string, binding canonicalSQLiteArtifactBinding) (canonicalSQLiteArtifactBinding, error) {
	driverFile := (*sqlite3.TunixFile)(unsafe.Pointer(pFile))
	if driverFile.Fh < 0 {
		return binding, fmt.Errorf("canonical SQLite sidecar has no descriptor")
	}
	var opened unix.Stat_t
	if err := unix.Fstat(int(driverFile.Fh), &opened); err != nil {
		return binding, fmt.Errorf("inspect exact SQLite sidecar descriptor: %w", err)
	}
	return bindCanonicalSQLiteUnixArtifactStat(path, binding, opened)
}

//nolint:govet // SQLite owns the linked VFS shared-memory structs.
func bindCanonicalSQLiteSharedMemory(pFile uintptr, path string, binding canonicalSQLiteArtifactBinding) (canonicalSQLiteArtifactBinding, error) {
	main := (*sqlite3.TunixFile)(unsafe.Pointer(pFile))
	if main.FpShm == 0 {
		return binding, fmt.Errorf("SQLite did not retain a shared-memory binding")
	}
	shm := (*sqlite3.TunixShm)(unsafe.Pointer(main.FpShm))
	if shm.FpShmNode == 0 {
		return binding, fmt.Errorf("SQLite shared-memory node is nil")
	}
	node := (*sqlite3.TunixShmNode)(unsafe.Pointer(shm.FpShmNode))
	if node.FhShm < 0 {
		return binding, fmt.Errorf("SQLite shared-memory node has no descriptor")
	}
	var opened unix.Stat_t
	if err := unix.Fstat(int(node.FhShm), &opened); err != nil {
		return binding, fmt.Errorf("inspect exact SQLite shared-memory descriptor: %w", err)
	}
	return bindCanonicalSQLiteUnixArtifactStat(path, binding, opened)
}

func bindCanonicalSQLiteUnixArtifactStat(path string, binding canonicalSQLiteArtifactBinding, opened unix.Stat_t) (canonicalSQLiteArtifactBinding, error) {
	if opened.Mode&unix.S_IFMT != unix.S_IFREG {
		return binding, fmt.Errorf("SQLite artifact handle is not regular")
	}
	actual := bindingFromUnixStat(opened, true)
	if binding.present && actual != binding {
		return binding, fmt.Errorf("SQLite opened a different canonical artifact epoch")
	}
	if err := attestCanonicalSQLiteUnixNamedStat(path, opened); err != nil {
		return binding, err
	}
	return actual, nil
}

//nolint:govet // SQLite supplies this exact sqlite3_file pointer to the VFS.
func attestCanonicalSQLiteVFSFile(pFile uintptr, bound *os.File, path string) error {
	if bound == nil || pFile == 0 {
		return fmt.Errorf("canonical SQLite retained or driver binding is nil")
	}
	driverFile := (*sqlite3.TunixFile)(unsafe.Pointer(pFile))
	if driverFile.Fh < 0 {
		return fmt.Errorf("canonical SQLite driver file has no descriptor")
	}
	var expected unix.Stat_t
	if err := unix.Fstat(int(bound.Fd()), &expected); err != nil { // #nosec G115 -- native descriptors fit int
		return fmt.Errorf("inspect retained canonical SQLite descriptor: %w", err)
	}
	var opened unix.Stat_t
	if err := unix.Fstat(int(driverFile.Fh), &opened); err != nil {
		return fmt.Errorf("inspect exact SQLite driver descriptor: %w", err)
	}
	if opened.Mode&unix.S_IFMT != unix.S_IFREG || opened.Dev != expected.Dev || opened.Ino != expected.Ino {
		return fmt.Errorf("exact SQLite driver connection is not bound to retained index.db")
	}
	return attestCanonicalSQLiteUnixNamedStat(path, opened)
}

//nolint:govet // SQLite supplies this exact sqlite3_file pointer to the VFS.
func attestCanonicalSQLiteVFSNamedFile(pFile uintptr, path string) error {
	if pFile == 0 {
		return fmt.Errorf("canonical SQLite sidecar binding is nil")
	}
	driverFile := (*sqlite3.TunixFile)(unsafe.Pointer(pFile))
	if driverFile.Fh < 0 {
		return fmt.Errorf("canonical SQLite sidecar has no descriptor")
	}
	var opened unix.Stat_t
	if err := unix.Fstat(int(driverFile.Fh), &opened); err != nil {
		return fmt.Errorf("inspect exact SQLite sidecar descriptor: %w", err)
	}
	if opened.Mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("SQLite sidecar is not regular")
	}
	return attestCanonicalSQLiteUnixNamedStat(path, opened)
}

func attestCanonicalSQLiteUnixNamedStat(path string, opened unix.Stat_t) error {
	var named unix.Stat_t
	if err := unix.Lstat(filepath.Clean(path), &named); err != nil {
		return fmt.Errorf("inspect canonical SQLite named binding: %w", err)
	}
	if named.Mode&unix.S_IFMT != unix.S_IFREG || named.Dev != opened.Dev || named.Ino != opened.Ino {
		return fmt.Errorf("SQLite handle is not the current no-follow canonical file")
	}
	return nil
}
