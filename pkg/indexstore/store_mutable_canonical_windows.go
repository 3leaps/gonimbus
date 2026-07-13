//go:build windows

package indexstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"
	sqlite3 "modernc.org/sqlite/lib"
)

type canonicalSQLiteArtifactBinding struct {
	present bool
	volume  uint32
	high    uint32
	low     uint32
}

const canonicalSQLiteFileAddFileAccess = 0x00000002

func openCanonicalSQLiteDirectory(path string) (*os.File, error) {
	handle, err := openCanonicalSQLiteDirectoryHandle(path)
	if err != nil {
		return nil, err
	}
	directory := os.NewFile(uintptr(handle), filepath.Clean(path))
	if directory == nil {
		_ = windows.CloseHandle(handle)
		return nil, os.ErrInvalid
	}
	return directory, nil
}

func openCanonicalSQLiteDirectoryHandle(path string) (windows.Handle, error) {
	p, err := windows.UTF16PtrFromString(filepath.Clean(path))
	if err != nil {
		return 0, err
	}
	handle, err := windows.CreateFile(p, windows.FILE_LIST_DIRECTORY|windows.FILE_TRAVERSE|canonicalSQLiteFileAddFileAccess|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return 0, err
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil || info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 || info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 {
		_ = windows.CloseHandle(handle)
		return 0, errors.Join(fmt.Errorf("canonical SQLite path is not a no-follow directory"), err)
	}
	return handle, nil
}

func verifyCanonicalSQLiteDirectory(directory *os.File, path string) error {
	if directory == nil {
		return fmt.Errorf("canonical SQLite directory binding is nil")
	}
	var bound windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(directory.Fd()), &bound); err != nil {
		return fmt.Errorf("inspect bound canonical SQLite directory: %w", err)
	}
	namedHandle, err := openCanonicalSQLiteDirectoryHandle(path)
	if err != nil {
		return fmt.Errorf("open named canonical SQLite directory: %w", err)
	}
	defer func() { _ = windows.CloseHandle(namedHandle) }()
	var named windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(namedHandle, &named); err != nil {
		return fmt.Errorf("inspect named canonical SQLite directory: %w", err)
	}
	if bound.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 || named.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 ||
		bound.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 || named.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 ||
		bound.VolumeSerialNumber != named.VolumeSerialNumber || bound.FileIndexHigh != named.FileIndexHigh || bound.FileIndexLow != named.FileIndexLow {
		return fmt.Errorf("canonical SQLite directory binding changed")
	}
	return nil
}

func reserveCanonicalSQLiteArtifact(directory *os.File, directoryPath, path string) (*os.File, canonicalSQLiteArtifactBinding, error) {
	if err := verifyCanonicalSQLiteDirectory(directory, directoryPath); err != nil {
		return nil, canonicalSQLiteArtifactBinding{}, err
	}
	name := filepath.Base(filepath.Clean(path))
	handle, err := openCanonicalSQLiteRelativeWindows(windows.Handle(directory.Fd()), name, windows.FILE_CREATE, false)
	if err != nil {
		return nil, canonicalSQLiteArtifactBinding{}, err
	}
	file := os.NewFile(uintptr(handle), name)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, canonicalSQLiteArtifactBinding{}, os.ErrInvalid
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		_ = file.Close()
		return nil, canonicalSQLiteArtifactBinding{}, err
	}
	binding := canonicalSQLiteWindowsArtifactBinding(info)
	if !binding.present {
		_ = file.Close()
		return nil, canonicalSQLiteArtifactBinding{}, fmt.Errorf("reserved canonical SQLite artifact is not regular")
	}
	if err := verifyCanonicalSQLiteDirectory(directory, directoryPath); err != nil {
		_ = file.Close()
		_ = removeCanonicalSQLiteArtifact(directory, directoryPath, path, binding)
		return nil, canonicalSQLiteArtifactBinding{}, err
	}
	return file, binding, nil
}

func openCanonicalSQLiteRelativeWindows(parent windows.Handle, name string, disposition uint32, deleteAccess bool) (windows.Handle, error) {
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return 0, err
	}
	attrs := &windows.OBJECT_ATTRIBUTES{RootDirectory: parent, ObjectName: objectName, Attributes: windows.OBJ_CASE_INSENSITIVE | windows.OBJ_DONT_REPARSE}
	attrs.Length = uint32(unsafe.Sizeof(*attrs))
	access := uint32(windows.FILE_GENERIC_READ | windows.FILE_GENERIC_WRITE | windows.FILE_READ_ATTRIBUTES | windows.SYNCHRONIZE)
	if deleteAccess {
		access |= windows.DELETE
	}
	var handle windows.Handle
	var status windows.IO_STATUS_BLOCK
	allocation := int64(0)
	options := uint32(windows.FILE_NON_DIRECTORY_FILE | windows.FILE_OPEN_REPARSE_POINT | windows.FILE_SYNCHRONOUS_IO_NONALERT)
	if err := windows.NtCreateFile(&handle, access, attrs, &status, &allocation, 0, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, disposition, options, 0, 0); err != nil {
		return 0, err
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil || !canonicalSQLiteWindowsArtifactBinding(info).present {
		_ = windows.CloseHandle(handle)
		return 0, errors.Join(fmt.Errorf("refusing non-regular canonical SQLite artifact"), err)
	}
	return handle, nil
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
	// Windows opens the named object then deletes by handle disposition, which
	// is already exact-object deletion for the opened file index. Capture hooks
	// still run around open so tests can exercise substitution refuse paths.
	if hooks.afterCapture != nil {
		if err := hooks.afterCapture(path, filepath.Join(directoryPath, name)); err != nil {
			return fmt.Errorf("after exact-epoch capture: %w", err)
		}
	}
	handle, err := openCanonicalSQLiteRelativeWindows(windows.Handle(directory.Fd()), name, windows.FILE_OPEN, true)
	if canonicalSQLiteWindowsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = windows.CloseHandle(handle) }()
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return err
	}
	if canonicalSQLiteWindowsArtifactBinding(info) != binding {
		return fmt.Errorf("refusing to remove a different canonical SQLite artifact epoch")
	}
	if hooks.afterAttest != nil {
		if err := hooks.afterAttest(path, filepath.Join(directoryPath, name)); err != nil {
			return fmt.Errorf("after exact-epoch attest: %w", err)
		}
		if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
			return err
		}
		if canonicalSQLiteWindowsArtifactBinding(info) != binding {
			return fmt.Errorf("refusing to destroy a different canonical SQLite artifact epoch")
		}
	}
	disposition := struct{ DeleteFile byte }{DeleteFile: 1}
	var status windows.IO_STATUS_BLOCK
	return windows.NtSetInformationFile(handle, &status, (*byte)(unsafe.Pointer(&disposition)), uint32(unsafe.Sizeof(disposition)), windows.FileDispositionInformation)
}

func canonicalSQLiteWindowsNotFound(err error) bool {
	return errors.Is(err, windows.ERROR_FILE_NOT_FOUND) || errors.Is(err, windows.ERROR_PATH_NOT_FOUND) ||
		errors.Is(err, windows.STATUS_NO_SUCH_FILE) || errors.Is(err, windows.STATUS_OBJECT_NAME_NOT_FOUND) ||
		errors.Is(err, windows.STATUS_OBJECT_PATH_NOT_FOUND)
}

func captureCanonicalSQLiteArtifactBinding(path string) (canonicalSQLiteArtifactBinding, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return canonicalSQLiteArtifactBinding{}, nil
		}
		return canonicalSQLiteArtifactBinding{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return canonicalSQLiteArtifactBinding{}, fmt.Errorf("canonical SQLite artifact is not a regular no-follow file")
	}
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return canonicalSQLiteArtifactBinding{}, err
	}
	defer f.Close()
	var opened windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(f.Fd()), &opened); err != nil {
		return canonicalSQLiteArtifactBinding{}, err
	}
	return canonicalSQLiteWindowsArtifactBinding(opened), nil
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
	driverFile := (*sqlite3.TwinFile)(unsafe.Pointer(pFile))
	var opened windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(driverFile.Fh), &opened); err != nil {
		return binding, fmt.Errorf("inspect exact SQLite sidecar handle: %w", err)
	}
	return bindCanonicalSQLiteWindowsArtifactInfo(path, binding, opened)
}

//nolint:govet // SQLite owns the linked VFS shared-memory structs.
func bindCanonicalSQLiteSharedMemory(pFile uintptr, path string, binding canonicalSQLiteArtifactBinding) (canonicalSQLiteArtifactBinding, error) {
	main := (*sqlite3.TwinFile)(unsafe.Pointer(pFile))
	if main.FpShm == 0 {
		return binding, fmt.Errorf("SQLite did not retain a shared-memory binding")
	}
	shm := (*sqlite3.TwinShm)(unsafe.Pointer(main.FpShm))
	var opened windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(shm.FhShm), &opened); err != nil {
		return binding, fmt.Errorf("inspect exact SQLite shared-memory handle: %w", err)
	}
	return bindCanonicalSQLiteWindowsArtifactInfo(path, binding, opened)
}

func bindCanonicalSQLiteWindowsArtifactInfo(path string, binding canonicalSQLiteArtifactBinding, opened windows.ByHandleFileInformation) (canonicalSQLiteArtifactBinding, error) {
	actual := canonicalSQLiteWindowsArtifactBinding(opened)
	if !actual.present {
		return binding, fmt.Errorf("SQLite artifact handle is not regular")
	}
	if binding.present && actual != binding {
		return binding, fmt.Errorf("SQLite opened a different canonical artifact epoch")
	}
	if err := attestCanonicalSQLiteWindowsNamedFile(path, opened); err != nil {
		return binding, err
	}
	return actual, nil
}

func canonicalSQLiteWindowsArtifactBinding(info windows.ByHandleFileInformation) canonicalSQLiteArtifactBinding {
	if info.FileAttributes&(windows.FILE_ATTRIBUTE_DIRECTORY|windows.FILE_ATTRIBUTE_REPARSE_POINT) != 0 {
		return canonicalSQLiteArtifactBinding{}
	}
	return canonicalSQLiteArtifactBinding{present: true, volume: info.VolumeSerialNumber, high: info.FileIndexHigh, low: info.FileIndexLow}
}

//nolint:govet // SQLite supplies this exact sqlite3_file pointer to the VFS.
func attestCanonicalSQLiteVFSFile(pFile uintptr, bound *os.File, path string) error {
	if bound == nil || pFile == 0 {
		return fmt.Errorf("canonical SQLite retained or driver binding is nil")
	}
	driverFile := (*sqlite3.TwinFile)(unsafe.Pointer(pFile))
	var expected windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(bound.Fd()), &expected); err != nil {
		return fmt.Errorf("inspect retained canonical SQLite handle: %w", err)
	}
	var opened windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(driverFile.Fh), &opened); err != nil {
		return fmt.Errorf("inspect exact SQLite driver handle: %w", err)
	}
	if !sameCanonicalSQLiteWindowsFile(expected, opened) {
		return fmt.Errorf("exact SQLite driver connection is not bound to retained index.db")
	}
	return attestCanonicalSQLiteWindowsNamedFile(path, opened)
}

//nolint:govet // SQLite supplies this exact sqlite3_file pointer to the VFS.
func attestCanonicalSQLiteVFSNamedFile(pFile uintptr, path string) error {
	if pFile == 0 {
		return fmt.Errorf("canonical SQLite sidecar binding is nil")
	}
	driverFile := (*sqlite3.TwinFile)(unsafe.Pointer(pFile))
	var opened windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(driverFile.Fh), &opened); err != nil {
		return fmt.Errorf("inspect exact SQLite sidecar handle: %w", err)
	}
	return attestCanonicalSQLiteWindowsNamedFile(path, opened)
}

func attestCanonicalSQLiteWindowsNamedFile(path string, opened windows.ByHandleFileInformation) error {
	if opened.FileAttributes&(windows.FILE_ATTRIBUTE_DIRECTORY|windows.FILE_ATTRIBUTE_REPARSE_POINT) != 0 {
		return fmt.Errorf("SQLite handle is not a regular no-follow file")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("inspect canonical SQLite named binding: %w", err)
	}
	named, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open canonical SQLite named binding: %w", err)
	}
	defer named.Close()
	var namedInfo windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(named.Fd()), &namedInfo); err != nil {
		return fmt.Errorf("inspect canonical SQLite named handle: %w", err)
	}
	if !sameCanonicalSQLiteWindowsFile(opened, namedInfo) {
		return fmt.Errorf("SQLite handle is not the current no-follow canonical file")
	}
	return nil
}

func sameCanonicalSQLiteWindowsFile(a, b windows.ByHandleFileInformation) bool {
	return a.FileAttributes&(windows.FILE_ATTRIBUTE_DIRECTORY|windows.FILE_ATTRIBUTE_REPARSE_POINT) == 0 &&
		b.FileAttributes&(windows.FILE_ATTRIBUTE_DIRECTORY|windows.FILE_ATTRIBUTE_REPARSE_POINT) == 0 &&
		a.VolumeSerialNumber == b.VolumeSerialNumber && a.FileIndexHigh == b.FileIndexHigh && a.FileIndexLow == b.FileIndexLow
}
