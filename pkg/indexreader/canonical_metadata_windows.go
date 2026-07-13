//go:build windows

package indexreader

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"
)

const canonicalMetadataFileAddFileAccess = 0x00000002

func publishCanonicalMetadataPlatform(dir, name string, data []byte, check func() error) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create canonical metadata directory: %w", err)
	}
	dirHandle, err := openCanonicalMetadataDirWindows(dir)
	if err != nil {
		return err
	}
	defer func() { _ = windows.CloseHandle(dirHandle) }()
	if err := rejectUnsafeCanonicalMetadataDestinationWindows(dirHandle, name); err != nil {
		return err
	}
	tmpName, err := canonicalMetadataTempNameWindows(name)
	if err != nil {
		return err
	}
	tmpHandle, err := openCanonicalMetadataRelativeWindows(dirHandle, tmpName, os.O_CREATE|os.O_EXCL|os.O_RDWR, true)
	if err != nil {
		return fmt.Errorf("create metadata transaction temp: %w", err)
	}
	tmp := os.NewFile(uintptr(tmpHandle), tmpName)
	defer func() {
		_ = tmp.Close()
		_ = deleteCanonicalMetadataRelativeWindows(dirHandle, tmpName)
	}()
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("write metadata transaction temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync metadata transaction temp: %w", err)
	}
	var tempInfo windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(tmp.Fd()), &tempInfo); err != nil {
		return fmt.Errorf("inspect metadata transaction temp: %w", err)
	}
	if canonicalMetadataBeforeReplace != nil {
		if err := canonicalMetadataBeforeReplace(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	if err := check(); err != nil {
		return err
	}
	if err := rejectUnsafeCanonicalMetadataDestinationWindows(dirHandle, name); err != nil {
		return err
	}
	if err := renameCanonicalMetadataRelativeWindows(windows.Handle(tmp.Fd()), dirHandle, name); err != nil {
		return fmt.Errorf("atomically replace canonical metadata: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync renamed canonical metadata: %w", err)
	}
	if err := verifyPublishedCanonicalMetadataWindows(dirHandle, name, data, tempInfo); err != nil {
		return err
	}
	return check()
}

func openCanonicalMetadataDirWindows(path string) (windows.Handle, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	h, err := windows.CreateFile(p, windows.FILE_LIST_DIRECTORY|windows.FILE_TRAVERSE|canonicalMetadataFileAddFileAccess|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return 0, fmt.Errorf("bind canonical metadata directory: %w", err)
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &info); err != nil || info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 || info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 {
		_ = windows.CloseHandle(h)
		return 0, errors.Join(fmt.Errorf("canonical metadata path is not a no-follow directory"), err)
	}
	return h, nil
}

func openCanonicalMetadataRelativeWindows(parent windows.Handle, name string, flags int, deleteAccess bool) (windows.Handle, error) {
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return 0, err
	}
	attrs := &windows.OBJECT_ATTRIBUTES{RootDirectory: parent, ObjectName: objectName, Attributes: windows.OBJ_CASE_INSENSITIVE | windows.OBJ_DONT_REPARSE}
	attrs.Length = uint32(unsafe.Sizeof(*attrs))
	access := uint32(windows.FILE_GENERIC_READ | windows.SYNCHRONIZE)
	if flags&(os.O_WRONLY|os.O_RDWR) != 0 {
		access = windows.FILE_GENERIC_WRITE | windows.FILE_READ_ATTRIBUTES | windows.SYNCHRONIZE
		if flags&os.O_RDWR != 0 {
			access |= windows.FILE_GENERIC_READ
		}
	}
	if deleteAccess {
		access |= windows.DELETE
	}
	disposition := uint32(windows.FILE_OPEN)
	if flags&os.O_CREATE != 0 && flags&os.O_EXCL != 0 {
		disposition = windows.FILE_CREATE
	}
	var h windows.Handle
	var iosb windows.IO_STATUS_BLOCK
	allocation := int64(0)
	options := uint32(windows.FILE_NON_DIRECTORY_FILE | windows.FILE_OPEN_REPARSE_POINT | windows.FILE_SYNCHRONOUS_IO_NONALERT)
	if flags&(os.O_WRONLY|os.O_RDWR) != 0 {
		options |= windows.FILE_WRITE_THROUGH
	}
	err = windows.NtCreateFile(&h, access, attrs, &iosb, &allocation, 0, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, disposition, options, 0, 0)
	if err != nil {
		return 0, err
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &info); err != nil || info.FileAttributes&(windows.FILE_ATTRIBUTE_REPARSE_POINT|windows.FILE_ATTRIBUTE_DIRECTORY) != 0 {
		_ = windows.CloseHandle(h)
		return 0, errors.Join(fmt.Errorf("refusing non-regular canonical metadata destination"), err)
	}
	return h, nil
}

func rejectUnsafeCanonicalMetadataDestinationWindows(parent windows.Handle, name string) error {
	h, err := openCanonicalMetadataRelativeWindows(parent, name, os.O_RDONLY, false)
	if errors.Is(err, windows.ERROR_FILE_NOT_FOUND) || errors.Is(err, windows.ERROR_PATH_NOT_FOUND) ||
		errors.Is(err, windows.STATUS_NO_SUCH_FILE) || errors.Is(err, windows.STATUS_OBJECT_NAME_NOT_FOUND) ||
		errors.Is(err, windows.STATUS_OBJECT_PATH_NOT_FOUND) {
		return nil
	}
	if err != nil {
		return err
	}
	return windows.CloseHandle(h)
}

type canonicalMetadataRenameInformation struct {
	ReplaceIfExists uint32
	RootDirectory   windows.Handle
	FileNameLength  uint32
	FileName        [1]uint16
}

func renameCanonicalMetadataRelativeWindows(file, parent windows.Handle, name string) error {
	encoded, err := windows.UTF16FromString(name)
	if err != nil {
		return err
	}
	nameBytes := (len(encoded) - 1) * 2
	var header canonicalMetadataRenameInformation
	buffer := make([]byte, int(unsafe.Offsetof(header.FileName))+nameBytes)
	info := (*canonicalMetadataRenameInformation)(unsafe.Pointer(&buffer[0]))
	info.ReplaceIfExists = windows.FILE_RENAME_REPLACE_IF_EXISTS | windows.FILE_RENAME_POSIX_SEMANTICS
	info.RootDirectory = parent
	info.FileNameLength = uint32(nameBytes)
	copy((*[windows.MAX_LONG_PATH]uint16)(unsafe.Pointer(&info.FileName[0]))[:nameBytes/2:nameBytes/2], encoded[:len(encoded)-1])
	var iosb windows.IO_STATUS_BLOCK
	return windows.NtSetInformationFile(file, &iosb, &buffer[0], uint32(len(buffer)), windows.FileRenameInformation)
}

func deleteCanonicalMetadataRelativeWindows(parent windows.Handle, name string) error {
	h, err := openCanonicalMetadataRelativeWindows(parent, name, os.O_RDWR, true)
	if err != nil {
		return err
	}
	defer func() { _ = windows.CloseHandle(h) }()
	info := struct{ DeleteFile byte }{DeleteFile: 1}
	var iosb windows.IO_STATUS_BLOCK
	return windows.NtSetInformationFile(h, &iosb, (*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)), windows.FileDispositionInformation)
}

func verifyPublishedCanonicalMetadataWindows(parent windows.Handle, name string, want []byte, temp windows.ByHandleFileInformation) error {
	h, err := openCanonicalMetadataRelativeWindows(parent, name, os.O_RDONLY, false)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(h), name)
	defer func() { _ = f.Close() }()
	var named windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &named); err != nil {
		return err
	}
	if named.VolumeSerialNumber != temp.VolumeSerialNumber || named.FileIndexHigh != temp.FileIndexHigh || named.FileIndexLow != temp.FileIndexLow {
		return fmt.Errorf("published canonical metadata does not name the committed regular file")
	}
	got, err := io.ReadAll(io.LimitReader(f, int64(len(want))+1))
	if err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("published canonical metadata content does not match committed payload")
	}
	return nil
}

func canonicalMetadataTempNameWindows(name string) (string, error) {
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return "." + name + ".txn-" + hex.EncodeToString(nonce[:]), nil
}
