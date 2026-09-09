//go:build windows

package indexreader

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

type acquiredFileRenameInformation struct {
	ReplaceIfExists uint32
	RootDirectory   windows.Handle
	FileNameLength  uint32
	FileName        [1]uint16
}

func renameDirectoryNoReplace(parent, source *os.File, oldName, newName string) error {
	if parent == nil || source == nil {
		return fmt.Errorf("bound parent and source directory handles are required")
	}
	objectName, err := windows.NewNTUnicodeString(oldName)
	if err != nil {
		return err
	}
	attrs := &windows.OBJECT_ATTRIBUTES{
		RootDirectory: windows.Handle(parent.Fd()),
		ObjectName:    objectName,
		Attributes:    windows.OBJ_CASE_INSENSITIVE | windows.OBJ_DONT_REPARSE,
	}
	attrs.Length = uint32(unsafe.Sizeof(*attrs))
	var renameHandle windows.Handle
	var openStatus windows.IO_STATUS_BLOCK
	allocation := int64(0)
	if err := windows.NtCreateFile(
		&renameHandle,
		windows.DELETE|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE,
		attrs,
		&openStatus,
		&allocation,
		windows.FILE_ATTRIBUTE_NORMAL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.FILE_OPEN,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		0,
		0,
	); err != nil {
		return err
	}
	renameFile := os.NewFile(uintptr(renameHandle), oldName)
	if renameFile == nil {
		_ = windows.CloseHandle(renameHandle)
		return os.ErrInvalid
	}
	defer func() { _ = renameFile.Close() }()
	expected, err := source.Stat()
	if err != nil {
		return err
	}
	bound, err := renameFile.Stat()
	if err != nil || !bound.IsDir() || !os.SameFile(expected, bound) {
		return fmt.Errorf("rename source directory binding changed")
	}

	name, err := windows.UTF16FromString(newName)
	if err != nil {
		return err
	}
	nameBytes := (len(name) - 1) * 2
	var layout acquiredFileRenameInformation
	bufferSize := int(unsafe.Offsetof(layout.FileName)) + nameBytes
	buffer := make([]byte, bufferSize)
	// #nosec G103 -- buffer is sized from this fixed header plus the bounded UTF-16 name.
	info := (*acquiredFileRenameInformation)(unsafe.Pointer(&buffer[0]))
	info.RootDirectory = windows.Handle(parent.Fd())
	info.FileNameLength = uint32(nameBytes) // #nosec G115 -- target names are bounded to one filesystem component
	// #nosec G103 -- slice capacity is the validated encoded component length.
	copy((*[windows.MAX_LONG_PATH]uint16)(unsafe.Pointer(&info.FileName[0]))[:nameBytes/2:nameBytes/2], name)
	var status windows.IO_STATUS_BLOCK
	// ReplaceIfExists remains zero: the final name is never overlaid.
	return windows.NtSetInformationFile(
		renameHandle,
		&status,
		&buffer[0],
		uint32(bufferSize), // #nosec G115 -- bufferSize is bounded by MAX_LONG_PATH
		windows.FileRenameInformation,
	)
}

func syncAcquiredDirectory(_ *os.File) error { return nil }
