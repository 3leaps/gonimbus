//go:build windows

package reflow

import (
	"math"
	"unsafe"

	"golang.org/x/sys/windows"
)

func defaultPlatformMemoryLimitBytes() (int64, string, error) {
	return 0, "", nil
}

// memoryStatusEx mirrors the Win32 MEMORYSTATUSEX layout consumed by
// GlobalMemoryStatusEx.
type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

var procGlobalMemoryStatusEx = windows.NewLazySystemDLL("kernel32.dll").NewProc("GlobalMemoryStatusEx")

func defaultPhysicalMemoryBytes() (int64, error) {
	var status memoryStatusEx
	status.Length = uint32(unsafe.Sizeof(status))
	ret, _, callErr := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&status)))
	if ret == 0 {
		return 0, callErr
	}
	if status.TotalPhys == 0 || status.TotalPhys > uint64(math.MaxInt64) {
		return 0, nil
	}
	return int64(status.TotalPhys), nil
}
