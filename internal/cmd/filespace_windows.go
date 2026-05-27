//go:build windows

package cmd

import (
	"fmt"
	"math"
	"strconv"

	"golang.org/x/sys/windows"
)

func availableBytes(path string) (int64, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var freeBytes uint64
	if err := windows.GetDiskFreeSpaceEx(pathPtr, &freeBytes, nil, nil); err != nil {
		return 0, err
	}
	if freeBytes > uint64(math.MaxInt64) {
		return 0, fmt.Errorf("available filesystem bytes exceed int64 range")
	}
	return strconv.ParseInt(strconv.FormatUint(freeBytes, 10), 10, 64)
}
