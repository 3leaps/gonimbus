//go:build !windows

package cmd

import (
	"fmt"
	"math"
	"strconv"
	"syscall"
)

func availableBytes(path string) (int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	blockSize := uint64(stat.Bsize)
	if blockSize != 0 && stat.Bavail > uint64(math.MaxInt64)/blockSize {
		return 0, fmt.Errorf("available filesystem bytes exceed int64 range")
	}
	return strconv.ParseInt(strconv.FormatUint(stat.Bavail*blockSize, 10), 10, 64)
}
