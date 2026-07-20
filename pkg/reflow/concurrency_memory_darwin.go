//go:build darwin

package reflow

import (
	"math"

	"golang.org/x/sys/unix"
)

func defaultPlatformMemoryLimitBytes() (int64, string, error) {
	return 0, "", nil
}

func defaultPhysicalMemoryBytes() (int64, error) {
	size, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0, err
	}
	if size == 0 || size > uint64(math.MaxInt64) {
		return 0, nil
	}
	return int64(size), nil
}
