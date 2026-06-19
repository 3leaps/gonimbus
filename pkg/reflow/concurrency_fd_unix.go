//go:build !windows

package reflow

import "golang.org/x/sys/unix"

func defaultFDSoftLimit() (int64, error) {
	var lim unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &lim); err != nil {
		return int64(resourceFDHeadroom + resourceMinCap), err
	}
	if lim.Cur > uint64(resourceMaxCap) {
		return int64(resourceMaxCap), nil
	}
	return int64(lim.Cur), nil
}
