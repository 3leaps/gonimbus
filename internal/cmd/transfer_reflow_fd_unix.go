//go:build !windows

package cmd

import "golang.org/x/sys/unix"

func defaultReflowFDSoftLimit() (int64, error) {
	var lim unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &lim); err != nil {
		return int64(reflowResourceFDHeadroom + reflowResourceMinCap), err
	}
	if lim.Cur > uint64(reflowResourceMaxCap) {
		return int64(reflowResourceMaxCap), nil
	}
	return int64(lim.Cur), nil
}
