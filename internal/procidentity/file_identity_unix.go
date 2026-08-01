//go:build unix

package procidentity

import (
	"os"
	"syscall"
)

// FileIdentityRequired reports whether reclaim receipts must include file IDs.
func FileIdentityRequired() bool { return true }

// FileDevIno returns device and inode for path when available.
func FileDevIno(path string) (dev, ino uint64) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, 0
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st == nil {
		return 0, 0
	}
	return uint64(st.Dev), uint64(st.Ino)
}
