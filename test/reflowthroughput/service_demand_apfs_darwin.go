//go:build darwin

package reflowthroughput

import (
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

// requireAPFSRoot verifies RootDir is on an APFS filesystem (formal medium gate).
func requireAPFSRoot(root string) error {
	var st unix.Statfs_t
	if err := unix.Statfs(root, &st); err != nil {
		return fmt.Errorf("statfs %s: %w", root, err)
	}
	// Fstypename is a fixed byte array on darwin.
	name := make([]byte, 0, len(st.Fstypename))
	for _, c := range st.Fstypename {
		if c == 0 {
			break
		}
		name = append(name, byte(c))
	}
	fs := strings.ToLower(string(name))
	if fs != "apfs" {
		return fmt.Errorf("root %s filesystem %q is not apfs", root, fs)
	}
	return nil
}
