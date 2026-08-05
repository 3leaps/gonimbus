//go:build !darwin

package reflowthroughput

import "fmt"

// requireAPFSRoot fails on non-darwin hosts: formal APFS medium is macOS-only.
func requireAPFSRoot(root string) error {
	return fmt.Errorf("formal APFS medium check unsupported on this OS (root=%s)", root)
}
