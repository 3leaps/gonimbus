//go:build !darwin

package reflowthroughput

import "errors"

// errAPFSUnsupported is returned as a variable rather than built inline so the
// shared caller keeps its error checks; on these hosts the result never changes,
// and a static checker would otherwise call them always true.
var errAPFSUnsupported = errors.New("formal APFS medium check unsupported on this OS")

// requireAPFSRoot fails on non-darwin hosts: formal APFS medium is macOS-only.
func requireAPFSRoot(root string) error {
	_ = root
	return errAPFSUnsupported
}
