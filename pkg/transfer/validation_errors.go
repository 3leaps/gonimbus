package transfer

import "fmt"

// SizeMismatchError indicates the object size changed between enumeration
// (list/index) and content retrieval.
//
// This is best-effort validation (validate=size) intended to fail early before
// deeper pipeline work (buffering/spooling/retries).
//
// It does not eliminate TOCTOU races.
type SizeMismatchError struct {
	Key      string
	Expected int64
	Got      int64
}

func (e *SizeMismatchError) Error() string {
	return fmt.Sprintf("source size mismatch for %s: expected=%d got=%d", e.Key, e.Expected, e.Got)
}
