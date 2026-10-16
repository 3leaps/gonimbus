//go:build unix && !linux

package procidentity

import (
	"fmt"
	"runtime"
)

// errNoInstanceStableSignal is a variable rather than an inline error so callers
// of these always-failing stubs keep their error checks: on these platforms the
// result never changes, and a static checker would otherwise call the
// cross-platform checks always true.
var errNoInstanceStableSignal = fmt.Errorf("%w: no instance-stable signaling primitive on %s", ErrUnsupportedPlatform, runtime.GOOS)

func checkDestructiveRecoverySupported() error {
	return errNoInstanceStableSignal
}
