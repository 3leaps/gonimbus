//go:build !unix && !windows

package procidentity

import "fmt"

// Variables rather than inline errors so callers keep their cross-platform error
// checks; see errNoInstanceStableSignal.
var (
	errDestructiveRecoveryUnsupported = fmt.Errorf("%w: destructive recovery unsupported on this platform", ErrUnsupportedPlatform)
	errSignallingUnsupported          = fmt.Errorf("identity-bound process signalling unsupported on this platform")
)

func checkDestructiveRecoverySupported() error {
	return errDestructiveRecoveryUnsupported
}
