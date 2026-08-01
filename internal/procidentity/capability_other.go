//go:build !unix && !windows

package procidentity

import "fmt"

func checkDestructiveRecoverySupported() error {
	return fmt.Errorf("%w: destructive recovery unsupported on this platform", ErrUnsupportedPlatform)
}
