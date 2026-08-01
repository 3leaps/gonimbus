//go:build unix && !linux

package procidentity

import (
	"fmt"
	"runtime"
)

func checkDestructiveRecoverySupported() error {
	return fmt.Errorf("%w: no instance-stable signaling primitive on %s", ErrUnsupportedPlatform, runtime.GOOS)
}
