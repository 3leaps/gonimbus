//go:build !unix && !windows

package procidentity

import "fmt"

func openTarget(expected Identity) (targetImpl, error) {
	return nil, fmt.Errorf("identity-bound process signalling unsupported on this platform")
}
