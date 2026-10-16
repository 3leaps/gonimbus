//go:build !unix && !windows

package procidentity

func openTarget(expected Identity) (targetImpl, error) {
	return nil, errSignallingUnsupported
}
