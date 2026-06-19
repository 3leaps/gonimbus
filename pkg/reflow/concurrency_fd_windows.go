//go:build windows

package reflow

func defaultFDSoftLimit() (int64, error) {
	return int64(resourceFDHeadroom + resourceMinCap), nil
}
