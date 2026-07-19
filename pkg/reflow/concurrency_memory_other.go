//go:build !linux && !darwin

package reflow

func defaultPlatformMemoryLimitBytes() (int64, string, error) {
	return 0, "", nil
}

func defaultPhysicalMemoryBytes() (int64, error) {
	return 0, nil
}
