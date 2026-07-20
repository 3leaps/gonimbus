//go:build !linux && !darwin && !windows

package reflow

func defaultPlatformMemoryLimitBytes() (int64, string, error) {
	return 0, "", nil
}

func defaultPhysicalMemoryBytes() (int64, error) {
	return 0, nil
}
