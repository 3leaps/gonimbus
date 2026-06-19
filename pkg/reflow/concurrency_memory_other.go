//go:build !linux

package reflow

func defaultPlatformMemoryLimitBytes() (int64, string, error) {
	return 0, "", nil
}
