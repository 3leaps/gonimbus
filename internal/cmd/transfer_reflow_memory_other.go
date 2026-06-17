//go:build !linux

package cmd

func defaultReflowPlatformMemoryLimitBytes() (int64, string, error) {
	return 0, "", nil
}
