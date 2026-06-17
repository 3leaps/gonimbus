//go:build windows

package cmd

func defaultReflowFDSoftLimit() (int64, error) {
	return int64(reflowResourceFDHeadroom + reflowResourceMinCap), nil
}
