//go:build windows

package file

import "os"

func openReadNoFollow(path string) (*os.File, error) {
	return os.Open(path)
}
