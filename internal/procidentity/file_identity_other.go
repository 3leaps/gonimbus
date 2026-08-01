//go:build !unix

package procidentity

// FileIdentityRequired is false when the platform has no stable path file ID API.
func FileIdentityRequired() bool { return false }

// FileDevIno is unavailable on non-unix platforms.
func FileDevIno(path string) (dev, ino uint64) {
	_ = path
	return 0, 0
}
