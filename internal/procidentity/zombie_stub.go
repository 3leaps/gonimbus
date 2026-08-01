//go:build unix && !darwin

package procidentity

func darwinIsZombie(pid int) (zombie bool, ok bool) {
	_ = pid
	return false, false
}
