//go:build unix && !linux

package procidentity

// openTarget refuses destructive bind on non-Linux unix platforms (Darwin, …).
// Panel quorum: Darwin revalidation is observation-only for stalled-recovery; it must not
// reach kill(pid). Plan/diagnose may still Observe/Classify without Bind.
func openTarget(expected Identity) (targetImpl, error) {
	_ = expected
	return nil, errNoInstanceStableSignal
}
