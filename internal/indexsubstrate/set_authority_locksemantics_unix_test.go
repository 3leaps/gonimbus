//go:build !windows

package indexsubstrate

// mandatoryFileLocks reports whether this platform's file locks block other
// processes from reading, writing, or rebinding the locked range. Unix flock is
// advisory: a held lock gates nothing but another flock attempt, so the holder
// document stays readable and the pathname stays replaceable while held.
const mandatoryFileLocks = false

// isLockedRangeError is always false here: advisory locking produces no such
// refusal, so any error a fixture hits is a real failure.
func isLockedRangeError(error) bool { return false }
