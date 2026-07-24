package jobregistry

// IsProcessAlive reports whether a process with the given PID is currently
// alive. It is the exported liveness signal used by lease-attribution joins to
// tell a live holder apart from dead-holder residue.
//
// Liveness is attribution only, never authority: a live PID does not prove a
// lock is held, and a dead or missing PID must never be read as "unheld". Only
// a non-mutating lock probe decides the held/unheld verdict.
func IsProcessAlive(pid int) bool {
	return isProcessAlive(pid)
}
