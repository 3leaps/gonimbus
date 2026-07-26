package jobregistry

// IsProcessAlive reports whether a process with the given PID is currently
// alive. It is the exported liveness signal used by lease-attribution joins to
// tell a live holder apart from dead-holder residue.
//
// Alive means the process has not exited. It does not mean the process is
// running: one that has been stopped has not exited, and is alive. A process
// that has exited is reported dead even while its PID is still addressable,
// which it remains until the exit status is collected. Where the platform cannot
// tell the two apart, the PID being addressable is the answer.
//
// Liveness never decides lock authority: a live PID does not prove a lock is
// held, and a dead or missing PID must never be read as "unheld". Only a
// non-mutating lock probe decides the held/unheld verdict.
//
// That limit is about lock verdicts alone. Within the registry liveness does
// carry weight: a job claiming to run under a PID that has exited is demoted.
func IsProcessAlive(pid int) bool {
	return isProcessAlive(pid)
}
