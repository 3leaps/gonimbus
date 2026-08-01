package procidentity

// Liveness is a four-way observation of a recorded process instance.
// All platforms share this contract (E-R7 / D-R7).
type Liveness int

const (
	// LiveMatching: live process matches the recorded birth identity.
	LiveMatching Liveness = iota
	// LiveMismatched: a live PID exists but birth identity does not match
	// (original instance is gone via reuse). This is identity-loss, not
	// death-side authority for pre-bind takeover (entarch E-R7-02).
	LiveMismatched
	// Gone: instance is proven dead (ESRCH, zombie/terminal, pidfd ESRCH, exit code).
	Gone
	// Indeterminate: observation failed (permission, unknown); never death.
	Indeterminate
)

// Classify reports liveness of the recorded instance. Implemented per-platform.
func Classify(expected Identity) Liveness {
	return classify(expected)
}
