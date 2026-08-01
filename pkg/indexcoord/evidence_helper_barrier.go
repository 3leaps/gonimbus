package indexcoord

// recoveryEvidencePark is an optional test-only hook installed by the E4
// re-exec helper process (see stalled_recoverer_crash_evidence_test.go).
// It is always nil in library/CLI/server builds: ordinary RecoverManagedStalled
// paths never write markers or park based on environment variables.
//
// Do not set this from production code. Env vars alone must not activate
// evidence behavior (D-R12-04 / secrev: environment is runtime input, not a
// safety boundary).
var recoveryEvidencePark func(name string)

// evidencePark invokes the test-only hook when installed; otherwise a no-op.
func evidencePark(name string) {
	if f := recoveryEvidencePark; f != nil {
		f(name)
	}
}
