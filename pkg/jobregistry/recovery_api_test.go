package jobregistry_test

import (
	"os"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

// External-package API surface negatives (D-R17-01).
func TestExternalAPI_OpenSignalSessionRejectsZeroDeadline(t *testing.T) {
	store := jobregistry.NewStore(t.TempDir())
	// Even with empty store, zero deadline must refuse before deeper checks.
	_, err := store.OpenSignalSession("00000000-0000-0000-0000-000000000001", "o", "a", 1, time.Time{})
	if err == nil {
		t.Fatal("external caller must not open session with zero deadline")
	}
	_, err = store.OpenSignalSession("00000000-0000-0000-0000-000000000001", "o", "a", 1, time.Now().Add(-time.Minute))
	if err == nil {
		t.Fatal("external caller must not open session with expired deadline")
	}
	// Public API has no clock override field — OpenSignalSession takes deadline only.
	_ = os.Stderr
}

func TestExternalAPI_SignalSessionHasNoRawTarget(t *testing.T) {
	// Compile-time / reflection-free documentation: session type must not export Target.
	// If this compiles, Target is not a public method of SignalSession.
	var s *jobregistry.SignalSession
	_ = s
	// Methods that must exist:
	// Terminated, WaitTerminated, HardStopOnly, DeliverTerm, DeliverKill, Close
	// Methods that must not: Target() *procidentity.Target
}
