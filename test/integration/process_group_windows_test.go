//go:build windows

package integration

import (
	"errors"
	"os/exec"
	"testing"
	"time"
)

// The managed-child process tests do not run on Windows: process groups and
// command-line enumeration both need a different mechanism there, and every test
// that uses them skips at runtime. These stubs keep the package compiling so the
// rest of the suite still builds and runs natively.

type processEntry struct {
	pgid int
	pid  int
	args []string
}

type managedChildGroup struct{ pgid int }

func newManagedChildGroup(*testing.T) *managedChildGroup { return &managedChildGroup{} }

func (g *managedChildGroup) launcher(binary string, args ...string) *exec.Cmd {
	return exec.Command(binary, args...) // #nosec G204 -- test-built binary and literal args
}

func (g *managedChildGroup) alive() bool { return false }

func (g *managedChildGroup) signal() error {
	return errors.New("process-group reaping is not implemented on windows")
}

func setOwnProcessGroup(*exec.Cmd) {}

func listProcesses() ([]processEntry, error) {
	return nil, errors.New("process enumeration is not implemented on windows")
}

func requireTerminatedWithin(t *testing.T, _ int, _ time.Duration) {
	t.Helper()
	t.Skip("managed-child reaping is not implemented on windows")
}
