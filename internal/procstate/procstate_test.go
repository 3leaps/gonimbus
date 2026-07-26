package procstate

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// writeStat lays down a single process entry under a fixture procfs root, in the
// real layout: <root>/<pid>/stat, whose third field is the state.
func writeStat(t *testing.T, root string, pid int, comm string, state byte) {
	t.Helper()
	dir := filepath.Join(root, strconv.Itoa(pid))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	line := strconv.Itoa(pid) + " (" + comm + ") " + string(state) + " 1 1 0 -1 4194304"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stat"), []byte(line), 0o600))
}

func TestStateReadsTheStateField(t *testing.T) {
	root := t.TempDir()
	writeStat(t, root, 42, "gonimbus", 'S')

	state, ok := State(root, 42)
	require.True(t, ok, "a readable stat file must yield a state")
	require.Equal(t, byte('S'), state)
}

// A process chooses its own command name and procfs does not escape it, so a
// name can be crafted to look like the fields that follow it.
func TestStateHandlesCommContainingSpacesAndParens(t *testing.T) {
	root := t.TempDir()
	writeStat(t, root, 43, "we i(rd) name) Z 9 9", 'R')

	state, ok := State(root, 43)
	require.True(t, ok)
	require.Equal(t, byte('R'), state, "the state must be read from after the final ')' in comm")
}

func TestIsTerminalClassifiesTheExitedStates(t *testing.T) {
	for _, tc := range []struct {
		state    byte
		terminal bool
		meaning  string
	}{
		{'Z', true, "awaiting collection by its parent"},
		{'X', true, "dead"},
		{'x', true, "dead, as older kernels spelled it"},
		{'R', false, "running"},
		{'S', false, "sleeping"},
		{'D', false, "uninterruptible sleep"},
		{'I', false, "idle"},
		{'T', false, "stopped, which is not finished"},
		{'t', false, "stopped by a tracer, which is not finished"},
		{'W', false, "paging or waking, depending on the kernel"},
		{'K', false, "wakekill, as some kernels spelled it"},
		{'P', false, "parked, as some kernels spelled it"},
	} {
		require.Equal(t, tc.terminal, IsTerminal(tc.state),
			"state %c is %s", tc.state, tc.meaning)
	}
}

// The default is part of the contract, not a fallthrough: a letter this package
// does not know is not exited. That direction can only delay noticing an exit,
// where the opposite would declare a live process dead on a letter nobody has
// classified. A kernel that adds a state is the case this protects.
func TestIsTerminalTreatsAnUnrecognizedStateAsNotTerminal(t *testing.T) {
	require.False(t, IsTerminal('?'), "an unrecognized state must not be read as exited")
}

func TestStateReportsUnreadableEntry(t *testing.T) {
	_, ok := State(t.TempDir(), 44)
	require.False(t, ok, "a missing stat file must report the state as unavailable")
}

func TestStateReportsMalformedEntry(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "45")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stat"), []byte("45 no-close-paren"), 0o600))

	_, ok := State(root, 45)
	require.False(t, ok, "a stat line with no ')' must report the state as unavailable")
}

func TestStateRejectsNonPositivePIDs(t *testing.T) {
	root := t.TempDir()
	_, ok := State(root, 0)
	require.False(t, ok)
	_, ok = State(root, -1)
	require.False(t, ok)
}

// The fixture tests prove the parsing against a procfs this package wrote. This
// one proves the format assumption against the kernel's own, which is the part
// a development machine on a procfs-less platform cannot see.
func TestStateReadsRealProcfs(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires a real procfs")
	}
	state, ok := State("/proc", os.Getpid())
	require.True(t, ok, "this process must be readable through the real /proc")
	require.NotEqual(t, byte(Zombie), state, "a process asking the question is not a zombie")
}
