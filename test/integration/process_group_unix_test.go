//go:build !windows

package integration

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/ownedproc"
)

// processEntry is one live process: its group, its id, and its argument vector.
type processEntry struct {
	pgid int
	pid  int
	args []string
}

// managedChildGroup is a process group this test owns, held in existence by an
// anchor process the test starts and keeps alive.
//
// A process group id is reserved only while the group has a member, so a group
// containing just the process under test carries the same reuse hazard as that
// process's own id. The anchor keeps the group non-empty from before the launcher
// starts: it is live when the group is signalled, and stays unreaped — so it
// still holds the id — through the confirmation that follows.
type managedChildGroup struct {
	anchor *ownedproc.Child
	pgid   int
}

// newManagedChildGroup starts the anchor and returns the group every managed
// launcher for this test should join.
func newManagedChildGroup(t *testing.T) *managedChildGroup {
	t.Helper()
	cmd := exec.Command("sleep", "120")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	anchor, err := ownedproc.Start(cmd)
	if err != nil {
		t.Fatalf("start process-group anchor: %v", err)
	}
	group := &managedChildGroup{anchor: anchor, pgid: anchor.PID()}
	// Registered before any reap cleanup, so it runs after them, and it signals the
	// whole group rather than just the anchor: this is the final safety net for a
	// test that returns before reaching an explicit reap. The anchor's sole wait
	// owner is what this joins; nothing here waits on a process itself.
	t.Cleanup(func() {
		_ = group.signal()
		select {
		case <-anchor.Done():
		case <-time.After(15 * time.Second):
			t.Errorf("process-group anchor did not exit after the group was signalled")
		}
	})
	return group
}

// launcher builds a command that joins this group, so every process it goes on to
// create — including a detached managed grandchild — is born inside it.
func (g *managedChildGroup) launcher(binary string, args ...string) *exec.Cmd {
	cmd := exec.Command(binary, args...) // #nosec G204 -- test-built binary and literal args
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: g.pgid}
	return cmd
}

// alive reports whether the anchor still holds the group id. Without it the
// number is no longer owned and must not be signalled.
func (g *managedChildGroup) alive() bool {
	if g == nil || g.anchor == nil {
		return false
	}
	// Signal 0 probes without delivering, and succeeds for an unreaped process,
	// which is while the group id is still reserved.
	return g.anchor.Signal(syscall.Signal(0)) == nil
}

// signal kills every member of the group, the anchor included. Callers signal
// only while the anchor is alive, so the id still names this group.
func (g *managedChildGroup) signal() error {
	if err := syscall.Kill(-g.pgid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

// setOwnProcessGroup puts cmd in a new group of its own, for processes that must
// stay outside any group this test signals.
func setOwnProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// listProcesses enumerates live processes with their group and command line.
//
// These controls prove a negative — that an unrelated process was left alone —
// so the enumeration has to work wherever the suite runs. A minimal container
// carries no ps, and skipping there would retire the evidence in exactly the
// place CI runs it. procfs is therefore preferred, with ps as the fallback for
// systems that have no /proc.
func listProcesses() ([]processEntry, error) {
	if entries, err := listProcessesProcfs("/proc"); err == nil {
		return entries, nil
	}
	return listProcessesPS()
}

var errNoProcfsEntries = errors.New("no processes found under procfs")

// listProcessesProcfs reads the process table from a procfs mounted at root.
// The root is a parameter so the parsing can be exercised against a fixture on
// a host that has no procfs of its own.
func listProcessesProcfs(root string) ([]processEntry, error) {
	dirents, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var entries []processEntry
	for _, dirent := range dirents {
		pid, err := strconv.Atoi(dirent.Name())
		if err != nil {
			continue
		}
		stat, err := os.ReadFile(filepath.Join(root, dirent.Name(), "stat"))
		if err != nil {
			continue // exited between the listing and the read
		}
		// comm is parenthesised and may itself contain spaces and parentheses, so
		// the remaining fields are counted from after its final ')'.
		endOfComm := strings.LastIndex(string(stat), ")")
		if endOfComm < 0 {
			continue
		}
		fields := strings.Fields(string(stat)[endOfComm+1:])
		if len(fields) < 3 { // state, ppid, pgrp
			continue
		}
		pgid, err := strconv.Atoi(fields[2])
		if err != nil {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, dirent.Name(), "cmdline"))
		if err != nil {
			continue
		}
		args := strings.FieldsFunc(string(raw), func(r rune) bool { return r == 0 })
		entries = append(entries, processEntry{pgid: pgid, pid: pid, args: args})
	}
	if len(entries) == 0 {
		return nil, errNoProcfsEntries
	}
	return entries, nil
}

// hasTerminated reports whether pid has stopped executing.
//
// A signal-zero probe is not that question. It succeeds for a process that has
// exited but not yet been reaped, and whether such a zombie lingers is a
// property of the environment rather than of the process under test: an
// orphan's reaper is pid 1, which reaps promptly on a developer machine and may
// never reap inside a minimal container. Termination is therefore read as
// "gone, or exited and awaiting a reap", which is what the callers actually
// assert.
func hasTerminated(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return true
	}
	if process.Signal(syscall.Signal(0)) != nil {
		return true // gone
	}
	state, ok := processState(pid)
	if !ok {
		return false // still addressable and no state to say otherwise
	}
	return state == 'Z'
}

// processState returns the single-letter state of pid, and whether it could be
// read at all.
func processState(pid int) (byte, bool) {
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err == nil {
		endOfComm := strings.LastIndex(string(stat), ")")
		if endOfComm < 0 {
			return 0, false
		}
		fields := strings.Fields(string(stat)[endOfComm+1:])
		if len(fields) == 0 || fields[0] == "" {
			return 0, false
		}
		return fields[0][0], true
	}
	out, err := exec.Command("ps", "-o", "state=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, false
	}
	trimmed := strings.TrimSpace(string(out))
	if trimmed == "" {
		return 0, false
	}
	return trimmed[0], true
}

// requireTerminatedWithin waits for pid to stop executing, and fails if it is
// still running when the bound elapses. The bound is what makes this evidence
// rather than a coin flip: a signal is delivered asynchronously, so an immediate
// check can observe a process that is about to die.
func requireTerminatedWithin(t *testing.T, pid int, bound time.Duration) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for {
		if hasTerminated(pid) {
			return
		}
		if time.Now().After(deadline) {
			state, ok := processState(pid)
			if ok {
				t.Fatalf("managed child pid=%d survived the reap, state=%c", pid, state)
			}
			t.Fatalf("managed child pid=%d survived the reap", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// listProcessesPS reads the process table through ps, for systems without procfs.
func listProcessesPS() ([]processEntry, error) {
	out, err := exec.Command("ps", "-A", "-o", "pgid=,pid=,args=").Output()
	if err != nil {
		return nil, err
	}
	var entries []processEntry
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		pgid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		pid, err := strconv.Atoi(fields[1])
		if err != nil {
			continue
		}
		entries = append(entries, processEntry{pgid: pgid, pid: pid, args: fields[2:]})
	}
	return entries, nil
}
