// Package procstate reads process state from a procfs.
//
// It exists as its own package so that the callers which need it are not obliged
// to make raw path-based reads inside packages whose own reads are held to
// directory-bound primitives.
//
// The root is a parameter, so what gets read is the caller's choice and this
// package makes no claim about it. The production caller supplies a kernel
// filesystem; the reads here are not bound against an operator-writable tree and
// must not be pointed at one.
//
// The package carries no build constraint, so that a platform without a procfs
// can still build and import it. There the read fails and the state is reported
// unavailable, which is what callers must handle in any case.
package procstate

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// The procfs state letters treated as meaning the process has exited. Field 3 of
// proc_pid_stat(5) defines Z for a process awaiting collection by its parent and
// X for one that is dead; older kernels spelled the latter lowercase x, which is
// accepted here because accepting it costs nothing.
//
// Every other letter, including one this package does not recognize, is treated
// as not exited. That is the conservative direction: it can delay noticing that
// a process has finished, but it cannot declare a live one dead. T and t in
// particular are stopped rather than finished.
const (
	Zombie     = 'Z'
	Dead       = 'X'
	DeadLegacy = 'x'
)

// IsTerminal reports whether state is one of the procfs letters that mean the
// process has exited. Every other letter is not terminal.
func IsTerminal(state byte) bool {
	switch state {
	case Zombie, Dead, DeadLegacy:
		return true
	default:
		return false
	}
}

// State returns the single-letter state of pid as recorded under root, and
// whether it could be read at all. The root is a parameter so the parsing can be
// exercised against a fixture on a host that has no procfs of its own.
func State(root string, pid int) (byte, bool) {
	if pid <= 0 {
		return 0, false
	}
	stat, err := os.ReadFile(filepath.Join(root, strconv.Itoa(pid), "stat")) // #nosec G304 -- procfs path built from a numeric PID under a caller-supplied kernel filesystem root
	if err != nil {
		return 0, false
	}
	// comm is parenthesised and may itself contain spaces and parentheses, and
	// procfs does not escape it. The field that follows comm is the state, a
	// single letter, and every field after that one is numeric — so no field
	// beyond comm can contain a ')', and comm's own closing paren is the last one
	// on the line. Counting from there, rather than splitting the whole line, is
	// what keeps a crafted command name from shifting which field is read.
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
