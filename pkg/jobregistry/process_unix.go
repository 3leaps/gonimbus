//go:build unix

package jobregistry

import (
	"os"
	"syscall"
)

func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// Signal zero checks for existence without delivering a signal on Unix.
	return process.Signal(os.Signal(syscall.Signal(0))) == nil
}
