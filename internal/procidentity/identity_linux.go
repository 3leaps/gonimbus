//go:build linux

package procidentity

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func observe(pid int) Identity {
	// Single /proc read so ticks + display ms cannot mix across PID reuse (D-R11-02).
	statPath := filepath.Join("/proc", strconv.Itoa(pid), "stat")
	raw, err := os.ReadFile(statPath) // #nosec G304
	if err != nil {
		return Identity{PID: pid, UnavailableReason: err.Error()}
	}
	endOfComm := strings.LastIndex(string(raw), ")")
	if endOfComm < 0 {
		return Identity{PID: pid, UnavailableReason: "parse process stat: missing command field"}
	}
	fields := strings.Fields(string(raw)[endOfComm+1:])
	if len(fields) < 20 {
		return Identity{PID: pid, UnavailableReason: "parse process stat: too few fields"}
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return Identity{PID: pid, UnavailableReason: err.Error()}
	}
	const clkTck uint64 = 100
	bootSec, err := linuxBootTimeSec()
	if err != nil {
		return Identity{PID: pid, UnavailableReason: err.Error()}
	}
	startMS := (bootSec+ticks/clkTck)*1000 + (ticks%clkTck)*1000/clkTck
	bootID, err := readBootID()
	if err != nil || strings.TrimSpace(bootID) == "" {
		return Identity{PID: pid, UnavailableReason: "linux boot_id required for v1 token"}
	}
	return Identity{
		PID:             pid,
		StartTimeUnixMS: startMS,
		BootID:          bootID,
		TokenVersion:    TokenVersionV1,
		StartTicks:      ticks,
		Proven:          true,
	}
}

func linuxStartTimeUnixMS(pid int) (uint64, error) {
	statPath := filepath.Join("/proc", strconv.Itoa(pid), "stat")
	raw, err := os.ReadFile(statPath) // #nosec G304 -- path is /proc/<numeric-pid>/stat
	if err != nil {
		return 0, fmt.Errorf("read process stat: %w", err)
	}
	endOfComm := strings.LastIndex(string(raw), ")")
	if endOfComm < 0 {
		return 0, fmt.Errorf("parse process stat: missing command field")
	}
	// Fields after comm: state(1) ... starttime is field 20 in that slice
	// (overall field 22 of proc_pid_stat).
	fields := strings.Fields(string(raw)[endOfComm+1:])
	if len(fields) < 20 {
		return 0, fmt.Errorf("parse process stat: too few fields")
	}
	startTicks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse process starttime: %w", err)
	}
	// USER_HZ is almost always 100 on Linux. golang.org/x/sys/unix does not
	// expose Sysconf(SC_CLK_TCK) for linux GOOS builds, so do not call it.
	const clkTck uint64 = 100
	bootSec, err := linuxBootTimeSec()
	if err != nil {
		return 0, err
	}
	startSec := bootSec + startTicks/clkTck
	// Sub-second remainder from leftover ticks (display only).
	remMS := (startTicks % clkTck) * 1000 / clkTck
	return startSec*1000 + remMS, nil
}

// linuxStartTicks returns raw /proc starttime ticks (field 22).
func linuxStartTicks(pid int) (uint64, error) {
	statPath := filepath.Join("/proc", strconv.Itoa(pid), "stat")
	raw, err := os.ReadFile(statPath) // #nosec G304
	if err != nil {
		return 0, err
	}
	endOfComm := strings.LastIndex(string(raw), ")")
	if endOfComm < 0 {
		return 0, fmt.Errorf("parse process stat: missing command field")
	}
	fields := strings.Fields(string(raw)[endOfComm+1:])
	if len(fields) < 20 {
		return 0, fmt.Errorf("parse process stat: too few fields")
	}
	return strconv.ParseUint(fields[19], 10, 64)
}

func linuxBootTimeSec() (uint64, error) {
	raw, err := os.ReadFile("/proc/stat") // #nosec G304 -- fixed kernel path
	if err != nil {
		return 0, fmt.Errorf("read boot time: %w", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "btime ") {
			continue
		}
		sec, err := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "btime ")), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse boot time: %w", err)
		}
		return sec, nil
	}
	return 0, fmt.Errorf("boot time not found")
}

func readBootID() (string, error) {
	raw, err := os.ReadFile("/proc/sys/kernel/random/boot_id") // #nosec G304 -- fixed kernel path
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(raw)), nil
}
