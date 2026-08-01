// Package procidentity observes OS process birth identity for PID-reuse protection.
//
// A PID alone is not durable identity: after exit the number may be reassigned.
// Callers that later signal or attribute a process must re-observe the birth
// token recorded at claim time and refuse on mismatch or when identity cannot
// be proven on the host platform.
package procidentity

import (
	"fmt"
	"strings"
)

// Identity is a platform-defined birth token for one process instance.
//
// TokenVersion 1 uses platform-native fields as authority (Linux start ticks +
// boot ID; Darwin sec/usec; Windows FILETIME). StartTimeUnixMS remains a
// display/legacy convenience and is not sole authority for v1 Match.
type Identity struct {
	PID               int
	StartTimeUnixMS   uint64 // display/legacy; non-authoritative when TokenVersion>=1
	BootID            string
	TokenVersion      int    // 1 = native authority
	StartTicks        uint64 // Linux
	StartSec          int64  // Darwin
	StartUsec         int64  // Darwin
	Filetime          uint64 // Windows FILETIME 100ns
	Proven            bool
	UnavailableReason string
}

// TokenVersionV1 is the versioned native birth-token schema.
const TokenVersionV1 = 1

// Observe returns the current birth identity for pid.
//
// When the platform cannot prove identity, Proven is false and UnavailableReason
// names why. Callers must treat that as indeterminate for any stop authority —
// never as "same process".
func Observe(pid int) Identity {
	if pid <= 0 {
		return Identity{PID: pid, UnavailableReason: "non-positive pid"}
	}
	return observe(pid)
}

// Match reports whether observed is the same process instance as expected.
// Both sides must be proven; a missing birth token never matches.
// TokenVersion 1 compares native fields. Legacy expected (version 0) may match
// via start_ms for plan/heartbeat, but Bind refuses legacy for destructive recovery.
func Match(expected, observed Identity) bool {
	if !expected.Proven || !observed.Proven {
		return false
	}
	if expected.PID != observed.PID || expected.PID <= 0 {
		return false
	}
	if expected.TokenVersion >= TokenVersionV1 {
		if observed.TokenVersion < TokenVersionV1 {
			return false
		}
		if expected.StartTicks != 0 || observed.StartTicks != 0 {
			if expected.StartTicks == 0 || observed.StartTicks == 0 || expected.StartTicks != observed.StartTicks {
				return false
			}
			// Linux v1: boot ID required on both sides.
			if expected.BootID == "" || observed.BootID == "" || expected.BootID != observed.BootID {
				return false
			}
			return true
		}
		if expected.StartSec != 0 || expected.StartUsec != 0 || observed.StartSec != 0 || observed.StartUsec != 0 {
			return expected.StartSec == observed.StartSec && expected.StartUsec == observed.StartUsec
		}
		if expected.Filetime != 0 || observed.Filetime != 0 {
			return expected.Filetime != 0 && expected.Filetime == observed.Filetime
		}
		return false
	}
	// Legacy expected: compare start_ms against observed display field.
	if expected.StartTimeUnixMS == 0 || observed.StartTimeUnixMS == 0 {
		return false
	}
	if expected.StartTimeUnixMS != observed.StartTimeUnixMS {
		return false
	}
	if expected.BootID != "" {
		if observed.BootID == "" || expected.BootID != observed.BootID {
			return false
		}
	}
	return true
}

// FromRecord reconstructs the identity persisted on a job record.
func FromRecord(pid int, startTimeUnixMS *uint64, bootID string) Identity {
	return FromRecordFull(pid, startTimeUnixMS, bootID, 0, 0, 0, 0, 0)
}

// FromRecordFull includes versioned native fields.
func FromRecordFull(pid int, startTimeUnixMS *uint64, bootID string, tokenVersion int, startTicks uint64, startSec, startUsec int64, filetime uint64) Identity {
	if pid <= 0 {
		return Identity{PID: pid, BootID: bootID, UnavailableReason: "job record has no pid"}
	}
	if tokenVersion >= TokenVersionV1 {
		if tokenVersion != TokenVersionV1 {
			return Identity{PID: pid, TokenVersion: tokenVersion, UnavailableReason: "unsupported token version"}
		}
		// Exactly one platform shape.
		shapes := 0
		if startTicks != 0 {
			shapes++
		}
		if startSec != 0 || startUsec != 0 {
			shapes++
		}
		if filetime != 0 {
			shapes++
		}
		if shapes != 1 {
			return Identity{PID: pid, TokenVersion: tokenVersion, UnavailableReason: "v1 birth token must have exactly one platform shape"}
		}
		if startTicks != 0 && strings.TrimSpace(bootID) == "" {
			return Identity{PID: pid, TokenVersion: tokenVersion, UnavailableReason: "linux v1 requires boot_id"}
		}
		id := Identity{
			PID: pid, BootID: bootID, TokenVersion: tokenVersion,
			StartTicks: startTicks, StartSec: startSec, StartUsec: startUsec, Filetime: filetime,
			Proven: true,
		}
		if startTimeUnixMS != nil {
			id.StartTimeUnixMS = *startTimeUnixMS
		}
		return id
	}
	if startTimeUnixMS == nil || *startTimeUnixMS == 0 {
		return Identity{PID: pid, BootID: bootID, UnavailableReason: "process birth identity missing from job record"}
	}
	return Identity{
		PID:             pid,
		StartTimeUnixMS: *startTimeUnixMS,
		BootID:          bootID,
		Proven:          true,
	}
}

// Format returns a short diagnostic form for logs and plan output.
func Format(id Identity) string {
	if !id.Proven {
		if id.UnavailableReason != "" {
			return fmt.Sprintf("pid=%d unproven(%s)", id.PID, id.UnavailableReason)
		}
		return fmt.Sprintf("pid=%d unproven", id.PID)
	}
	if id.BootID != "" {
		return fmt.Sprintf("pid=%d start_ms=%d boot=%s", id.PID, id.StartTimeUnixMS, id.BootID)
	}
	return fmt.Sprintf("pid=%d start_ms=%d", id.PID, id.StartTimeUnixMS)
}
