package jobregistry

import "github.com/3leaps/gonimbus/internal/procidentity"

// ProcessIdentity is the durable OS birth token stored on a managed job record.
type ProcessIdentity = procidentity.Identity

// observeProcessIdentity captures the birth token for pid at claim time.
func observeProcessIdentity(pid int) ProcessIdentity {
	return procidentity.Observe(pid)
}

// ProcessIdentityFromRecord reconstructs the token persisted on a job record.
func ProcessIdentityFromRecord(rec *JobRecord) ProcessIdentity {
	if rec == nil {
		return ProcessIdentity{UnavailableReason: "nil job record"}
	}
	return procidentity.FromRecordFull(
		rec.PID, rec.ProcessStartTimeUnixMS, rec.ProcessBootID,
		rec.ProcessTokenVersion, rec.ProcessStartTicks,
		rec.ProcessStartSec, rec.ProcessStartUsec, rec.ProcessFiletime,
	)
}

// ApplyProcessIdentity persists a versioned birth token onto the job record.
func ApplyProcessIdentity(rec *JobRecord, id ProcessIdentity) {
	if rec == nil || !id.Proven {
		return
	}
	start := id.StartTimeUnixMS
	rec.ProcessStartTimeUnixMS = &start
	rec.ProcessBootID = id.BootID
	rec.ProcessTokenVersion = id.TokenVersion
	rec.ProcessStartTicks = id.StartTicks
	rec.ProcessStartSec = id.StartSec
	rec.ProcessStartUsec = id.StartUsec
	rec.ProcessFiletime = id.Filetime
}

// ObserveProcessIdentity re-reads the live process birth token for pid.
func ObserveProcessIdentity(pid int) ProcessIdentity {
	return procidentity.Observe(pid)
}

// ProcessIdentityMatch reports whether two proven identities name the same
// process instance.
func ProcessIdentityMatch(expected, observed ProcessIdentity) bool {
	return procidentity.Match(expected, observed)
}

// FormatProcessIdentity returns a short diagnostic form.
func FormatProcessIdentity(id ProcessIdentity) string {
	return procidentity.Format(id)
}

// Liveness aliases for callers that must not use boolean IsProcessAlive.
type Liveness = procidentity.Liveness

const (
	LivenessLiveMatching   = procidentity.LiveMatching
	LivenessLiveMismatched = procidentity.LiveMismatched
	LivenessGone           = procidentity.Gone
	LivenessIndeterminate  = procidentity.Indeterminate
)

// ClassifyProcess returns typed liveness for a recorded identity.
func ClassifyProcess(id ProcessIdentity) Liveness {
	return procidentity.Classify(id)
}
