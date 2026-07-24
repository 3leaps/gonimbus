package indexcoord

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

// ErrLeaseHeld reports that a reclaim target is held by a live process and was
// refused. It aliases the substrate held-authority sentinel.
var ErrLeaseHeld = indexsubstrate.ErrSetAuthorityHeld

// LeaseVerdict is the non-mutating lock-state verdict for a set-authority lease.
type LeaseVerdict string

const (
	LeaseHeld    = LeaseVerdict(indexsubstrate.LeaseHeld)
	LeaseUnheld  = LeaseVerdict(indexsubstrate.LeaseUnheld)
	LeaseMissing = LeaseVerdict(indexsubstrate.LeaseMissing)
	LeaseInvalid = LeaseVerdict(indexsubstrate.LeaseInvalid)
)

// LeaseAttribution is the best-effort operator context joined from the job
// registry. It answers "who left this lease, and is that process still alive" —
// never "is the lease held". The held/unheld verdict comes solely from the lock
// probe; a matched-and-alive PID does not imply held, and an unmatched or
// dead-PID lease is never treated as unheld on that basis.
type LeaseAttribution struct {
	// Matched is true when a job record's identity matches the lease holder.
	Matched bool
	// JobID is the matched job's ID (empty when unmatched).
	JobID string
	// PID is the matched job's recorded process ID (0 when unmatched).
	PID int
	// ProcessAlive is the liveness of PID, evaluated only when Matched.
	ProcessAlive bool
	// JobState is the matched job's last recorded state.
	JobState string
}

// LeaseReport is a probed set-authority lease plus its joined attribution.
type LeaseReport struct {
	IndexSetID  string
	Path        string
	Verdict     LeaseVerdict
	Holder      string
	AcquiredAt  time.Time
	ModTime     time.Time
	Detail      string
	Attribution LeaseAttribution
}

// ReclaimReport is the outcome of a reclaim attempt for one lease.
type ReclaimReport struct {
	IndexSetID string
	Path       string
	Verdict    LeaseVerdict
	Reclaimed  bool
	Holder     string
}

// joinAttribution matches a lease holder against a byte-preserving job snapshot.
// Holders are minted as "index-build-<jobID>" for managed builds and
// "index-build-<uuid>" for foreground builds; only the former can match a job
// record. An unmatched holder is reported honestly (Matched=false) — it may be a
// foreground build or a build whose job record was already reaped.
func joinAttribution(holder string, jobs []jobregistry.JobRecord) LeaseAttribution {
	holder = strings.TrimSpace(holder)
	if holder == "" {
		return LeaseAttribution{}
	}
	for i := range jobs {
		jobID := strings.TrimSpace(jobs[i].JobID)
		if jobID == "" {
			continue
		}
		if "index-build-"+jobID == holder {
			return LeaseAttribution{
				Matched:      true,
				JobID:        jobID,
				PID:          jobs[i].PID,
				ProcessAlive: jobregistry.IsProcessAlive(jobs[i].PID),
				JobState:     string(jobs[i].State),
			}
		}
	}
	return LeaseAttribution{}
}

func toReport(lease indexsubstrate.SetAuthorityLease, jobs []jobregistry.JobRecord) LeaseReport {
	return LeaseReport{
		IndexSetID:  lease.IndexSetID,
		Path:        lease.Path,
		Verdict:     LeaseVerdict(lease.Verdict),
		Holder:      lease.Holder,
		AcquiredAt:  lease.AcquiredAt,
		ModTime:     lease.ModTime,
		Detail:      lease.Detail,
		Attribution: joinAttribution(lease.Holder, jobs),
	}
}

// EnumerateLeases probes every set-authority lease under authorityRoot and joins
// each to the supplied job snapshot for attribution. A missing authority root
// yields an empty slice. Enumeration is fully non-mutating.
func EnumerateLeases(authorityRoot string, jobs []jobregistry.JobRecord) ([]LeaseReport, error) {
	leases, err := indexsubstrate.EnumerateSetAuthorityLeases(authorityRoot)
	if err != nil {
		return nil, fmt.Errorf("enumerate set authority leases: %w", err)
	}
	reports := make([]LeaseReport, 0, len(leases))
	for _, lease := range leases {
		reports = append(reports, toReport(lease, jobs))
	}
	return reports, nil
}

// ProbeLease probes a single lease and joins attribution.
//
// The wrapper never downgrades a state it received: whatever verdict the library
// reached is passed through unchanged, including alongside an error. It follows
// the library's refusal contract exactly — an artifact-grounded refusal carries
// LeaseInvalid; an infrastructure failure (unusable authority root, unexpected
// lock error) returns an error claiming no artifact state. Check the error
// before trusting a verdict.
func ProbeLease(authorityRoot, indexSetID string, jobs []jobregistry.JobRecord) (LeaseReport, error) {
	lease, err := indexsubstrate.ProbeSetAuthorityLease(authorityRoot, indexSetID)
	report := toReport(lease, jobs)
	if err != nil {
		return report, fmt.Errorf("probe set authority lease: %w", err)
	}
	return report, nil
}

// ReclaimUnheldLease removes a provably-unheld set-authority lease. A held lease
// is refused with ErrLeaseHeld and never touched; a missing lease is a no-op
// success (idempotent). The held/unheld decision is made solely by the lock
// probe — never by attribution.
//
// It follows the library's refusal contract unchanged: an artifact-grounded
// refusal carries LeaseInvalid with its error; an infrastructure failure returns
// an error claiming no artifact state. Reclaimed is false on every error path.
func ReclaimUnheldLease(authorityRoot, indexSetID string) (ReclaimReport, error) {
	res, err := indexsubstrate.ReclaimUnheldSetAuthorityLease(authorityRoot, indexSetID)
	report := ReclaimReport{
		IndexSetID: res.IndexSetID,
		Path:       res.Path,
		Verdict:    LeaseVerdict(res.Verdict),
		Reclaimed:  res.Reclaimed,
		Holder:     res.Holder,
	}
	if err != nil {
		if errors.Is(err, ErrLeaseHeld) {
			return report, err
		}
		return report, fmt.Errorf("reclaim set authority lease: %w", err)
	}
	return report, nil
}
