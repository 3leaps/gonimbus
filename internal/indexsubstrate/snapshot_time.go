package indexsubstrate

import (
	"fmt"
	"strings"
	"time"
)

const (
	// CompleteMarkerTypeV1 is the legacy local durable commit marker.
	CompleteMarkerTypeV1 = "gonimbus.index.complete.v1"
	// CompleteMarkerTypeV2 is the local durable commit marker whose snapshot
	// completion time is sampled at the marker commit boundary.
	CompleteMarkerTypeV2 = "gonimbus.index.complete.v2"
	// SnapshotCompletionSemanticsCompleteMarkerCommit is the sole exact-time
	// admission discriminator. Producer version and timestamp inequality are
	// deliberately not substitutes for this value.
	SnapshotCompletionSemanticsCompleteMarkerCommit = "complete_marker_commit"
)

// ParseCanonicalUTCTime parses the exact RFC3339Nano UTC representation used
// by durable authority artifacts.
func ParseCanonicalUTCTime(raw string) (time.Time, error) {
	if raw != strings.TrimSpace(raw) {
		return time.Time{}, fmt.Errorf("timestamp is not canonical UTC")
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || parsed.IsZero() || raw != parsed.UTC().Format(time.RFC3339Nano) {
		return time.Time{}, fmt.Errorf("timestamp is not canonical UTC")
	}
	return parsed.UTC(), nil
}

// ValidateExactSnapshotCompletion is the shared exact-time admission gate for
// local publication, hub export/acquisition, verified metadata, and machine
// receipts. A zero hubCommittedAt validates a local completion fact; a non-zero
// value additionally binds the remote commit ordering.
func ValidateExactSnapshotCompletion(
	semantics string,
	runStartedAt, snapshotCompletedAt, hubCommittedAt time.Time,
) error {
	if semantics != SnapshotCompletionSemanticsCompleteMarkerCommit {
		return fmt.Errorf("snapshot completion semantics are not exact-time eligible")
	}
	for label, value := range map[string]time.Time{
		"run_started_at":        runStartedAt,
		"snapshot_completed_at": snapshotCompletedAt,
	} {
		if value.IsZero() {
			return fmt.Errorf("%s is required", label)
		}
		_, offset := value.Zone()
		if offset != 0 {
			return fmt.Errorf("%s must be UTC", label)
		}
	}
	if snapshotCompletedAt.Before(runStartedAt) {
		return fmt.Errorf("snapshot_completed_at precedes run_started_at")
	}
	if !hubCommittedAt.IsZero() {
		_, offset := hubCommittedAt.Zone()
		if offset != 0 {
			return fmt.Errorf("hub_committed_at must be UTC")
		}
		if hubCommittedAt.Before(snapshotCompletedAt) {
			return fmt.Errorf("hub_committed_at precedes snapshot_completed_at")
		}
	}
	return nil
}
