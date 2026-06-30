package reflow

import "sync"

// runStats accumulates per-object outcomes into the terminal SummaryRecord. It is
// safe for concurrent use so workers can record as they complete. The dry-run
// plane records only statuses and collisions; the copy plane extends it with
// error/fallback accounting as those paths migrate.
type runStats struct {
	mu            sync.Mutex
	statuses      map[string]int64
	collisions    map[string]int64
	invalidInputs int64
}

func newRunStats() *runStats {
	return &runStats{statuses: map[string]int64{}, collisions: map[string]int64{}}
}

func (s *runStats) record(rec Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec.Status != "" {
		s.statuses[rec.Status]++
	}
	if rec.Collision != nil && rec.Collision.Kind != "" {
		s.collisions[rec.Collision.Kind]++
	}
}

func (s *runStats) recordInvalidInput() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.invalidInputs++
}

// summary builds the terminal SummaryRecord for the run.
func (s *runStats) summary(destURI, collisionMode string, dryRun bool, capability IfAbsentCapability, concurrency ConcurrencyStats) SummaryRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SummaryRecord{
		DestURI:                 destURI,
		DryRun:                  dryRun,
		OnCollision:             collisionMode,
		ConcurrencyStats:        concurrency,
		DestIfAbsentHonored:     capability.Honored,
		DestIfAbsentProbeStatus: string(capability.ProbeStatus),
		FallbackActive:          capability.FallbackActive,
		Statuses:                cloneInt64Map(s.statuses),
		Collisions:              cloneInt64Map(s.collisions),
		InvalidInputs:           s.invalidInputs,
	}
}

func cloneInt64Map(in map[string]int64) map[string]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
